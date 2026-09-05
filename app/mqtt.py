import asyncio
import json
import logging
import math

import aiomqtt
from pydantic import ValidationError

from app.analytics import process_vibration_window
from app.config import settings
from app.database import AsyncSessionLocal
from app.models import Telemetry
from app.redis import publish_live_telemetry, redis_client
from app.schemas import TelemetryCreate, TelemetryResponse

logger = logging.getLogger("iot-backend")

TELEMETRY_TOPIC = settings.mqtt_topic

_INITIAL_BACKOFF_S = 1.0
_MAX_BACKOFF_S = 30.0

BUFFER_KEY_PREFIX = "vibration_buffer:"
BUFFER_MAX_LENGTH = 30
BUFFER_DISPATCH_THRESHOLD = 10
BUFFER_SAMPLE_RATE_HZ = 1


async def _process_message(message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload)
    except (ValueError, UnicodeDecodeError):
        logger.warning(
            "Dropping malformed JSON payload: %r",
            bytes(message.payload)[:200],
        )
        return
    try:
        telemetry = TelemetryCreate(**data)
    except ValidationError:
        logger.warning(
            "Dropping payload failing schema validation: %r",
            data,
        )
        return
    record = Telemetry(**telemetry.model_dump())
    try:
        async with AsyncSessionLocal() as session:
            session.add(record)
            await session.commit()
            await session.refresh(record)
    except Exception:
        logger.exception(
            "Database write failed for device %s", telemetry.device_id
        )
        return
    response = TelemetryResponse.model_validate(record)
    payload = json.dumps(
        {"type": "telemetry", "data": response.model_dump(mode="json")}
    )
    await publish_live_telemetry(payload)
    await _buffer_vibration_window(telemetry.device_id, telemetry.accel_x, telemetry.accel_y, telemetry.accel_z)


async def _buffer_vibration_window(
    device_id: str, accel_x: float, accel_y: float, accel_z: float
) -> None:
    magnitude = math.sqrt(accel_x**2 + accel_y**2 + accel_z**2)
    key = f"{BUFFER_KEY_PREFIX}{device_id}"
    try:
        await redis_client.rpush(key, magnitude)
        await redis_client.ltrim(key, -BUFFER_MAX_LENGTH, -1)
        buffer_length = await redis_client.llen(key)
        if buffer_length >= BUFFER_DISPATCH_THRESHOLD:
            raw = await redis_client.lrange(key, 0, -1)
            samples = [float(v) for v in raw]
            process_vibration_window.delay(
                device_id, samples, sample_rate_hz=BUFFER_SAMPLE_RATE_HZ
            )
    except Exception:
        logger.exception(
            "Vibration buffer update failed for device %s", device_id
        )


async def mqtt_consumer() -> None:
    backoff = _INITIAL_BACKOFF_S
    while True:
        try:
            async with aiomqtt.Client(
                hostname=settings.mqtt_host,
                port=settings.mqtt_port,
            ) as client:
                await client.subscribe(TELEMETRY_TOPIC)
                logger.info(
                    "MQTT connected, subscribed to %s", TELEMETRY_TOPIC
                )
                backoff = _INITIAL_BACKOFF_S
                async for message in client.messages:
                    await _process_message(message)
        except asyncio.CancelledError:
            logger.info("MQTT consumer shutting down")
            raise
        except aiomqtt.MqttError as exc:
            logger.warning(
                "MQTT error: %s — reconnecting in %.1fs", exc, backoff
            )
        except Exception:
            logger.exception(
                "Unexpected consumer error — retrying in %.1fs", backoff
            )
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _MAX_BACKOFF_S)
