import asyncio
import json
import logging

import aiomqtt
from pydantic import ValidationError

from app.analytics import process_vibration_window
from app.config import settings, vibration
from app.database import AsyncSessionLocal
from app.models import Telemetry
from app.redis import publish_live_telemetry, redis_client
from app.schemas import TelemetryCreate, TelemetryResponse

logger = logging.getLogger("iot-backend")

TELEMETRY_TOPIC = settings.mqtt_topic

_INITIAL_BACKOFF_S = 1.0
_MAX_BACKOFF_S = 30.0

BUFFER_KEY_PREFIX = "vibration_buffer:"
# A full window is dispatched as soon as this many samples have accumulated.
BUFFER_DISPATCH_THRESHOLD = vibration.window_samples
# Hard cap on buffered-but-not-yet-dispatched samples. Overflow beyond the cap
# is stale backlog (a reconnecting device replaying, or a stalled analytics
# worker) and is dropped from the front so memory stays bounded.
BUFFER_MAX_LENGTH = BUFFER_DISPATCH_THRESHOLD * 3


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
    await _buffer_vibration_window(telemetry)


def _decode_samples(raw: list[str]) -> list[dict]:
    """Parse buffered JSON samples, skipping anything unreadable."""
    samples: list[dict] = []
    for entry in raw:
        try:
            sample = json.loads(entry)
            samples.append(
                {
                    "x": float(sample["x"]),
                    "y": float(sample["y"]),
                    "z": float(sample["z"]),
                    "ts": sample.get("ts"),
                }
            )
        except (ValueError, TypeError, KeyError):
            logger.warning("Dropping malformed vibration buffer entry: %r", entry)
    return samples


async def _buffer_vibration_window(telemetry: TelemetryCreate) -> None:
    """Accumulate per-axis samples into a per-device window.

    Gravity is deliberately *not* removed here. Removing the DC component is
    cheap and exact once a full window exists, and doing it downstream means we
    never have to assume which axis the board is mounted on.
    """
    sample = json.dumps(
        {
            "x": telemetry.accel_x,
            "y": telemetry.accel_y,
            "z": telemetry.accel_z,
            "ts": telemetry.ts,
        },
        separators=(",", ":"),
    )
    key = f"{BUFFER_KEY_PREFIX}{telemetry.device_id}"
    try:
        await redis_client.rpush(key, sample)
        buffer_length = await redis_client.llen(key)

        # Fixed windowing: dispatch once a full window is available, consuming
        # exactly that many samples. Any excess samples beyond the window are
        # retained so they seed the next window instead of being discarded.
        if buffer_length < BUFFER_DISPATCH_THRESHOLD:
            return

        raw = await redis_client.lrange(key, 0, BUFFER_DISPATCH_THRESHOLD - 1)
        # Consume the window before dispatching: if the broker call fails we
        # lose one window rather than re-analysing the same window forever.
        await redis_client.ltrim(key, BUFFER_DISPATCH_THRESHOLD, -1)

        samples = _decode_samples(raw)
        if len(samples) < BUFFER_DISPATCH_THRESHOLD:
            logger.warning(
                "Short vibration window for device %s: %d/%d samples",
                telemetry.device_id,
                len(samples),
                BUFFER_DISPATCH_THRESHOLD,
            )
            return

        # Bound the backlog.
        remaining = await redis_client.llen(key)
        if remaining > BUFFER_MAX_LENGTH:
            await redis_client.ltrim(key, -BUFFER_MAX_LENGTH, -1)
            logger.warning(
                "Trimmed vibration backlog for %s: %d -> %d samples",
                telemetry.device_id,
                remaining,
                BUFFER_MAX_LENGTH,
            )

        process_vibration_window.delay(telemetry.device_id, samples)
    except Exception:
        logger.exception(
            "Vibration buffer update failed for device %s",
            telemetry.device_id,
        )


async def mqtt_consumer() -> None:
    backoff = _INITIAL_BACKOFF_S
    while True:
        try:
            async with aiomqtt.Client(
                hostname=settings.mqtt_host,
                port=settings.mqtt_port,
            ) as client:
                await client.subscribe(TELEMETRY_TOPIC, qos=1)
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
