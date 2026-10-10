import json
import logging
import time

import redis.asyncio as redis
from redis import Redis
from redis.asyncio.client import PubSub

from app.alert_state import NORMAL, AlertState
from app.config import settings, vibration

logger = logging.getLogger("iot-backend")

LIVE_TELEMETRY_CHANNEL = "live_telemetry"

ALERT_STATE_KEY_PREFIX = "alert_state:"


redis_client = redis.from_url(settings.redis_url, decode_responses=True)

# Celery workers are synchronous processes, so they get their own blocking
# client rather than bridging into asyncio for every window.
sync_redis_client = Redis.from_url(settings.redis_url, decode_responses=True)


async def publish_live_telemetry(payload: str) -> None:
    try:
        await redis_client.publish(LIVE_TELEMETRY_CHANNEL, payload)
    except Exception:
        logger.exception(
            "Redis publish to %s failed", LIVE_TELEMETRY_CHANNEL
        )


def get_live_telemetry_pubsub() -> PubSub:
    return redis_client.pubsub()


def _alert_state_key(device_id: str) -> str:
    return f"{ALERT_STATE_KEY_PREFIX}{device_id}"


def load_alert_state(device_id: str) -> AlertState:
    """Read the device's previous severity. Never raises: a Redis outage must
    not stop vibration analysis, it only costs us the hysteresis history."""
    key = _alert_state_key(device_id)
    try:
        raw = sync_redis_client.get(key)
    except Exception:
        logger.exception("Alert state read failed for %s", device_id)
        return AlertState()
    if not raw:
        return AlertState()
    try:
        data = json.loads(raw)
        return AlertState(
            severity=str(data.get("severity", NORMAL)),
            last_alert_at=float(data.get("last_alert_at", 0.0)),
            updated_at=float(data.get("updated_at", 0.0)),
        )
    except (ValueError, TypeError):
        logger.warning("Discarding malformed alert state for %s", device_id)
        return AlertState()


def save_alert_state(
    device_id: str,
    severity: str,
    last_alert_at: float,
    now: float | None = None,
) -> None:
    key = _alert_state_key(device_id)
    payload = json.dumps(
        {
            "severity": severity,
            "last_alert_at": last_alert_at,
            "updated_at": time.time() if now is None else now,
        }
    )
    try:
        sync_redis_client.set(key, payload, ex=vibration.state_ttl_s)
    except Exception:
        logger.exception("Alert state write failed for %s", device_id)
