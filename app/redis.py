import logging

import redis.asyncio as redis
from redis.asyncio.client import PubSub

from app.config import settings

logger = logging.getLogger("iot-backend")

LIVE_TELEMETRY_CHANNEL = "live_telemetry"

redis_client = redis.from_url(settings.redis_url, decode_responses=True)


async def publish_live_telemetry(payload: str) -> None:
    try:
        await redis_client.publish(LIVE_TELEMETRY_CHANNEL, payload)
    except Exception:
        logger.exception(
            "Redis publish to %s failed", LIVE_TELEMETRY_CHANNEL
        )


def get_live_telemetry_pubsub() -> PubSub:
    return redis_client.pubsub()
