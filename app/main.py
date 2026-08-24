import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal, Base, engine
from app.models import Telemetry
from app.mqtt import mqtt_consumer
from app.redis import (
    LIVE_TELEMETRY_CHANNEL,
    get_live_telemetry_pubsub,
    redis_client,
)
from app.schemas import TelemetryCreate, TelemetryResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot-backend")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured")

    mqtt_task = asyncio.create_task(mqtt_consumer(), name="mqtt-consumer")
    yield

    mqtt_task.cancel()
    with suppress(asyncio.CancelledError):
        await mqtt_task
    await redis_client.aclose()
    await engine.dispose()


app = FastAPI(title="IoT Machine Monitoring API", lifespan=lifespan)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post(
    "/telemetry",
    response_model=TelemetryResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_telemetry(
    payload: TelemetryCreate,
    db: AsyncSession = Depends(get_db),
):
    record = Telemetry(**payload.model_dump())
    db.add(record)
    try:
        await db.commit()
        await db.refresh(record)
    except Exception:
        await db.rollback()
        logger.exception("Failed to store telemetry from %s", payload.device_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store telemetry",
        )
    return record


@app.get("/telemetry/latest", response_model=list[TelemetryResponse])
async def get_latest_telemetry(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Telemetry).order_by(Telemetry.id.desc()).limit(10)
    )
    return list(result.scalars().all())


SNAPSHOT_SIZE = 10
_WS_IDLE_PING_S = 20.0


@app.websocket("/ws/telemetry")
async def telemetry_websocket(websocket: WebSocket):
    await websocket.accept()
    pubsub = get_live_telemetry_pubsub()
    try:
        await pubsub.subscribe(LIVE_TELEMETRY_CHANNEL)

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Telemetry)
                .order_by(Telemetry.id.desc())
                .limit(SNAPSHOT_SIZE)
            )
            records = list(result.scalars().all())
        snapshot = {
            "type": "snapshot",
            "data": [
                TelemetryResponse.model_validate(record).model_dump(
                    mode="json"
                )
                for record in reversed(records)
            ],
        }
        await websocket.send_json(snapshot)
        logger.info("Dashboard connected, snapshot sent")

        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=_WS_IDLE_PING_S,
            )
            if message is None or message["type"] != "message":
                await websocket.send_text('{"type":"ping"}')
                continue
            await websocket.send_text(message["data"])
    except WebSocketDisconnect:
        logger.info("Dashboard client disconnected")
    except Exception:
        logger.exception("WebSocket relay error")
    finally:
        try:
            await pubsub.unsubscribe(LIVE_TELEMETRY_CHANNEL)
            await pubsub.aclose()
        except Exception:
            logger.exception("PubSub cleanup failed")
