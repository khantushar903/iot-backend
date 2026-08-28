import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.database import AsyncSessionLocal, Base, engine
from app.models import Telemetry
from app.mqtt import mqtt_consumer
from app.redis import (
    LIVE_TELEMETRY_CHANNEL,
    get_live_telemetry_pubsub,
    redis_client,
)
from app.schemas import (
    AlertCreate,
    AlertResponse,
    TelemetryCreate,
    TelemetryResponse,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot-backend")

_mqtt_task: asyncio.Task | None = None


def _get_mqtt_task() -> asyncio.Task | None:
    return _mqtt_task


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured")

    global _mqtt_task
    _mqtt_task = asyncio.create_task(mqtt_consumer(), name="mqtt-consumer")
    yield

    _mqtt_task.cancel()
    with suppress(asyncio.CancelledError):
        await _mqtt_task
    await redis_client.aclose()
    await engine.dispose()


app = FastAPI(title="IoT Machine Monitoring API", lifespan=lifespan)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/v1/health")
async def health_v1():
    db_status = "down"
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception:
        logger.exception("DB health check failed")

    redis_status = "down"
    try:
        await redis_client.ping()
        redis_status = "ok"
    except Exception:
        logger.exception("Redis health check failed")

    mqtt_status = "down"
    mqtt_task = _get_mqtt_task()
    if mqtt_task is not None and not mqtt_task.done():
        mqtt_status = "ok"

    components = {
        "database": db_status,
        "redis": redis_status,
        "mqtt": mqtt_status,
    }
    overall = "ok" if all(s == "ok" for s in components.values()) else "degraded"
    return {"status": overall, "components": components}


@app.post(
    "/api/v1/alerts",
    response_model=AlertResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_alert(
    payload: AlertCreate,
    db: AsyncSession = Depends(get_db),
):
    alert = await crud.create_alert(db, payload)
    return alert


@app.get("/api/v1/alerts", response_model=list[AlertResponse])
async def get_alerts(
    limit: int = Query(50, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    return await crud.get_alerts(db, limit=limit)


@app.get(
    "/api/v1/telemetry/history",
    response_model=list[TelemetryResponse],
)
async def get_telemetry_history(
    device_id: str | None = Query(None, max_length=64),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    return await crud.get_telemetry_history(
        db,
        device_id=device_id,
        limit=limit,
        offset=offset,
    )


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
