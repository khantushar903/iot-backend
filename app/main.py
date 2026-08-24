import logging
from contextlib import asynccontextmanager

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    status,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal, Base, engine
from app.models import Telemetry
from app.schemas import TelemetryCreate, TelemetryResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot-backend")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ensured")
    yield
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
