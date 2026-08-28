from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Alert, Telemetry
from app.schemas import AlertCreate


async def create_alert(db: AsyncSession, alert_in: AlertCreate) -> Alert:
    alert = Alert(**alert_in.model_dump())
    db.add(alert)
    await db.commit()
    await db.refresh(alert)
    return alert


async def get_alerts(db: AsyncSession, limit: int = 50) -> list[Alert]:
    result = await db.execute(
        select(Alert).order_by(Alert.id.desc()).limit(limit)
    )
    return list(result.scalars().all())


async def get_telemetry_history(
    db: AsyncSession,
    device_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[Telemetry]:
    statement = select(Telemetry).order_by(Telemetry.id.desc()).limit(limit)
    if device_id is not None:
        statement = statement.where(Telemetry.device_id == device_id)
    if offset:
        statement = statement.offset(offset)
    result = await db.execute(statement)
    return list(result.scalars().all())
