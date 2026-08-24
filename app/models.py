import time
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Telemetry(Base):
    __tablename__ = "telemetry_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(String(64), index=True)
    ts: Mapped[int | None] = mapped_column(
        BigInteger,
        default=lambda: int(time.time()),
    )
    accel_x: Mapped[float]
    accel_y: Mapped[float]
    accel_z: Mapped[float]
    temp_c: Mapped[float]
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
