from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TelemetryCreate(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    ts: int | None = None
    accel_x: float
    accel_y: float
    accel_z: float
    temp_c: float


class TelemetryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    device_id: str
    ts: int | None
    accel_x: float
    accel_y: float
    accel_z: float
    temp_c: float
    created_at: datetime


class AlertCreate(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    severity: str
    metric: str
    value: float
    threshold: float
    message: str


class AlertResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    device_id: str
    severity: str
    metric: str
    value: float
    threshold: float
    message: str
    created_at: datetime
