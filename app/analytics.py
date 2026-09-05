import asyncio
import json
import logging

import numpy as np
from scipy import fft
from scipy.integrate import cumulative_trapezoid

from app.celery_app import celery_app
from app.database import AsyncSessionLocal, engine
from app.models import Alert
from app.redis import LIVE_TELEMETRY_CHANNEL, redis_client
from app.schemas import AlertResponse

logger = logging.getLogger("iot-backend")

ISO_10816_ZONES = [
    (None, 1.12, "A", "Good"),
    (1.12, 2.80, "B", "Satisfactory"),
    (2.80, 7.10, "C", "Unsatisfactory"),
    (7.10, None, "D", "Unacceptable"),
]

METRIC = "vibration"

_worker_loop: asyncio.AbstractEventLoop | None = None


@celery_app.signals.worker_process_init.connect
def _init_worker_loop(**kwargs) -> None:
    """Create a single persistent event loop for each Celery worker process."""
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        _worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_worker_loop)
        logger.info("Worker process event loop initialized")


def run_async(coro) -> None:
    """Run an async coroutine on the worker process's persistent event loop."""
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        _worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_worker_loop)
    _worker_loop.run_until_complete(coro)


@celery_app.signals.worker_process_shutdown.connect
def _shutdown_worker_loop(**kwargs) -> None:
    """Cleanly dispose of DB and Redis connections before the worker exits."""
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        return
    try:
        _worker_loop.run_until_complete(engine.dispose())
    except Exception:
        logger.exception("Failed to dispose database engine on worker shutdown")
    try:
        _worker_loop.run_until_complete(redis_client.aclose())
    except Exception:
        logger.exception("Failed to close Redis client on worker shutdown")
    try:
        _worker_loop.close()
    except Exception:
        logger.exception("Failed to close worker event loop")
    _worker_loop = None
    logger.info("Worker process event loop shut down cleanly")



async def _persist_and_broadcast(alert_data: dict) -> None:
    alert = Alert(**alert_data)
    async with AsyncSessionLocal() as session:
        session.add(alert)
        await session.commit()
        await session.refresh(alert)
    payload = json.dumps(
        {
            "type": "alert",
            "data": AlertResponse.model_validate(alert).model_dump(mode="json"),
        }
    )
    try:
        await redis_client.publish(LIVE_TELEMETRY_CHANNEL, payload)
    except Exception:
        logger.exception("Redis publish for alert failed")


@celery_app.task(name="app.analytics.process_vibration_window", bind=True)
def process_vibration_window(
    self,
    device_id: str,
    accel_samples: list[float],
    sample_rate_hz: int = 1,
) -> dict:
    if not accel_samples:
        raise ValueError("accel_samples must not be empty")

    arr = np.asarray(accel_samples, dtype=float)
    dt = 1.0 / sample_rate_hz

    velocity = cumulative_trapezoid(arr - arr.mean(), dx=dt, initial=0.0)
    rms_velocity = float(np.sqrt(np.mean(velocity**2)) * 1000.0)

    freqs = fft.rfftfreq(len(arr), d=dt)
    spectrum = np.abs(fft.rfft(arr - arr.mean()))
    peak_index = int(np.argmax(spectrum))
    peak_freq = float(freqs[peak_index])

    zone = None
    zone_name = None
    severity = None
    threshold = None
    for lower, upper, code, name in ISO_10816_ZONES:
        if (lower is None or rms_velocity >= lower) and (
            upper is None or rms_velocity < upper
        ):
            zone = code
            zone_name = name
            threshold = upper if upper is not None else lower
            break

    if zone in ("C", "D"):
        severity = "WARNING" if zone == "C" else "CRITICAL"
        message = (
            f"Vibration {rms_velocity:.2f} mm/s in ISO 10816 zone {zone} "
            f"({zone_name}) — requires attention"
        )
    else:
        severity = None
        message = ""

    result = {
        "device_id": device_id,
        "rms_velocity_mm_s": rms_velocity,
        "peak_frequency_hz": peak_freq,
        "iso_zone": zone,
        "iso_zone_name": zone_name,
        "severity": severity,
        "alert_raised": severity is not None,
    }

    if severity is not None:
        alert_data = {
            "device_id": device_id,
            "severity": severity,
            "metric": METRIC,
            "value": rms_velocity,
            "threshold": threshold,
            "message": message,
        }
        run_async(_persist_and_broadcast(alert_data))
        logger.warning(
            "Alert raised for device %s: %s (%.2f mm/s)",
            device_id,
            severity,
            rms_velocity,
        )

    return result
