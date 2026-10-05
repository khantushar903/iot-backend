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
    sample_rate_hz: int = 500,
) -> dict:
    if not accel_samples:
        raise ValueError("accel_samples must not be empty")

    arr = np.asarray(accel_samples, dtype=float)
    dt = 1.0 / sample_rate_hz

    # Subtract mean to remove gravity bias
    arr_zero_mean = arr - arr.mean()
    
    # Calculate acceleration metrics
    rms_accel = float(np.sqrt(np.mean(arr_zero_mean**2)))
    peak_accel = float(np.max(np.abs(arr_zero_mean)))
    peak_to_peak = float(np.max(arr) - np.min(arr))
    crest_factor = float(peak_accel / rms_accel) if rms_accel > 0 else 1.0

    # Calculate dominant frequency
    freqs = fft.rfftfreq(len(arr_zero_mean), d=dt)
    spectrum = np.abs(fft.rfft(arr_zero_mean))
    peak_index = int(np.argmax(spectrum))
    peak_freq = float(freqs[peak_index])

    # Simple threshold-based alerting
    WARNING_THRESHOLD = 2.0  # m/s^2
    CRITICAL_THRESHOLD = 5.0 # m/s^2

    severity = None
    threshold = None
    message = ""
    if rms_accel >= CRITICAL_THRESHOLD:
        severity = "CRITICAL"
        threshold = CRITICAL_THRESHOLD
        message = f"Acceleration RMS ({rms_accel:.2f} m/s²) exceeds critical threshold."
    elif rms_accel >= WARNING_THRESHOLD:
        severity = "WARNING"
        threshold = WARNING_THRESHOLD
        message = f"Acceleration RMS ({rms_accel:.2f} m/s²) exceeds warning threshold."

    result = {
        "device_id": device_id,
        "rms_accel_m_s2": rms_accel,
        "peak_accel_m_s2": peak_accel,
        "peak_to_peak_m_s2": peak_to_peak,
        "crest_factor": crest_factor,
        "peak_frequency_hz": peak_freq,
        "severity": severity,
        "alert_raised": severity is not None,
    }

    if severity is not None:
        alert_data = {
            "device_id": device_id,
            "severity": severity,
            "metric": METRIC,
            "value": rms_accel,
            "threshold": threshold,
            "message": message,
        }
        run_async(_persist_and_broadcast(alert_data))
        logger.warning(
            "Alert raised for device %s: %s (%.2f m/s²)",
            device_id,
            severity,
            rms_accel,
        )

    return result
