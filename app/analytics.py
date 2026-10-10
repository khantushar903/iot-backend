"""Vibration analysis for a buffered window of accelerometer samples.

`analyze_window` is pure: samples in, metrics dict out. It holds all of the
signal-processing math and can be exercised without Redis, Celery, or a
database. `process_vibration_window` is the thin Celery wrapper that adds
alert-lifecycle handling and persistence.

Signal model
------------
Each sample is a 3-axis acceleration reading taken by the device and timestamped
with `ts`. Gravity is a constant offset on whichever axis the board is mounted
on, so rather than assuming an orientation we remove the DC component of each
axis independently. That subtraction *is* the gravity removal, and it also
removes any slow bias, which is why every metric below is computed from the
detrended (AC) signal.

Sampling rate is derived from the sample timestamps rather than assumed. An
assumed rate silently scales the whole frequency axis by the ratio between the
real and assumed rate, so the timestamps are used when they are usable and the
configured fallback is used (and flagged) when they are not.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import numpy as np
from asgiref.sync import async_to_sync
from scipy import fft

from app.alert_state import (
    Decision,
    alert_severity,
    classify_rms,
    decide,
    entry_threshold,
)
from app.celery_app import celery_app
from app.config import vibration
from app.database import AsyncSessionLocal
from app.models import Alert
from app.redis import (
    LIVE_TELEMETRY_CHANNEL,
    load_alert_state,
    redis_client,
    save_alert_state,
)
from app.schemas import AlertResponse

logger = logging.getLogger("iot-backend")

METRIC = "vibration"

AXES = ("x", "y", "z")

_TS_UNIT_SECONDS = {"ms": 1e-3, "s": 1.0, "us": 1e-6}


def estimate_sample_rate_hz(
    timestamps: list[int] | None, sample_count: int
) -> tuple[float, str]:
    """Derive the sampling rate from device timestamps.

    Returns ``(rate_hz, source)`` where source is ``"timestamps"`` when the
    measurement is trustworthy or ``"fallback"`` when it had to be guessed.
    """
    fallback = vibration.fallback_sample_rate_hz
    scale = _TS_UNIT_SECONDS[vibration.ts_unit.lower()]

    if timestamps is None or len(timestamps) < 2:
        return fallback, "fallback"

    values = np.asarray(timestamps, dtype=float)
    if not np.all(np.isfinite(values)):
        return fallback, "fallback"

    span_s = (values[-1] - values[0]) * scale
    if span_s <= 0:
        logger.warning(
            "Non-increasing timestamps over %d samples (span %.3fs); "
            "falling back to %.1f Hz",
            sample_count,
            span_s,
            fallback,
        )
        return fallback, "fallback"

    rate = (sample_count - 1) / span_s
    if not (
        vibration.min_sample_rate_hz
        <= rate
        <= vibration.max_sample_rate_hz
    ):
        logger.warning(
            "Implausible sample rate %.2f Hz from timestamps; "
            "falling back to %.1f Hz",
            rate,
            fallback,
        )
        return fallback, "fallback"

    return float(rate), "timestamps"


def analyze_window(samples: list[dict]) -> dict:
    """Compute time-domain and frequency-domain metrics for one window.

    ``samples`` is a list of ``{"x": float, "y": float, "z": float,
    "ts": int | None}`` dicts in arrival order.
    """
    if len(samples) < 2:
        raise ValueError("a vibration window needs at least 2 samples")

    axes = {
        axis: np.asarray([float(s[axis]) for s in samples], dtype=float)
        for axis in AXES
    }
    for axis, values in axes.items():
        if not np.all(np.isfinite(values)):
            raise ValueError(f"axis {axis} contains non-finite samples")

    raw_timestamps = [s.get("ts") for s in samples]
    timestamps = None if any(t is None for t in raw_timestamps) else raw_timestamps
    rate_hz, rate_source = estimate_sample_rate_hz(timestamps, len(samples))
    dt = 1.0 / rate_hz

    # DC removal per axis == gravity removal + bias removal.
    ac = {axis: values - values.mean() for axis, values in axes.items()}

    n = len(samples)
    resultant = np.sqrt(sum(ac[axis] ** 2 for axis in AXES))

    rms = {axis: float(np.sqrt(np.mean(ac[axis] ** 2))) for axis in AXES}
    rms_resultant = float(np.sqrt(np.mean(resultant**2)))
    peak = {axis: float(np.max(np.abs(ac[axis]))) for axis in AXES}
    peak_resultant = float(np.max(resultant))
    crest = (
        float(peak_resultant / rms_resultant) if rms_resultant > 0 else 0.0
    )

    # Windowing matters here: a rectangular window leaks enough energy into the
    # neighbouring bins that argmax can report a frequency that is not in the
    # signal, which makes every derived frequency number untrustworthy.
    taper = np.hanning(n)
    freqs = fft.rfftfreq(n, d=dt)
    nyquist = rate_hz / 2.0

    spectra: dict[str, np.ndarray] = {}
    energy = np.zeros(freqs.size)
    dominant_by_axis: dict[str, float] = {}

    for axis in AXES:
        magnitude = np.abs(fft.rfft(ac[axis] * taper))
        magnitude[0] = 0.0  # ignore the residual DC bin
        spectra[axis] = magnitude
        energy += magnitude**2
        dominant_by_axis[axis] = float(freqs[int(np.argmax(magnitude))])

    total_energy = float(energy.sum())
    hf_mask = freqs >= nyquist * vibration.hf_band_start_fraction
    hf_energy = float(energy[hf_mask].sum())
    hf_energy_ratio = hf_energy / total_energy if total_energy > 0 else 0.0

    peak_bin = int(np.argmax(energy))
    dominant_axis = max(AXES, key=lambda a: spectra[a][peak_bin])

    return {
        "window_samples": n,
        "window_duration_s": round(n / rate_hz, 6),
        "sample_rate_hz": round(rate_hz, 4),
        "sample_rate_source": rate_source,
        "rms_accel_m_s2": round(rms_resultant, 6),
        "rms_x_m_s2": round(rms["x"], 6),
        "rms_y_m_s2": round(rms["y"], 6),
        "rms_z_m_s2": round(rms["z"], 6),
        "peak_accel_m_s2": round(peak_resultant, 6),
        "peak_x_m_s2": round(peak["x"], 6),
        "peak_y_m_s2": round(peak["y"], 6),
        "peak_z_m_s2": round(peak["z"], 6),
        "crest_factor": round(crest, 4),
        "dominant_frequency_hz": round(float(freqs[peak_bin]), 4),
        "dominant_axis": dominant_axis,
        "dominant_frequency_x_hz": round(dominant_by_axis["x"], 4),
        "dominant_frequency_y_hz": round(dominant_by_axis["y"], 4),
        "dominant_frequency_z_hz": round(dominant_by_axis["z"], 4),
        "hf_energy_ratio": round(hf_energy_ratio, 4),
        "spectral_escalation": bool(
            hf_energy_ratio >= vibration.hf_energy_ratio_escalate
            and crest >= vibration.crest_factor_escalate
        ),
        "rms_severity": classify_rms(rms_resultant),
    }


def _build_message(
    metrics: dict, decision: Decision, rms_severity: str
) -> str:
    rms = metrics["rms_accel_m_s2"]
    note = (
        ""
        if metrics["sample_rate_source"] == "timestamps"
        else " (sample rate assumed - device timestamps unusable)"
    )

    if decision.kind == "resolution":
        return f"Vibration returned to normal: RMS {rms:.2f} m/s^2{note}."

    if decision.kind == "deescalation":
        return (
            f"Vibration reduced to {decision.severity}: RMS "
            f"{rms:.2f} m/s^2{note}."
        )

    if decision.kind == "reminder":
        lead = "Still"
    elif rms_severity != decision.severity:
        lead = f"Escalated from {rms_severity} - spectral signature"
    else:
        lead = ""

    detail = (
        f"dominant {metrics['dominant_frequency_hz']:.1f} Hz on "
        f"{metrics['dominant_axis'].upper()}, crest "
        f"{metrics['crest_factor']:.2f}, HF energy "
        f"{metrics['hf_energy_ratio'] * 100:.0f}%"
    )
    return (
        f"{lead} acceleration RMS {rms:.2f} m/s^2 "
        f"(threshold {entry_threshold(decision.severity):.2f}), {detail}"
        f"{note}."
    ).lstrip()


def _alert_payload(
    device_id: str,
    metrics: dict,
    decision: Decision,
    rms_severity: str,
) -> dict:
    threshold = entry_threshold(decision.severity)
    return {
        "device_id": device_id,
        "severity": alert_severity(decision),
        "metric": METRIC,
        "value": metrics["rms_accel_m_s2"],
        "threshold": threshold,
        "message": _build_message(metrics, decision, rms_severity),
    }


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
    samples: list[dict],
) -> dict:
    """Analyze one buffered window and run the alert lifecycle for a device."""
    metrics = analyze_window(samples)
    rms_severity = metrics["rms_severity"]
    rms_value = metrics["rms_accel_m_s2"]

    now = datetime.now(timezone.utc).timestamp()
    previous = load_alert_state(device_id)
    decision = decide(
        previous,
        rms_value,
        metrics["spectral_escalation"],
        now,
    )

    if decision.emit:
        last_alert_at = now
    else:
        last_alert_at = previous.last_alert_at
    save_alert_state(device_id, decision.severity, last_alert_at, now=now)

    result = {
        "device_id": device_id,
        **metrics,
        "previous_severity": previous.severity,
        "severity": decision.severity,
        "alert_kind": decision.kind,
        "decision_reason": decision.reason,
        "alert_raised": decision.emit,
    }

    if not decision.emit:
        return result

    alert_data = _alert_payload(device_id, metrics, decision, rms_severity)
    try:
        async_to_sync(_persist_and_broadcast)(alert_data)
    except Exception:
        logger.exception("Failed to persist/broadcast alert for %s", device_id)
    logger.warning(
        "Alert (%s/%s) for %s: %s",
        decision.kind,
        decision.severity,
        device_id,
        alert_data["message"],
    )

    return result
