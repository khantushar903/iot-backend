"""Dispatch one synthetic vibration window through the Celery task.

Requires the stack to be running (Redis + Postgres + a Celery worker):

    docker compose up -d
    python -m scripts.test_vibration_task
"""

from __future__ import annotations

import math
import time

from app.analytics import process_vibration_window
from app.config import vibration

SAMPLE_RATE_HZ = 200.0
TONE_HZ = 25.0
GRAVITY = 9.80665


def build_window(device_id: str, seconds: float, amplitude: float) -> list[dict]:
    n = int(seconds * SAMPLE_RATE_HZ)
    start_ms = int(time.time() * 1000)
    samples: list[dict] = []
    for i in range(n):
        value = amplitude * math.sin(2 * math.pi * TONE_HZ * i / SAMPLE_RATE_HZ)
        samples.append(
            {
                "x": value,
                "y": 0.0,
                "z": GRAVITY,
                "ts": start_ms + int(i * 1000.0 / SAMPLE_RATE_HZ),
            }
        )
    return samples


def main() -> None:
    print(
        f"Window: {vibration.window_samples} samples @ {SAMPLE_RATE_HZ:.0f} Hz "
        f"= {vibration.window_samples / SAMPLE_RATE_HZ:.3f} s\n"
    )

    scenarios = [
        ("sensor_healthy", 1.0),
        ("sensor_warning", 4.0),
        ("sensor_critical", 10.0),
    ]

    for device_id, amplitude in scenarios:
        samples = build_window(device_id, amplitude=amplitude, seconds=3.0)
        task = process_vibration_window.delay(device_id, samples)
        print(f"[{device_id}] dispatched {task.id}")
        result = task.get(timeout=30)
        print(f"  RMS        {result['rms_accel_m_s2']:.3f} m/s^2")
        print(f"  peak       {result['peak_accel_m_s2']:.3f} m/s^2")
        print(f"  crest      {result['crest_factor']:.2f}")
        print(f"  dominant   {result['dominant_frequency_hz']:.1f} Hz "
              f"({result['dominant_axis'].upper()})")
        print(f"  HF energy  {result['hf_energy_ratio'] * 100:.0f}%")
        print(f"  rate       {result['sample_rate_hz']:.1f} Hz "
              f"({result['sample_rate_source']})")
        print(f"  severity   {result['previous_severity']} -> "
              f"{result['severity']} ({result['alert_kind']})")
        print(f"  reason     {result['decision_reason']}")
        print(f"  alert      {result['alert_raised']}\n")


if __name__ == "__main__":
    main()
