"""Sanity checks for the vibration analysis and alert lifecycle.

Runs with plain numpy - no Redis, Celery, or database required.

    python -m scripts.check_analytics
"""

from __future__ import annotations

import math

import numpy as np

from app.alert_state import (
    CRITICAL,
    NORMAL,
    RESOLVED,
    WARNING,
    AlertState,
    alert_severity,
    decide,
)
from app.analytics import AXES, analyze_window, estimate_sample_rate_hz
from app.config import vibration

SAMPLE_RATE_HZ = 200.0
WINDOW = vibration.window_samples


def window(
    *,
    dc: tuple[float, float, float] = (0.0, 0.0, 0.0),
    tones: tuple[tuple[float, float, str], ...] = (),
    noise_rms: float = 0.0,
    sample_rate_hz: float = SAMPLE_RATE_HZ,
    with_ts: bool = True,
    seed: int = 0,
) -> list[dict]:
    """Build a synthetic window.

    ``dc`` is a static per-axis offset (this is where gravity goes).
    ``tones`` is a tuple of ``(frequency_hz, amplitude, axis)`` applied to a
    single axis, so the resultant RMS of one tone is ``amplitude / sqrt(2)``.
    """
    rng = np.random.default_rng(seed)
    n = WINDOW
    t = np.arange(n) / sample_rate_hz
    offsets = {"x": dc[0], "y": dc[1], "z": dc[2]}
    samples: list[dict] = []
    for i in range(n):
        value = {
            axis: offsets[axis] + noise_rms * rng.standard_normal()
            for axis in AXES
        }
        for freq, amp, axis in tones:
            value[axis] += amp * math.sin(2 * math.pi * freq * t[i])
        samples.append(
            {
                "x": float(value["x"]),
                "y": float(value["y"]),
                "z": float(value["z"]),
                "ts": int(1_700_000_000_000 + i * 1000.0 / sample_rate_hz)
                if with_ts
                else None,
            }
        )
    return samples


_failures: list[str] = []


def check(label: str, actual: object, expected: object, tol: float = 0.0) -> None:
    if isinstance(actual, float) and isinstance(expected, float) and tol:
        ok = math.isclose(actual, expected, rel_tol=tol, abs_tol=tol)
    else:
        ok = actual == expected
    status = "PASS" if ok else "FAIL"
    if not ok:
        _failures.append(label)
    print(f"  [{status}] {label}: got {actual!r}, expected {expected!r}")


def section(title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))


def test_gravity_removal() -> None:
    section("Gravity is removed by per-axis DC removal")
    gravity = 9.80665
    # Sensor at rest, mounted flat: gravity sits entirely on Z.
    metrics = analyze_window(window(dc=(0.0, 0.0, gravity)))
    check("RMS at rest (gravity removed)", metrics["rms_accel_m_s2"], 0.0, tol=1e-9)
    check("no alert-worthy severity", metrics["rms_severity"], NORMAL)

    # Same physical situation, board rotated: gravity on X.
    metrics = analyze_window(window(dc=(gravity, 0.0, 0.0)))
    check("RMS at rest, gravity on X", metrics["rms_accel_m_s2"], 0.0, tol=1e-9)

    # A real 1 m/s^2 RMS sinusoid must survive gravity removal intact.
    metrics = analyze_window(
        window(dc=(0.0, 0.0, gravity), tones=((20.0, math.sqrt(2.0), "x"),))
    )
    check(
        "RMS of a 1 m/s^2 sine on top of gravity",
        metrics["rms_accel_m_s2"],
        1.0,
        tol=0.01,
    )
    check("dominant axis is X", metrics["dominant_axis"], "x")


def test_frequency_accuracy() -> None:
    section("Frequency axis comes from device timestamps")
    # A 20 Hz tone published at only 40 Hz: a hardcoded 500 Hz assumption would
    # report 125 Hz, which is the bug this replaces.
    true_hz = 20.0
    publish_hz = 40.0
    metrics = analyze_window(
        window(
            dc=(0.0, 0.0, 9.80665),
            tones=((true_hz, math.sqrt(2.0), "z"),),
            sample_rate_hz=publish_hz,
        )
    )
    check("sample rate derived", metrics["sample_rate_hz"], publish_hz, tol=1e-3)
    check("sample rate source", metrics["sample_rate_source"], "timestamps")
    check(
        "dominant frequency recovered",
        metrics["dominant_frequency_hz"],
        true_hz,
        tol=0.3,
    )
    check("dominant axis is Z", metrics["dominant_axis"], "z")
    check(
        "per-axis dominant frequency agrees",
        metrics["dominant_frequency_z_hz"],
        true_hz,
        tol=0.3,
    )


def test_windowing_reduces_leakage() -> None:
    section("Hann taper keeps argmax on the true bin")
    # 37 Hz is deliberately off-bin (bin spacing is 0.39 Hz at 200 Hz over 512
    # samples). Without the taper this smears across neighbours.
    true_hz = 37.0
    metrics = analyze_window(
        window(
            dc=(0.0, 0.0, 9.80665),
            tones=((true_hz, math.sqrt(2.0), "z"),),
        )
    )
    check(
        "off-bin tone still resolved",
        metrics["dominant_frequency_hz"],
        true_hz,
        tol=0.2,
    )


def test_spectral_escalation_participates() -> None:
    section("Spectrum can escalate severity on its own")
    # Broadband impulsive content: a 6 m/s^2 peak every 16 samples. Overall RMS
    # stays under 2.0 m/s^2, so a pure RMS method sees a healthy machine while
    # the spectrum and crest factor see a clearly impulsive one.
    rng = np.random.default_rng(7)
    n = WINDOW
    signal = np.zeros(n)
    signal[::16] = 6.0
    signal += 0.05 * rng.standard_normal(n)
    samples = []
    for i in range(n):
        samples.append(
            {
                "x": float(signal[i]),
                "y": 0.0,
                "z": 9.80665,
                "ts": int(1_700_000_000_000 + i * 1000.0 / SAMPLE_RATE_HZ),
            }
        )
    metrics = analyze_window(samples)
    print(
        f"    (RMS {metrics['rms_accel_m_s2']:.2f} m/s^2, "
        f"crest {metrics['crest_factor']:.2f}, "
        f"HF energy {metrics['hf_energy_ratio'] * 100:.0f}%)"
    )
    check("RMS is below the warning entry level", metrics["rms_accel_m_s2"] < 2.0, True)
    check("RMS alone says NORMAL", metrics["rms_severity"], NORMAL)
    check("spectral escalation fired", metrics["spectral_escalation"], True)

    decision = decide(
        AlertState(), metrics["rms_accel_m_s2"], metrics["spectral_escalation"], now=0.0
    )
    check("severity escalated to WARNING", decision.severity, WARNING)
    check("an alert is actually emitted", decision.emit, True)

    # The same signal must clear once it is gone.
    quiet = analyze_window(window(dc=(0.0, 0.0, 9.80665)))
    decision = decide(
        AlertState(severity=WARNING, last_alert_at=0.0),
        quiet["rms_accel_m_s2"],
        quiet["spectral_escalation"],
        now=0.0,
    )
    check("returns to NORMAL", decision.severity, NORMAL)
    check("resolution emitted", decision.kind, "resolution")

    # Smooth broadband noise must NOT trip the escalation, or the method is
    # useless.
    smooth = analyze_window(
        window(dc=(0.0, 0.0, 9.80665), noise_rms=1.5, seed=3)
    )
    check(
        "smooth noise does not escalate",
        smooth["spectral_escalation"],
        False,
    )


def test_hysteresis_stops_flapping() -> None:
    section("Hysteresis holds severity inside the band")
    # RMS sits between the warning entry (2.0) and clear (1.6) levels.
    rms = 1.8
    first = decide(AlertState(), rms, False, now=0.0)
    check("enters WARNING at 2.0+ ... below entry it stays NORMAL", first.severity, NORMAL)

    rising = decide(AlertState(severity=WARNING, last_alert_at=0.0), rms, False, now=0.0)
    check("WARNING is held at 1.8", rising.severity, WARNING)
    check("but nothing is emitted", rising.emit, False)

    falling = decide(AlertState(severity=WARNING, last_alert_at=0.0), 1.5, False, now=0.0)
    check("released below 1.6", falling.severity, NORMAL)

    # Same at the critical boundary.
    critical_hold = decide(AlertState(severity=CRITICAL, last_alert_at=0.0), 4.5, False, now=0.0)
    check("CRITICAL held at 4.5 (clear=4.0)", critical_hold.severity, CRITICAL)
    critical_drop = decide(AlertState(severity=CRITICAL, last_alert_at=0.0), 3.9, False, now=0.0)
    check("CRITICAL released at 3.9", critical_drop.severity, WARNING)


def test_cooldown_and_resolution() -> None:
    section("Cooldown suppresses repeats, resolution is emitted")
    rms = 6.0
    now = 1_000_000.0

    first = decide(AlertState(), rms, False, now=now)
    check("first breach emits", first.emit, True)
    check("kind is escalation", first.kind, "escalation")

    inside = decide(
        AlertState(severity=CRITICAL, last_alert_at=now), rms, False, now=now + 10.0
    )
    check("repeat within cooldown suppressed", inside.emit, False)

    after = decide(
        AlertState(severity=CRITICAL, last_alert_at=now),
        rms,
        False,
        now=now + vibration.alert_cooldown_s + 1.0,
    )
    check("reminder after cooldown", after.kind, "reminder")
    check("reminder emits", after.emit, True)

    resolved = decide(
        AlertState(severity=CRITICAL, last_alert_at=now), 0.2, False, now=now + 1.0
    )
    check("resolution emitted", resolved.kind, "resolution")
    check("resolution severity on the row", alert_severity(resolved), RESOLVED)

    quiet = decide(
        AlertState(severity=NORMAL, last_alert_at=now), 0.2, False, now=now + 1.0
    )
    check("no duplicate resolution", quiet.emit, False)


def test_sample_rate_fallback() -> None:
    section("Sample rate falls back and says so")
    rate, source = estimate_sample_rate_hz(None, 512)
    check("no timestamps -> fallback", source, "fallback")
    check("fallback value used", rate, vibration.fallback_sample_rate_hz)

    rate, source = estimate_sample_rate_hz([0, 0, 0, 0], 4)
    check("duplicate timestamps -> fallback", source, "fallback")

    # Timestamps implying one sample every 100 s are not a real sample rate.
    _, source = estimate_sample_rate_hz([0, 100_000, 200_000, 300_000], 4)
    check("implausible rate -> fallback", source, "fallback")

    metrics = analyze_window(window(with_ts=False))
    check("metrics flag the fallback", metrics["sample_rate_source"], "fallback")


def test_thresholds_are_defensible() -> None:
    section("End-to-end severity on a plain sine")
    # Resultant RMS must equal sqrt(sum of per-axis RMS^2): two axes at 3/sqrt(2)
    # RMS each give a resultant of 3.0.
    amp = 3.0
    metrics = analyze_window(
        window(
            dc=(0.0, 0.0, 9.80665),
            tones=((25.0, amp, "x"), (25.0, amp, "y")),
        )
    )
    print(f"    (RMS {metrics['rms_accel_m_s2']:.2f} m/s^2)")
    check(
        "resultant RMS of two 2.12 m/s^2 axes",
        metrics["rms_accel_m_s2"],
        3.0,
        tol=0.02,
    )
    check("3.0 m/s^2 is a WARNING", metrics["rms_severity"], WARNING)

    # Now push it past the critical entry level of 5.0 m/s^2. A single tone of
    # amplitude A has RMS A/sqrt(2), so A = 6*sqrt(2).
    amp = 6.0 * math.sqrt(2.0)
    metrics = analyze_window(
        window(dc=(0.0, 0.0, 9.80665), tones=((25.0, amp, "x"),))
    )
    print(f"    (RMS {metrics['rms_accel_m_s2']:.2f} m/s^2)")
    check("6.0 m/s^2 is a CRITICAL", metrics["rms_severity"], CRITICAL)

    # A single axis at 1.5 m/s^2 stays NORMAL.
    metrics = analyze_window(
        window(
            dc=(0.0, 0.0, 9.80665),
            tones=((25.0, 1.5 / math.sqrt(2.0), "x"),),
        )
    )
    check("mild vibration stays NORMAL", metrics["rms_severity"], NORMAL)


def main() -> int:
    test_gravity_removal()
    test_frequency_accuracy()
    test_windowing_reduces_leakage()
    test_spectral_escalation_participates()
    test_hysteresis_stops_flapping()
    test_cooldown_and_resolution()
    test_sample_rate_fallback()
    test_thresholds_are_defensible()

    print()
    if _failures:
        print(f"{len(_failures)} check(s) FAILED:")
        for name in _failures:
            print(f"  - {name}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
