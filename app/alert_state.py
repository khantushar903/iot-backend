"""Severity state machine for vibration alerts.

The decision logic is deliberately pure and synchronous so it can be unit
tested and reasoned about without Redis, Celery, or a database. Persistence of
the previous state lives in :mod:`app.redis`.

Two problems are solved here that a bare threshold comparison cannot:

Hysteresis
    A motor whose RMS sits exactly on 2.0 m/s^2 would otherwise flap between
    NORMAL and WARNING on every window (a window lands every few hundred
    milliseconds). Severity is only *released* once the metric falls below a
    lower "clear" threshold.

Cooldown
    Without a cooldown a motor that stays unhealthy produces one alert row per
    window forever. While a severity remains active it is re-emitted only after
    ``alert_cooldown_s`` has elapsed, and a transition back to NORMAL emits an
    explicit resolution.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.config import vibration

NORMAL = "NORMAL"
WARNING = "WARNING"
CRITICAL = "CRITICAL"
RESOLVED = "RESOLVED"

RANK = {NORMAL: 0, WARNING: 1, CRITICAL: 2}

# Alert-row severities. NORMAL never reaches the database as an alert; it is
# only used to describe the current state.
KIND_ESCALATION = "escalation"
KIND_DEESCALATION = "deescalation"
KIND_REMINDER = "reminder"
KIND_RESOLUTION = "resolution"
KIND_NONE = "none"


@dataclass(frozen=True)
class AlertState:
    """Previous condition of a device, as persisted in Redis."""

    severity: str = NORMAL
    last_alert_at: float = 0.0
    updated_at: float = 0.0


@dataclass(frozen=True)
class Decision:
    """Outcome of evaluating one analysis window against the previous state."""

    severity: str
    emit: bool
    kind: str
    reason: str


def escalate(severity: str) -> str:
    if severity == NORMAL:
        return WARNING
    if severity == WARNING:
        return CRITICAL
    return CRITICAL


def classify_rms(rms_m_s2: float) -> str:
    """Map acceleration RMS onto a severity using the *entry* thresholds."""
    if rms_m_s2 >= vibration.critical_rms_m_s2:
        return CRITICAL
    if rms_m_s2 >= vibration.warning_rms_m_s2:
        return WARNING
    return NORMAL


def entry_threshold(severity: str) -> float:
    if severity == CRITICAL:
        return vibration.critical_rms_m_s2
    if severity == WARNING:
        return vibration.warning_rms_m_s2
    return 0.0


def clear_threshold(severity: str) -> float:
    """RMS below which ``severity`` is allowed to be released."""
    if severity == CRITICAL:
        return vibration.critical_clear_rms_m_s2
    if severity == WARNING:
        return vibration.warning_clear_rms_m_s2
    return 0.0


def decide(
    state: AlertState,
    rms_m_s2: float,
    spectral_escalation: bool,
    now: float,
) -> Decision:
    """Decide the new severity and whether an alert row should be emitted.

    ``spectral_escalation`` is produced by the FFT stage: impulsive,
    high-frequency-dominated vibration is promoted one severity step because
    such faults are visible in the spectrum long before they move the RMS.
    """
    raw = classify_rms(rms_m_s2)
    if spectral_escalation:
        if raw != NORMAL:
            candidate = escalate(raw)
        elif vibration.spectral_escalation_from_normal:
            # Impulsive broadband energy while the overall RMS is still
            # nominal: the classic early-bearing-fault signature, and the
            # whole reason the spectrum is in the loop.
            candidate = WARNING
        else:
            candidate = NORMAL
    else:
        candidate = raw

    previous = state.severity if state.severity in RANK else NORMAL

    # Hysteresis: never drop below the previous severity while the metric is
    # still at or above that severity's release level.
    reason = "RMS %s threshold" % ("met" if candidate != NORMAL else "below entry")
    if RANK[candidate] < RANK[previous]:
        floor = clear_threshold(previous)
        if rms_m_s2 >= floor:
            candidate = previous
            reason = "hysteresis hold above clear level %.2f m/s^2" % floor

    if spectral_escalation and candidate != NORMAL:
        reason += " + spectral escalation"

    if candidate != previous:
        kind = (
            KIND_RESOLUTION
            if candidate == NORMAL
            else (KIND_ESCALATION if RANK[candidate] > RANK[previous] else KIND_DEESCALATION)
        )
        return Decision(candidate, True, kind, reason)

    if candidate == NORMAL:
        return Decision(candidate, False, KIND_NONE, "steady state, no alert")

    if now - state.last_alert_at >= vibration.alert_cooldown_s:
        return Decision(
            candidate,
            True,
            KIND_REMINDER,
            "condition still active after %.0fs cooldown" % vibration.alert_cooldown_s,
        )

    return Decision(candidate, False, KIND_NONE, "suppressed by cooldown")


def alert_severity(decision: Decision) -> str:
    """Severity string stored on the Alert row."""
    return RESOLVED if decision.kind == KIND_RESOLUTION else decision.severity
