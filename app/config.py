from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Standard gravity, used only to sanity-check accelerometer mounting.
G_M_S2 = 9.80665


class Settings(BaseSettings):
    database_url: str = (
        "postgresql+asyncpg://iot_user:iot_password@postgres:5432/iot_db"
    )
    redis_url: str = "redis://redis:6379/0"
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_topic: str = "telemetry/motors"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class VibrationSettings(BaseSettings):
    """Condition-monitoring parameters.

    Everything is overridable through the environment (``VIBRATION_*``) or a
    ``.env`` file so the alerting experiment can be repeated with different
    thresholds without touching code.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="VIBRATION_", extra="ignore"
    )

    # --- Windowing / sampling -------------------------------------------------
    window_samples: int = 512
    fallback_sample_rate_hz: float = 50.0
    min_sample_rate_hz: float = 1.0
    max_sample_rate_hz: float = 20000.0
    # Unit of the `ts` field in the MQTT payload. Declared explicitly because
    # the frequency axis is meaningless if this is wrong.
    ts_unit: str = "ms"

    # --- Time-domain thresholds (resultant acceleration RMS, m/s^2) -----------
    # "Entry" levels raise severity; "clear" levels release it. The gap between
    # them is the hysteresis band that stops alert chatter at the boundary.
    warning_rms_m_s2: float = 2.0
    critical_rms_m_s2: float = 5.0
    warning_clear_rms_m_s2: float = 1.6
    critical_clear_rms_m_s2: float = 4.0

    # --- Spectral escalation --------------------------------------------------
    # Impulsive (bearing-style) faults raise broadband high-frequency energy and
    # crest factor long before the overall RMS moves. When both are present the
    # severity is escalated one step so the spectrum can actually raise an alert.
    hf_band_start_fraction: float = 0.5  # of Nyquist
    hf_energy_ratio_escalate: float = 0.30
    crest_factor_escalate: float = 3.5
    # Allow the spectrum to raise a WARNING on a device whose RMS is still
    # nominal. This is the case that matters for early fault detection, but it
    # is also the easiest way to generate false positives, so it is a switch.
    spectral_escalation_from_normal: bool = True

    # --- Alert lifecycle ------------------------------------------------------
    alert_cooldown_s: float = 300.0
    state_ttl_s: int = 86400

    @model_validator(mode="after")
    def _check_thresholds(self) -> "VibrationSettings":
        pairs = (
            (self.warning_clear_rms_m_s2, self.warning_rms_m_s2),
            (self.critical_clear_rms_m_s2, self.critical_rms_m_s2),
        )
        for clear, entry in pairs:
            if clear >= entry:
                raise ValueError(
                    "clear threshold must be below its entry threshold "
                    f"(clear={clear}, entry={entry})"
                )
        if self.warning_rms_m_s2 > self.critical_rms_m_s2:
            raise ValueError("warning threshold must not exceed critical")
        if not 0.0 < self.hf_band_start_fraction < 1.0:
            raise ValueError("hf_band_start_fraction must be within (0, 1)")
        if self.window_samples < 8:
            raise ValueError("window_samples must be >= 8 for a usable spectrum")
        if self.ts_unit.lower() not in {"ms", "s", "us"}:
            raise ValueError("ts_unit must be one of: ms, s, us")
        return self


settings = Settings()
vibration = VibrationSettings()
