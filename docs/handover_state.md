# Handover State

Phase-by-phase record of what is implemented, frozen, and verified in this repository. New contributors and integration teams should use this as the single source of truth for the current backend contract.

Legend: 🔒 **Frozen** (contract locked, no further schema/API changes without a new phase) · ✅ **Verified** (exercised end-to-end in the running stack) · 🧪 **In progress** (still changing)

---

## Phase 1 — Real-Time Telemetry Pipeline (🔒 Frozen / ✅ Verified)

The foundational ingest → persist → broadcast pipeline.

- **MQTT ingestion** — `app/mqtt.py`: long-lived `aiomqtt` consumer on `telemetry/motors`, Pydantic validation, poison-pill containment, capped exponential reconnect backoff, and a per-device Redis buffer (`vibration_buffer:{device_id}`) holding **per-axis** `{x, y, z, ts}` samples. A full fixed window (512 samples) is consumed off the front and dispatched; overflow seeds the next window, and the backlog is capped at 3× the window size.
- **Storage** — `app/database.py`, `app/models.py` (`telemetry_records`), SQLAlchemy 2.0 async + asyncpg.
- **Broadcast** — `app/redis.py` (`live_telemetry` channel) + `/ws/telemetry` WebSocket fanout with snapshot/ping frames.
- **Infrastructure** — Mosquitto, PostgreSQL, Redis, API, Adminer; health-gated Compose startup.
- **REST** — `GET /health`, `POST /telemetry`, `GET /telemetry/latest`.

## Phase 2 — Alerts, Celery / SciPy FFT Engine & REST Endpoints (🔒 Frozen / ✅ Verified)

> **Corrected 2026-10.** The signal analysis previously described here claimed ISO 10816-1 classification on RMS *velocity* in mm/s. That code no longer existed. The engine now classifies on resultant acceleration RMS (m/s²) after per-axis gravity removal, with a spectral escalation rule, hysteresis, cooldown, and explicit resolution. See `docs/architecture.md`.

Adds background signal analysis, alert persistence/broadcast, and a historical REST surface.

- **Alert domain** — `app/models.py` (`alerts` table + `Alert` ORM), `app/schemas.py` (`AlertCreate`, `AlertResponse`).
- **DB helpers** — `app/crud.py`: `create_alert`, `get_alerts` (limit 50), `get_telemetry_history` (device_id filter + limit/offset).
- **Task queue** — `app/celery_app.py`: Celery instance using Redis as broker + backend.
- **Analytics engine** — `app/analytics.py`:
  - `analyze_window(samples)` is **pure** (no Redis/Celery/DB) so the math is testable in isolation.
  - Per-axis DC removal removes gravity and bias regardless of mounting orientation.
  - Sample rate is **derived from the device timestamps**, falling back to a configured value (and reporting `sample_rate_source`) when they are missing, non-increasing, or implausible.
  - Time domain: per-axis and resultant RMS, peak, peak-to-peak, crest factor.
  - Frequency domain: **Hann-tapered** `scipy.fft.rfft` / `rfftfreq` giving the dominant frequency, the contributing `dominant_axis`, and `hf_energy_ratio` (energy above `hf_band_start_fraction` × Nyquist).
  - `process_vibration_window(device_id, samples)` is the thin Celery wrapper; it bridges to async I/O with `async_to_sync`, which is safe because the engine uses `NullPool`.
- **Alert lifecycle** — `app/alert_state.py` (pure, sync) plus state in Redis at `alert_state:{device_id}`:
  - RMS entry thresholds (2.0 / 5.0 m/s²) and lower release levels (1.6 / 4.0) give **hysteresis**, so a motor idling on a threshold cannot flap.
  - **Spectral escalation**: `hf_energy_ratio ≥ 0.30` **and** `crest_factor ≥ 3.5` promotes one severity step, so impulsive-but-quiet faults are caught. Governed by `VIBRATION_SPECTRAL_ESCALATION_FROM_NORMAL`.
  - **Cooldown** (`VIBRATION_ALERT_COOLDOWN_S`, default 300 s) caps an ongoing condition to one alert per interval.
  - **Resolution**: returning to `NORMAL` writes a `RESOLVED` alert row.
  - Alert kinds: `escalation`, `deescalation`, `reminder`, `resolution`, `none`.
  - Emitted alerts are persisted to PostgreSQL and published as `{"type":"alert","data":{...}}` to `live_telemetry`.
- **Tuning** — `app/config.py::VibrationSettings`, every threshold overridable as a `VIBRATION_*` environment variable and validated on startup.
- **Verification scripts**:
  - `scripts/check_analytics.py` — assertions against synthetic signals with known answers (gravity removal, off-bin frequency recovery, sample-rate fallback, every hysteresis boundary, cooldown/resolution, spectral escalation on impulsive-but-quiet vibration, and that smooth broadband noise does *not* escalate). Runs with no stack: `python -m scripts.check_analytics`.
  - `scripts/test_vibration_task.py` — dispatches healthy / warning / critical synthetic windows through a live worker and prints every metric plus the severity transition. Requires the running stack.
- **Container** — `celery_worker` service in `docker-compose.yml` (`celery -A app.analytics.celery_app worker --loglevel=info`).
- **REST endpoints** —
  - `GET /api/v1/health` (DB / Redis / MQTT status)
  - `GET /api/v1/telemetry/history?device_id=&limit=&offset=`
  - `GET /api/v1/alerts?limit=`
  - `POST /api/v1/alerts`
- **WebSocket channel** now carries 4 frame types: `snapshot`, `telemetry`, `alert`, `ping`.

**Verification performed:** `scripts/check_analytics.py` passes in full; frontend type-check passes; MQTT/Redis/Postgres health-gated Compose stack; the fixed-window buffer accumulates per-axis samples and dispatches 512-sample windows without re-dispatching a consumed window; alerts persist and broadcast; history endpoints return expected rows.

## Phase 3 — (Planned / Not Started)

Reserved for follow-up work — e.g. device threshold configuration, aggregate windows, temperature-based alerts, or frontend integration. Nothing here is frozen yet.

---

## Contract Snapshot (Frozen)

### Containers (6)
`iot-api` · `iot-celery-worker` · `iot-postgres` · `iot-redis` · `iot-mosquitto` · `iot-adminer`

### REST
| Method | Path | Notes |
| --- | --- | --- |
| `GET` | `/health` | Legacy liveness probe |
| `POST` | `/telemetry` | Manual ingestion |
| `GET` | `/telemetry/latest` | Last 10, newest first |
| `GET` | `/api/v1/health` | DB/Redis/MQTT status |
| `GET` | `/api/v1/telemetry/history` | Optional `device_id`, `limit` (≤1000), `offset` |
| `GET` | `/api/v1/alerts` | Default `limit` 50, newest first |
| `POST` | `/api/v1/alerts` | Create an alert (201) |
| `WS` | `/ws/telemetry` | `snapshot` · `telemetry` · `alert` · `ping` |

### Vibration Severity (resultant acceleration RMS)
| Severity | Entry | Release | Outcome |
| --- | --- | --- | --- |
| `NORMAL` | < 2.0 m/s² | — | No alert |
| `WARNING` | ≥ 2.0 m/s² | < 1.6 m/s² | `WARNING` alert |
| `CRITICAL` | ≥ 5.0 m/s² | < 4.0 m/s² | `CRITICAL` alert |

Promotion by one step when `hf_energy_ratio ≥ 0.30` and `crest_factor ≥ 3.5`.

These are **project-defined** thresholds, not ISO 10816/20816 zones. Those zones are defined on RMS *velocity* in mm/s, measured at a defined point on a defined machine class; this project measures acceleration from a single MPU6050 with no anti-alias filtering and no velocity integration, so applying them here would not be defensible.

### Tables
`telemetry_records` (`id`, `device_id` idx, `ts`, `accel_x/y/z`, `temp_c`, `created_at`) · `alerts` (`id`, `device_id` idx, `severity`, `metric`, `value`, `threshold`, `message`, `created_at`)
