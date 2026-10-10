# Handover State

Phase-by-phase record of what is implemented, frozen, and verified in this repository. New contributors and integration teams should use this as the single source of truth for the current backend contract.

Legend: 🔒 **Frozen** (contract locked, no further schema/API changes without a new phase) · ✅ **Verified** (exercised end-to-end in the running stack) · 🧪 **In progress** (still changing)

---

## Phase 1 — Real-Time Telemetry Pipeline (🔒 Frozen / ✅ Verified)

The foundational ingest → persist → broadcast pipeline.

- **MQTT ingestion** — `app/mqtt.py`: long-lived `aiomqtt` consumer on `telemetry/motors`, Pydantic validation, poison-pill containment, capped exponential reconnect backoff, and a per-device Redis sliding buffer (`vibration_buffer:{device_id}`, last 30 magnitudes) that dispatches windows to the analytics engine once ≥ 10 readings accumulate.
- **Storage** — `app/database.py`, `app/models.py` (`telemetry_records`), SQLAlchemy 2.0 async + asyncpg.
- **Broadcast** — `app/redis.py` (`live_telemetry` channel) + `/ws/telemetry` WebSocket fanout with snapshot/ping frames.
- **Infrastructure** — Mosquitto, PostgreSQL, Redis, API, Adminer; health-gated Compose startup.
- **REST** — `GET /health`, `POST /telemetry`, `GET /telemetry/latest`.

## Phase 2 — Alerts, Celery / SciPy FFT Engine & REST Endpoints (🔒 Frozen / ✅ Verified)

Adds background signal analysis, alert persistence/broadcast, and a historical REST surface.

- **Alert domain** — `app/models.py` (`alerts` table + `Alert` ORM), `app/schemas.py` (`AlertCreate`, `AlertResponse`).
- **DB helpers** — `app/crud.py`: `create_alert`, `get_alerts` (limit 50), `get_telemetry_history` (device_id filter + limit/offset).
- **Task queue** — `app/celery_app.py`: Celery instance using Redis as broker + backend.
- **Analytics engine** — `app/analytics.py`: `process_vibration_window` task:
  - Integration of acceleration → RMS velocity (mm/s) via `scipy.integrate.cumulative_trapezoid`.
  - Dominant frequency via `scipy.fft.rfft` / `rfftfreq`.
  - ISO 10816-1 classification into Zones A–D; Zone C → `WARNING`, Zone D → `CRITICAL`.
  - On breach: persists an `Alert` to PostgreSQL (async session) and publishes `{"type":"alert","data":{...}}` to `live_telemetry`.
  - Async I/O runs on **one persistent event loop per worker process**: `worker_process_init` creates it, `run_async` runs coroutines on it, and `worker_process_shutdown` disposes the DB engine and closes the Redis client. The shared async engine uses `NullPool` (`app/database.py`) so no connection is cached across loops — eliminating `Event loop is closed` / `attached to a different loop` errors under consecutive dispatches.
- **Verification script** — `scripts/test_vibration_task.py`: dispatches 5 sequential `process_vibration_window` tasks across Celery workers and prints each task id + completion status.
- **Container** — `celery_worker` service in `docker-compose.yml` (`celery -A app.analytics.celery_app worker --loglevel=info`).
- **REST endpoints** —
  - `GET /api/v1/health` (DB / Redis / MQTT status)
  - `GET /api/v1/telemetry/history?device_id=&limit=&offset=`
  - `GET /api/v1/alerts?limit=`
  - `POST /api/v1/alerts`
- **WebSocket channel** now carries 4 frame types: `snapshot`, `telemetry`, `alert`, `ping`.

**Verification performed:** syntax/compile checks pass; MQTT/Redis/Postgres health-gated Compose stack; sliding-window buffer accumulates magnitudes and dispatches ≥10-reading windows; alerts persisted and broadcast on a persistent per-worker event loop; history endpoints return expected rows; worker task dispatch exercised in-container and via `scripts/test_vibration_task.py`.

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

### ISO 10816-1 Zones (RMS velocity)
| Zone | mm/s | Outcome |
| --- | --- | --- |
| A | < 1.12 | Good |
| B | 1.12 – 2.80 | Satisfactory |
| C | 2.80 – 7.10 | `WARNING` |
| D | > 7.10 | `CRITICAL` |

### Tables
`telemetry_records` (`id`, `device_id` idx, `ts`, `accel_x/y/z`, `temp_c`, `created_at`) · `alerts` (`id`, `device_id` idx, `severity`, `metric`, `value`, `threshold`, `message`, `created_at`)
