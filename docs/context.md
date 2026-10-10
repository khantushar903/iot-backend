# Project Context: Industrial IoT Machine Monitoring System (Thesis Project)

## Overview
A scalable, event-driven IoT backend that monitors motor health using vibration (MPU6050) and temperature (DS18B20) data. The system ingests high-frequency MQTT telemetry, persists it durably, streams it live to a frontend, and runs background vibration analysis (time-domain RMS + frequency-domain FFT) with an automatic alert lifecycle. **Status:** both the ingestion pipeline and the Phase 2 analytics layer (alerts, Celery, REST endpoints) are implemented, containerized, and verified end-to-end.

## Tech Stack
*   **Backend Framework:** FastAPI (Python 3.12, fully async)
*   **Message Broker:** Eclipse Mosquitto 2.x (MQTT over TCP :1883 / WebSocket :9001)
*   **Database:** PostgreSQL 16 (SQLAlchemy 2.0 async ORM + asyncpg) — telemetry + alerts
*   **In-Memory Store / Pub/Sub / Celery Transport:** Redis 7
*   **Task Queue / Analytics:** Celery 5.x + SciPy/NumPy (FFT, windowing, spectral band energy)
*   **Deployment:** Docker Compose (health-gated service startup) + Adminer for DB administration

## Backend Pipeline

```
Infrastructure → Schemas → Async Ingestion → DB Persistence → Redis Pub/Sub → WebSocket Fanout
                                      ↘
        Redis fixed window (per device) → Celery RMS + FFT → Alert Lifecycle → Persistence → Broadcast
```

| Stage | Implementation | Responsibility |
| --- | --- | --- |
| 1. Infrastructure | `docker-compose.yml`, `mosquitto/mosquitto.conf`, `Dockerfile` | Mosquitto, PostgreSQL, Redis, API, Celery worker, Adminer; healthchecks gate startup order |
| 2. Schemas | `app/config.py`, `app/schemas.py` | pydantic-settings configuration; Pydantic v2 request/response contracts (telemetry + alerts) |
| 3. Async Ingestion | `app/mqtt.py` | Long-lived `aiomqtt` consumer on `telemetry/motors`; validate-then-persist per message |
| 4. DB Persistence | `app/database.py`, `app/models.py` | Async engine/session factory; `telemetry_records` and `alerts` tables with indexed `device_id` |
| 5. Redis Pub/Sub | `app/redis.py` | Fan-out of persisted records on channel `live_telemetry` |
| 6. Analytics Engine | `app/celery_app.py`, `app/analytics.py`, `app/alert_state.py` | Celery worker computes per-axis RMS/peak/crest plus a windowed FFT; `alert_state.py` applies hysteresis, cooldown, and resolution before persisting + broadcasting alerts |
| 7. WebSocket Fanout | `app/main.py` (`/ws/telemetry`) | Snapshot-on-connect + verbatim relay of live frames + alert frames + idle keepalive |

## Data Flow (as implemented)
1. ESP32 nodes publish JSON readings to Mosquitto on topic `telemetry/motors`.
2. A background `aiomqtt` task inside FastAPI consumes messages (auto-reconnect with capped exponential backoff).
3. Each payload is parsed and validated by Pydantic (`TelemetryCreate`); failures are logged and dropped without disturbing the consumer loop.
4. Valid readings are inserted into PostgreSQL through a dedicated `AsyncSession` per message; the row is refreshed so server-generated fields (`id`, `created_at`) are populated.
5. The serialized record is published once to the Redis channel `live_telemetry`; streaming failures never block ingestion.
6. After each valid reading, the consumer pushes the **per-axis** sample `{x, y, z, ts}` onto a per-device window keyed `vibration_buffer:{device_id}` in Redis. Once a full window (512 samples) has accumulated, it is consumed and dispatched to a Celery worker. The axes are preserved rather than pre-reduced to a magnitude so the analyser can remove gravity per axis and resolve a per-axis frequency.
7. The Celery worker removes each axis's DC component (gravity + bias), derives the sample rate from the device timestamps, and computes resultant RMS, peak, peak-to-peak, crest factor, and a Hann-windowed FFT giving the dominant frequency and high-frequency energy ratio.
8. `alert_state.decide(...)` maps the metrics onto `NORMAL`/`WARNING`/`CRITICAL` using RMS entry thresholds, a spectral-escalation rule, and hysteresis. It emits an alert only on escalation, de-escalation, cooldown expiry, or return to normal; an active condition then re-emits only after the cooldown, and recovery emits a `RESOLVED` row. Each emitted alert is persisted to PostgreSQL and published as `{"type":"alert","data":{...}}` to `live_telemetry`.
9. Dashboard clients connect to `/ws/telemetry`, receive a snapshot of the last 10 records, then receive every subsequent telemetry/alert frame in real time.

## Vibration Severity Classification

The analytics engine converts the measured vibration window into one of three severities. These are **project-defined** thresholds on resultant acceleration RMS, not ISO 10816/20816 zones (see the note below).

| Severity | RMS entry | RMS release | Action |
| --- | --- | --- | --- |
| `NORMAL` | < 2.0 m/s² | — | No alert |
| `WARNING` | ≥ 2.0 m/s² | < 1.6 m/s² | `WARNING` alert raised |
| `CRITICAL` | ≥ 5.0 m/s² | < 4.0 m/s² | `CRITICAL` alert raised |

**Spectral escalation.** When `hf_energy_ratio ≥ 0.30` **and** `crest_factor ≥ 3.5`, the severity is promoted one step. This catches impulsive, HF-dominated vibration (a typical early bearing-fault signature) whose overall RMS is still nominal — precisely the case a pure RMS threshold is blind to. Both conditions must hold, so smooth broadband noise cannot trigger it.

**Hysteresis** is the gap between each entry and release level. Without it a motor idling near a threshold would flap between severities every window and write an alert row on each flap.

**Cooldown** caps an ongoing condition to one alert per `VIBRATION_ALERT_COOLDOWN_S` (default 300 s) until it clears.

> **Why not ISO 10816?** ISO 10816/20816 zones are defined on RMS *velocity* in mm/s, measured at a defined point on a machine of a defined class over a defined frequency range. This project measures *acceleration* from a single MPU6050 on a small motor, with no anti-alias filtering and no velocity integration, so quoting those limits against these numbers would not be defensible.

Alert persistence uses the async session (`AsyncSessionLocal`) and, once committed, broadcasts the serialized `AlertResponse` to the shared `live_telemetry` channel so dashboards receive alerts live alongside telemetry.

## Payload Schema (JSON)

Telemetry (ingested via MQTT or `POST /telemetry`):
```
{
  "device_id": "string",
  "ts": "integer (unix timestamp in ms, optional - the analytics engine derives the sample rate from it)",
  "accel_x": "float",
  "accel_y": "float",
  "accel_z": "float",
  "temp_c": "float"
}
```

Alert (persisted by the analytics engine, readable via `GET /api/v1/alerts`, and created manually via `POST /api/v1/alerts`):
```
{
  "device_id": "string",
  "severity": "WARNING" | "CRITICAL" | "RESOLVED",
  "metric": "vibration",
  "value": "float (resultant acceleration RMS, m/s^2)",
  "threshold": "float (severity entry level breached)",
  "message": "string",
  "created_at": "datetime (server-assigned)"
}
```

## Development Rules
*   Use fully asynchronous Python code (`async def`, `aiomqtt`, asyncpg via `async_sessionmaker`); Celery tasks run in a sync context and bridge to async I/O via `async_to_sync`. The engine uses `NullPool`, so no connection is cached across loops.
*   Keep signal math in pure functions (`analyze_window`, `app.alert_state.decide`) with no I/O, so they can be verified against synthetic signals with known answers (`python -m scripts.check_analytics`).
*   Keep files modular (`config.py`, `database.py`, `models.py`, `schemas.py`, `crud.py`, `mqtt.py`, `redis.py`, `celery_app.py`, `analytics.py`, `alert_state.py`, `main.py`).
*   Include proper error handling and logging; a bad message or a degraded dependency must never crash the pipeline.
