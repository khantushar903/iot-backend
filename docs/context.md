# Project Context: Industrial IoT Machine Monitoring System (Thesis Project)

## Overview
A scalable, event-driven IoT backend that monitors motor health using vibration (MPU6050) and temperature (DS18B20) data. The system ingests high-frequency MQTT telemetry, persists it durably, streams it live to a frontend, and runs background FFT/ISO 10816-1 vibration analysis with automatic alert generation. **Status:** both the ingestion pipeline and the Phase 2 analytics layer (alerts, Celery, REST endpoints) are implemented, containerized, and verified end-to-end.

## Tech Stack
*   **Backend Framework:** FastAPI (Python 3.12, fully async)
*   **Message Broker:** Eclipse Mosquitto 2.x (MQTT over TCP :1883 / WebSocket :9001)
*   **Database:** PostgreSQL 16 (SQLAlchemy 2.0 async ORM + asyncpg) — telemetry + alerts
*   **In-Memory Store / Pub/Sub / Celery Transport:** Redis 7
*   **Task Queue / Analytics:** Celery 5.x + SciPy/NumPy (FFT, numerical integration)
*   **Deployment:** Docker Compose (health-gated service startup) + Adminer for DB administration

## Backend Pipeline

```
Infrastructure → Schemas → Async Ingestion → DB Persistence → Redis Pub/Sub → WebSocket Fanout
                                      ↘
        Redis sliding buffer (per device) → Celery FFT / ISO 10816 → Alert Persistence → Broadcast
```

| Stage | Implementation | Responsibility |
| --- | --- | --- |
| 1. Infrastructure | `docker-compose.yml`, `mosquitto/mosquitto.conf`, `Dockerfile` | Mosquitto, PostgreSQL, Redis, API, Celery worker, Adminer; healthchecks gate startup order |
| 2. Schemas | `app/config.py`, `app/schemas.py` | pydantic-settings configuration; Pydantic v2 request/response contracts (telemetry + alerts) |
| 3. Async Ingestion | `app/mqtt.py` | Long-lived `aiomqtt` consumer on `telemetry/motors`; validate-then-persist per message |
| 4. DB Persistence | `app/database.py`, `app/models.py` | Async engine/session factory; `telemetry_records` and `alerts` tables with indexed `device_id` |
| 5. Redis Pub/Sub | `app/redis.py` | Fan-out of persisted records on channel `live_telemetry` |
| 6. Analytics Engine | `app/celery_app.py`, `app/analytics.py` | Celery worker runs FFT + ISO 10816-1 RMS velocity classification; persists + broadcasts alerts on a single persistent event loop per worker process |
| 7. WebSocket Fanout | `app/main.py` (`/ws/telemetry`) | Snapshot-on-connect + verbatim relay of live frames + alert frames + idle keepalive |

## Data Flow (as implemented)
1. ESP32 nodes publish JSON readings to Mosquitto on topic `telemetry/motors`.
2. A background `aiomqtt` task inside FastAPI consumes messages (auto-reconnect with capped exponential backoff).
3. Each payload is parsed and validated by Pydantic (`TelemetryCreate`); failures are logged and dropped without disturbing the consumer loop.
4. Valid readings are inserted into PostgreSQL through a dedicated `AsyncSession` per message; the row is refreshed so server-generated fields (`id`, `created_at`) are populated.
5. The serialized record is published once to the Redis channel `live_telemetry`; streaming failures never block ingestion.
6. After each valid reading, the consumer pushes the raw acceleration magnitude `sqrt(aₓ²+a_y²+a_z²)` onto a per-device sliding window keyed `vibration_buffer:{device_id}` in Redis (retaining only the last 30 readings). Once a window holds ≥ 10 readings, it is dispatched to a Celery worker.
7. The Celery worker integrates the acceleration window into RMS velocity (mm/s), computes the dominant frequency via FFT, and classifies against ISO 10816-1 zones (A–D).
8. When a WARNING (Zone C) or CRITICAL (Zone D) threshold is breached, the worker persists an `Alert` row to PostgreSQL and publishes `{"type":"alert","data":{...}}` to `live_telemetry`.
9. Dashboard clients connect to `/ws/telemetry`, receive a snapshot of the last 10 records, then receive every subsequent telemetry/alert frame in real time.

## ISO 10816-1 Vibration Velocity Classification

The Celery analytics engine converts measured RMS vibration velocity (mm/s) into one of four machine-health zones defined by ISO 10816-1. Zones C and D raise alerts that are persisted and broadcast in real time.

| Zone | RMS Velocity (mm/s) | Status | Action |
| --- | --- | --- | --- |
| **A** | < 1.12 | Good | No alert |
| **B** | 1.12 – 2.80 | Satisfactory | No alert |
| **C** | 2.80 – 7.10 | Unsatisfactory | `WARNING` alert raised |
| **D** | > 7.10 | Unacceptable | `CRITICAL` alert raised |

Alert persistence uses the async session (`AsyncSessionLocal`) and, once committed, broadcasts the serialized `AlertResponse` to the shared `live_telemetry` channel so dashboards receive alerts live alongside telemetry.

## Payload Schema (JSON)

Telemetry (ingested via MQTT or `POST /telemetry`):
```
{
  "device_id": "string",
  "ts": "integer (unix timestamp, optional)",
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
  "severity": "WARNING" | "CRITICAL",
  "metric": "vibration",
  "value": "float (rms velocity, mm/s)",
  "threshold": "float (zone boundary)",
  "message": "string",
  "created_at": "datetime (server-assigned)"
}
```

## Development Rules
*   Use fully asynchronous Python code (`async def`, `aiomqtt`, asyncpg via `async_sessionmaker`); Celery tasks run in a sync context and bridge to async I/O via `run_async(...)` on a single persistent event loop per worker process (`worker_process_init`/`worker_process_shutdown` in `app/analytics.py`) — never `asyncio.run(...)` per task.
*   Keep files modular (`config.py`, `database.py`, `models.py`, `schemas.py`, `crud.py`, `mqtt.py`, `redis.py`, `celery_app.py`, `analytics.py`, `main.py`).
*   Include proper error handling and logging; a bad message or a degraded dependency must never crash the pipeline.
