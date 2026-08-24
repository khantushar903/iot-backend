# Project Context: Industrial IoT Machine Monitoring System (Thesis Project)

## Overview
A scalable, event-driven IoT backend that monitors motor health using vibration (MPU6050) and temperature (DS18B20) data. The system ingests high-frequency MQTT telemetry, persists it durably, and streams it live to a frontend. **Status:** the complete backend pipeline described below is implemented, containerized, and verified end-to-end.

## Tech Stack
*   **Backend Framework:** FastAPI (Python 3.12, fully async)
*   **Message Broker:** Eclipse Mosquitto 2.x (MQTT over TCP :1883 / WebSocket :9001)
*   **Database:** PostgreSQL 16 (SQLAlchemy 2.0 async ORM + asyncpg)
*   **In-Memory Store / Pub/Sub:** Redis 7
*   **Deployment:** Docker Compose (health-gated service startup) + Adminer for DB administration

## Backend Pipeline

```
Infrastructure → Schemas → Async Ingestion → DB Persistence → Redis Pub/Sub → WebSocket Fanout
```

| Stage | Implementation | Responsibility |
| --- | --- | --- |
| 1. Infrastructure | `docker-compose.yml`, `mosquitto/mosquitto.conf`, `Dockerfile` | Mosquitto, PostgreSQL, Redis, API; healthchecks gate startup order |
| 2. Schemas | `app/config.py`, `app/schemas.py` | pydantic-settings configuration; Pydantic v2 request/response contracts |
| 3. Async Ingestion | `app/mqtt.py` | Long-lived `aiomqtt` consumer on `telemetry/motors`; validate-then-persist per message |
| 4. DB Persistence | `app/database.py`, `app/models.py` | Async engine/session factory; `telemetry_records` table with indexed `device_id` |
| 5. Redis Pub/Sub | `app/redis.py` | Fan-out of persisted records on channel `live_telemetry` |
| 6. WebSocket Fanout | `app/main.py` (`/ws/telemetry`) | Snapshot-on-connect + verbatim relay of live frames + idle keepalive |

## Data Flow (as implemented)
1. ESP32 nodes publish JSON readings to Mosquitto on topic `telemetry/motors`.
2. A background `aiomqtt` task inside FastAPI consumes messages (auto-reconnect with capped exponential backoff).
3. Each payload is parsed and validated by Pydantic (`TelemetryCreate`); failures are logged and dropped without disturbing the consumer loop.
4. Valid readings are inserted into PostgreSQL through a dedicated `AsyncSession` per message; the row is refreshed so server-generated fields (`id`, `created_at`) are populated.
5. The serialized record is published once to the Redis channel `live_telemetry`; streaming failures never block ingestion.
6. Dashboard clients connect to `/ws/telemetry`, receive a snapshot of the last 10 records, then receive every subsequent reading in real time.

## Payload Schema (JSON)
{
  "device_id": "string",
  "ts": "integer (unix timestamp, optional)",
  "accel_x": "float",
  "accel_y": "float",
  "accel_z": "float",
  "temp_c": "float"
}

## Development Rules
*   Use fully asynchronous Python code (`async def`, `aiomqtt`, asyncpg via `async_sessionmaker`).
*   Keep files modular (`config.py`, `database.py`, `models.py`, `schemas.py`, `mqtt.py`, `redis.py`, `main.py`).
*   Include proper error handling and logging; a bad message or a degraded dependency must never crash the pipeline.
