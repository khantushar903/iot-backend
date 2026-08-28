# Industrial IoT Machine Health Monitoring System

Event-driven telemetry backend for motor vibration and temperature analysis. ESP32 sensor nodes publish accelerometer (MPU6050) and temperature (DS18B20) readings over MQTT; this service validates, persists, and streams them to live dashboards in real time.

## Tech Stack

| Technology | Role |
| --- | --- |
| [FastAPI](https://fastapi.tiangolo.com/) | Async REST API, WebSocket streaming, background MQTT consumer (`aiomqtt`) |
| [Eclipse Mosquitto](https://mosquitto.org/) 2.x | Lightweight MQTT message broker (TCP + WebSocket listeners) |
| PostgreSQL 16 + `asyncpg` + SQLAlchemy 2 (async ORM) | Durable telemetry + alert storage |
| Redis 7 (Pub/Sub) | In-memory fan-out of live telemetry & alert frames; Celery broker/backend |
| [Celery](https://docs.celeryq.dev/) 5.x | Background worker for FFT / ISO 10816 vibration analysis |
| SciPy + NumPy | Fast Fourier Transform & numerical integration for RMS velocity |
| Docker Compose | One-command orchestration of the full stack |
| Adminer | Browser-based database administration |

## System Architecture

```
 ┌────────────┐  JSON / MQTT   ┌──────────────┐   consume    ┌──────────────┐
 │   ESP32    │ ─────────────▶ │  Mosquitto   │ ──────────▶  │   FastAPI    │
 │ MPU6050 +  │ telemetry/     │    broker    │   aiomqtt    │   (async)    │
 │  DS18B20   │ motors         └──────────────┘              └───┬─────┬───┘
 └────────────┘                                    persist       │     │ publish
                                               (SQLAlchemy +      │     │ channel:
                                                asyncpg)          ▼     ▼ "live_telemetry"
                                                         ┌────────────┐ ┌────────────────┐
                                                         │ PostgreSQL │ │ Redis Pub/Sub  │
                                                         └────────────┘ └───────┬────────┘
                                                                                │ fan-out
                                       ┌──────────────┐        ┌───────────────┐ │
                                       │   Celery     │       │  WebSocket    │ │
                                       │ worker:SciPy │◀───┐  │ /ws/telemetry │◀┘
                                       │ FFT + ISO    │    │  └───────┬───────┘
                                       └──────┬───────┘    │          │
                                              │alerts       │          ▼
                                              ▼             │      ┌──────────────┐
                                       PostgreSQL ◀─────────┘      │  Dashboard   │
                                       (alerts table)              │  (Next.js)   │
                                                                   └──────────────┘
```

**Pipeline (6 stages):** every message is validated with Pydantic before it touches storage; records that fail validation are logged and dropped without affecting the consumer loop. After a successful insert the serialized record is published once to Redis, and each connected dashboard client receives it verbatim. Concurrently, windows of accelerometer samples are queued to a Celery worker that runs an FFT and ISO 10816-1 vibration classification — when a WARNING/CRITICAL threshold is breached, an `Alert` is persisted to PostgreSQL and broadcast to the same `live_telemetry` channel.

### API Surface

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Liveness probe (`{"status":"ok"}`) |
| `POST` | `/telemetry` | Manual ingestion (returns `201`) |
| `GET` | `/telemetry/latest` | Last 10 records, newest first |
| `GET` | `/api/v1/health` | System status: DB, Redis & MQTT component health |
| `GET` | `/api/v1/telemetry/history` | Historical telemetry — optional `device_id`, `limit`, `offset` |
| `GET` | `/api/v1/alerts` | Recent alerts (default `limit` 50, newest first) |
| `POST` | `/api/v1/alerts` | Manually create an alert (returns `201`) |
| `WS` | `/ws/telemetry` | Live stream (snapshot on connect, then live frames) |

### WebSocket Wire Protocol

Four frame types are supported on the `live_telemetry` channel:

| Frame | Shape | When |
| --- | --- | --- |
| `snapshot` | `{"type": "snapshot", "data": [<last 10 records, oldest → newest>]}` | Immediately after connect |
| `telemetry` | `{"type": "telemetry", "data": {<record>}}` | On every ingested reading |
| `alert` | `{"type": "alert", "data": {<alert record>}}` | When the Celery engine raises a WARNING/CRITICAL alert |
| `ping` | `{"type": "ping"}` | Keepalive after ~20 s idle |

Telemetry record shape:

```json
{
  "id": 8,
  "device_id": "motor-01",
  "ts": 1787592826,
  "accel_x": 0.12,
  "accel_y": -0.45,
  "accel_z": 9.81,
  "temp_c": 63.5,
  "created_at": "2026-08-24T17:33:46.733083Z"
}
```

`ts` is an optional client-side Unix timestamp; the server falls back to ingest time.

Alert record shape:

```json
{
  "id": 2,
  "device_id": "motor-03",
  "severity": "CRITICAL",
  "metric": "vibration",
  "value": 8.42,
  "threshold": 7.1,
  "message": "Vibration 8.42 mm/s in ISO 10816 zone D (Unacceptable) — requires attention",
  "created_at": "2026-08-28T09:15:02.510123Z"
}
```

## Quickstart

Prerequisites: Docker Engine (or Docker Desktop) with Compose v2+.

```bash
git clone https://github.com/khantushar903/iot-backend.git
cd iot-backend
cp .env.example .env        # defaults match docker-compose.yml; edit freely
docker compose up -d --build
```

Confirm the stack is up:

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

All credentials, connection URLs, and MQTT settings are controlled through `.env` (git-ignored). `.env.example` documents every variable with working defaults.

## Port Mapping

The stack runs **6 Docker containers**: `iot-api`, `iot-celery-worker`, `iot-postgres`, `iot-redis`, `iot-mosquitto`, and `iot-adminer`.

| Port | Container | Purpose |
| --- | --- | --- |
| **8000** | `iot-api` | REST API, WebSocket `/ws/telemetry`, Swagger UI at [`/docs`](http://localhost:8000/docs) |
| **8080** | `iot-adminer` | Database UI — System: *PostgreSQL* · Server: `postgres` · User/Password/DB from `.env` |
| **1883** | `iot-mosquitto` | MQTT over TCP (device-facing) |
| **9001** | `iot-mosquitto` | MQTT over WebSockets (browser-based devices/tools) |
| 5432 | `iot-postgres` | Direct access for `psql` or desktop clients |
| 6379 | `iot-redis` | Direct access for inspection/debugging (also Celery broker/backend) |
| — (internal) | `iot-celery-worker` | No published port; consumes Celery tasks from Redis, writes alerts to Postgres |

Bold ports are the primary development surfaces.

## Verification & Testing

Publish a simulated device reading and watch it flow end-to-end:

```bash
# 1. Ingest via MQTT (picked up by the background consumer)
docker exec iot-mosquitto mosquitto_pub -t telemetry/motors -m \
  '{"device_id":"motor-01","accel_x":0.12,"accel_y":-0.45,"accel_z":9.81,"temp_c":63.5}'

# 2. Confirm persistence (newest first)
curl http://localhost:8000/telemetry/latest

# 2b. Alternative: direct HTTP ingestion
curl -X POST http://localhost:8000/telemetry \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"motor-02","accel_x":0.10,"accel_y":-0.40,"accel_z":9.79,"temp_c":61.0}' \
  -w '\nHTTP %{http_code}\n'   # expect 201
```

Test the WebSocket stream with Postman:

1. **New → WebSocket Request**, URL `ws://localhost:8000/ws/telemetry`, click **Connect**.
2. A `snapshot` frame arrives immediately.
3. Re-run step 1 above in a terminal — a live `telemetry` frame appears in Postman within milliseconds.
4. Leave the connection idle for ~20 s to observe a `ping` keepalive frame.

```json
// snapshot (on connect)
{"type": "snapshot", "data": [{"id": 7, "...": "..."}, {"id": 8, "...": "..."}]}
// telemetry (live push)
{"type": "telemetry", "data": {"id": 9, "device_id": "motor-01", "accel_x": 0.12}}
// ping (idle keepalive)
{"type": "ping"}
```

Malformed payloads are safe by design — try sending garbage; it is logged and dropped while the pipeline keeps running:

```bash
docker exec iot-mosquitto mosquitto_pub -t telemetry/motors -m 'this-is-not-json'
```

## Directory Overview

```
iot-backend/
├── app/
│   ├── config.py        # pydantic-settings configuration (.env aware)
│   ├── database.py      # async engine, session factory, declarative base
│   ├── models.py        # SQLAlchemy ORM models (Telemetry, Alert)
│   ├── schemas.py       # Pydantic request/response schemas
│   ├── crud.py          # DB helpers: alerts, telemetry history
│   ├── mqtt.py          # aiomqtt consumer: validate → persist → publish
│   ├── redis.py         # Redis Pub/Sub client & helpers
│   ├── celery_app.py    # Celery instance (broker/backend = Redis)
│   ├── analytics.py     # process_vibration_window Celery task (FFT + ISO 10816)
│   └── main.py          # FastAPI routes, lifespan, WebSocket endpoint
├── docs/
│   ├── architecture.md    # in-depth technical analysis (pipeline, DB, broadcast, faults)
│   ├── context.md         # project scope, pipeline stages & data contracts
│   ├── developer_guide.md # beginner-friendly tutorial & troubleshooting
│   └── handover_state.md  # phase-by-phase frozen/verified status tracker
├── mosquitto/
│   └── mosquitto.conf   # broker config (dev: anonymous access enabled)
├── .env.example         # environment template (copy to .env)
├── docker-compose.yml   # full stack definition (6 services)
├── Dockerfile           # shared image (python:3.12-slim) for api & celery_worker
└── requirements.txt     # pinned Python dependencies
```

For data-flow diagrams, payload contracts, and engineering conventions, see [`docs/context.md`](docs/context.md); the deep technical dive lives in [`docs/architecture.md`](docs/architecture.md), and new contributors should start with [`docs/developer_guide.md`](docs/developer_guide.md).
