# Industrial IoT Machine Health Monitoring System

Event-driven telemetry backend for motor vibration and temperature analysis. ESP32 sensor nodes publish accelerometer (MPU6050) and temperature (DS18B20) readings over MQTT; this service validates, persists, and streams them to live dashboards in real time.

## Tech Stack

| Technology | Role |
| --- | --- |
| [FastAPI](https://fastapi.tiangolo.com/) | Async REST API, WebSocket streaming, background MQTT consumer (`aiomqtt`) |
| [Eclipse Mosquitto](https://mosquitto.org/) 2.x | Lightweight MQTT message broker (TCP + WebSocket listeners) |
| PostgreSQL 16 + `asyncpg` + SQLAlchemy 2 (async ORM) | Durable telemetry storage |
| Redis 7 (Pub/Sub) | In-memory fan-out of live telemetry frames |
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
                                                                               ▼
                                                                ┌──────────────────────────┐
                                                     ┌───────── │  WebSocket /ws/telemetry │
                                                     │          └──────────────────────────┘
                                              ┌──────┴───────┐
                                              │  Dashboard   │
                                              │  (Next.js)   │
                                              └──────────────┘
```

**Pipeline:** every message is validated with Pydantic before it touches storage; records that fail validation are logged and dropped without affecting the consumer loop. After a successful insert the serialized record is published once to Redis, and each connected dashboard client receives it verbatim.

### API Surface

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Liveness probe |
| `POST` | `/telemetry` | Manual ingestion (returns `201`) |
| `GET` | `/telemetry/latest` | Last 10 records, newest first |
| `WS` | `/ws/telemetry` | Live stream (snapshot on connect, then live frames) |

### WebSocket Wire Protocol

| Frame | Shape | When |
| --- | --- | --- |
| `snapshot` | `{"type": "snapshot", "data": [<last 10 records, oldest → newest>]}` | Immediately after connect |
| `telemetry` | `{"type": "telemetry", "data": {<record>}}` | On every ingested reading |
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

| Port | Service | Purpose |
| --- | --- | --- |
| **8000** | FastAPI | REST API, WebSocket `/ws/telemetry`, Swagger UI at [`/docs`](http://localhost:8000/docs) |
| **8080** | Adminer | Database UI — System: *PostgreSQL* · Server: `postgres` · User/Password/DB from `.env` |
| **1883** | Mosquitto | MQTT over TCP (device-facing) |
| **9001** | Mosquitto | MQTT over WebSockets (browser-based devices/tools) |
| 5432 | PostgreSQL | Direct access for `psql` or desktop clients |
| 6379 | Redis | Direct access for inspection/debugging |

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
│   ├── models.py        # SQLAlchemy ORM models
│   ├── schemas.py       # Pydantic request/response schemas
│   ├── mqtt.py          # aiomqtt consumer: validate → persist → publish
│   ├── redis.py         # Redis Pub/Sub client & helpers
│   └── main.py          # FastAPI routes, lifespan, WebSocket endpoint
├── docs/
│   ├── architecture.md    # in-depth technical analysis (pipeline, DB, broadcast, faults)
│   ├── context.md         # project scope, pipeline stages & data contracts
│   └── developer_guide.md # beginner-friendly tutorial & troubleshooting
├── mosquitto/
│   └── mosquitto.conf   # broker config (dev: anonymous access enabled)
├── .env.example         # environment template (copy to .env)
├── docker-compose.yml   # full stack definition
├── Dockerfile           # API image (python:3.12-slim + uvicorn)
└── requirements.txt     # pinned Python dependencies
```

For data-flow diagrams, payload contracts, and engineering conventions, see [`docs/context.md`](docs/context.md); the deep technical dive lives in [`docs/architecture.md`](docs/architecture.md), and new contributors should start with [`docs/developer_guide.md`](docs/developer_guide.md).
