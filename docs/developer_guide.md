# Developer Guide

A ground-up tutorial for developers new to this stack. No prior MQTT, Redis, or async Python experience assumed. Read Part 1 for the mental model, Part 2 to understand every file, then work through Part 3 hands-on. Part 4 is your first stop when something breaks.

---

## Part 1 — Core Concepts Simplified

### 1.1 Event-Driven Architecture

Traditional web backends are **request-driven**: a client asks, a server answers, and nothing happens in between. This system is instead **event-driven**: things happen *in response to occurrences*, and components never call each other directly.

In this project the central event is simple: *a sensor took a reading*. When that happens:

- the database layer persists it,
- the broadcast layer pushes it to dashboards,
- and neither knows the other exists.

The glue is a **broker**: producers drop messages into topics, consumers subscribe to topics, and neither side waits on the other. Benefits you will see concretely in this codebase:

- A dashboard being slow or offline cannot slow down ingestion.
- New consumers (an alerting service, an ML pipeline) can join by subscribing — zero changes to existing code.
- Every component can fail and restart independently.

### 1.2 MQTT vs HTTP Polling

Imagine 50 dashboards that need live machine temperature. Two designs:

| | HTTP polling | MQTT push |
| --- | --- | --- |
| How data arrives | Each dashboard asks "anything new?" every N seconds | Broker delivers the reading the instant it exists |
| Load at 50 clients × 10 s interval | 300 requests/minute, most answered "no change" | ~0 extra requests; traffic only when data exists |
| Freshness | Up to N seconds stale | Milliseconds |
| Connection overhead per check | Full HTTP request/response cycle | Persistent TCP connection; MQTT headers can be as small as 2 bytes |
| Coupling | Dashboards must know the server's API and availability | Dashboards know only a topic name |

MQTT is a lightweight publish/subscribe protocol built for exactly this: flaky networks, tiny devices, high message rates. The ESP32 publishes JSON to the topic `telemetry/motors`; our backend subscribes once and receives everything. Devices never need to know how many consumers exist.

### 1.3 Async Python and the Event Loop

Python's `asyncio` runs many tasks on **one thread** by juggling them whenever they wait:

```python
record = await session.commit()   # waiting on Postgres? yield control...
frame  = await pubsub.get_message(...)   # ...and handle other clients meanwhile
```

The word after `await` marks the only places a task pauses. While one task waits on the network or disk, the event loop runs another. For I/O-heavy work like ours (almost entirely waiting on brokers and databases), a single loop handles thousands of concurrent connections — no thread-per-request costs, no lock-based headaches.

Where it appears in this project:

- **FastAPI endpoints** are `async def` — concurrent REST/WebSocket clients share one loop.
- The MQTT consumer runs as a background **task** on the same loop (see `asyncio.create_task` in Part 2).
- Everything is async end-to-end (`aiomqtt`, asyncpg via SQLAlchemy, `redis.asyncio`) because one synchronous link would block the whole loop.

Rule of thumb: if a function spends its life waiting, make it async. If it burns CPU (heavy math), offload it — don't block the loop.

### 1.4 Why Redis Sits Between MQTT and WebSockets

Could `/ws/telemetry` just receive frames straight from the MQTT consumer function call? At small scale, yes. The indirection buys three real properties:

1. **Horizontal scaling of dashboards.** WebSocket connections are state pinned to one process. With Redis Pub/Sub as the backbone, every API replica subscribes to `live_telemetry` and fans out to whichever clients it happens to hold. Ingestion publishes once; N replicas deliver everywhere.
2. **Failure isolation.** Publishing to Redis is fire-and-forget: if broadcasting hiccups, ingestion still commits to Postgres (see the guarded `publish_live_telemetry`). The durable record is always the source of truth.
3. **Fan-out cost moves off the hot path.** The consumer does one cheap publish instead of tracking client lists; each WebSocket handler pulls from the channel at its own pace.

Postgres = memory. Redis Pub/Sub = nervous system. WebSockets = nerve endings.

---

## Part 2 — Code Walkthrough

Files in dependency order. Read alongside the actual source — every snippet below is lifted from it.

### 2.1 `app/config.py` — configuration

```python
class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://iot_user:iot_password@postgres:5432/iot_db"
    redis_url: str = "redis://redis:6379/0"
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_topic: str = "telemetry/motors"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
```

Plain English: pydantic-settings fills each field from environment variables (case-insensitive: `MQTT_HOST` → `mqtt_host`), falling back to `.env`, falling back to these defaults. One import anywhere gives you typed config: `from app.config import settings`. `extra="ignore"` lets `.env` carry extra keys (like `POSTGRES_USER`, which Docker Compose consumes) without crashing the app.

### 2.2 `app/database.py` — engine and sessions

```python
from sqlalchemy.pool import NullPool

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    poolclass=NullPool,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
```

Three ideas:

- **Engine** — a factory for database connections created once at import time. `pool_pre_ping=True` sends a lightweight check before reusing a pooled connection, so a Postgres restart doesn't surface as random dead-connection errors. `poolclass=NullPool` goes further: every checkout opens a fresh connection and closes it on return, so **no connection is ever cached across event loops**. This is what lets the same shared engine be used from Celery workers without tripping `RuntimeError: Event loop is closed` / `Future attached to a different loop`.
- **Session factory** — calling `AsyncSessionLocal()` produces a fresh session (a single logical database conversation). Code always uses `async with AsyncSessionLocal() as session:` so the session closes deterministically even on exceptions.
- **`expire_on_commit=False`** — normally, SQLAlchemy "expires" every object after commit, so touching any attribute triggers a surprise lazy-load query. That pattern fights async code (implicit I/O where you least expect it). Disabling expiration keeps objects fully readable after commit. Corollary: server-generated values (like `created_at`) do *not* appear automatically — which is why callers run `await session.refresh(record)` explicitly when they need them.

### 2.3 `app/models.py` — the ORM table

```python
class Telemetry(Base):
    __tablename__ = "telemetry_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(String(64), index=True)
    ts: Mapped[int | None] = mapped_column(BigInteger, default=lambda: int(time.time()))
    accel_x: Mapped[float]
    ...
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
```

- Modern SQLAlchemy 2.0 style: `Mapped[type]` declares both the Python type and the column type.
- `id` auto-increments in insert order. Since exactly one consumer task inserts serially, **id order == arrival order** — that is why "latest records" sorts by `id DESC`, not by `ts` (device clocks drift; `ts` is optional).
- `device_id` is indexed for per-machine queries.
- Two timestamps, two jobs: `ts` is what the device *claims* (optional, may be null), `created_at` is what the server *knows* (set by Postgres itself).

### 2.4 `app/schemas.py` — validation contracts

```python
class TelemetryCreate(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    ts: int | None = None
    accel_x: float
    ...

class TelemetryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    ...
```

Two Pydantic models, two directions: `TelemetryCreate` validates *incoming* payloads strictly (reject bad data before it touches anything). `TelemetryResponse` describes *outgoing* rows; `from_attributes=True` lets it build directly from ORM objects via `TelemetryResponse.model_validate(record)`. Keeping the pair separate means the API can expose fields (like `created_at`) that clients never send.

### 2.5 `app/mqtt.py` — the ingestion consumer

```python
async def mqtt_consumer() -> None:
    backoff = _INITIAL_BACKOFF_S            # 1s
    while True:
        try:
            async with aiomqtt.Client(hostname=..., port=...) as client:
                await client.subscribe(TELEMETRY_TOPIC)
                backoff = _INITIAL_BACKOFF_S          # reset on success
                async for message in client.messages: # sleeps until a message arrives
                    await _process_message(message)
        except asyncio.CancelledError:
            logger.info("MQTT consumer shutting down")
            raise
        except aiomqtt.MqttError as exc:
            logger.warning("MQTT error: %s — reconnecting in %.1fs", exc, backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _MAX_BACKOFF_S)    # 1→2→4→…→30s cap
```

This task lives forever: connect, consume, and on broker loss retry with doubling backoff (capped at 30 s, reset to 1 s after a healthy connect). `async for message in client.messages` is aiomqtt's trick of turning callbacks into an async iterator — the task simply naps until the broker has something.

Each message flows through `_process_message`, a gauntlet of early returns:

```python
data = json.loads(message.payload)      # gate 1: parseable JSON?
telemetry = TelemetryCreate(**data)     # gate 2: matches contract?
async with AsyncSessionLocal() as session:   # own session per message
    session.add(record); await session.commit()
    await session.refresh(record)       # pull id + created_at back
await publish_live_telemetry(payload)   # only AFTER a successful commit
await _buffer_vibration_window(device_id, x, y, z)   # feed the analytics sliding window
```

Fail any gate → log a warning and return. A poison pill costs one log line, never the loop.

**Sliding-window buffering.** `_buffer_vibration_window` computes the raw magnitude `sqrt(x²+y²+z²)`, `RPUSH`es it to the Redis list `vibration_buffer:{device_id}`, then `LTRIM`s the list to its last 30 entries. Only when the list holds ≥ 10 readings does it read the list back and dispatch `process_vibration_window.delay(device_id, samples, sample_rate_hz=1)` to the Celery queue. The window slides continuously: every new reading displaces the oldest, and a new analysis fires roughly every 10 readings per device. Any buffer failure is logged (`Vibration buffer update failed for device ...`) and swallowed — it never affects the telemetry record already committed or broadcast.

### 2.6 `app/redis.py` — the broadcast backbone

```python
LIVE_TELEMETRY_CHANNEL = "live_telemetry"
redis_client = redis.from_url(settings.redis_url, decode_responses=True)

async def publish_live_telemetry(payload: str) -> None:
    try:
        await redis_client.publish(LIVE_TELEMETRY_CHANNEL, payload)
    except Exception:
        logger.exception("Redis publish to %s failed", LIVE_TELEMETRY_CHANNEL)
```

`decode_responses=True` makes frames arrive as plain strings — ready to forward to WebSockets untouched. Note the try/except *inside* the helper: streaming failures are logged and swallowed so a Redis outage degrades gracefully instead of killing ingestion. `get_live_telemetry_pubsub()` returns a **fresh Pub/Sub object per caller** — each WebSocket gets its own dedicated subscription connection; sharing one across sockets would interleave other clients' frames.

### 2.7 `app/main.py` — wiring, routes, WebSockets

The lifespan manages everything that must outlive a single request:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)   # create missing tables
    mqtt_task = asyncio.create_task(mqtt_consumer(), name="mqtt-consumer")
    yield                                                # app serves requests here
    mqtt_task.cancel()
    with suppress(asyncio.CancelledError):
        await mqtt_task                                  # wait for clean exit
    await redis_client.aclose()
    await engine.dispose()
```

Key patterns:

- **`asyncio.create_task(...)`** schedules `mqtt_consumer` onto the running loop *without blocking startup* — uvicorn begins serving while the consumer connects in parallel. The handle is kept so shutdown can cancel it.
- **Shutdown ordering matters**: cancel ingestion *first* (stop new work), *then* close Redis, *then* dispose DB connections last. Reverse the order and you risk handlers grabbing connections already closed.
- `run_sync(Base.metadata.create_all)` bridges sync SQLAlchemy DDL into async land; it creates tables that don't exist yet (but never alters existing ones — see Part 3!).

REST routes use dependency injection — `db: AsyncSession = Depends(get_db)` — so each request gets a session that closes automatically afterward.

The WebSocket endpoint follows a fixed lifecycle: `accept()` → subscribe to the channel → send a snapshot (last 10 rows reversed into chronological order) → relay loop:

```python
message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=_WS_IDLE_PING_S)
if message is None or message["type"] != "message":
    await websocket.send_text('{"type":"ping"}')   # idle keepalive / liveness probe
else:
    await websocket.send_text(message["data"])     # forwarded verbatim
```

Writing the ping doubles as a liveness probe — dead sockets raise on write, converting silent disconnects into handled errors. A `finally` block unsubscribes and closes the Pub/Sub on every exit path, so churn can't leak connections.

### 2.8 `app/crud.py` — thin DB-access helpers

A small data-access layer that keeps query logic out of the route handlers and centralizes reuse. Every function takes an `AsyncSession` and returns plain ORM objects (`from_attributes` schemas serialize them later):

```python
async def create_alert(db, alert_in: AlertCreate) -> Alert:
    alert = Alert(**alert_in.model_dump())     # build ORM row from validated input
    db.add(alert)
    await db.commit()                          # persist + release (expire_on_commit=False)
    await db.refresh(alert)                    # pull server-generated id / created_at
    return alert

async def get_alerts(db, limit=50) -> list[Alert]:
    # newest first, capped
    return (await db.execute(select(Alert).order_by(Alert.id.desc()).limit(limit))).scalars().all()

async def get_telemetry_history(db, device_id=None, limit=100, offset=0):
    stmt = select(Telemetry).order_by(Telemetry.id.desc()).limit(limit)
    if device_id is not None:
        stmt = stmt.where(Telemetry.device_id == device_id)   # optional per-device filter
    if offset:
        stmt = stmt.offset(offset)                             # cursor-style pagination
    return (await db.execute(stmt)).scalars().all()
```

### 2.9 `app/celery_app.py` — the task queue instance

```python
celery_app = Celery(
    "iot_analytics",
    broker=settings.redis_url,     # Redis is the task queue
    backend=settings.redis_url,    # and the result store
)
celery_app.conf.update(task_serializer="json", result_serializer="json",
                       accept_content=["json"], timezone="UTC", enable_utc=True)
```

`settings.redis_url` is read via pydantic-settings from `.env`, so the worker and API agree on the broker. The worker container runs `celery -A app.analytics.celery_app worker --loglevel=info` — note the app module is `analytics.py`, because that is where the actual task lives.

### 2.10 `app/analytics.py` — the FFT / ISO 10816 worker

`process_vibration_window` is a `@celery_app.task` decorated with `bind=True` (so `self` gives task controls). It is a normal sync Python function — worker threads run it — and it deliberately does the pure-math first, then bridges into async I/O only for the alert side-effect:

```python
velocity = cumulative_trapezoid(arr - arr.mean(), dx=dt, initial=0.0)   # accel → velocity
rms_velocity = float(np.sqrt(np.mean(velocity**2)) * 1000.0)            # → mm/s RMS
freqs  = fft.rfftfreq(len(arr), d=dt)                                    # FFT bins
spectrum = np.abs(fft.rfft(arr - arr.mean()))
peak_freq = float(freqs[int(np.argmax(spectrum))])                       # dominant Hz

for lower, upper, code, name in ISO_10816_ZONES:   # A/B/C/D lookup
    if (lower is None or rms_velocity >= lower) and (upper is None or rms_velocity < upper):
        zone, threshold = code, upper; break
```

When the zone is `C` or `D`, the task builds an alert and calls the async bridge `run_async(_persist_and_broadcast(alert_data))`. Inside that coroutine a dedicated `AsyncSessionLocal` inserts the `Alert`, then the committed record is serialized with `AlertResponse` and published as `{"type":"alert","data":{...}}` to `live_telemetry`. Because this is a raw `redis.asyncio` client, it must run inside an event loop.

**Why `run_async`, not `asyncio.run`.** Calling `asyncio.run(...)` per task would create and destroy an event loop on every dispatch. The module-level `AsyncSessionLocal` engine and `redis_client` would then try to reuse connections bound to a *closed* loop — the classic `RuntimeError: Event loop is closed` / `Future attached to a different loop`. Instead, `app/analytics.py` keeps one loop alive per worker process:

```python
_worker_loop: asyncio.AbstractEventLoop | None = None

@celery_app.signals.worker_process_init.connect
def _init_worker_loop(**kwargs):
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        _worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_worker_loop)

def run_async(coro):
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        _worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_worker_loop)
    _worker_loop.run_until_complete(coro)

@celery_app.signals.worker_process_shutdown.connect
def _shutdown_worker_loop(**kwargs):
    if _worker_loop is None or _worker_loop.is_closed():
        return
    _worker_loop.run_until_complete(engine.dispose())
    _worker_loop.run_until_complete(redis_client.aclose())
    _worker_loop.close()
```

Every subsequent task dispatch reuses that same loop, so the pooled DB/Redis connections stay bound to a loop that is still running, and the `worker_process_shutdown` listener disposes everything cleanly before the fork exits. `NullPool` on the engine (Part 2.2) is a second, independent safety net.

---

## Part 3 — Hands-On Tutorial: Add a New Metric

Goal: add a vibration RMS metric (`vibration_rms`) flowing through every layer: schema → model → database → MQTT ingestion → Adminer → WebSocket. Budget ~15 minutes.

> **Layer map** — a metric must exist in: `app/schemas.py` (validation), `app/models.py` (storage), the database table itself, and any test payload you publish.

### Step 1 — Accept the field in `app/schemas.py`

Add one line to `TelemetryCreate` and one to `TelemetryResponse`:

```python
class TelemetryCreate(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    ts: int | None = None
    accel_x: float
    accel_y: float
    accel_z: float
    temp_c: float
    vibration_rms: float          # NEW
```

```python
class TelemetryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    device_id: str
    ts: int | None
    accel_x: float
    accel_y: float
    accel_z: float
    temp_c: float
    vibration_rms: float          # NEW
    created_at: datetime
```

### Step 2 — Persist it in `app/models.py`

```python
    accel_x: Mapped[float]
    accel_y: Mapped[float]
    accel_z: Mapped[float]
    temp_c: Mapped[float]
    vibration_rms: Mapped[float] = mapped_column(default=0.0)   # NEW
```

(The `default=0.0` covers inserts that omit the field, e.g. direct ORM writes.)

### Step 3 — Restart and hit a classic gotcha

```bash
docker compose up -d --build api
```

Publish a test payload including the new field:

```bash
docker exec iot-mosquitto mosquitto_pub -t telemetry/motors -m \
  '{"device_id":"motor-01","accel_x":0.12,"accel_y":-0.45,"accel_z":9.81,"temp_c":63.5,"vibration_rms":0.87}'
```

Now watch the logs:

```bash
docker compose logs api --tail 20
# WARNING ... Database write failed for device motor-01
```

**What happened?** `create_all` only creates *missing tables* — it never alters existing ones. Your Python model knows about `vibration_rms`, but the physical table does not yet have that column. This exact failure (`UndefinedColumnError` inside "Database write failed") is why migrations tools like Alembic exist. For local dev, add the column manually:

```bash
docker compose exec postgres psql -U iot_user -d iot_db -c \
  "ALTER TABLE telemetry_records ADD COLUMN vibration_rms REAL NOT NULL DEFAULT 0;"
```

(Prefer keeping your test data? This ALTER preserves it — old rows get `0`. The nuclear alternative `docker compose down -v && docker compose up -d --build` recreates the table from scratch and **wipes all data**.)

### Step 4 — Verify persistence

Republish the payload from Step 3. No warnings should appear. Then confirm in three places:

**Adminer** — browse to http://localhost:8080 · System: *PostgreSQL* · Server: `postgres` · User: `iot_user` · Password: `iot_password` · Database: `iot_db`. Open `telemetry_records`; the new column shows with your value.

**REST** —

```bash
curl http://localhost:8000/telemetry/latest
# {"id": ..., "device_id": "motor-01", ..., "vibration_rms": 0.87, "created_at": "..."}
```

**WebSocket** — Postman → *New → WebSocket Request* → `ws://localhost:8000/ws/telemetry` → **Connect**. You'll receive a `snapshot` frame containing historical rows (with `vibration_rms: 0` from the DEFAULT). Re-publish the payload — a live `telemetry` frame arrives carrying `"vibration_rms": 0.87`.

### Step 5 — Update your sample-payload tests

Any payload used for testing must now include the field. Update your canonical test command (README examples, ESP32 firmware sketches, Postman saved requests):

```bash
docker exec iot-mosquitto mosquitto_pub -t telemetry/motors -m \
  '{"device_id":"motor-02","accel_x":0.10,"accel_y":-0.40,"accel_z":9.79,"temp_c":61.0,"vibration_rms":1.12}'
```

Sanity-check the contract without a broker — validation alone, right in your shell:

```bash
docker compose exec api python -c "
from app.schemas import TelemetryCreate
print(TelemetryCreate.model_validate({'device_id':'x','accel_x':1,'accel_y':1,'accel_z':1,'temp_c':1,'vibration_rms':0.5}))
"
```

Omitting `vibration_rms` from a payload now fails schema validation (logged and dropped) — expected, since we declared it required.

> **Production note:** when real devices still run old firmware, declare the field optional during rollout — `vibration_rms: float | None = None` in both schemas plus a nullable column — then tighten to required once every device ships the metric.

### Step 6 — Close the loop

- [ ] Both schemas updated
- [ ] Model updated
- [ ] Column added (ALTER or volume reset)
- [ ] Sample payloads updated everywhere they live
- [ ] Verified in Adminer + REST + WebSocket
- [ ] `docs/context.md` payload contract updated

## Part 3½ — Hands-On: Alerts & the Celery Analytics Worker

Two ways to exercise the Phase 2 alert path without the real signal chain.

### Option A — Manual alert via the REST API

`POST /api/v1/alerts` inserts an `Alert` row directly and returns `201`. It does **not** re-run the FFT (that is the analytics engine's job), but it is the quickest way to populate history and see the response shape:

```bash
curl -X POST http://localhost:8000/api/v1/alerts \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "motor-04",
    "severity": "CRITICAL",
    "metric": "vibration",
    "value": 9.2,
    "threshold": 7.1,
    "message": "Vibration 9.20 mm/s in ISO 10816 zone D (Unacceptable)"
  }' -w '\nHTTP %{http_code}\n'   # expect 201 with the created alert

# Read it back (newest first)
curl "http://localhost:8000/api/v1/alerts?limit=5"
```

### Option B — Watch the Celery worker process a real window

First make sure the worker is up:

```bash
docker compose ps                        # iot-celery-worker should be running
docker compose logs -f celery_worker     # follow the analytics task output
```

Then, from `docker compose exec api` (or anywhere the `app` package is importable), dispatch a synthetic high-vibration window straight onto the task queue:

```bash
docker compose exec api python -c "
import random
from app.analytics import process_vibration_window
samples = [9.8 + random.gauss(0, 1.5) for _ in range(256)]
print(process_vibration_window.signature(
    args=('motor-99', samples), kwargs={'sample_rate_hz': 50}
).delay().get(timeout=30))
"
```

A window this energetic lands in Zone D, so the worker persists an `Alert`, broadcasts `{"type":"alert",...}` to `live_telemetry`, and the returned dict shows `severity: "CRITICAL"` / `alert_raised: True`. Watch `docker compose logs -f celery_worker` to see the `Alert raised for device ...` line, then confirm the row at `GET /api/v1/alerts` or in Adminer (`alerts` table).

---

## Part 4 — Troubleshooting & Debugging Guide

### 4.1 Reading container logs

Logs are the fastest signal. Follow one service live:

```bash
docker compose logs -f api             # follow the API (ingestion, WS, errors)
docker compose logs -f celery_worker   # follow the analytics worker (FFT/alerts)
docker compose logs -f postgres
docker compose logs --tail 100 api     # last 100 lines, no following
```

Known log lines and what they mean:

| Log line | Meaning | Action |
| --- | --- | --- |
| `Dropping malformed JSON payload` | Non-JSON arrived on the topic | Check publisher firmware |
| `Dropping payload failing schema validation` | Valid JSON, wrong shape | Compare against `TelemetryCreate` |
| `Database write failed for device X` | Insert rejected (missing column? DB down?) | See 4.2 |
| `Vibration buffer update failed for device X` | Redis list push/trim for the sliding window errored | Check redis container; the telemetry record itself is already stored |
| `Redis publish to live_telemetry failed` | Streaming degraded; storage unaffected | Check redis container |
| `MQTT error: ... reconnecting in Ns` | Broker blip; backoff in progress | Usually self-heals |
| `Alert raised for device X: CRITICAL (...) mm/s` | Analytics engine persisted + broadcast an alert | Expected WARNING/CRITICAL output — inspect `GET /api/v1/alerts` |
| `Dashboard client disconnected` | Normal WS close | Nothing |

### 4.2 Database connection problems

Symptom: timeouts, `ConnectionRefusedError`, or every write failing. Work down this list:

1. **Is Postgres healthy?**
   ```bash
   docker compose ps                    # want "(healthy)" for iot-postgres
   docker compose exec postgres pg_isready -U iot_user -d iot_db
   ```
   If it's restarting/crash-looping: `docker compose logs postgres`.

2. **Wrong host.** Inside the Docker network the database is `postgres`; from your host machine it is `localhost:5432`. A `DATABASE_URL` pointing at `postgres` while running uvicorn outside Docker will hang on DNS resolution — the classic "connection timeout". Conversely, `localhost` inside a container points at the container itself.

3. **Credentials changed but old volume persists.** PostgreSQL applies `POSTGRES_USER/PASSWORD/DB` only on *first ever* start; afterwards they live inside the volume. Editing `.env` credentials later changes nothing until you reset volumes (4.3) — symptoms are auth failures, not timeouts.

4. **Stale connections.** Already handled: `pool_pre_ping=True` evicts connections killed by a Postgres restart. If you still see one-off dead-connection errors right after restarting postgres, they self-resolve on the next request.

### 4.3 Resetting the environment

```bash
docker compose down          # stop containers; volumes/data survive
docker compose down -v       # ALSO DELETE named volumes — full data wipe
docker compose up -d --build # fresh stack
```

Named volumes: `postgres_data`, `redis_data`, `mosquitto_data`.

Use `-v` when: switching credential sets, testing `create_all` schema changes from scratch, or un-corrupting a weird state. It destroys **all stored telemetry** — think twice before running against anything you care about.

### 4.4 Testing broken payloads on purpose

Prove the pipeline's immunity by feeding it garbage:

```bash
# Not JSON at all
docker exec iot-mosquitto mosquitto_pub -t telemetry/motors -m 'this-is-not-json'

# JSON but missing required fields
docker exec iot-mosquitto mosquitto_pub -t telemetry/motors -m '{"device_id":"x"}'
```

Expected: two distinct warnings in `docker compose logs api`, then a valid payload sails through:

```bash
docker exec iot-mosquitto mosquitto_pub -t telemetry/motors -m \
  '{"device_id":"after-poison","accel_x":0.1,"accel_y":0.2,"accel_z":9.8,"temp_c":22}'
curl http://localhost:8000/telemetry/latest    # newest row is "after-poison"
```

The REST route rejects bad input independently (HTTP 422):

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/telemetry \
  -H 'Content-Type: application/json' -d '{"nope": true}'    # 422
```

Watch the raw broadcast stream while testing (Ctrl+C to exit):

```bash
docker compose exec redis redis-cli SUBSCRIBE live_telemetry
```

### 4.5 Quick reference card

| Container | Ports | Role |
| --- | --- | --- |
| `iot-api` | 8000 | FastAPI: REST + `/ws/telemetry` + Swagger at `/docs` |
| `iot-celery-worker` | (none) | Celery worker: FFT + ISO 10816 analysis, alert persistence |
| `iot-postgres` | 5432 | Storage: `telemetry_records` + `alerts` (`iot_user` / `iot_password` / `iot_db`) |
| `iot-redis` | 6379 | Pub/Sub backbone (`live_telemetry`) + Celery broker/backend |
| `iot-mosquitto` | 1883 / 9001 | MQTT broker (TCP / WS) |
| `iot-adminer` | 8080 | Browser DB UI |

```bash
docker compose exec postgres psql -U iot_user -d iot_db                 # SQL shell
docker compose exec postgres psql -U iot_user -d iot_db -c '\d telemetry_records'
docker compose exec postgres psql -U iot_user -d iot_db -c '\d alerts'
docker compose exec redis redis-cli MONITOR                             # raw Redis traffic
docker compose restart mosquitto                                        # rehearse broker failure
docker compose logs -f celery_worker                                    # FFT/alert activity
```
