# Architecture: In-Depth Technical Analysis

Technical reference for the telemetry backend. For project scope and data contracts, see [`context.md`](context.md).

## System Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor ESP as ESP32 device
    participant MQ as Mosquitto broker
    participant ING as Ingestion (app/mqtt.py)
    participant VAL as Pydantic validation
    participant PG as PostgreSQL
    participant RD as Redis Pub/Sub
    participant CEL as Celery worker (app/analytics.py)
    actor DASH as Dashboard client

    ESP->>MQ: PUBLISH telemetry/motors (JSON)
    MQ-->>ING: message delivered
    ING->>VAL: json.loads + TelemetryCreate.model_validate()

    alt malformed or schema-invalid payload
        VAL--xING: raises
        Note over ING: warning logged, payload dropped<br/>(poison pill never reaches storage)
    else valid reading
        ING->>PG: INSERT telemetry_records<br/>(dedicated AsyncSession, commit + refresh)
        PG-->>ING: id, created_at materialized
        ING->>RD: PUBLISH live_telemetry<br/>{"type":"telemetry","data":{...}}
        RD-->>DASH: frame fanned out to every subscriber

        opt per valid reading
            ING->>RD: RPUSH vibration_buffer:<device_id><br/>+ LTRIM last 30 magnitudes
            opt buffer length ≥ 10
                ING-->>CEL: process_vibration_window.delay(device_id, samples, 1 Hz)
                CEL->>CEL: FFT + integrate → RMS velocity (mm/s)<br/>ISO 10816 zone A–D
                alt Zone C or D (WARNING / CRITICAL)
                    CEL->>PG: INSERT alerts<br/>(run_async on persistent worker loop)
                    CEL->>RD: PUBLISH live_telemetry<br/>{"type":"alert","data":{...}}
                    RD-->>DASH: alert frame fanned out
                end
            end
        end
    end
```

## 1. Ingestion Pipeline (`app/mqtt.py`)

### Connection handling
The consumer is a single `asyncio` task created in FastAPI's `lifespan`, so its lifetime exactly matches the application's. It runs an outer `while True` loop that constructs a fresh `aiomqtt.Client` per connection attempt and iterates `client.messages` — aiomqtt converts the paho-mqtt callback API into a native async iterator. Inside a healthy connection there are no reconnects and no polling; messages arrive as they are brokered.

### Reconnect with capped exponential backoff
Any `aiomqtt.MqttError` (broker restart, network blip) bubbles out of the iterator and is caught by the outer loop:

```python
delay = min(delay * 2, MAX_RECONNECT_DELAY_S)   # 1s → 2 → 4 → ... → 30s cap
logger.warning("MQTT connection lost (%s); retrying in %ss", error, delay)
await asyncio.sleep(delay)
```

- Backoff starts at **1 s** and doubles to a **30 s ceiling**, preventing hammering a downed broker.
- On a successful connect the delay **resets** to 1 s, so recovery after brief restarts stays snappy.
- The task is cancelled during shutdown; `asyncio.CancelledError` propagates through the sleep/iterator and is suppressed by the lifespan, guaranteeing clean teardown.

### Per-message processing and validation
Each incoming MQTT payload passes two independent gates:

1. **JSON parse** — `json.loads(payload)` failure logs `Dropping malformed JSON payload` and returns.
2. **Schema validation** — `TelemetryCreate.model_validate(...)` enforces the contract: `device_id` required string of 1–64 chars, numeric accel axes, optional integer `ts`. Failures log `Dropping payload failing schema validation`.

Both gates *return early* rather than raise past the loop — a poison pill costs one warning line and nothing else. Ordering is preserved because messages are processed serially in arrival order on the single consumer task; combined with the auto-increment primary key this yields a server-authoritative ingestion sequence.

Delivery semantics are QoS 0 (fire-and-forget): for high-frequency vibration monitoring an occasional dropped reading is preferable to broker-side queuing lag. The pipeline is therefore at-most-once per attempt, with no duplicate-suppression complexity.

### Sliding-window vibration buffering
After a record is persisted and broadcast, the consumer feeds the analytics engine through a Redis-backed sliding window (see `_buffer_vibration_window` in `app/mqtt.py`):

```python
mag = sqrt(accel_x**2 + accel_y**2 + accel_z**2)   # raw acceleration magnitude
await redis_client.rpush(key, mag)                 # key: vibration_buffer:<device_id>
await redis_client.ltrim(key, -30, -1)             # retain only the last 30 readings
```

- Each valid reading **RPUSH**es its magnitude to `vibration_buffer:{device_id}` and then **LTRIM**s the list to its last **30** entries, so a device's buffer never grows unbounded.
- Only when `LLEN >= 10` does the consumer read the list back, convert entries to `float`, and dispatch **`process_vibration_window.delay(device_id, samples, sample_rate_hz=1)`** to the Celery queue (`BUFFER_DISPATCH_THRESHOLD = 10`, `BUFFER_SAMPLE_RATE_HZ = 1`).
- A buffer update failure is caught and logged (`Vibration buffer update failed for device ...`) — it never aborts ingestion or affects the already-committed telemetry record/broadcast.

## 2. Database Design (`app/database.py`, `app/models.py`)

### SQLAlchemy 2.0 async patterns
- `DeclarativeBase` with `Mapped[...]` / `mapped_column` typed declarations — full IDE/type-checker support.
- Engine created once with `pool_pre_ping=True` **and** `poolclass=NullPool`: stale connections killed by Postgres restarts are detected and replaced transparently, and no connection is cached across event loops. `NullPool` (one connection per checkout, closed on return) is the key to keeping the shared async engine safe when async I/O runs inside Celery workers — pooled connections bound to a closed event loop are never handed back out.
- Sessions come from `async_sessionmaker(engine, expire_on_commit=False)`: committed objects stay usable without implicit lazy-load round trips.

### Table `telemetry_records`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `BigInteger` PK | Auto-increment; authoritative ordering key |
| `device_id` | `String(64)` | Indexed — dashboard per-device filtering |
| `ts` | `BigInteger`, nullable | Client-side Unix epoch seconds |
| `accel_x/y/z` | `Float` | Vibration axes |
| `temp_c` | `Float` | Temperature |
| `created_at` | `DateTime(timezone=True)` | `server_default=func.now()` |

### Table `alerts`

Produced by the Celery analytics engine (and writable via `POST /api/v1/alerts`); consumed by the history/alert REST endpoints.

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `BigInteger` PK | Auto-increment |
| `device_id` | `String(64)` | Indexed — per-device alert filtering |
| `severity` | `String(32)` | `WARNING` or `CRITICAL` |
| `metric` | `String(32)` | `vibration` (currently the only metric) |
| `value` | `Float` | Observed RMS velocity (mm/s) at alert time |
| `threshold` | `Float` | Zone boundary that was breached |
| `message` | `String(255)` | Human-readable alert text |
| `created_at` | `DateTime(timezone=True)` | `server_default=func.now()` |

### Why auto-increment `id` orders records (not `ts`)
`ts` is client-supplied and optional: ESP32 clocks drift, and nodes that omit it get a server fallback at insert time. Using it as a global sort key would interleave records incorrectly across devices. Instead:

- **Ingestion order == commit order == `id` order**, because the single consumer task commits serially.
- `/telemetry/latest` and the WebSocket snapshot both use `ORDER BY id DESC LIMIT n`; the snapshot reverses the result set to present chronological (oldest→newest) frames.
- `id` also serves as a natural cursor for future pagination.

### Time-series field handling
- `ts` is stored as raw epoch seconds (`BigInteger`) — compact, timezone-free, and directly comparable if the sensor provides it; `NULL` means "device had no clock".
- `created_at` is assigned by Postgres (`now()`), making ingest time trustworthy regardless of device state. Because it is server-generated, the consumer calls `await session.refresh(record)` immediately after commit — with `expire_on_commit=False` the object would otherwise carry `created_at=None` and fail response serialization.
- `Float` precision is sufficient for analytics-range accelerometer and temperature values and keeps rows narrow for high write volume.

## 3. Real-Time Broadcast (`app/redis.py`, `app/main.py`)

### Channel architecture
One channel, `live_telemetry`, carries one message per persisted record — published exactly once, only after the database commit succeeds (a failed write returns before any publish). Serialization uses `model_dump(mode="json")` so datetimes become ISO strings up front, and the Redis client runs with `decode_responses=True` so frames are plain `str` ready for `send_text` with zero re-encoding.

Publishing is wrapped defensively: a Redis outage logs an exception but cannot abort ingestion — persistence remains the source of truth, streaming degrades gracefully.

### WebSocket lifecycle (`/ws/telemetry`)
Per connected dashboard client:

```mermaid
sequenceDiagram
    autonumber
    actor DASH as Dashboard client
    participant API as FastAPI /ws/telemetry
    participant PS as Redis Pub/Sub
    participant PG as PostgreSQL

    DASH->>API: WebSocket connect
    API->>API: accept()
    API->>PS: subscribe(live_telemetry)
    API->>PG: SELECT last 10 records (id DESC)
    PG-->>API: rows
    API-->>DASH: {"type":"snapshot","data":[oldest→newest]}
    loop until disconnect
        API->>PS: get_message(timeout=20s)
        alt frame available
            PS-->>API: published envelope
            API-->>DASH: send_text(frame) — verbatim
        else idle timeout
            API-->>DASH: {"type":"ping"}
        end
    end
    DASH-->>API: close / reset
    API->>PS: unsubscribe + aclose()
```

Key design points:

- **One `pubsub()` per client, never shared.** In redis-py each Pub/Sub object owns a dedicated pooled connection; sharing one across sockets would interleave frames randomly between clients.
- **Snapshot + live contract.** The snapshot (`last 10`, reversed to oldest→newest) closes the gap between "connect" and "first publish" — no record can slip through unobserved, and the UI can paint history instantly. Live frames are forwarded byte-for-byte from the channel, so every client sees identical payloads.
- **Keepalive doubles as a liveness probe.** `get_message(timeout=20 s)` returning `None` triggers a `ping` frame; writing to a dead socket raises there, converting silent disconnects into handled exceptions promptly.
- **Guaranteed cleanup.** A `finally` block unsubscribes and calls `pubsub.aclose()` on every exit path — clean `WebSocketDisconnect`, abrupt TCP reset, or relay exception — so Pub/Sub connections cannot leak under churn.

| Frame | Shape | Timing |
| --- | --- | --- |
| `snapshot` | `{"type":"snapshot","data":[≤10 records]}` | Once, immediately after accept |
| `telemetry` | `{"type":"telemetry","data":{record}}` | Per ingested reading |
| `alert` | `{"type":"alert","data":{alert record}}` | When the Celery engine raises WARNING/CRITICAL |
| `ping` | `{"type":"ping"}` | Every ~20 s of silence |

## 4. Analytics Engine (`app/celery_app.py`, `app/analytics.py`)

### Celery instance
`celery_app = Celery("iot_analytics", broker=settings.redis_url, backend=settings.redis_url)` — Redis doubles as both the task queue and the result backend. The worker runs in a separate container (`iot-celery-worker`) that shares the project image, so the analytics code and everything it imports (`crud`, models, schemas, Redis) are always in sync with the API.

### `process_vibration_window` task
Distributed as `app.analytics.process_vibration_window(device_id, accel_samples, sample_rate_hz=1)`. The worker is synchronous by nature, so the task's math runs on the worker thread and all persistence/broadcast I/O happens *after* classification:

1. **Velocity integration.** `numpy.asarray` wraps the sample list; the mean is removed (detrend) and `scipy.integrate.cumulative_trapezoid(..., dx=1/sample_rate)` converts acceleration to velocity. RMS velocity is `sqrt(mean(v²))` scaled to mm/s (`× 1000`).
2. **FFT.** `scipy.fft.rfft` on the detrended signal with `rfftfreq(n, d=dt)` yields the spectrum; the index of the maximum magnitude gives the dominant frequency (Hz).
3. **ISO 10816-1 zoning.** The RMS velocity is matched against the `ISO_10816_ZONES` table (`(lower, upper, code, name)`):

   | Zone | RMS Velocity (mm/s) | Status |
   | --- | --- | --- |
   | **A** | < 1.12 | Good |
   | **B** | 1.12 – 2.80 | Satisfactory |
   | **C** | 2.80 – 7.10 | Unsatisfactory → `WARNING` |
   | **D** | > 7.10 | Unacceptable → `CRITICAL` |

### Alert persistence & broadcast
When the zone is C or D, the task builds an `alert_data` dict (device_id, severity, metric, value, threshold, message) and bridges into the async world with `run_async(_persist_and_broadcast(alert_data))`:

- **Persist:** a dedicated `AsyncSessionLocal` inserts an `Alert` row, then `refresh` materializes `id`/`created_at` — the same session-isolation pattern ingestion uses.
- **Broadcast:** the committed alert is serialized with `AlertResponse.model_dump(mode="json")` and published to the shared `live_telemetry` channel as `{"type":"alert","data":{...}}`, so it arrives on every subscribed dashboard. Publish failures are logged but never abort the worker (persistence is the source of truth).

### Async bridge: one persistent event loop per worker process
The worker is synchronous (ForkPoolWorker threads), but persistence/broadcast need async I/O. Instead of `asyncio.run(...)` — which creates and destroys a fresh loop per task and strands pooled connections (the classic `RuntimeError: Event loop is closed` / `Future attached to a different loop`) — `app/analytics.py` keeps **one loop alive per worker process**:

- `@worker_process_init.connect` → `_init_worker_loop` creates a single `asyncio.new_event_loop()` and installs it as the process loop at worker startup.
- `run_async(coro)` runs a coroutine on that stable loop via `loop.run_until_complete(coro)` (lazily (re)creating the loop if missing/closed). Because the same loop persists across task dispatches, the module-level `AsyncSessionLocal` engine and `redis_client` pools stay bound to one live loop.
- `@worker_process_shutdown.connect` → `_shutdown_worker_loop` runs `engine.dispose()` and `redis_client.aclose()` on the loop, then closes the loop itself — clean teardown before the fork exits.
- Combined with `NullPool` on the engine (`app/database.py`), no DB connection is ever cached across loops, so consecutive task dispatches on the same worker are safe.

The worker returns a JSON-serializable dict (`device_id`, `rms_velocity_mm_s`, `peak_frequency_hz`, `iso_zone`, `iso_zone_name`, `severity`, `alert_raised`) as the task result.

## 5. Fault Tolerance

### Session isolation per message
Every message opens its own `AsyncSession` inside an `async with` block. Commit failures roll back and release that session alone; the next message starts from a clean session rather than inheriting broken transaction state. This is what makes poison pills structurally impossible to escalate.

### Poison-pill containment
Invalid input is neutralized at the cheapest possible layer and never reaches storage or the broadcast path:

| Payload | Gate | Outcome |
| --- | --- | --- |
| Non-JSON bytes | `json.loads` | Logged, dropped |
| Valid JSON, wrong shape | Pydantic schema | Logged, dropped |
| DB write failure | Session rollback | Logged, dropped (at-most-once semantics) |
| Redis unavailable | Publish guard | Logged; record still persisted |

Verified behavior: interleaving garbage and schema-violating frames between valid readings leaves the loop fully operational — subsequent valid readings persist and stream normally.

### Dependency restarts
- **Mosquitto restart:** consumer hits `MqttError`, backs off (1→30 s), resubscribes on recovery. Messages published during the gap are lost (QoS 0), never duplicated.
- **PostgreSQL restart:** `pool_pre_ping` evicts dead connections on first touch; writes during the outage are logged and dropped, then resume automatically.
- **Redis restart:** broadcast frames are lost for the outage window; snapshots on new connects self-heal any missed history. Celery tasks queued during the outage are redelivered once Redis recovers (Redis is also the Celery broker); in-flight alert persistence/broadcast retry on restart.
- **Celery worker restart / Redis broker outage:** pending vibration windows sit in the Redis queue; when the worker and broker are back, tasks resume. A WARNING/CRITICAL alert is persisted only after the worker successfully reaches the write step, keeping the `alerts` table the single source of truth.

### Graceful shutdown sequencing
Lifespan teardown is ordered to prevent work against closed resources:

```python
mqtt_task.cancel()                     # 1. stop ingress at the source
with suppress(asyncio.CancelledError):
    await mqtt_task                    # 2. wait for the consumer to exit cleanly
await redis_client.aclose()            # 3. close broadcast transport
await engine.dispose()                 # 4. retire DB connections last
```

Cancelling before closing pools guarantees no in-flight handler can grab a connection that is already gone.
