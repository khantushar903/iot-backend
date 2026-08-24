# Project Context: Industrial IoT Machine Monitoring System (Thesis Project)

## Architecture Overview
We are building a scalable, event-driven IoT backend to monitor motor health using vibration (MPU6050) and temperature (DS18B20) data. The system ingests high-frequency MQTT data, stores it, and streams it live to a frontend.

## Tech Stack
*   **Backend Framework:** FastAPI (Python, Async)
*   **Message Broker:** Eclipse Mosquitto (MQTT)
*   **Database:** PostgreSQL (with SQLAlchemy async ORM)
*   **In-Memory Store/PubSub:** Redis
*   **Deployment:** Docker Compose

## Data Flow
1. IoT Edge (ESP32) publishes JSON payload to Mosquitto MQTT broker on topic: `telemetry/motors`
2. FastAPI app runs a background async MQTT client (e.g., `aiomqtt`) to consume these messages.
3. FastAPI validates the payload using Pydantic.
4. FastAPI asynchronously inserts the record into PostgreSQL.
5. FastAPI immediately publishes the raw data to a Redis Pub/Sub channel.
6. A Next.js frontend connects to FastAPI via WebSockets. FastAPI subscribes to the Redis channel and pushes live updates to the WebSocket clients.

## Payload Schema (JSON)
{
  "device_id": "string",
  "ts": "integer (unix timestamp, optional)",
  "accel_x": "float",
  "accel_y": "float",
  "accel_z": "float",
  "temp_c": "float"
}

## Development Rules for AI
*   Use fully asynchronous Python code (`async def`, `aiomqtt`, `asyncpg` or `async_sessionmaker` for SQLAlchemy).
*   Keep files modular (e.g., `models.py`, `database.py`, `mqtt.py`, `main.py`).
*   Include proper error handling and logging.