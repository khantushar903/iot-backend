import logging

from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iot-backend")

app = FastAPI(title="IoT Machine Monitoring API")


@app.get("/health")
async def health():
    logger.info("Health check requested")
    return {"status": "ok"}
