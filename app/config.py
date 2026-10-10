from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = (
        "postgresql+asyncpg://iot_user:iot_password@postgres:5432/iot_db"
    )
    redis_url: str = "redis://redis:6379/0"
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_topic: str = "telemetry/motors"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
