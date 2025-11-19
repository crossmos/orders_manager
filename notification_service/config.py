from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    NOTIFICATIONS_TOPIC: str = "notifications"


settings = Settings()
