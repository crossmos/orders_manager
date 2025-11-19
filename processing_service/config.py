from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    ORDERS_TOPIC: str = "orders"
    NOTIFICATIONS_TOPIC: str = "notifications"


settings = Settings()
