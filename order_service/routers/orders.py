import json
from typing import Any, Dict

from confluent_kafka import Producer, KafkaError
from fastapi import APIRouter, HTTPException, Request

from order_service.config import settings
from config import order_logger
from utils.kafka_utils import delivery_report

order_router = APIRouter(
    prefix="/orders",
    tags=["Orders"],
)

producer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "client.id": settings.PRODUCER_CLIENT_ID,
    "acks": "all",
}
producer = Producer(producer_conf)


@order_router.post("/")
async def create_order(request: Request):
    """
    Принимает сырой JSON.
    Валидирует обязательные поля.
    Публикует JSON в Kafka.
    """
    try:
        try:
            payload: Dict[str, Any] = await request.json()
        except Exception as e:
            order_logger.error(f"Failed to parse JSON: {e}")
            raise HTTPException(
                status_code=400,
                detail=f"Invalid JSON: {str(e)}",
            )

        required_fields = ["order_id", "user_id", "item", "quantity"]

        missing = [field for field in required_fields if field not in payload]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required fields: {', '.join(missing)}",
            )

        key = str(payload["order_id"])
        value = json.dumps(payload).encode("utf-8")

        producer.produce(
            topic=settings.ORDERS_TOPIC,
            key=key,
            value=value,
            callback=delivery_report,
        )
        producer.flush(timeout=5)

        return {"status": "published", "order_id": payload["order_id"]}

    except KafkaError as e:
        order_logger.exception(f"Kafka error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Kafka error: {e}",
        )

    except HTTPException:
        raise

    except Exception as e:
        order_logger.exception(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: {e}",
        )
