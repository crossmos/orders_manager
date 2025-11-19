import json
import random
import signal
import time

from confluent_kafka import Consumer, Producer, KafkaError

from config import processing_logger
from processing_service.config import settings


consumer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "processing-service-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # ручной коммит для надёжности
}
producer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "processing-producer",
    "acks": "all",
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

running = True


def handle_sigterm(sig, frame):
    global running
    running = False


signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)


def process_order(order: dict) -> dict:
    status = "confirmed" if random.random() > 0.2 else "rejected"
    return {
        "order_id": order["order_id"],
        "status": status,
        "timestamp": int(time.time()),
    }


def main():
    consumer.subscribe([settings.ORDERS_TOPIC])
    processing_logger.info(
        "Processing service started, subscribed to %s", settings.ORDERS_TOPIC
    )

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    processing_logger.error("Consumer error: %s", msg.error())
                    continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                processing_logger.info("Received order: %s", payload)
                result = process_order(payload)

                producer.produce(
                    topic=settings.NOTIFICATIONS_TOPIC,
                    key=result["order_id"],
                    value=json.dumps(result).encode("utf-8"),
                )
                producer.flush(timeout=5)

                consumer.commit(message=msg, asynchronous=False)
                processing_logger.info(
                    "Processed and published result: %s",
                    result,
                )
            except Exception:
                processing_logger.exception("Processing error")
    finally:
        consumer.close()
        processing_logger.info("Processing service stopped")


if __name__ == "__main__":
    main()
