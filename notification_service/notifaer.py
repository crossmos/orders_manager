import json
import signal

from confluent_kafka import Consumer, KafkaError

from config import notification_logger
from notification_service.config import settings


consumer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "notification-service-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

consumer = Consumer(consumer_conf)
running = True


def handle_sigterm(sig, frame):
    global running
    running = False


signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)


def main():
    consumer.subscribe([settings.NOTIFICATIONS_TOPIC])
    notification_logger.info(
        "Notification service started, subscribed to %s",
        settings.NOTIFICATIONS_TOPIC,
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
                    notification_logger.error("Consumer error: %s", msg.error())
                    continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                notification_logger.info(
                    "Notification: order_id=%s status=%s",
                    payload.get("order_id"),
                    payload.get("status"),
                )
            except Exception:
                notification_logger.exception("Failed to parse notification")
    finally:
        consumer.close()
        notification_logger.info("Notification service stopped")


if __name__ == "__main__":
    main()
