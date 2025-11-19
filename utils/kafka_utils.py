from config import order_logger


def delivery_report(err, msg):
    if err is not None:
        order_logger.error("Message delivery failed: %s", err)
    else:
        order_logger.info(
            "Message delivered to %s [%d] at offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )
