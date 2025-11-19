import logging


# Базовая настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Логгер для order-service
order_logger = logging.getLogger("order-service")

# Логгер для notification-service
notification_logger = logging.getLogger("notification-service")

# Логгер для processing-service
processing_logger = logging.getLogger("processing-service")
