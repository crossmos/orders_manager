Мини-система обработки заказов (Kafka + FastAPI + Python)

Прототип микросервисной системы, использующей Apache Kafka для асинхронной обработки заказов.
Проект включает три сервиса:

Order Service — принимает JSON-заказы и публикует их в Kafka.
Processing Service — получает заказы, обрабатывает и отправляет результат.
Notification Service — выводит результат обработки.

1. Требования
```
Python 3.11+
Docker + Docker Compose
```

2. Установка зависимостей
```
pip install -r requirements.txt
```

3. Запуск Kafka через Docker

Проект содержит docker-compose.yml для Zookeeper и Kafka.

Запуск:
```
docker compose up -d
```

Проверить работу:
```
docker compose logs kafka --follow
```

Остановить:
```
docker compose down
```

4. Запуск микросервисов

Каждый сервис запускается отдельно.

```
4.1 Order Service (FastAPI API)
python -m order_service.main
```
```
4.2 Processing Service
python -m processing_service.processor
```
```
4.3 Notification Service
python -m notification_service.notifaer
```

5. Топики Kafka

Топики создаются автоматически при первом использовании:
```
orders
notifications
```