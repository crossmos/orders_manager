from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from order_service.routers import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # start
    yield
    # shutdown
    pass


app = FastAPI(
    lifespan=lifespan,
)
app.include_router(api_router)


if __name__ == "__main__":
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": "localhost:9092"})

    topics = [
        NewTopic("orders", num_partitions=1, replication_factor=1),
        NewTopic("notifications", num_partitions=1, replication_factor=1),
    ]

    fs = admin.create_topics(topics)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Topic {topic} already exists: {e}")

    uvicorn.run(
        "order_service.main:app",
        host="0.0.0.0",
        port=8000,
    )
