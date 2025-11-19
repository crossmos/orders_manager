import asyncio
import json

# import requests
import uuid
import time

import aiohttp

API = "http://localhost:8000/api/orders"


async def send_order(i):
    order = {
        "order_id": str(uuid.uuid4()),
        "user_id": f"user_{i}",
        "item": "widget",
        "quantity": i % 5 + 1,
    }

    json_data = json.dumps(order)
    print(f"Order {i}: Sending to {API}")
    print(f"  Data: {json_data[:100]}...")
    async with aiohttp.ClientSession() as session:
        r = await session.post(
            API,
            data=json_data,
        )

    response_json = await r.json()
    print(f"  Response: {response_json}")


# Главная асинхронная функция.
async def main():
    # Создать список задач для асинхронного выполнения.
    tasks = [asyncio.ensure_future(send_order(i)) for i in range(10)]
    # Подождать, пока выполнятся все задачи.
    await asyncio.wait(tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
