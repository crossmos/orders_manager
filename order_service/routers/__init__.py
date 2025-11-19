from fastapi import APIRouter

from .orders import order_router


api_router = APIRouter(
    prefix="/api",
)
api_router.include_router(order_router)
