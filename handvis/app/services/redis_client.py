"""Redis 客户端工厂。"""

from redis.asyncio import Redis

from app.core.config import REDIS_URL

_redis_client: Redis | None = None


def get_redis() -> Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    return _redis_client
