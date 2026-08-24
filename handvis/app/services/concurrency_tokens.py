"""Redis 令牌桶：跨进程并发控制。

适用于 chunk 构建、视频转码等 CPU/GPU 密集任务，限制全局同时执行数量。
多 worker 部署时由 Redis 协调，避免进程间各自独立计数导致总并发失控。
"""

import logging
from contextlib import asynccontextmanager

from app.services.redis_client import get_redis

logger = logging.getLogger(__name__)


def _tokens_key(name: str) -> str:
    """令牌桶 List 键。"""
    return f"handvis:tokens:{name}"


def _tokens_init_key(name: str) -> str:
    """令牌桶初始化标记键（防止多 worker 重复初始化）。"""
    return f"handvis:tokens:{name}:init"


async def ensure_tokens_initialized(name: str, capacity: int) -> None:
    """启动时确保令牌桶已初始化（幂等，跨进程安全）。

    使用 SETNX 抢初始化权；24h 过期后自动重置，避免长期累积导致 token 泄漏。
    """
    redis = get_redis()
    init_key = _tokens_init_key(name)
    tokens_key = _tokens_key(name)
    if await redis.set(init_key, "1", nx=True, ex=60):
        await redis.delete(tokens_key)
        if capacity > 0:
            await redis.rpush(tokens_key, *[str(i) for i in range(capacity)])
        logger.info("初始化 Redis 令牌桶。name=%s capacity=%s", name, capacity)


async def acquire_token(name: str, *, timeout: int = 600) -> str | None:
    """阻塞抢一个令牌，超时返回 None。"""
    redis = get_redis()
    result = await redis.blpop(_tokens_key(name), timeout=timeout)
    return result[1] if result else None


async def release_token(name: str, token: str) -> None:
    """归还令牌。"""
    redis = get_redis()
    await redis.rpush(_tokens_key(name), token)


@asynccontextmanager
async def token_slot(name: str, *, timeout: int = 600):
    """async with token_slot('build'): ... 自动归还令牌。"""
    token = await acquire_token(name, timeout=timeout)
    if token is None:
        raise TimeoutError(f"acquire token timeout: {name}")
    try:
        yield token
    finally:
        await release_token(name, token)
