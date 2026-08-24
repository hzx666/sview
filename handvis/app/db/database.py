"""数据库连接与应用生命周期。"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import (
    APP_DB_CONNECT_TIMEOUT,
    APP_DB_URL,
    BUILD_MAX_CONCURRENCY,
    HANDVIS_DISABLE_PG,
    HANDVIS_SKIP_CREATE_ALL,
    TRANSCODE_MAX_CONCURRENCY,
)
from app.models.ep_asset import Base
from app.services.concurrency_tokens import ensure_tokens_initialized

# 无 PG 模式不建引擎；SessionLocal 置 None，任何路径误用都会立刻报错。
if HANDVIS_DISABLE_PG:
    engine = None
    SessionLocal = None
else:
    engine = create_async_engine(
        APP_DB_URL,
        echo=False,
        connect_args={"timeout": APP_DB_CONNECT_TIMEOUT},
    )
    SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
logger = logging.getLogger(__name__)


def _import_all_models() -> None:
    # noqa 导入模型的副作用是注册到 Base.metadata。
    from app.models import ep_asset_item, media_asset, playback_asset  # noqa: F401


async def init_app_schema() -> None:
    """初始化 HandVis 主业务库表结构。"""
    _import_all_models()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if HANDVIS_DISABLE_PG:
        logger.info("HANDVIS_DISABLE_PG=1：跳过建表与转码恢复（队列/状态走 Redis，产物走 OSS）。")
    elif not HANDVIS_SKIP_CREATE_ALL:
        # 常规本地启动保留自动建表；重启脚本已提前校准，多 worker 内会跳过。
        await init_app_schema()
    # 初始化全局令牌桶（chunk 构建 + 视频转码），跨进程并发控制。
    try:
        await ensure_tokens_initialized("build", BUILD_MAX_CONCURRENCY)
        await ensure_tokens_initialized("transcode", TRANSCODE_MAX_CONCURRENCY)
    except Exception as exc:
        logger.warning("Redis 令牌桶初始化失败（功能仍可用，但并发控制可能失效）。err=%s", exc)

    if not HANDVIS_DISABLE_PG:
        try:
            from app.services.transcode_queue import recover_pending_transcodes

            resumed = await recover_pending_transcodes()
            logger.info("转码任务恢复检查完成。resumed=%s", resumed)
        except Exception as exc:
            logger.warning("转码任务恢复检查失败。err=%s", exc)

    logger.info("应用生命周期启动完成。disable_pg=%s", HANDVIS_DISABLE_PG)
    try:
        yield
    finally:
        logger.info("应用生命周期结束。")
