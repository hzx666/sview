"""chunk 构建 API 任务入口。

HTTP 只作为本仓库播放页的适配器：去重、写详情、RPUSH 到 Redis。
外部集群 master 应直接往队列投递，不必走这个接口。
"""

import logging
import uuid
from datetime import datetime

from fastapi import HTTPException
from redis.exceptions import RedisError

from app.core.config import BUILD_JOB_TTL_SECONDS
from app.services.build_queue import (
    JOB_STATUS_QUEUED,
    enqueue_build_job,
    get_job_detail,
    job_submit_payload,
    save_job_detail,
    task_map_key,
    build_task_key,
)
from app.services.redis_client import get_redis

logger = logging.getLogger(__name__)


async def _redis_or_503(awaitable, *, log_message: str, detail: str):
    """统一兜底 RedisError，转为可读的 503。"""
    try:
        return await awaitable
    except RedisError as exc:
        logger.warning(log_message, exc)
        raise HTTPException(status_code=503, detail=detail) from exc


async def submit_build_job(
    *,
    ep_dir: str,
    item_name: str,
    force_rebuild: bool,
    video_path: str | None = None,
    posehand_path: str | None = None,
) -> dict:
    """提交 chunk 构建任务到 Redis 队列，运行中同键请求复用同一 task_id。

    带上 video_path / posehand_path 时，worker 不连 PostgreSQL，直接读 OSS。
    """
    redis = get_redis()
    task_key = build_task_key(ep_dir=ep_dir, item_name=item_name, force_rebuild=force_rebuild)
    map_key = task_map_key(task_key)
    existing_task_id = await _redis_or_503(
        redis.get(map_key),
        log_message="Redis 不可用，无法提交 chunk 构建任务。err=%s",
        detail="Redis 未连接，无法提交构建任务。请先启动 Redis 后重试。",
    )
    if existing_task_id:
        existing = await get_job_detail(existing_task_id)
        if existing and existing.get("status") in {"queued", "running"}:
            return job_submit_payload(existing_task_id, existing)

    task_id = uuid.uuid4().hex
    now = datetime.now().isoformat()
    job = {
        "task_id": task_id,
        "type": "build_chunks",
        "ep_dir": ep_dir,
        "item_name": item_name,
        "force_rebuild": bool(force_rebuild),
        "status": JOB_STATUS_QUEUED,
        "result": None,
        "error": None,
        "created_at": now,
        "updated_at": now,
    }
    if (video_path or "").strip():
        job["video_path"] = video_path.strip()
    if (posehand_path or "").strip():
        job["posehand_path"] = posehand_path.strip()
    created = await _redis_or_503(
        redis.set(map_key, task_id, ex=BUILD_JOB_TTL_SECONDS, nx=True),
        log_message="Redis 不可用，无法创建 chunk 构建任务。err=%s",
        detail="Redis 未连接，无法提交构建任务。请先启动 Redis 后重试。",
    )
    if not created:
        existing_task_id = await redis.get(map_key)
        if existing_task_id:
            existing = await get_job_detail(existing_task_id)
            if existing and existing.get("status") in {"queued", "running"}:
                return job_submit_payload(existing_task_id, existing)

    await _redis_or_503(
        redis.set(map_key, task_id, ex=BUILD_JOB_TTL_SECONDS),
        log_message="Redis 不可用，无法绑定 chunk 构建任务。err=%s",
        detail="Redis 未连接，无法提交构建任务。请先启动 Redis 后重试。",
    )
    await save_job_detail(task_id, job)
    await _redis_or_503(
        enqueue_build_job(
            task_id=task_id,
            ep_dir=ep_dir,
            item_name=item_name,
            force_rebuild=force_rebuild,
            video_path=video_path,
            posehand_path=posehand_path,
        ),
        log_message="Redis 不可用，无法入队 chunk 构建任务。err=%s",
        detail="Redis 未连接，无法提交构建任务。请先启动 Redis 后重试。",
    )
    logger.info("构建任务已提交到队列。task_id=%s ep_dir=%s item_name=%s", task_id, ep_dir, item_name)
    return job_submit_payload(task_id, job)


async def get_build_job(task_id: str) -> dict:
    """查询异步 chunk 构建任务状态。"""
    try:
        job = await get_job_detail(task_id)
    except RedisError as exc:
        logger.warning("Redis 不可用，无法查询 chunk 构建任务。task_id=%s err=%s", task_id, exc)
        raise HTTPException(status_code=503, detail="Redis 未连接，无法查询构建任务。请先启动 Redis 后重试。") from exc
    if not job:
        raise HTTPException(status_code=404, detail="Build task not found")
    return job
