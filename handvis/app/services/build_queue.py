"""HandVis 计算任务的 Redis 协议。

这个模块是插入外部工作集群时的唯一消息合同：master 只 RPUSH，
worker 只消费，结果只写 Redis。不要求对端调用 HandVis HTTP。

入队（FIFO）：
    RPUSH handvis:jobs:queue <json>

消息：
    {
  "task_id": "必填",
  "type": "build_chunks",
  "payload": {
    "ep_dir": "oss://bucket/path/ep1/",
    "item_name": "clip_001",
    "video_path": "oss://bucket/.../clip.mp4",
    "posehand_path": "oss://bucket/.../clip.pose3d_hand",
    "force_rebuild": false
  },
  "reply_queue": "可选，默认 handvis:jobs:result"
}

集群 worker 不连 Postgres：带上 video_path + posehand_path 即可直接拉 OSS。
只给 ep_dir/item_name 时会回退查库（本地 HandVis 网页）。

兼容旧平铺字段（无 type/payload 时视为 build_chunks）。

Redis 键：
- handvis:jobs:queue                      LIST，待执行
- handvis:jobs:processing:{worker_id}     LIST，该 worker 正在执行
- handvis:jobs:result                     LIST，终态结果（succeeded/failed）
- handvis:jobs:events                     PUBSUB，进度+终态
- handvis:build_task:{task_id}            STRING，任务详情（GET 可轮询）
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from app.core.config import (
    BUILD_EVENT_CHANNEL,
    BUILD_JOB_TTL_SECONDS,
    BUILD_PROCESSING_KEY_PREFIX,
    BUILD_QUEUE_BLOCK_SECONDS,
    BUILD_QUEUE_KEY,
    BUILD_RESULT_QUEUE_KEY,
    BUILD_WORKER_ID,
)
from app.services.redis_client import get_redis

logger = logging.getLogger(__name__)

JOB_TYPE_BUILD_CHUNKS = "build_chunks"
SUPPORTED_JOB_TYPES = {JOB_TYPE_BUILD_CHUNKS}

JOB_STATUS_QUEUED = "queued"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_FAILED = "failed"
_TERMINAL_STATUSES = {JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED}


def build_task_key(*, ep_dir: str, item_name: str, force_rebuild: bool) -> str:
    return f"{ep_dir}::item={item_name}::force={int(force_rebuild)}"


def task_map_key(task_key: str) -> str:
    return f"handvis:build_task_map:{task_key}"


def task_detail_key(task_id: str) -> str:
    return f"handvis:build_task:{task_id}"


def processing_key(worker_id: str | None = None) -> str:
    return f"{BUILD_PROCESSING_KEY_PREFIX}:{worker_id or BUILD_WORKER_ID}"


def job_submit_payload(task_id: str, job: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "status": job.get("status", JOB_STATUS_QUEUED),
        "ep_dir": job.get("ep_dir", ""),
        "item_name": job.get("item_name", ""),
        "poll_url": f"/api/playback/build-chunks/tasks/{task_id}",
    }


def parse_queue_message(raw: str) -> dict[str, Any]:
    """解析队列 JSON，统一成 envelope。字段不合法时抛 ValueError。"""
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("queue message must be a JSON object")

    nested = payload.get("payload")
    source = nested if isinstance(nested, dict) else payload
    task_id = str(payload.get("task_id") or "").strip()
    job_type = str(payload.get("type") or JOB_TYPE_BUILD_CHUNKS).strip() or JOB_TYPE_BUILD_CHUNKS
    ep_dir = str(source.get("ep_dir") or payload.get("ep_dir") or "").strip()
    item_name = str(source.get("item_name") or payload.get("item_name") or "").strip()
    video_path = str(source.get("video_path") or payload.get("video_path") or "").strip()
    posehand_path = str(source.get("posehand_path") or payload.get("posehand_path") or "").strip()
    reply_queue = str(payload.get("reply_queue") or "").strip() or BUILD_RESULT_QUEUE_KEY

    if not task_id:
        raise ValueError("queue message missing task_id")
    if job_type == JOB_TYPE_BUILD_CHUNKS and not posehand_path and (not ep_dir or not item_name):
        raise ValueError("build_chunks message missing posehand_path or ep_dir/item_name")

    return {
        "task_id": task_id,
        "type": job_type,
        "ep_dir": ep_dir,
        "item_name": item_name,
        "video_path": video_path,
        "posehand_path": posehand_path,
        "force_rebuild": bool(source.get("force_rebuild", payload.get("force_rebuild"))),
        "reply_queue": reply_queue,
        "payload": source if isinstance(nested, dict) else payload,
    }


def dumps_queue_message(
    *,
    task_id: str,
    ep_dir: str,
    item_name: str,
    force_rebuild: bool,
    job_type: str = JOB_TYPE_BUILD_CHUNKS,
    reply_queue: str | None = None,
    video_path: str | None = None,
    posehand_path: str | None = None,
) -> str:
    payload = {
        "ep_dir": ep_dir,
        "item_name": item_name,
        "force_rebuild": bool(force_rebuild),
    }
    if (video_path or "").strip():
        payload["video_path"] = video_path.strip()
    if (posehand_path or "").strip():
        payload["posehand_path"] = posehand_path.strip()
    return json.dumps(
        {
            "task_id": task_id,
            "type": job_type,
            "payload": payload,
            "reply_queue": (reply_queue or "").strip() or BUILD_RESULT_QUEUE_KEY,
        },
        ensure_ascii=False,
    )


async def get_job_detail(task_id: str) -> dict[str, Any] | None:
    redis = get_redis()
    raw = await redis.get(task_detail_key(task_id))
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


async def save_job_detail(task_id: str, payload: dict[str, Any]) -> None:
    redis = get_redis()
    await redis.set(
        task_detail_key(task_id),
        json.dumps(payload, ensure_ascii=False, default=str),
        ex=BUILD_JOB_TTL_SECONDS,
    )


def _event_payload(
    job: dict[str, Any],
    *,
    status: str,
    result: dict[str, Any] | None,
    error: str | None,
    partial: bool,
) -> dict[str, Any]:
    return {
        "task_id": job.get("task_id"),
        "type": job.get("type") or JOB_TYPE_BUILD_CHUNKS,
        "status": status,
        "partial": bool(partial),
        "result": result,
        "error": (error or "").strip() or None,
        "ep_dir": job.get("ep_dir"),
        "item_name": job.get("item_name"),
        "worker_id": job.get("worker_id"),
        "updated_at": job.get("updated_at"),
    }


async def publish_job_event(job: dict[str, Any], *, partial: bool = False) -> None:
    """把任务更新推给外部集群：PUBSUB 全量，结果队列只收终态。"""
    status = str(job.get("status") or "")
    event = _event_payload(
        job,
        status=status,
        result=job.get("result") if isinstance(job.get("result"), dict) else None,
        error=job.get("error"),
        partial=partial,
    )
    raw = json.dumps(event, ensure_ascii=False, default=str)
    redis = get_redis()
    await redis.publish(BUILD_EVENT_CHANNEL, raw)
    if status in _TERMINAL_STATUSES:
        reply_queue = str(job.get("reply_queue") or "").strip() or BUILD_RESULT_QUEUE_KEY
        await redis.rpush(reply_queue, raw)
        logger.info(
            "构建终态已写入结果队列。task_id=%s status=%s queue=%s",
            job.get("task_id"),
            status,
            reply_queue,
        )


async def update_job(
    task_id: str,
    *,
    status: str,
    result: dict[str, Any] | None,
    error: str | None,
    extra: dict[str, Any] | None = None,
    partial: bool = False,
    notify: bool = True,
) -> dict[str, Any] | None:
    job = await get_job_detail(task_id)
    if not job:
        return None
    job["status"] = status
    job["result"] = result
    job["error"] = (error or "").strip() or None
    job["updated_at"] = datetime.now().isoformat()
    if extra:
        job.update(extra)
    await save_job_detail(task_id, job)
    if notify:
        await publish_job_event(job, partial=partial)
    return job


async def set_partial_result(task_id: str, partial: dict[str, Any]) -> None:
    """流式构建中：保留 status=running，写入首 chunk 即可播的 result。"""
    job = await update_job(
        task_id,
        status=JOB_STATUS_RUNNING,
        result=partial,
        error=None,
        partial=True,
        notify=True,
    )
    if not job:
        return
    logger.info(
        "首 chunk 已推送任务状态。task_id=%s unique_id=%s",
        task_id,
        partial.get("unique_id"),
    )


async def ensure_job_record(
    *,
    task_id: str,
    ep_dir: str,
    item_name: str,
    force_rebuild: bool,
    worker_id: str | None = None,
    job_type: str = JOB_TYPE_BUILD_CHUNKS,
    reply_queue: str | None = None,
) -> dict[str, Any]:
    """worker 侧兜底：集群 master 只入队、未写详情时补一条记录。"""
    job = await get_job_detail(task_id)
    if job:
        if reply_queue and not job.get("reply_queue"):
            job["reply_queue"] = reply_queue
            await save_job_detail(task_id, job)
        return job
    now = datetime.now().isoformat()
    job = {
        "task_id": task_id,
        "type": job_type,
        "ep_dir": ep_dir,
        "item_name": item_name,
        "force_rebuild": bool(force_rebuild),
        "status": JOB_STATUS_QUEUED,
        "result": None,
        "error": None,
        "created_at": now,
        "updated_at": now,
        "worker_id": worker_id,
        "reply_queue": (reply_queue or "").strip() or BUILD_RESULT_QUEUE_KEY,
    }
    await save_job_detail(task_id, job)
    return job


async def enqueue_build_job(
    *,
    task_id: str,
    ep_dir: str,
    item_name: str,
    force_rebuild: bool,
    reply_queue: str | None = None,
    video_path: str | None = None,
    posehand_path: str | None = None,
) -> None:
    """把任务推入 worker 队列（FIFO）。本进程不执行计算。"""
    redis = get_redis()
    message = dumps_queue_message(
        task_id=task_id,
        ep_dir=ep_dir,
        item_name=item_name,
        force_rebuild=force_rebuild,
        reply_queue=reply_queue,
        video_path=video_path,
        posehand_path=posehand_path,
    )
    await redis.rpush(BUILD_QUEUE_KEY, message)
    logger.info(
        "构建任务已入队。task_id=%s ep_dir=%s item_name=%s queue=%s",
        task_id,
        ep_dir,
        item_name,
        BUILD_QUEUE_KEY,
    )


async def claim_next_job(worker_id: str) -> str | None:
    """阻塞领取下一条任务，原子移到该 worker 的 processing 列表。"""
    redis = get_redis()
    raw = await redis.execute_command(
        "BLMOVE",
        BUILD_QUEUE_KEY,
        processing_key(worker_id),
        "LEFT",
        "RIGHT",
        BUILD_QUEUE_BLOCK_SECONDS,
    )
    if raw is None:
        return None
    return raw if isinstance(raw, str) else raw.decode("utf-8")


async def ack_job(worker_id: str, raw_message: str) -> None:
    """任务结束（成功或失败）后从 processing 列表删除。"""
    redis = get_redis()
    await redis.lrem(processing_key(worker_id), 1, raw_message)


async def reclaim_processing(worker_id: str) -> int:
    """启动时把本 worker 上次未 ack 的任务塞回队列头部。"""
    redis = get_redis()
    key = processing_key(worker_id)
    reclaimed = 0
    while True:
        raw = await redis.rpop(key)
        if not raw:
            break
        await redis.lpush(BUILD_QUEUE_KEY, raw)
        reclaimed += 1
    if reclaimed:
        logger.warning("回收未完成构建任务。worker_id=%s count=%s", worker_id, reclaimed)
    return reclaimed
