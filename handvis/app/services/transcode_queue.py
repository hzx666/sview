"""视频转码 Redis 队列：跨进程提交、消费、状态查询。

设计要点：
- 任务详情用 Hash 存储，支持随时查询状态；
- 全局令牌桶（handvis:tokens:transcode）限制并发；
- 提交任务后由当前进程异步调度执行，不再依赖常驻消费 worker。
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path

from app.core.config import (
    BUILD_JOB_TTL_SECONDS,
    TOKEN_ACQUIRE_TIMEOUT_SECONDS,
    TRANSCODE_VIDEO_DIR,
)
from app.db.database import SessionLocal
from app.services.concurrency_tokens import token_slot
from app.services.media_asset_service import update_media_probe_and_status
from app.services.oss_signer import sign_video_url
from app.services.redis_client import get_redis
from app.services.video_transcode_service import build_transcoded_name, transcode_to_web_mp4

logger = logging.getLogger(__name__)

_running_tasks: dict[int, asyncio.Task] = {}


def _task_key(media_id: int) -> str:
    return f"handvis:transcode:task:{media_id}"


def _transcoded_url(file_name: str) -> str:
    """这里手动拼接静态 URL，避免与 routes/playback/paths 形成循环依赖。"""
    from app.core.config import TRANSCODE_VIDEO_STATIC_PREFIX

    return f"{TRANSCODE_VIDEO_STATIC_PREFIX.rstrip('/')}/{file_name}"


def _schedule_transcode(media_id: int) -> None:
    """在当前进程异步调度单个转码任务（幂等）。"""
    task = _running_tasks.get(media_id)
    if task is not None and not task.done():
        return

    async def _runner() -> None:
        try:
            await _do_transcode(media_id)
        except Exception as exc:
            logger.exception("转码后台任务异常。media_id=%s err=%s", media_id, exc)

    created = asyncio.create_task(_runner())
    _running_tasks[media_id] = created

    def _cleanup(done_task: asyncio.Task) -> None:
        current = _running_tasks.get(media_id)
        if current is done_task:
            _running_tasks.pop(media_id, None)

    created.add_done_callback(_cleanup)


async def recover_pending_transcodes(*, limit: int = 1000) -> int:
    """启动时恢复历史 queued/running 任务，避免重启后任务悬挂。"""
    redis = get_redis()
    resumed = 0
    async for key in redis.scan_iter(match="handvis:transcode:task:*", count=200):
        if resumed >= max(1, int(limit)):
            break
        data = await redis.hgetall(key)
        status = str(data.get("status") or "").strip().lower()
        if status not in {"queued", "running"}:
            continue
        media_text = str(data.get("media_id") or "").strip()
        if not media_text:
            continue
        try:
            media_id = int(media_text)
        except ValueError:
            continue
        _schedule_transcode(media_id)
        resumed += 1

    if resumed:
        logger.info("启动恢复转码任务完成。resumed=%s", resumed)
    return resumed


async def submit_transcode_task(*, media_id: int, item_id: int, source_url: str) -> dict:
    """提交转码任务到 Redis 队列。返回当前状态字典。

    幂等规则：
    - 已 success 且产物文件仍存在：直接返回 success。
    - 已 queued/running：返回当前状态，不重复入队。
    - 其他（pending/failed）：重新入队。
    """
    redis = get_redis()
    key = _task_key(media_id)
    existing = await redis.hgetall(key)

    output_name = build_transcoded_name(source_url, item_id)
    output_path = TRANSCODE_VIDEO_DIR / output_name

    if existing:
        status = (existing.get("status") or "").strip()
        web_url = (existing.get("web_url") or "").strip()
        if status == "success" and web_url and output_path.exists():
            return _format_status(existing)
        if status in ("queued", "running"):
            _schedule_transcode(media_id)
            return _format_status(existing)

    payload = {
        "status": "queued",
        "media_id": str(media_id),
        "item_id": str(item_id),
        "source_url": source_url,
        "output_name": output_name,
        "web_url": "",
        "error": "",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }
    await redis.hset(key, mapping=payload)
    await redis.expire(key, BUILD_JOB_TTL_SECONDS)
    logger.info("视频转码任务已提交。media_id=%s item_id=%s", media_id, item_id)
    _schedule_transcode(media_id)
    return _format_status(payload)


async def get_transcode_status(media_id: int) -> dict | None:
    """查询转码状态，未提交过返回 None。"""
    redis = get_redis()
    data = await redis.hgetall(_task_key(media_id))
    return _format_status(data) if data else None


async def _set_status(media_id: int, **fields: str) -> None:
    redis = get_redis()
    key = _task_key(media_id)
    fields["updated_at"] = datetime.now().isoformat()
    await redis.hset(key, mapping=fields)
    await redis.expire(key, BUILD_JOB_TTL_SECONDS)


def _format_status(data: dict) -> dict:
    """规范化对外字段，避免泄露内部字段。"""
    if not data:
        return {}
    return {
        "media_id": int(data.get("media_id") or 0),
        "status": data.get("status") or "queued",
        "video_url": (data.get("web_url") or "").strip() or None,
        "error": (data.get("error") or "").strip() or None,
        "updated_at": data.get("updated_at") or "",
    }


async def _do_transcode(media_id: int) -> None:
    """单个转码任务执行：抢令牌 → ffmpeg → 回写 Redis + DB。"""
    redis = get_redis()
    key = _task_key(media_id)
    data = await redis.hgetall(key)
    if not data:
        logger.warning("转码任务详情已过期，跳过。media_id=%s", media_id)
        return

    item_id = int(data.get("item_id") or 0)
    source_url = data.get("source_url") or ""
    output_name = data.get("output_name") or ""
    if not (item_id and source_url and output_name):
        await _set_status(media_id, status="failed", error="任务参数缺失")
        return

    output_path: Path = TRANSCODE_VIDEO_DIR / output_name
    output_url = _transcoded_url(output_name)

    # 已有产物时直接复用，避免重复工作（多 worker 抢到同一任务时也安全）。
    if output_path.exists():
        await _set_status(media_id, status="success", web_url=output_url, error="")
        async with SessionLocal() as session:
            await update_media_probe_and_status(
                session,
                media_id=media_id,
                transcode_status="success",
                probe_payload=None,
                transcode_error=None,
                web_url=output_url,
            )
        return

    try:
        async with token_slot("transcode", timeout=TOKEN_ACQUIRE_TIMEOUT_SECONDS):
            await _set_status(media_id, status="running", error="")
            async with SessionLocal() as session:
                await update_media_probe_and_status(
                    session,
                    media_id=media_id,
                    transcode_status="running",
                    probe_payload=None,
                    transcode_error=None,
                )
            signed_source_url = sign_video_url(source_url)
            await asyncio.to_thread(transcode_to_web_mp4, signed_source_url, output_path)

        await _set_status(media_id, status="success", web_url=output_url, error="")
        async with SessionLocal() as session:
            await update_media_probe_and_status(
                session,
                media_id=media_id,
                transcode_status="success",
                probe_payload=None,
                transcode_error=None,
                web_url=output_url,
            )
        logger.info("视频转码成功。media_id=%s output=%s", media_id, output_name)
    except TimeoutError:
        await _set_status(media_id, status="failed", error="等待转码槽超时，请稍后重试")
        async with SessionLocal() as session:
            await update_media_probe_and_status(
                session,
                media_id=media_id,
                transcode_status="failed",
                probe_payload=None,
                transcode_error="acquire transcode token timeout",
            )
        logger.warning("视频转码等待令牌超时。media_id=%s", media_id)
    except Exception as exc:
        err_text = str(exc)[:1000]
        await _set_status(media_id, status="failed", error=err_text)
        async with SessionLocal() as session:
            await update_media_probe_and_status(
                session,
                media_id=media_id,
                transcode_status="failed",
                probe_payload=None,
                transcode_error=err_text,
            )
        logger.warning("视频转码失败。media_id=%s err=%s", media_id, exc)
