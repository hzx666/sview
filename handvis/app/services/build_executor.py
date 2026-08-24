"""执行 Redis 投递的计算任务。

当前支持 type=build_chunks（扫描配对 + MANO + chunk）。
状态和结果只回写 Redis，供外部集群 master 消费。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from app.api.routes.playback.builder import build_chunks_impl
from app.services.build_queue import (
    JOB_STATUS_FAILED,
    JOB_STATUS_RUNNING,
    JOB_STATUS_SUCCEEDED,
    JOB_TYPE_BUILD_CHUNKS,
    ensure_job_record,
    set_partial_result,
    update_job,
)

logger = logging.getLogger(__name__)


def _browser_video_url(url: str) -> str:
    """签名 URL 给浏览器用：内网 endpoint 换公网 + 升 https。

    OSS 签名按资源路径计算、不含 host，换 host 后签名仍然有效。
    """
    u = (url or "").strip()
    if not u:
        return u
    u = u.replace("-internal.aliyuncs.com", ".aliyuncs.com")
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]
    return u


async def execute_build_job(
    *,
    task_id: str,
    ep_dir: str,
    item_name: str,
    force_rebuild: bool = False,
    worker_id: str | None = None,
    reply_queue: str | None = None,
    video_path: str | None = None,
    posehand_path: str | None = None,
) -> None:
    """执行构建并回写 Redis 任务状态 / 结果队列。"""
    await ensure_job_record(
        task_id=task_id,
        ep_dir=ep_dir,
        item_name=item_name,
        force_rebuild=force_rebuild,
        worker_id=worker_id,
        job_type=JOB_TYPE_BUILD_CHUNKS,
        reply_queue=reply_queue,
    )
    extra = {
        "worker_id": worker_id,
        "claimed_at": datetime.now().isoformat(),
    }
    if reply_queue:
        extra["reply_queue"] = reply_queue
    await update_job(
        task_id,
        status=JOB_STATUS_RUNNING,
        result=None,
        error=None,
        extra=extra,
        notify=True,
    )
    try:
        from app.services.play_cache import cache_enabled, sync_sview_job

        if cache_enabled():
            sync_sview_job(
                task_id,
                status="processing",
                kind="handvis",
                source=ep_dir,
                frames_done=0,
                frames_total=0,
                playable=0,
                cache_status="writing",
                error="",
            )
    except Exception as exc:
        logger.warning("初始化 sview:job 失败。task_id=%s err=%s", task_id, exc)

    loop = asyncio.get_running_loop()

    def _push_first_chunk(partial_result: dict) -> None:
        try:
            asyncio.run_coroutine_threadsafe(
                set_partial_result(task_id, partial_result),
                loop,
            )
        except Exception as exc:
            logger.warning("首 chunk 回调推送失败。task_id=%s err=%s", task_id, exc)
        # 视频签名 URL 尽早写进 sview:job，Master 模式下的页面不用等全部构建完。
        try:
            from app.services.play_cache import cache_enabled, sync_sview_job

            video_url = _browser_video_url(str((partial_result or {}).get("video_url") or ""))
            if cache_enabled() and video_url:
                sync_sview_job(task_id, video_url=video_url)
        except Exception:
            pass

    try:
        result = await build_chunks_impl(
            ep_dir,
            item_name=item_name,
            force_rebuild=force_rebuild,
            on_first_chunk_ready=_push_first_chunk,
            job_id=task_id,
            video_path=video_path,
            posehand_path=posehand_path,
        )
        await update_job(task_id, status=JOB_STATUS_SUCCEEDED, result=result, error=None)
        try:
            from app.services.play_cache import cache_enabled, sync_sview_job

            if cache_enabled():
                video_url = _browser_video_url(str((result or {}).get("video_url") or ""))
                if video_url:
                    sync_sview_job(task_id, video_url=video_url)
        except Exception:
            pass
        logger.info(
            "构建任务成功。task_id=%s ep_dir=%s item_name=%s worker_id=%s",
            task_id,
            ep_dir,
            item_name,
            worker_id,
        )
    except Exception as exc:
        logger.exception(
            "chunk 异步构建失败。ep_dir=%s item_name=%s task_id=%s worker_id=%s",
            ep_dir,
            item_name,
            task_id,
            worker_id,
        )
        await update_job(task_id, status=JOB_STATUS_FAILED, result=None, error=str(exc))
        try:
            from app.services.play_cache import cache_enabled, sync_sview_job

            if cache_enabled():
                sync_sview_job(task_id, status="failed", playable=0, error=str(exc))
        except Exception:
            pass


async def execute_job(message: dict, *, worker_id: str | None = None) -> None:
    """按 type 分发。未知类型记 failed，避免毒消息卡住队列。"""
    job_type = str(message.get("type") or JOB_TYPE_BUILD_CHUNKS)
    task_id = str(message.get("task_id") or "")
    if job_type != JOB_TYPE_BUILD_CHUNKS:
        await ensure_job_record(
            task_id=task_id,
            ep_dir=str(message.get("ep_dir") or ""),
            item_name=str(message.get("item_name") or ""),
            force_rebuild=False,
            worker_id=worker_id,
            job_type=job_type,
            reply_queue=message.get("reply_queue"),
        )
        await update_job(
            task_id,
            status=JOB_STATUS_FAILED,
            result=None,
            error=f"unsupported job type: {job_type}",
        )
        return

    await execute_build_job(
        task_id=task_id,
        ep_dir=message["ep_dir"],
        item_name=message["item_name"],
        force_rebuild=bool(message.get("force_rebuild")),
        worker_id=worker_id,
        reply_queue=message.get("reply_queue"),
        video_path=message.get("video_path"),
        posehand_path=message.get("posehand_path"),
    )
