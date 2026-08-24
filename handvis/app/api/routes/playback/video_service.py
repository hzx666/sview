"""视频 URL 解析：优先直连可播视频，不可播再入转码链路。

无 PG 模式（HANDVIS_DISABLE_PG=1）：只探测可播性；不可播返回 failed 但不阻断
mesh 构建（视频只是叠加层，转码可交给 SView 视频链路）。
"""

import asyncio
import logging

from app.core.config import HANDVIS_DISABLE_PG, TRANSCODE_VIDEO_DIR

if not HANDVIS_DISABLE_PG:
    from app.db.database import SessionLocal
    from app.services.media_asset_service import (
        ensure_media_asset_for_item,
        update_media_probe_and_status,
    )
    from app.services.transcode_queue import submit_transcode_task
from app.services.oss_signer import sign_video_url
from app.services.video_transcode_service import ffprobe_summary, is_browser_playable

logger = logging.getLogger(__name__)


def _probe_dimensions(probe_payload: dict | None) -> dict:
    """从 ffprobe 摘要里取视频宽高，取不到则返回空值。"""
    probe = probe_payload or {}
    width = int(probe.get("width") or 0)
    height = int(probe.get("height") or 0)
    return {"width": width if width > 0 else None, "height": height if height > 0 else None}


async def resolve_video_for_item(item) -> dict:
    """解析视频可播状态。

    返回字段：
      - media_id: media_assets.id（无 PG 模式恒为 0）
      - video_status: 'ready' | 'queued' | 'running' | 'failed'
      - video_url: 可播放 URL（ready 时必有；其他状态可能为 None）
      - error: 失败原因（仅 failed）
    """
    source_path = (item.video_path or "").strip()
    if not source_path:
        return {"media_id": 0, "video_status": "failed", "video_url": None, "error": "missing video_path"}

    signed_source_url = sign_video_url(source_path)

    # 1) 快路径：先探测源视频可播性。可播则直接返回，不查不写 DB。
    probe_payload: dict | None = None
    try:
        probe_payload = await asyncio.to_thread(ffprobe_summary, signed_source_url)
        if is_browser_playable(probe_payload):
            return {
                "media_id": 0,
                "video_status": "ready",
                "video_url": signed_source_url,
                "error": None,
                **_probe_dimensions(probe_payload),
            }
    except Exception as exc:
        logger.warning("视频探测失败。item_id=%s error=%s", getattr(item, "id", 0), exc)
        if HANDVIS_DISABLE_PG:
            # 视频只是叠加层：探测失败不阻断 mesh 构建。
            return {
                "media_id": 0,
                "video_status": "failed",
                "video_url": None,
                "error": f"video probe failed (no-PG mode, no transcode): {exc}",
            }

    if HANDVIS_DISABLE_PG:
        return {
            "media_id": 0,
            "video_status": "failed",
            "video_url": None,
            "error": "视频浏览器不可播；无 PG 模式不转码（可交给 SView 视频链路）。",
            **_probe_dimensions(probe_payload),
        }

    # 2) 慢路径：不可播/探测失败 → 落库并入转码队列（SSE 推送状态）。
    async with SessionLocal() as session:
        media = await ensure_media_asset_for_item(
            session,
            item_id=item.id,
            source_url=source_path,
            media_id=item.media_id,
        )
        media_id = int(media.id)

        # 已有转码产物且文件存在时直接复用。
        existing_web = (media.web_url or "").strip()
        if existing_web:
            file_name = existing_web.rsplit("/", 1)[-1]
            if (TRANSCODE_VIDEO_DIR / file_name).is_file():
                return {
                    "media_id": media_id,
                    "video_status": "ready",
                    "video_url": existing_web,
                    "error": None,
                    **_probe_dimensions(probe_payload),
                }

    if probe_payload is not None:
        async with SessionLocal() as session:
            await update_media_probe_and_status(
                session,
                media_id=media_id,
                transcode_status="pending",
                probe_payload=probe_payload,
                transcode_error=None,
            )

    submitted = await submit_transcode_task(
        media_id=media_id,
        item_id=item.id,
        source_url=source_path,
    )
    return {
        "media_id": media_id,
        "video_status": submitted.get("status") or "queued",
        "video_url": submitted.get("video_url"),
        "error": submitted.get("error"),
        **_probe_dimensions(probe_payload),
    }
