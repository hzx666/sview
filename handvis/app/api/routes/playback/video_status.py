"""视频转码状态查询接口。

前端在收到 video_status='queued'/'running' 时通过本接口轮询，
转码完成后获取最终 video_url。
"""

import logging

from fastapi import HTTPException

from app.db.database import SessionLocal
from app.services.media_asset_service import get_media_asset_by_id
from app.services.transcode_queue import get_transcode_status

logger = logging.getLogger(__name__)


async def query_video_status(media_id: int) -> dict | None:
    """查询视频状态快照。

    返回 None 表示 media 不存在。
    """
    if media_id <= 0:
        return None

    redis_status = await get_transcode_status(media_id)
    if redis_status:
        return redis_status

    async with SessionLocal() as session:
        media = await get_media_asset_by_id(session, media_id)
    if media is None:
        return None

    return {
        "media_id": int(media.id),
        "status": str(media.transcode_status or "pending"),
        "video_url": (media.web_url or "").strip() or None,
        "error": (media.transcode_error or "").strip() or None,
        "updated_at": media.updated_at.isoformat() if media.updated_at else "",
    }


async def get_video_status_payload(media_id: int) -> dict:
    """优先读 Redis 任务状态，回退查 DB（覆盖 Redis 已过期的情况）。"""
    if media_id <= 0:
        raise HTTPException(status_code=400, detail="invalid media_id")

    status = await query_video_status(media_id)
    if status is None:
        raise HTTPException(status_code=404, detail="media not found")
    return status
