"""视频转码状态 SSE 流接口。"""

import asyncio
import json
import logging
import time

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from app.core.config import TRANSCODE_WAIT_TIMEOUT_SECONDS

from .video_status import query_video_status

logger = logging.getLogger(__name__)


def _sse_event(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


async def get_video_status_stream(media_id: int, request: Request) -> StreamingResponse:
    """持续推送视频转码状态，直到成功/失败/超时。"""
    if media_id <= 0:
        raise HTTPException(status_code=400, detail="invalid media_id")

    first = await query_video_status(media_id)
    if first is None:
        raise HTTPException(status_code=404, detail="media not found")

    async def _event_generator():
        start_at = time.monotonic()
        last_payload: dict | None = None
        last_ping_at = start_at

        while True:
            if await request.is_disconnected():
                return

            status = await query_video_status(media_id)
            if status is None:
                yield _sse_event(
                    "status",
                    {
                        "media_id": media_id,
                        "status": "failed",
                        "video_url": None,
                        "error": "media not found",
                        "updated_at": "",
                    },
                )
                return

            if status != last_payload:
                yield _sse_event("status", status)
                last_payload = status

            current_status = str(status.get("status") or "").strip().lower()
            if current_status in {"success", "failed"}:
                return

            elapsed = time.monotonic() - start_at
            if elapsed >= float(TRANSCODE_WAIT_TIMEOUT_SECONDS):
                yield _sse_event(
                    "status",
                    {
                        "media_id": media_id,
                        "status": "failed",
                        "video_url": None,
                        "error": "视频转码等待超时，请稍后重试",
                        "updated_at": "",
                    },
                )
                return

            now = time.monotonic()
            if now - last_ping_at >= 15:
                yield _sse_event("ping", {"ts": int(time.time())})
                last_ping_at = now

            await asyncio.sleep(0.5)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(_event_generator(), media_type="text/event-stream", headers=headers)
