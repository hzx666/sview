"""chunk 构建任务 SSE 流接口。"""

import asyncio
import json
import time

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from .jobs import get_build_job


def _sse_event(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


async def get_build_task_stream(task_id: str, request: Request) -> StreamingResponse:
    """持续推送 chunk 构建任务状态，直到成功或失败。"""
    if not str(task_id or "").strip():
        raise HTTPException(status_code=400, detail="invalid task_id")

    first = await get_build_job(task_id)
    if not first:
        raise HTTPException(status_code=404, detail="Build task not found")

    async def _event_generator():
        last_payload: dict | None = None
        last_ping_at = time.monotonic()
        while True:
            if await request.is_disconnected():
                return

            status = await get_build_job(task_id)
            if status != last_payload:
                yield _sse_event("status", status)
                last_payload = status

            state = str(status.get("status") or "").strip().lower()
            if state in {"succeeded", "failed"}:
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
