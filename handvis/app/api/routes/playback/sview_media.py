"""把 SView Master 同款 /api/jobs/{id}/ 媒体路径接到 HandVis HTTP。

Worker 推到 OSS player-bin 后，本仓库播放页仍打 HandVis :8000；
这些路由从 OSS 回源，路径与 sview_http 一致。
"""

from __future__ import annotations

import asyncio
import re

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from app.services.play_cache import cache_enabled, get_bytes

router = APIRouter(prefix="/api/jobs", tags=["sview-media"])
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,200}$")
_SEG_RE = re.compile(r"^[0-9]{6}\.bin$")


def _check_job_id(job_id: str) -> None:
    if not _JOB_ID_RE.match(job_id or ""):
        raise HTTPException(status_code=400, detail="invalid job_id")


async def _oss_or_404(job_id: str, *parts: str, content_type: str) -> Response:
    if not cache_enabled():
        raise HTTPException(status_code=404, detail="OSS_CACHE disabled")
    data = await asyncio.to_thread(get_bytes, job_id, *parts)
    if not data:
        raise HTTPException(status_code=404, detail="object not found")
    cache = "no-store" if content_type.startswith("application/json") else "public, max-age=86400, immutable"
    return Response(
        content=data,
        media_type=content_type,
        headers={"Cache-Control": cache},
    )


@router.get("/{job_id}/index.json")
async def get_index(job_id: str) -> Response:
    _check_job_id(job_id)
    return await _oss_or_404(job_id, "index.json", content_type="application/json; charset=utf-8")


@router.get("/{job_id}/manifest.json")
async def get_manifest(job_id: str) -> Response:
    _check_job_id(job_id)
    return await _oss_or_404(job_id, "manifest.json", content_type="application/json; charset=utf-8")


@router.get("/{job_id}/faces.u32")
async def get_faces(job_id: str) -> Response:
    _check_job_id(job_id)
    return await _oss_or_404(job_id, "faces.u32", content_type="application/octet-stream")


@router.get("/{job_id}/video_projection.bin")
async def get_video_projection(job_id: str) -> Response:
    _check_job_id(job_id)
    return await _oss_or_404(job_id, "video_projection.bin", content_type="application/octet-stream")


@router.get("/{job_id}/segments/{name}")
async def get_segment(job_id: str, name: str) -> Response:
    _check_job_id(job_id)
    if not _SEG_RE.match(name):
        raise HTTPException(status_code=400, detail="invalid segment name")
    return await _oss_or_404(job_id, "segments", name, content_type="application/octet-stream")
