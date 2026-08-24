"""播放资产查询与本地 chunk 文件读取。"""

import mimetypes

from fastapi import HTTPException
from fastapi.responses import Response

from app.db.database import SessionLocal
from app.services.playback_asset_service import get_playback_asset_by_unique_id

from .paths import local_chunk_root_url, local_index_url, local_posthands_root


async def get_playback_asset_payload(unique_id: str) -> dict:
    """按 unique_id 获取播放资产元数据。"""
    async with SessionLocal() as session:
        row = await get_playback_asset_by_unique_id(session, unique_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Playback asset not found")

    return {
        "id": row.id,
        "unique_id": row.unique_id,
        "ep_asset_id": row.ep_asset_id,
        "ep_asset_item_id": row.ep_asset_item_id,
        "source_name": row.source_name,
        # 播放链路统一走本地静态目录，避免回退到 OSS chunk 读取。
        "chunk_root": local_chunk_root_url(unique_id),
        "index_path": local_index_url(unique_id),
        "index_url": local_index_url(unique_id),
        "preset": row.preset,
        "fps": row.fps,
        "manifest_version": row.manifest_version,
        "default_quality": row.default_quality,
        "face_count": row.face_count,
        "stride": row.stride,
        "frame_count": row.frame_count,
        "vertex_count": row.vertex_count,
        "total_duration": row.total_duration,
        "chunk_duration": row.chunk_duration,
        "chunk_count": row.chunk_count,
        "updated_at": row.updated_at,
    }


async def get_chunk_file_response(unique_id: str, file_path: str) -> Response:
    """读取并返回本地 chunk 文件（仅本地 posehand）。"""
    normalized = (file_path or "").strip().lstrip("/")
    if not normalized:
        raise HTTPException(status_code=400, detail="file_path is required")

    async with SessionLocal() as session:
        row = await get_playback_asset_by_unique_id(session, unique_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Playback asset not found")

    ext_hint = normalized
    local_root = local_posthands_root(unique_id).resolve()
    if normalized == "manifest.json":
        target = (local_root / "manifest.json").resolve()
        ext_hint = "manifest.json"
    else:
        target = (local_root / "chunks" / normalized).resolve()

    # 防路径穿越：目标必须在 unique_id 根目录下。
    if local_root != target and local_root not in target.parents:
        raise HTTPException(status_code=400, detail="Invalid chunk file path")
    if not target.exists() or not target.is_file():
        raise HTTPException(
            status_code=404,
            detail="Local chunk file not found. Please rebuild chunks to regenerate local assets.",
        )

    data = target.read_bytes()
    media_type = mimetypes.guess_type(ext_hint)[0] or "application/octet-stream"
    return Response(content=data, media_type=media_type)
