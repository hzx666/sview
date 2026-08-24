"""播放资产服务层。

负责 playback_assets 表的写入与查询。
"""

import logging

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.timezone import shanghai_now_naive
from app.models.playback_asset import PlaybackAsset

logger = logging.getLogger(__name__)


async def upsert_playback_asset(
    session: AsyncSession,
    *,
    ep_asset_id: int,
    ep_asset_item_id: int,
    unique_id: str,
    source_name: str | None,
    chunk_root: str,
    index_path: str,
    preset: str,
    fps: int,
    manifest_version: int,
    default_quality: str,
    face_count: int,
    stride: int,
    frame_count: int,
    vertex_count: int,
    total_duration: float,
    chunk_duration: float,
    chunk_count: int,
) -> PlaybackAsset:
    """按 ep_asset_item_id 执行 upsert，并返回最新记录。"""
    now_ts = shanghai_now_naive()
    # 以 ep_asset_item_id 作为冲突键：
    # 同一配对组重复构建时，仅覆盖元信息，不新增脏记录。
    stmt = insert(PlaybackAsset).values(
        ep_asset_id=ep_asset_id,
        ep_asset_item_id=ep_asset_item_id,
        unique_id=unique_id,
        source_name=source_name,
        chunk_root=chunk_root,
        index_path=index_path,
        preset=preset,
        fps=fps,
        manifest_version=manifest_version,
        default_quality=default_quality,
        face_count=face_count,
        stride=stride,
        frame_count=frame_count,
        vertex_count=vertex_count,
        total_duration=total_duration,
        chunk_duration=chunk_duration,
        chunk_count=chunk_count,
        updated_at=now_ts,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[PlaybackAsset.ep_asset_item_id],
        set_={
            "ep_asset_id": ep_asset_id,
            "unique_id": unique_id,
            "source_name": source_name,
            "chunk_root": chunk_root,
            "index_path": index_path,
            "preset": preset,
            "fps": fps,
            "manifest_version": manifest_version,
            "default_quality": default_quality,
            "face_count": face_count,
            "stride": stride,
            "frame_count": frame_count,
            "vertex_count": vertex_count,
            "total_duration": total_duration,
            "chunk_duration": chunk_duration,
            "chunk_count": chunk_count,
            "updated_at": now_ts,
        },
    )
    await session.execute(stmt)
    await session.commit()

    # 统一回查一次，确保上层拿到数据库实际落盘后的最新记录。
    result = await session.execute(select(PlaybackAsset).where(PlaybackAsset.ep_asset_item_id == ep_asset_item_id))
    row = result.scalar_one_or_none()
    if row is None:
        logger.warning("upsert_playback_asset 后未找到记录。ep_asset_item_id=%s", ep_asset_item_id)
        raise HTTPException(status_code=500, detail="Upsert playback asset failed")
    return row


async def get_playback_asset_by_unique_id(session: AsyncSession, unique_id: str) -> PlaybackAsset | None:
    """按 unique_id 查询播放资产。"""
    result = await session.execute(select(PlaybackAsset).where(PlaybackAsset.unique_id == unique_id))
    return result.scalar_one_or_none()


async def get_playback_asset_by_ep_asset_item_id(session: AsyncSession, ep_asset_item_id: int) -> PlaybackAsset | None:
    """按 ep_asset_item_id 查询播放资产。"""
    result = await session.execute(
        select(PlaybackAsset).where(PlaybackAsset.ep_asset_item_id == ep_asset_item_id)
    )
    return result.scalar_one_or_none()
