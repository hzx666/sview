"""媒体资产服务层。

职责：
- 获取或创建 media_assets 记录。
- 绑定 ep_asset_items.media_id。
- 更新转码状态与可播 URL。
"""

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.timezone import shanghai_now_naive
from app.models.ep_asset_item import EpAssetItem
from app.models.media_asset import MediaAsset


async def get_media_asset_by_id(session: AsyncSession, media_id: int) -> MediaAsset | None:
    result = await session.execute(select(MediaAsset).where(MediaAsset.id == media_id))
    return result.scalar_one_or_none()


async def get_media_asset_by_source_url(session: AsyncSession, source_url: str) -> MediaAsset | None:
    result = await session.execute(
        select(MediaAsset)
        .where(MediaAsset.source_url == source_url)
        .order_by(MediaAsset.id.asc())
    )
    return result.scalars().first()


async def ensure_media_asset_for_item(
    session: AsyncSession,
    *,
    item_id: int,
    source_url: str,
    media_id: int | None,
) -> MediaAsset:
    """确保 item 对应 media_assets 记录存在，并回写 media_id。"""
    cleaned_source = (source_url or "").strip()
    now_ts = shanghai_now_naive()

    if media_id:
        media = await get_media_asset_by_id(session, int(media_id))
        if media is not None:
            if media.source_url != cleaned_source:
                media.source_url = cleaned_source
                media.updated_at = now_ts
            await _bind_item_media_id(session, item_id=item_id, media_id=media.id)
            await session.commit()
            await session.refresh(media)
            return media

    media = await get_media_asset_by_source_url(session, cleaned_source)
    if media is None:
        media = MediaAsset(
            source_url=cleaned_source,
            web_url=None,
            transcode_status="pending",
            transcode_error=None,
            probe_json=None,
        )
        session.add(media)
        await session.flush()

    await _bind_item_media_id(session, item_id=item_id, media_id=media.id)
    await session.commit()
    await session.refresh(media)
    return media


async def update_media_probe_and_status(
    session: AsyncSession,
    *,
    media_id: int,
    transcode_status: str,
    probe_payload: dict | None,
    transcode_error: str | None,
    web_url: str | None = None,
) -> MediaAsset | None:
    """更新探测/转码状态。"""
    media = await get_media_asset_by_id(session, media_id)
    if media is None:
        return None

    media.transcode_status = transcode_status
    media.transcode_error = (transcode_error or "").strip() or None
    media.probe_json = json.dumps(probe_payload, ensure_ascii=False) if probe_payload else media.probe_json
    if web_url is not None:
        media.web_url = (web_url or "").strip() or None
    media.updated_at = shanghai_now_naive()

    await session.commit()
    await session.refresh(media)
    return media


async def _bind_item_media_id(session: AsyncSession, *, item_id: int, media_id: int) -> None:
    item = await session.get(EpAssetItem, item_id)
    if item is None:
        return
    if item.media_id == media_id:
        return
    item.media_id = media_id
    item.updated_at = shanghai_now_naive()
