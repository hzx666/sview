"""EP 资产解析服务：缓存优先，远程扫描兜底。

无 PG 模式（消息里已带 video_path/posehand_path 时不走这里）。
"""

import asyncio
import logging
from types import SimpleNamespace

from fastapi import HTTPException

from app.core.config import HANDVIS_DISABLE_PG

if not HANDVIS_DISABLE_PG:
    from app.db.database import SessionLocal
    from app.services.ep_asset_service import (
        get_cached_by_ep_dir,
        get_ep_asset_item_by_ep_asset_id_and_name,
        replace_ep_asset_items,
        save_failed_scan,
        upsert_ep_asset,
    )
from app.services.remote_scanner import scan_ep_dir_over_oss

logger = logging.getLogger(__name__)


async def resolve_asset_by_ep_dir_and_item_name(ep_dir: str, item_name: str):
    """按 ep_dir + item_name 解析资产。

    有 PG：缓存优先，OSS 扫描兜底并写回。
    无 PG（HANDVIS_DISABLE_PG=1）：每次直连 OSS 扫描，返回轻量 item。
    """
    normalized_item_name = (item_name or "").strip()
    if not normalized_item_name:
        raise HTTPException(status_code=422, detail="item_name is required")

    if HANDVIS_DISABLE_PG:
        pairs, _ = await asyncio.to_thread(
            scan_ep_dir_over_oss, ep_dir, prefix=normalized_item_name
        )
        match = next(
            (p for p in pairs if p.item_name == normalized_item_name),
            None,
        )
        if match is None or not match.video_path:
            raise HTTPException(
                status_code=422,
                detail=f"item_name not found under ep_dir: {normalized_item_name}",
            )
        item = SimpleNamespace(
            id=0,
            media_id=None,
            item_name=match.item_name,
            video_path=match.video_path,
            posehand_path=match.posehand_path or "",
        )
        return None, item, "oss"

    async with SessionLocal() as session:
        cached = await get_cached_by_ep_dir(session, ep_dir)
        if cached and cached.scan_status == "ok":
            # 先走缓存：避免重复远程 SSH 扫描，减少请求时延。
            cached_item = await get_ep_asset_item_by_ep_asset_id_and_name(
                session,
                ep_asset_id=cached.id,
                item_name=normalized_item_name,
            )
            if cached_item and cached_item.video_path:
                return cached, cached_item, "cache"

    try:
        logger.info("资产缓存未命中，开始直连 OSS 扫描。ep_dir=%s item_name=%s", ep_dir, normalized_item_name)
        pairs, source_host = await asyncio.to_thread(scan_ep_dir_over_oss, ep_dir, prefix=normalized_item_name)

        async with SessionLocal() as session:
            # 扫描结果先更新主表，再全量替换子配对项，保证两层数据一致。
            saved = await upsert_ep_asset(
                session,
                ep_dir=ep_dir,
                source_host=source_host,
                scan_status="ok",
                scan_error=None,
                video_path=pairs[0].video_path,
                posehand_path=pairs[0].posehand_path,
            )
            await replace_ep_asset_items(session, ep_asset_id=saved.id, pairs=pairs)
            resolved_item = await get_ep_asset_item_by_ep_asset_id_and_name(
                session,
                ep_asset_id=saved.id,
                item_name=normalized_item_name,
            )
            if resolved_item is None:
                raise HTTPException(
                    status_code=422,
                    detail=f"item_name not found under ep_dir: {normalized_item_name}",
                )

        logger.info(
            "直连 OSS 扫描成功并写入缓存。ep_dir=%s asset_id=%s item_name=%s",
            ep_dir,
            saved.id,
            normalized_item_name,
        )
        return saved, resolved_item, "remote"
    except HTTPException as exc:
        logger.warning("解析资产失败。ep_dir=%s item_name=%s detail=%s", ep_dir, normalized_item_name, exc.detail)
        async with SessionLocal() as session:
            await save_failed_scan(session, ep_dir, str(exc.detail))
        raise
