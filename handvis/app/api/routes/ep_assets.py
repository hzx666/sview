"""EP 资产 API 路由。

职责：
- 查询或刷新 EP 资产元数据（缓存优先，OSS 直连兜底）
- 提供列表/详情接口给前端
- 支持资产名称编辑
"""

import logging

from fastapi import APIRouter, HTTPException, Query

from app.db.database import SessionLocal
from app.schemas.ep_asset import (
    DirectPairPreviewRequest,
    EpChildDirItem,
    EpChildDirListResponse,
    EpAssetDebugResponse,
    EpAssetItemResponse,
    EpAssetListResponse,
    EpAssetPreviewResponse,
    EpAssetNameUpdateRequest,
    EpAssetRefreshResponse,
    EpDirRequest,
    EpPreviewRequest,
)
from app.services.ep_asset_service import (
    count_ep_asset_items_by_ep_asset_ids,
    infer_ep_asset_name,
    list_cached_by_ep_dirs,
    list_ep_assets_paginated,
    replace_ep_asset_items,
    save_failed_scan,
    update_ep_asset_name,
    upsert_ep_asset,
)
from app.services.direct_pair_matcher import DIRECT_PAIR_LIMIT, match_manual_pair_prefixes
from app.services.remote_scanner import (
    ensure_leaf_ep_dir,
    preview_ep_dir_over_oss,
    scan_ep_children_over_oss,
    scan_ep_dir_over_oss,
    validate_ep_dir,
)
from app.services.oss_path_utils import normalize_oss_path

router = APIRouter(prefix="/api/ep-assets", tags=["ep-assets"])
logger = logging.getLogger(__name__)


def split_preview_input(raw_path: str) -> tuple[str, str]:
    """把首页输入拆成 OSS 父目录与文件名前缀。"""
    raw = (raw_path or "").strip()
    if raw.endswith("/"):
        return validate_ep_dir(raw), ""

    try:
        normalized = normalize_oss_path(raw, require_trailing_slash=False)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid ep_dir: {exc}") from exc

    parent, prefix = normalized.rsplit("/", 1)
    parent_dir = f"{parent}/"
    if not prefix:
        raise HTTPException(status_code=400, detail="Invalid ep_dir: prefix cannot be empty")
    return parent_dir, prefix


def build_ep_asset_refresh_response(
    *,
    name: str,
    ep_dir: str,
    items: list[EpAssetItemResponse],
    source: str,
    updated_at,
) -> EpAssetRefreshResponse:
    """构造刷新响应。"""
    return EpAssetRefreshResponse(
        name=name,
        ep_dir=ep_dir,
        items=items,
        source=source,
        updated_at=updated_at,
    )


@router.post("/preview", response_model=EpAssetPreviewResponse)
async def preview_ep_assets(payload: EpPreviewRequest) -> EpAssetPreviewResponse:
    """只读预览当前 OSS 目录候选项，不写入数据库。"""
    ep_dir, prefix = split_preview_input(payload.normalized_ep_dir)
    page = max(1, int(payload.page or 1))
    page_size = max(1, min(100, int(payload.page_size or payload.limit or 50)))
    offset = (page - 1) * page_size
    result, _source_host = preview_ep_dir_over_oss(
        ep_dir,
        prefix=prefix,
        limit=page_size,
        cursor=payload.cursor or "",
    )
    child_items = [
        EpChildDirItem(name=infer_ep_asset_name(child_dir), ep_dir=child_dir, cached=False)
        for child_dir in result.child_dirs
    ]
    pair_items = [
        EpAssetItemResponse(
            item_name=pair.item_name,
            video_path=pair.video_path,
            posehand_path=pair.posehand_path,
        )
        for pair in result.pairs
    ]
    return EpAssetPreviewResponse(
        ep_dir=ep_dir,
        prefix=prefix,
        limit=page_size,
        page=page,
        page_size=page_size,
        offset=offset,
        cursor=payload.cursor or "",
        next_cursor=result.next_cursor,
        has_more=result.has_more,
        items=pair_items,
        child_dirs=child_items,
        total=len(pair_items) + len(child_items),
        scanned_lines=result.scanned_lines,
        truncated=result.truncated,
        pair_truncated=result.pair_truncated,
        message=result.message,
    )


@router.post("/preview-pair", response_model=EpAssetPreviewResponse)
async def preview_manual_pair(payload: DirectPairPreviewRequest) -> EpAssetPreviewResponse:
    """根据手动输入的视频前缀与 posehand 前缀生成配对预览。"""
    ep_dir, pairs, message = match_manual_pair_prefixes(
        payload.normalized_video_path,
        payload.normalized_posehand_path,
        limit=DIRECT_PAIR_LIMIT,
    )
    return EpAssetPreviewResponse(
        ep_dir=ep_dir,
        prefix="",
        limit=DIRECT_PAIR_LIMIT,
        page=1,
        page_size=DIRECT_PAIR_LIMIT,
        offset=0,
        cursor="",
        next_cursor="",
        has_more=False,
        source="manual-pair",
        items=[
            EpAssetItemResponse(
                item_name=pair.item_name,
                video_path=pair.video_path,
                posehand_path=pair.posehand_path,
            )
            for pair in pairs
        ],
        child_dirs=[],
        total=len(pairs),
        scanned_lines=0,
        truncated=False,
        pair_truncated=False,
        message=message,
    )


@router.post("/refresh", response_model=EpAssetRefreshResponse)
async def refresh_ep_assets(payload: EpDirRequest) -> EpAssetRefreshResponse:
    """强制直连 OSS 刷新单个 EP 资产并落库。"""
    ep_dir = ensure_leaf_ep_dir(payload.normalized_ep_dir)
    logger.info("开始刷新 EP 资产。ep_dir=%s", ep_dir)

    try:
        pairs, source_host = scan_ep_dir_over_oss(ep_dir)

        async with SessionLocal() as session:
            saved = await upsert_ep_asset(
                session,
                ep_dir=ep_dir,
                source_host=source_host,
                scan_status="ok",
                scan_error=None,
                video_path=pairs[0].video_path,
                posehand_path=pairs[0].posehand_path,
            )
            saved_items = await replace_ep_asset_items(session, ep_asset_id=saved.id, pairs=pairs)

        logger.info(
            "刷新 EP 资产成功。ep_dir=%s asset_id=%s item_count=%s source_host=%s",
            ep_dir,
            saved.id,
            len(saved_items),
            source_host,
        )
        return build_ep_asset_refresh_response(
            name=saved.name,
            ep_dir=saved.ep_dir,
            items=[
                EpAssetItemResponse(
                    item_name=item.item_name,
                    video_path=item.video_path,
                    posehand_path=item.posehand_path,
                )
                for item in saved_items
            ],
            source="oss-sdk",
            updated_at=saved.updated_at,
        )
    except HTTPException as exc:
        logger.warning("刷新 EP 资产失败。ep_dir=%s detail=%s", ep_dir, exc.detail)
        async with SessionLocal() as session:
            await save_failed_scan(session, ep_dir, str(exc.detail))
        raise


@router.post("/discover-children", response_model=EpChildDirListResponse)
async def discover_ep_children(payload: EpDirRequest) -> EpChildDirListResponse:
    """解析父目录下一层 ep 子目录，并合并缓存信息。"""
    parent_dir = validate_ep_dir(payload.normalized_ep_dir)
    logger.info("开始解析父目录子 EP。parent_dir=%s", parent_dir)
    ep_dirs, _source_host = scan_ep_children_over_oss(parent_dir)

    async with SessionLocal() as session:
        cached_rows = await list_cached_by_ep_dirs(session, ep_dirs)
        count_map = await count_ep_asset_items_by_ep_asset_ids(session, [row.id for row in cached_rows])
    cached_map = {row.ep_dir: row for row in cached_rows}

    items: list[EpChildDirItem] = []
    for ep_dir in ep_dirs:
        cached = cached_map.get(ep_dir)
        resolved_name = ((cached.name if cached else "") or "").strip() or infer_ep_asset_name(ep_dir)
        items.append(
            EpChildDirItem(
                name=resolved_name,
                ep_dir=ep_dir,
                cached=bool(cached and cached.scan_status == "ok" and int(count_map.get(cached.id, 0)) > 0),
            )
        )

    logger.info("父目录子 EP 解析完成。parent_dir=%s count=%s", parent_dir, len(items))
    return EpChildDirListResponse(parent_dir=parent_dir, total=len(items), items=items)


@router.get("/list", response_model=EpAssetListResponse)
async def get_ep_assets_list(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
) -> EpAssetListResponse:
    """分页获取 EP 资产列表（前端表格使用）。"""
    async with SessionLocal() as session:
        total, rows = await list_ep_assets_paginated(
            session,
            page=page,
            page_size=page_size,
        )
        count_map = await count_ep_asset_items_by_ep_asset_ids(session, [x.id for x in rows])
        return EpAssetListResponse(
            total=total,
            page=page,
            page_size=page_size,
            items=[
                EpAssetDebugResponse.model_validate(
                    {
                        "id": x.id,
                        "name": x.name,
                        "ep_dir": x.ep_dir,
                        "video_path": x.video_path,
                        "posehand_path": x.posehand_path,
                        "items_count": int(count_map.get(x.id, 0)),
                        "source_host": x.source_host,
                        "scan_status": x.scan_status,
                        "scan_error": x.scan_error,
                        "created_at": x.created_at,
                        "updated_at": x.updated_at,
                    }
                )
                for x in rows
            ],
        )


@router.patch("/id/{asset_id}/name", response_model=EpAssetDebugResponse)
async def patch_ep_asset_name(asset_id: int, payload: EpAssetNameUpdateRequest) -> EpAssetDebugResponse:
    """更新单个 EP 资产展示名称。"""
    async with SessionLocal() as session:
        updated = await update_ep_asset_name(
            session,
            asset_id=asset_id,
            name=payload.normalized_name,
        )
        if updated is None:
            logger.warning("更新 EP 名称失败：资产不存在。asset_id=%s", asset_id)
            raise HTTPException(status_code=404, detail="Asset not found")
        logger.info("更新 EP 名称成功。asset_id=%s", asset_id)
        return EpAssetDebugResponse.model_validate(updated)
