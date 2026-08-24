"""EP 资产服务层。

职责：
- 资产命名推断
- EP 资产查询/写入/分页
- 扫描失败状态回写
"""

import logging

from fastapi import HTTPException
from sqlalchemy import delete, desc, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.timezone import shanghai_now_naive
from app.models.ep_asset import EpAsset
from app.models.ep_asset_item import EpAssetItem
from app.services.remote_scanner import PairedEpAsset

logger = logging.getLogger(__name__)


def infer_ep_asset_name(ep_dir: str) -> str:
    """根据 ep_dir 推断默认展示名称。"""
    raw = (ep_dir or "").strip().rstrip("/")
    if not raw:
        return ""

    path = raw
    if path.startswith("oss://"):
        without_scheme = path[len("oss://") :]
        slash = without_scheme.find("/")
        path = without_scheme[slash + 1 :] if slash >= 0 else ""

    parts = [p for p in path.split("/") if p]
    if not parts:
        return ""

    def _join_tail(start_idx: int) -> str:
        if start_idx >= len(parts):
            return ""
        return "/".join(parts[start_idx:]) + "/"

    for idx, token in enumerate(parts):
        if token.lower() == "dataset":
            # 约定路径：
            # .../dataset/<domain>/<name>/<uuid>/epX/
            # 默认名称：
            # <name>/<uuid>/epX/
            if idx + 2 < len(parts):
                return _join_tail(idx + 2)
            if idx + 1 < len(parts):
                return _join_tail(idx + 1)

    if len(parts) >= 2:
        return _join_tail(1)
    return _join_tail(0)


async def get_cached_by_ep_dir(session: AsyncSession, ep_dir: str) -> EpAsset | None:
    """按 ep_dir 查询缓存资产。"""
    result = await session.execute(select(EpAsset).where(EpAsset.ep_dir == ep_dir))
    return result.scalar_one_or_none()


async def list_cached_by_ep_dirs(session: AsyncSession, ep_dirs: list[str]) -> list[EpAsset]:
    """按 ep_dir 批量查询缓存资产。"""
    normalized = [str(x or "").strip() for x in ep_dirs if str(x or "").strip()]
    if not normalized:
        return []
    result = await session.execute(select(EpAsset).where(EpAsset.ep_dir.in_(normalized)))
    return list(result.scalars().all())


async def upsert_ep_asset(
    session: AsyncSession,
    *,
    ep_dir: str,
    source_host: str,
    scan_status: str,
    scan_error: str | None,
    video_path: str | None,
    posehand_path: str | None,
) -> EpAsset:
    """写入或更新 EP 资产。"""
    existing = await get_cached_by_ep_dir(session, ep_dir)
    default_name = infer_ep_asset_name(ep_dir)
    resolved_name = ((existing.name if existing else "") or "").strip() or default_name

    now_ts = shanghai_now_naive()
    stmt = insert(EpAsset).values(
        name=resolved_name,
        ep_dir=ep_dir,
        video_path=video_path,
        posehand_path=posehand_path,
        source_host=source_host,
        scan_status=scan_status,
        scan_error=scan_error,
        updated_at=now_ts,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[EpAsset.ep_dir],
        set_={
            "name": resolved_name,
            "video_path": video_path,
            "posehand_path": posehand_path,
            "source_host": source_host,
            "scan_status": scan_status,
            "scan_error": scan_error,
            "updated_at": now_ts,
        },
    )

    await session.execute(stmt)
    await session.commit()
    refreshed = await get_cached_by_ep_dir(session, ep_dir)
    if refreshed is None:
        logger.warning("upsert_ep_asset 后未找到记录。ep_dir=%s", ep_dir)
        raise HTTPException(status_code=500, detail="Upsert succeeded but row not found")
    return refreshed


async def replace_ep_asset_items(
    session: AsyncSession,
    *,
    ep_asset_id: int,
    pairs: list[PairedEpAsset],
) -> list[EpAssetItem]:
    """按最新扫描结果全量替换指定 EP 的配对条目。"""
    # 采用“先删后写”的原因：
    # 远程目录可能发生重命名/删文件，仅 upsert 会留下历史孤儿条目。
    await session.execute(delete(EpAssetItem).where(EpAssetItem.ep_asset_id == ep_asset_id))
    now_ts = shanghai_now_naive()

    for pair in pairs:
        # 这里保留 upsert 语义，便于兼容并发刷新时的冲突重试。
        stmt = insert(EpAssetItem).values(
            ep_asset_id=ep_asset_id,
            item_name=pair.item_name,
            video_path=pair.video_path,
            posehand_path=pair.posehand_path,
            scan_status="ok",
            scan_error=None,
            updated_at=now_ts,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[EpAssetItem.ep_asset_id, EpAssetItem.item_name],
            set_={
                "video_path": pair.video_path,
                "posehand_path": pair.posehand_path,
                "scan_status": "ok",
                "scan_error": None,
                "updated_at": now_ts,
            },
        )
        await session.execute(stmt)

    await session.commit()
    # 固定按 item_name 返回，前端展示顺序可预测，避免每次刷新抖动。
    result = await session.execute(
        select(EpAssetItem)
        .where(EpAssetItem.ep_asset_id == ep_asset_id)
        .order_by(EpAssetItem.item_name.asc(), EpAssetItem.id.asc())
    )
    return list(result.scalars().all())


async def upsert_ep_asset_item(
    session: AsyncSession,
    *,
    ep_asset_id: int,
    pair: PairedEpAsset,
) -> EpAssetItem:
    """写入或更新单个 EP 配对条目，不影响同目录其他缓存。"""
    now_ts = shanghai_now_naive()
    stmt = insert(EpAssetItem).values(
        ep_asset_id=ep_asset_id,
        item_name=pair.item_name,
        video_path=pair.video_path,
        posehand_path=pair.posehand_path,
        scan_status="ok",
        scan_error=None,
        updated_at=now_ts,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[EpAssetItem.ep_asset_id, EpAssetItem.item_name],
        set_={
            "video_path": pair.video_path,
            "posehand_path": pair.posehand_path,
            "scan_status": "ok",
            "scan_error": None,
            "updated_at": now_ts,
        },
    )
    await session.execute(stmt)
    await session.commit()
    refreshed = await get_ep_asset_item_by_ep_asset_id_and_name(
        session,
        ep_asset_id=ep_asset_id,
        item_name=pair.item_name,
    )
    if refreshed is None:
        raise HTTPException(status_code=500, detail="Upsert item succeeded but row not found")
    return refreshed


async def list_ep_asset_items_by_ep_asset_id(session: AsyncSession, ep_asset_id: int) -> list[EpAssetItem]:
    """按 ep_asset_id 获取配对条目列表。"""
    result = await session.execute(
        select(EpAssetItem)
        .where(EpAssetItem.ep_asset_id == ep_asset_id)
        .order_by(EpAssetItem.item_name.asc(), EpAssetItem.id.asc())
    )
    return list(result.scalars().all())


async def get_ep_asset_item_by_ep_asset_id_and_name(
    session: AsyncSession,
    *,
    ep_asset_id: int,
    item_name: str,
) -> EpAssetItem | None:
    """按 ep_asset_id + item_name 查询单个配对条目。"""
    cleaned = (item_name or "").strip()
    if not cleaned:
        return None
    result = await session.execute(
        select(EpAssetItem).where(
            EpAssetItem.ep_asset_id == ep_asset_id,
            EpAssetItem.item_name == cleaned,
        )
    )
    return result.scalar_one_or_none()


async def count_ep_asset_items_by_ep_asset_ids(session: AsyncSession, ep_asset_ids: list[int]) -> dict[int, int]:
    """按资产 id 批量统计配对条目数量。"""
    normalized = [int(x) for x in ep_asset_ids if int(x) > 0]
    if not normalized:
        return {}

    # 一次 group-by 拉取所有计数，避免列表页 N+1 查询。
    result = await session.execute(
        select(EpAssetItem.ep_asset_id, func.count(EpAssetItem.id))
        .where(EpAssetItem.ep_asset_id.in_(normalized))
        .group_by(EpAssetItem.ep_asset_id)
    )
    return {int(ep_asset_id): int(total or 0) for ep_asset_id, total in result.all()}


async def update_ep_asset_name(session: AsyncSession, *, asset_id: int, name: str) -> EpAsset | None:
    """更新资产展示名称。"""
    cleaned = (name or "").strip()
    if not cleaned:
        raise HTTPException(status_code=422, detail="name cannot be empty")

    row = await session.get(EpAsset, asset_id)
    if row is None:
        return None

    row.name = cleaned
    row.updated_at = shanghai_now_naive()
    await session.commit()
    await session.refresh(row)
    return row


async def save_failed_scan(session: AsyncSession, ep_dir: str, error_message: str) -> None:
    """保存扫描失败状态，便于后续排查。"""
    await upsert_ep_asset(
        session,
        ep_dir=ep_dir,
        source_host="oss-sdk",
        scan_status="failed",
        scan_error=error_message,
        video_path=None,
        posehand_path=None,
    )


async def list_all_ep_assets(session: AsyncSession) -> list[EpAsset]:
    """获取全部资产（按更新时间倒序）。"""
    result = await session.execute(select(EpAsset).order_by(desc(EpAsset.updated_at), desc(EpAsset.id)))
    return list(result.scalars().all())


async def list_ep_assets_paginated(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
) -> tuple[int, list[EpAsset]]:
    """分页查询资产列表。"""
    total = await session.scalar(select(func.count()).select_from(EpAsset))
    total_count = int(total or 0)

    offset = (page - 1) * page_size
    result = await session.execute(
        select(EpAsset)
        .order_by(desc(EpAsset.updated_at), desc(EpAsset.id))
        .offset(offset)
        .limit(page_size)
    )
    items = list(result.scalars().all())
    return total_count, items
