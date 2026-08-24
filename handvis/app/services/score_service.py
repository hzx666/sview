"""素材评分路径组装与持久化服务。"""

import math

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.material_score import MaterialScore
from app.services.oss_path_utils import normalize_oss_path


def build_score_context(ep_dir: str, item_name: str) -> tuple[str, str]:
    """按“OSS 目录 + item_name”生成评分对象路径。"""
    cleaned_item_name = str(item_name or "").strip().strip("/")
    if not cleaned_item_name or "/" in cleaned_item_name:
        raise HTTPException(status_code=422, detail="item_name 必须是单个有效路径段")
    try:
        normalized_dir = normalize_oss_path(ep_dir, require_trailing_slash=True)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"无效的 OSS 路径: {exc}") from exc
    return f"{normalized_dir}{cleaned_item_name}", cleaned_item_name


async def save_score_record(
    session: AsyncSession,
    *,
    ep_dir: str,
    item_name: str,
    is_ego: bool,
    score: float,
    comment: str,
) -> MaterialScore:
    """按素材路径新增一条评分记录，允许同一素材重复评分。"""
    if not math.isfinite(score) or not 0.0 <= score <= 5.0 or not math.isclose(score * 2, round(score * 2)):
        raise HTTPException(status_code=422, detail="score 必须是 0 到 5.0 之间的半分值")
    oss_path, file_name = build_score_context(ep_dir, item_name)
    row = MaterialScore(oss_path=oss_path)
    session.add(row)
    row.file_name = file_name
    row.is_ego = bool(is_ego)
    row.score = float(score)
    cleaned_comment = str(comment or "").strip()
    row.comment = cleaned_comment or None
    await session.commit()
    await session.refresh(row)
    return row
