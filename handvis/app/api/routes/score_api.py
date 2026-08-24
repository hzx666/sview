"""素材评分 API。"""

import logging

from fastapi import APIRouter, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from app.db.score_database import ScoreSessionLocal
from app.schemas.score_payloads import ScoreContextResponse, ScoreCreatePayload, ScoreRecordResponse
from app.services.score_service import build_score_context, save_score_record

router = APIRouter(prefix="/api/oss-file-info", tags=["material-scores"])
logger = logging.getLogger(__name__)


@router.get("/context", response_model=ScoreContextResponse)
async def get_score_context(ep_dir: str, item_name: str) -> ScoreContextResponse:
    """返回规范化评分路径，不自动查询历史评分。"""
    oss_path, file_name = build_score_context(ep_dir, item_name)
    return ScoreContextResponse(
        oss_path=oss_path,
        file_name=file_name,
    )


@router.post("", response_model=ScoreRecordResponse)
async def save_score(payload: ScoreCreatePayload) -> ScoreRecordResponse:
    """按素材路径新增评分记录。"""
    try:
        async with ScoreSessionLocal() as session:
            row = await save_score_record(
                session,
                ep_dir=payload.ep_dir,
                item_name=payload.item_name,
                is_ego=payload.is_ego,
                score=payload.score,
                comment=payload.comment,
            )
        logger.info("素材评分保存成功。id=%s oss_path=%s", row.id, row.oss_path)
        return ScoreRecordResponse.model_validate(row)
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        logger.exception("素材评分保存失败。ep_dir=%s item_name=%s", payload.ep_dir, payload.item_name)
        raise HTTPException(status_code=503, detail="评分数据库写入失败，请检查 PostgreSQL 连接和表结构") from exc
