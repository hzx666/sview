"""播放与分块构建 API 路由。

职责：
- 管理 chunk 构建任务（含并发幂等保护）
- 提供播放资产信息与 chunk 文件访问
- 暴露视频转码状态查询接口
"""

import logging

from fastapi import APIRouter
from fastapi import Request
from fastapi.responses import Response

from app.core.config import HANDVIS_DISABLE_PG
from app.db.database import SessionLocal
from app.models.ep_asset_item import EpAssetItem
from app.schemas.ep_asset import BuildChunksRequest
from app.services.direct_pair_matcher import manual_pair_from_paths
from app.services.ep_asset_service import upsert_ep_asset, upsert_ep_asset_item
from app.services.oss_signer import sign_video_url
from app.services.remote_scanner import ensure_leaf_ep_dir

from .chunk_files import get_chunk_file_response, get_playback_asset_payload
from .build_task_stream import get_build_task_stream
from .jobs import get_build_job, submit_build_job
from .video_status import get_video_status_payload
from .video_status_stream import get_video_status_stream

router = APIRouter(prefix="/api/playback", tags=["playback"])
logger = logging.getLogger(__name__)


async def _resolve_video_url_hint(ep_dir: str, item_name: str) -> str | None:
    """尽力从缓存中拿 video_path 并签名,失败返回 None(不阻塞主流程)。无 PG 直接略过。"""
    if HANDVIS_DISABLE_PG:
        return None
    try:
        from sqlalchemy import select, and_
        from app.models.ep_asset import EpAsset

        async with SessionLocal() as session:
            # 先查 ep_asset
            stmt = select(EpAsset.id).where(EpAsset.ep_dir == ep_dir)
            result = await session.execute(stmt)
            ep_asset_row = result.scalar_one_or_none()
            if not ep_asset_row:
                return None
            # 再查 item
            stmt2 = select(EpAssetItem.video_path).where(
                and_(
                    EpAssetItem.ep_asset_id == ep_asset_row,
                    EpAssetItem.item_name == item_name,
                )
            )
            result2 = await session.execute(stmt2)
            video_path = result2.scalar_one_or_none()
            if not video_path:
                return None
            return sign_video_url(str(video_path).strip())
    except Exception:
        return None


async def _cache_direct_pair_if_present(payload: BuildChunksRequest) -> tuple[str, str]:
    """手动输入文件路径时，先把精确配对写入缓存供构建链路复用。

    无 PG 模式跳过写库；配对只随队列消息传给 worker。
    """
    video_path = payload.normalized_video_path
    posehand_path = payload.normalized_posehand_path
    if not video_path:
        return ensure_leaf_ep_dir(payload.normalized_ep_dir), payload.normalized_item_name

    ep_dir, pair = manual_pair_from_paths(video_path, posehand_path)
    item_name = pair.item_name
    if HANDVIS_DISABLE_PG:
        logger.info("无 PG 模式：手动路径配对不入库。ep_dir=%s item_name=%s", ep_dir, item_name)
        return ep_dir, item_name
    async with SessionLocal() as session:
        saved = await upsert_ep_asset(
            session,
            ep_dir=ep_dir,
            source_host="manual-input",
            scan_status="ok",
            scan_error=None,
            video_path=pair.video_path,
            posehand_path=pair.posehand_path,
        )
        await upsert_ep_asset_item(session, ep_asset_id=saved.id, pair=pair)
    logger.info("手动路径配对已写入缓存。ep_dir=%s item_name=%s", ep_dir, item_name)
    return ep_dir, item_name


@router.post("/build-chunks")
async def build_chunks(payload: BuildChunksRequest) -> dict:
    """提交 chunk 构建任务（异步），返回 task_id + video_url_hint。

    video_url_hint: 立即签发的 OSS 视频 URL（大部分可直接播放），
    前端不需要等 build task 返回 video_url 就能开始加载视频。
    """
    ep_dir, item_name = await _cache_direct_pair_if_present(payload)
    force_rebuild = bool(payload.force_rebuild)
    logger.info("收到 chunk 异步构建请求。ep_dir=%s item_name=%s", ep_dir, item_name)

    # 快速查询 video_path(从缓存或 DB),立即签名给前端预加载。
    video_url_hint = await _resolve_video_url_hint(ep_dir, item_name)

    result = await submit_build_job(
        ep_dir=ep_dir,
        item_name=item_name,
        force_rebuild=force_rebuild,
        video_path=payload.normalized_video_path or None,
        posehand_path=payload.normalized_posehand_path or None,
    )
    if video_url_hint:
        result["video_url_hint"] = video_url_hint
    return result


@router.get("/build-chunks/tasks/{task_id}")
async def get_build_chunks_task(task_id: str) -> dict:
    """查询异步 chunk 构建任务状态。"""
    return await get_build_job(task_id)


@router.get("/build-chunks/stream/{task_id}")
async def get_build_chunks_task_stream(task_id: str, request: Request) -> Response:
    """SSE 推送 chunk 构建任务状态。"""
    return await get_build_task_stream(task_id, request)


@router.get("/video-status/{media_id}")
async def get_video_status(media_id: int) -> dict:
    """查询视频转码状态（前端在视频不可播时轮询此接口）。"""
    return await get_video_status_payload(media_id)


@router.get("/video-status/stream/{media_id}")
async def get_video_status_sse(media_id: int, request: Request) -> Response:
    """SSE 推送视频转码状态，直到成功或失败。"""
    return await get_video_status_stream(media_id, request)


@router.get("/asset/{unique_id}")
async def get_playback_asset(unique_id: str) -> dict:
    """按 unique_id 获取播放资产元数据。"""
    return await get_playback_asset_payload(unique_id)


@router.get("/chunk-file/{unique_id}/{file_path:path}")
async def get_chunk_file(unique_id: str, file_path: str) -> Response:
    """读取并返回本地 chunk 文件（仅本地 posehand）。"""
    return await get_chunk_file_response(unique_id, file_path)
