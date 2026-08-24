"""chunk 构建核心流程。

支持流式输出：首 chunk 一就绪就通过 task 状态推送给前端，
让前端尽早开始 3D 渲染，剩余 chunk 在后台继续构建。

HANDVIS_DISABLE_PG=1：不查/写 PostgreSQL。缓存直接看 OSS player-bin/{job_id}/，
而不是 playback_assets 表。
"""

import asyncio
import json
import logging
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

from fastapi import HTTPException

from app.core.config import BALANCED_STRIDE, HANDVIS_DISABLE_PG

if not HANDVIS_DISABLE_PG:
    from app.db.database import SessionLocal
    from app.services.playback_asset_service import (
        get_playback_asset_by_ep_asset_item_id,
        upsert_playback_asset,
    )
from app.services.chunk_builder import build_mesh_and_chunks
from app.services.oss_object_store import parse_oss_path, read_oss_object_bytes
from app.services.oss_signer import sign_oss_url

from .asset_resolver import resolve_asset_by_ep_dir_and_item_name
from .paths import (
    local_chunk_root,
    local_chunk_root_url,
    local_index_url,
    local_manifest_url,
    local_posthands_root,
    local_root_url,
)
from .video_service import resolve_video_for_item

logger = logging.getLogger(__name__)


def _playback_media_urls(unique_id: str, job_id: str | None) -> dict:
    """OSS 推流开启时走 /api/jobs/{id}/，否则仍用本地 /posehand。"""
    if job_id:
        try:
            from app.services.play_cache import cache_enabled, job_media_urls

            if cache_enabled():
                urls = job_media_urls(job_id)
                return {
                    "local_root": local_root_url(unique_id),
                    "chunk_root": urls["chunk_root"],
                    "index_path": urls["index_path"],
                    "index_url": urls["index_url"],
                    "manifest_path": urls["manifest_path"],
                    "manifest_url": urls["manifest_url"],
                    "play_base": urls["play_base"],
                    "job_id": job_id,
                }
        except Exception as exc:
            logger.warning("play-cache URL 生成失败，回退本地路径。job_id=%s err=%s", job_id, exc)
    return {
        "local_root": local_root_url(unique_id),
        "chunk_root": local_chunk_root_url(unique_id),
        "index_path": local_index_url(unique_id),
        "index_url": local_index_url(unique_id),
        "manifest_path": local_manifest_url(unique_id),
        "manifest_url": local_manifest_url(unique_id),
        "job_id": job_id,
    }


def _local_projection_cache_ready(unique_id: str, index_path: Path, manifest_path: Path) -> bool:
    """确认本地缓存的关键产物文件齐全（world mesh + faces + index/manifest）。"""
    if not index_path.exists() or not manifest_path.exists():
        return False
    try:
        index_data = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.info("本地 index 读取失败，将重建。unique_id=%s err=%s", unique_id, exc)
        return False

    # 流式构建过程中可能读到 still_building=True 的中间态，此时不能视为缓存命中。
    if bool(index_data.get("still_building")):
        return False

    balanced = (index_data.get("presets") or {}).get("balanced") or {}
    chunks = balanced.get("chunks")
    if not isinstance(chunks, list) or not chunks:
        return False

    chunk_root = local_chunk_root(unique_id)
    for chunk in chunks:
        if not chunk or not chunk.get("ready", True):
            return False
        bin_rel = str(chunk.get("bin") or "").strip()
        if not bin_rel:
            return False
        bin_path = chunk_root / bin_rel
        if not bin_path.exists() or not bin_path.is_file() or bin_path.stat().st_size < 8:
            logger.info(
                "本地 segment 缺失，将重建。unique_id=%s bin=%s",
                unique_id,
                bin_rel,
            )
            return False
    return True


async def build_chunks_impl(
    ep_dir: str,
    *,
    item_name: str,
    force_rebuild: bool = False,
    on_first_chunk_ready: Callable[[dict], None] | None = None,
    job_id: str | None = None,
    video_path: str | None = None,
    posehand_path: str | None = None,
) -> dict:
    """chunk 构建核心流程。

    队列里带了 video_path/posehand_path 时不查 Postgres，直接读 OSS。
    """
    direct_video = (video_path or "").strip()
    direct_pose = (posehand_path or "").strip()
    asset = None
    if direct_pose:
        video_path = direct_video
        posehand_path = direct_pose
        item = SimpleNamespace(
            id=0,
            media_id=None,
            item_name=(item_name or Path(direct_pose).stem),
            video_path=video_path,
            posehand_path=posehand_path,
        )
        source = "payload"
    else:
        asset, item, source = await resolve_asset_by_ep_dir_and_item_name(ep_dir, item_name=item_name)
        video_path = item.video_path
        posehand_path = str(item.posehand_path or "").strip()
    # 先启动视频解析；posehand 缺少图像尺寸时，用视频宽高作为投影基准尺寸。
    video_task = asyncio.create_task(resolve_video_for_item(item))

    if not posehand_path:
        video_info = await video_task
        logger.info(
            "未检测到 posehand，按纯视频模式返回。ep_dir=%s item_name=%s",
            ep_dir,
            item_name,
        )
        return {
            "ep_dir": ep_dir,
            "item_name": item.item_name,
            "source": source,
            "video_path": video_path,
            "has_3d": False,
            "cleanup_intermediate": True,
            **_video_payload(video_info),
        }

    # 无 PG：缓存不查 playback_assets，直接看 OSS 上是否已有 player-bin/{job_id}。
    if HANDVIS_DISABLE_PG:
        existing_playback = None
        if job_id and not force_rebuild:
            try:
                from app.services.play_cache import cache_enabled, get_bytes

                if cache_enabled():
                    raw_index = await asyncio.to_thread(get_bytes, job_id, "index.json")
                    if raw_index:
                        idx = json.loads(raw_index.decode("utf-8"))
                        if not bool(idx.get("still_building")):
                            balanced = (idx.get("presets") or {}).get("balanced") or {}
                            if int(balanced.get("stride") or 0) == int(BALANCED_STRIDE):
                                video_info = await video_task
                                logger.info("命中 OSS player-bin 缓存。job_id=%s", job_id)
                                # 缓存命中也要把 sview:job 刷回 ready，否则 Master SSE
                                # 永远停在入队时写入的 processing/0，页面一直等。
                                try:
                                    from app.services.play_cache import sync_sview_job

                                    fc = int(balanced.get("frame_count") or 0)
                                    sync_sview_job(
                                        job_id,
                                        status="ready",
                                        kind="handvis",
                                        frames_done=fc,
                                        frames_total=fc,
                                        frame_count=fc,
                                        playable=1,
                                        cache_status="ready",
                                        error="",
                                    )
                                except Exception:
                                    pass
                                return {
                                    "ep_dir": ep_dir,
                                    "item_name": item.item_name,
                                    "source": "oss-cache",
                                    "video_path": video_path,
                                    "has_3d": True,
                                    "posehand_path": posehand_path,
                                    "posehand_url": sign_oss_url(posehand_path),
                                    "local_posehand_path": None,
                                    "unique_id": job_id,
                                    "ep_asset_item_id": 0,
                                    "playback_asset_id": 0,
                                    "playback_unique_id": job_id,
                                    "manifest_data": {},
                                    "preset": "balanced",
                                    "cleanup_intermediate": True,
                                    "still_building": False,
                                    **_playback_media_urls(job_id, job_id),
                                    **_video_payload(video_info),
                                }
            except Exception as exc:
                logger.warning("OSS 缓存检查失败，将继续构建。job_id=%s err=%s", job_id, exc)
    else:
        async with SessionLocal() as session:
            existing_playback = await get_playback_asset_by_ep_asset_item_id(session, item.id)

    if existing_playback and not force_rebuild:
        local_index = local_chunk_root(existing_playback.unique_id) / "index.json"
        local_manifest = local_posthands_root(existing_playback.unique_id) / "manifest.json"
        if (
            int(existing_playback.stride or 0) == int(BALANCED_STRIDE)
            and _local_projection_cache_ready(existing_playback.unique_id, local_index, local_manifest)
        ):
            logger.info(
                "命中本地 chunk 缓存。ep_dir=%s item_name=%s playback_id=%s unique_id=%s",
                ep_dir,
                item_name,
                existing_playback.id,
                existing_playback.unique_id,
            )
            video_info = await video_task
            if job_id:
                try:
                    from app.services.play_cache import cache_enabled, publish_existing_tree

                    if cache_enabled() and local_index.is_file():
                        index_payload = json.loads(local_index.read_text(encoding="utf-8"))
                        await asyncio.to_thread(
                            publish_existing_tree,
                            job_id,
                            local_chunk_root(existing_playback.unique_id),
                            index_payload,
                        )
                except Exception as exc:
                    logger.warning("缓存命中后推 OSS 失败。job_id=%s err=%s", job_id, exc)
            media = _playback_media_urls(existing_playback.unique_id, job_id)
            return {
                "ep_dir": ep_dir,
                "item_name": item.item_name,
                "source": "local-cache",
                "video_path": video_path,
                "has_3d": True,
                "posehand_path": posehand_path,
                "posehand_url": sign_oss_url(posehand_path),
                "local_posehand_path": None,
                "unique_id": existing_playback.unique_id,
                "ep_asset_item_id": item.id,
                "playback_asset_id": existing_playback.id,
                "playback_unique_id": existing_playback.unique_id,
                "manifest_data": {},
                "preset": existing_playback.preset,
                "cleanup_intermediate": True,
                "still_building": False,
                **media,
                **_video_payload(video_info),
            }

    try:
        posehand_bytes = await asyncio.to_thread(read_oss_object_bytes, posehand_path)
    except HTTPException as exc:
        logger.warning("读取 posehand 失败，无法构建 chunk。ep_dir=%s item_name=%s detail=%s", ep_dir, item_name, exc.detail)
        raise HTTPException(
            status_code=exc.status_code,
            detail=f"{exc.detail} (OSS posehand 读取失败，无法构建3D缓存)",
        ) from exc

    _bucket, object_key = parse_oss_path(posehand_path)
    source_name = Path(object_key).name
    unique_id = uuid.uuid4().hex
    local_root_dir = local_posthands_root(unique_id)

    video_info = await video_task
    video_width = int((video_info or {}).get("width") or 0)
    video_height = int((video_info or {}).get("height") or 0)
    fallback_image_size = (float(video_width), float(video_height)) if video_width > 0 and video_height > 0 else None

    # 视频信息已解析完成，首 chunk 就绪时可以直接带上当前视频状态。
    first_chunk_pushed = {"flag": False}

    def _push_first_chunk_payload(_first_entry: dict) -> None:
        """首 chunk 写完时触发：组装一份"部分就绪"的 result 推给上层回调。"""
        if first_chunk_pushed["flag"] or on_first_chunk_ready is None:
            return
        result = {
            "ep_dir": ep_dir,
            "item_name": item.item_name,
            "source": source,
            "video_path": video_path,
            "has_3d": True,
            "posehand_path": posehand_path,
            "posehand_url": sign_oss_url(posehand_path),
            "unique_id": unique_id,
            "ep_asset_item_id": item.id,
            "preset": "balanced",
            "still_building": True,
            "cleanup_intermediate": True,
            **_playback_media_urls(unique_id, job_id),
            **_video_payload(video_info),
        }
        try:
            on_first_chunk_ready(result)
        except Exception as exc:
            logger.warning("on_first_chunk_ready 回调失败。task ep_dir=%s err=%s", ep_dir, exc)
        first_chunk_pushed["flag"] = True

    build_result = await asyncio.to_thread(
        build_mesh_and_chunks,
        pose3d_hand_bytes=posehand_bytes,
        source_name=source_name,
        local_root_dir=local_root_dir,
        unique_id=unique_id,
        fps=30,
        fallback_image_size=fallback_image_size,
        on_first_chunk_ready=_push_first_chunk_payload,
        job_id=job_id,
    )
    manifest_payload = build_result.get("manifest_payload") or {}
    index_payload = build_result.get("index_payload") or {}
    balanced = (index_payload.get("presets") or {}).get("balanced") or {}

    if HANDVIS_DISABLE_PG:
        playback_asset_id = 0
        playback_unique_id = unique_id
    else:
        async with SessionLocal() as session:
            saved_playback = await upsert_playback_asset(
                session,
                ep_asset_id=asset.id if asset is not None else 0,
                ep_asset_item_id=item.id,
                unique_id=unique_id,
                source_name=manifest_payload.get("source_name"),
                chunk_root=build_result["chunk_root"],
                index_path=build_result["index_path"],
                preset=build_result["preset"],
                fps=int(manifest_payload.get("fps") or balanced.get("fps") or 30),
                manifest_version=int(index_payload.get("version") or 1),
                default_quality=str(index_payload.get("default_quality") or "balanced"),
                face_count=int(index_payload.get("face_count") or 0),
                stride=int(balanced.get("stride") or 1),
                frame_count=int(balanced.get("frame_count") or 0),
                vertex_count=int(balanced.get("vertex_count") or 0),
                total_duration=float(balanced.get("total_duration") or 0.0),
                chunk_duration=float(balanced.get("chunk_duration") or 0.0),
                chunk_count=len(balanced.get("chunks") or []),
            )
        playback_asset_id = saved_playback.id
        playback_unique_id = saved_playback.unique_id
    logger.info(
        "chunk 构建完成。ep_dir=%s item_name=%s playback_id=%s unique_id=%s source=%s",
        ep_dir,
        item_name,
        playback_asset_id,
        unique_id,
        source,
    )
    return {
        "ep_dir": ep_dir,
        "item_name": item.item_name,
        "source": source,
        "video_path": video_path,
        "has_3d": True,
        "posehand_path": posehand_path,
        "posehand_url": sign_oss_url(posehand_path),
        "local_posehand_path": None,
        "unique_id": unique_id,
        "ep_asset_item_id": item.id,
        "playback_asset_id": playback_asset_id,
        "playback_unique_id": playback_unique_id,
        "manifest_data": manifest_payload,
        "preset": build_result["preset"],
        "cleanup_intermediate": True,
        "still_building": False,
        **_playback_media_urls(unique_id, job_id),
        **_video_payload(video_info),
    }


def _video_payload(video_info: dict | None) -> dict:
    """从视频解析结果生成对外 payload。"""
    info = video_info or {}
    return {
        "video_url": info.get("video_url"),
        "video_status": info.get("video_status") or "queued",
        "media_id": int(info.get("media_id") or 0),
        "video_error": info.get("error"),
    }
