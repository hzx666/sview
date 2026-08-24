"""HandVis → SView 同一套 OSS player-bin 边算边推。

布局（与 sview_worker 一致）：
    player-bin/{job_id}/manifest.json
    player-bin/{job_id}/index.json
    player-bin/{job_id}/faces.u32
    player-bin/{job_id}/video_projection.bin
    player-bin/{job_id}/segments/000000.bin

进度写入 Redis ``sview:job:{job_id}``，Master SSE / signed-media 可直接用。
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from app.core.config import (
    OSS_ACCESS_KEY_ID,
    OSS_ACCESS_KEY_SECRET,
    OSS_CACHE,
    OSS_CACHE_BUCKET,
    OSS_CACHE_PREFIX,
    OSS_CACHE_PUBLIC,
    OSS_ENDPOINT,
    OSS_PUBLIC_BASE,
    OSS_PUBLIC_ENDPOINT,
    OSS_REGION,
    PLAYABLE_MIN_FRAMES,
    REDIS_URL,
    SVIEW_JOB_KEY_PREFIX,
    SVIEW_MASTER_BASE,
)

logger = logging.getLogger(__name__)

_oss_bucket = None
_oss_public_acl_blocked = False
_redis = None


def cache_enabled() -> bool:
    return bool(OSS_CACHE)


def oss_play_base(job_id: str) -> str:
    base = OSS_PUBLIC_BASE
    if not base:
        pub = OSS_PUBLIC_ENDPOINT
        if not pub or "internal" in pub.lower():
            pub = "oss-cn-shanghai.aliyuncs.com"
        base = f"https://{OSS_CACHE_BUCKET}.{pub}"
    if "-internal." in base:
        base = base.replace("-internal.aliyuncs.com", ".aliyuncs.com")
    return f"{base.rstrip('/')}/{OSS_CACHE_PREFIX}/{job_id}"


def oss_cache_key(job_id: str, *parts: str) -> str:
    rel = "/".join(p.strip("/") for p in parts if p)
    return f"{OSS_CACHE_PREFIX}/{job_id}/{rel}".replace("//", "/")


def job_media_urls(job_id: str) -> dict[str, str]:
    """Master 同款媒体路径，供前端一次拉 index + segments。"""
    prefix = f"{SVIEW_MASTER_BASE}/api/jobs/{job_id}" if SVIEW_MASTER_BASE else f"/api/jobs/{job_id}"
    return {
        "job_id": job_id,
        "index_url": f"{prefix}/index.json",
        "index_path": f"{prefix}/index.json",
        "chunk_root": prefix,
        "manifest_url": f"{prefix}/manifest.json",
        "manifest_path": f"{prefix}/manifest.json",
        "play_base": oss_play_base(job_id),
    }


def _oss_endpoint() -> str:
    ep = (OSS_ENDPOINT or "").strip() or f"https://oss-{OSS_REGION}.aliyuncs.com"
    if "://" not in ep:
        ep = "https://" + ep
    return ep


def _get_bucket():
    global _oss_bucket
    if _oss_bucket is not None:
        return _oss_bucket
    import oss2

    if not OSS_ACCESS_KEY_ID or not OSS_ACCESS_KEY_SECRET:
        raise RuntimeError("OSS_CACHE=1 需要 OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET")
    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    _oss_bucket = oss2.Bucket(auth, _oss_endpoint(), OSS_CACHE_BUCKET)
    logger.info("play-cache OSS connected: %s / %s", _oss_endpoint(), OSS_CACHE_BUCKET)
    return _oss_bucket


def _redis_url() -> str:
    url = (REDIS_URL or "").strip()
    if url:
        return url
    host = os.getenv("REDIS_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = os.getenv("REDIS_PORT", "6379").strip() or "6379"
    db = os.getenv("REDIS_DB", "0").strip() or "0"
    user = os.getenv("REDIS_USERNAME") or ""
    password = os.getenv("REDIS_PASSWORD") or ""
    if user and password:
        auth = f"{user}:{password}@"
    elif password:
        auth = f":{password}@"
    else:
        auth = ""
    return f"redis://{auth}{host}:{port}/{db}"


def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    import redis

    _redis = redis.Redis.from_url(_redis_url(), decode_responses=True)
    return _redis


def upload_bytes(
    job_id: str,
    data: bytes,
    *parts: str,
    content_type: str = "application/octet-stream",
) -> None:
    import oss2

    global _oss_public_acl_blocked
    key = oss_cache_key(job_id, *parts)
    headers = {"Content-Type": content_type}
    want_public = OSS_CACHE_PUBLIC and not _oss_public_acl_blocked
    if want_public:
        headers["x-oss-object-acl"] = oss2.OBJECT_ACL_PUBLIC_READ
    bucket = _get_bucket()
    try:
        bucket.put_object(key, data, headers=headers)
    except Exception as exc:
        if want_public and "public" in str(exc).lower():
            _oss_public_acl_blocked = True
            headers.pop("x-oss-object-acl", None)
            bucket.put_object(key, data, headers=headers)
            logger.warning("OSS public-read 被拒，后续 player-bin 保持私有。key=%s", key)
        else:
            raise
    logger.info("play-cache 已上传 oss://%s/%s (%.1f KB)", OSS_CACHE_BUCKET, key, len(data) / 1024.0)


def get_bytes(job_id: str, *parts: str) -> bytes | None:
    """Master / HandVis HTTP 回源 player-bin 对象。"""
    try:
        key = oss_cache_key(job_id, *parts)
        return _get_bucket().get_object(key).read()
    except Exception:
        return None


def build_manifest(
    *,
    job_id: str,
    frame_count: int,
    frames_available: int,
    gop_size: int,
    duration_sec: float,
    status: str,
) -> dict[str, Any]:
    gop = max(1, int(gop_size))
    available = max(0, int(frames_available))
    key_frames = list(range(0, available, gop)) if available else [0]
    return {
        "job_id": job_id,
        "format": "handvis",
        "status": status,
        "bundle": {
            "frame_count": int(frame_count),
            "frames_available": available,
            "gop_size": gop,
            "key_frame_indices": key_frames,
            "timeline": {"duration_sec": float(duration_sec)},
        },
        "play_base": oss_play_base(job_id),
    }


def sync_sview_job(job_id: str, **fields: Any) -> None:
    """写入 SView Master 认的 job hash，供 SSE / status / touch 使用。"""
    data = {k: str(v) for k, v in fields.items() if v is not None}
    data["updated_at"] = str(int(time.time()))
    try:
        r = _get_redis()
        r.hset(f"{SVIEW_JOB_KEY_PREFIX}{job_id}", mapping=data)
    except Exception:
        logger.exception("写入 sview:job 失败。job_id=%s", job_id)


def publish_progress(
    *,
    job_id: str,
    chunk_root_dir: Path,
    segment_rel: str | None,
    index_payload: dict[str, Any],
    still_building: bool,
    frames_available: int,
    frame_count: int,
    duration_sec: float,
    gop_size: int,
    sidecars: bool = False,
) -> None:
    """一段写完后：推 segment + index + SView manifest，并刷新 Redis 进度。"""
    if not cache_enabled() or not job_id:
        return
    try:
        _get_bucket()
    except Exception:
        logger.exception("play-cache OSS 不可用，跳过推流。job_id=%s", job_id)
        return

    if sidecars:
        faces = chunk_root_dir / "faces.u32"
        if faces.is_file():
            upload_bytes(job_id, faces.read_bytes(), "faces.u32")
        proj = chunk_root_dir / "video_projection.bin"
        if proj.is_file():
            upload_bytes(job_id, proj.read_bytes(), "video_projection.bin")

    if segment_rel:
        seg_path = chunk_root_dir / segment_rel
        if seg_path.is_file():
            upload_bytes(job_id, seg_path.read_bytes(), *Path(segment_rel).parts)

    index_bytes = json.dumps(index_payload, indent=2, ensure_ascii=False).encode("utf-8")
    upload_bytes(job_id, index_bytes, "index.json", content_type="application/json; charset=utf-8")

    status = "partial" if still_building else "done"
    manifest = build_manifest(
        job_id=job_id,
        frame_count=frame_count,
        frames_available=frames_available,
        gop_size=gop_size,
        duration_sec=duration_sec,
        status=status,
    )
    upload_bytes(
        job_id,
        json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
        "manifest.json",
        content_type="application/json; charset=utf-8",
    )

    playable = 1 if int(frames_available) >= int(PLAYABLE_MIN_FRAMES) else 0
    sync_sview_job(
        job_id,
        status="processing" if still_building else "ready",
        kind="handvis",
        frames_done=int(frames_available),
        frames_total=int(frame_count),
        frame_count=int(frame_count),
        playable=playable,
        play_base=oss_play_base(job_id),
        cache_status="writing" if still_building else "ready",
        error="",
    )


def publish_existing_tree(job_id: str, chunk_root_dir: Path, index_payload: dict[str, Any]) -> None:
    """缓存命中：把已有本地产物整树推到 player-bin/{job_id}/。"""
    if not cache_enabled() or not job_id:
        return
    balanced = (index_payload.get("presets") or {}).get("balanced") or {}
    frame_count = int(balanced.get("frame_count") or 0)
    duration_sec = float(balanced.get("total_duration") or 0.0)
    gop_size = max(1, int(os.getenv("BALANCED_CHUNK_FRAMES", "30") or "30"))
    publish_progress(
        job_id=job_id,
        chunk_root_dir=chunk_root_dir,
        segment_rel=None,
        index_payload=index_payload,
        still_building=False,
        frames_available=frame_count,
        frame_count=frame_count,
        duration_sec=duration_sec,
        gop_size=gop_size,
        sidecars=True,
    )
    for chunk in balanced.get("chunks") or []:
        rel = str((chunk or {}).get("bin") or "").strip()
        if not rel:
            continue
        path = chunk_root_dir / rel
        if path.is_file():
            upload_bytes(job_id, path.read_bytes(), *Path(rel).parts)
