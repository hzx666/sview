#!/usr/bin/env python3
"""OSS play-cache GC: idle TTL + 500GiB LRU, with Lifecycle as ops fallback.

Env:
  CACHE_MAX_BYTES=536870912000   soft cap (~500 GiB)
  CACHE_IDLE_TTL_SEC=14400       delete when last_access older than this
  CACHE_GC_INTERVAL_SEC=300      master loop interval
  CACHE_PIN_SEC=600              protect recently touched / playable jobs
  OSS_CACHE_PREFIX=player-bin
  OSS_BUCKET / OSS_ENDPOINT / OSS_ACCESS_KEY_*
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Optional

log = logging.getLogger("sview.cache_gc")

CACHE_MAX_BYTES = max(
    0,
    int(os.environ.get("CACHE_MAX_BYTES", str(500 * 1024 ** 3)) or str(500 * 1024 ** 3)),
)
CACHE_IDLE_TTL_SEC = max(60, int(os.environ.get("CACHE_IDLE_TTL_SEC", "14400") or "14400"))
CACHE_GC_INTERVAL_SEC = max(30, int(os.environ.get("CACHE_GC_INTERVAL_SEC", "300") or "300"))
CACHE_PIN_SEC = max(0, int(os.environ.get("CACHE_PIN_SEC", "600") or "600"))
OSS_CACHE_PREFIX = (os.environ.get("OSS_CACHE_PREFIX", "player-bin") or "player-bin").strip("/")
OSS_BUCKET = os.environ.get("OSS_BUCKET", "ss-oss-data-platform")
_oss_endpoint = os.environ.get("OSS_ENDPOINT", "https://oss-cn-shanghai.aliyuncs.com")
if _oss_endpoint and "://" not in _oss_endpoint:
    _oss_endpoint = "https://" + _oss_endpoint
OSS_ENDPOINT = _oss_endpoint
OSS_ACCESS_KEY_ID = os.environ.get("OSS_ACCESS_KEY_ID", "")
OSS_ACCESS_KEY_SECRET = os.environ.get("OSS_ACCESS_KEY_SECRET", "")

_gc_thread: Optional[threading.Thread] = None
_gc_stop = threading.Event()
_oss_bucket = None


def _env_bool(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "yes", "on")


def get_oss_bucket():
    global _oss_bucket
    if _oss_bucket is not None:
        return _oss_bucket
    import oss2

    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    _oss_bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)
    _oss_bucket.get_bucket_info()
    log.info("OSS GC connected: %s / %s prefix=%s/", OSS_ENDPOINT, OSS_BUCKET, OSS_CACHE_PREFIX)
    return _oss_bucket


def job_prefix(job_id: str) -> str:
    return f"{OSS_CACHE_PREFIX}/{job_id}/"


def delete_job_prefix(job_id: str) -> int:
    """Delete all objects under player-bin/{job_id}/. Returns deleted count."""
    bucket = get_oss_bucket()
    prefix = job_prefix(job_id)
    deleted = 0
    import oss2

    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        try:
            bucket.delete_object(obj.key)
            deleted += 1
        except Exception:
            log.exception("failed deleting %s", obj.key)
    return deleted


def list_oss_segment_starts(job_id: str) -> Optional[set[int]]:
    """Return set of segment start indices present on OSS, or None on list failure."""
    try:
        bucket = get_oss_bucket()
    except Exception:
        log.exception("OSS connect failed while listing segments for %s", job_id)
        return None
    import oss2

    prefix = f"{job_prefix(job_id)}segments/"
    starts: set[int] = set()
    try:
        for obj in oss2.ObjectIterator(bucket, prefix=prefix):
            name = obj.key.rsplit("/", 1)[-1]
            if not name.endswith(".bin"):
                continue
            try:
                starts.add(int(name[:-4]))
            except ValueError:
                continue
    except Exception:
        log.exception("list segments failed for %s", job_id)
        return None
    return starts


def contiguous_frames_from_starts(
    starts: set[int],
    *,
    segment_frames: int = 30,
    claimed: Optional[int] = None,
) -> int:
    """Frames playable from index 0 given complete segment objects (no holes)."""
    seg_n = max(1, int(segment_frames or 30))
    if not starts:
        return 0
    avail = 0
    s = 0
    while s in starts:
        avail = s + seg_n
        s += seg_n
    if claimed is not None and claimed >= 0:
        # Allow a short final partial segment: claimed may be < last full boundary.
        if claimed < avail and (avail - seg_n) in starts and claimed > (avail - seg_n):
            return int(claimed)
        return int(min(avail, claimed))
    return int(avail)


def contiguous_oss_frames(
    job_id: str,
    *,
    segment_frames: int = 30,
    claimed: Optional[int] = None,
) -> Optional[int]:
    """Contiguous playable frames from OSS segments starting at 0; None if list fails."""
    starts = list_oss_segment_starts(job_id)
    if starts is None:
        return None
    return contiguous_frames_from_starts(
        starts, segment_frames=segment_frames, claimed=claimed
    )


def oss_segment_hole(starts: set[int]) -> bool:
    """True if objects exist but segment 0 is missing (orphaned tail / partial Lifecycle)."""
    return bool(starts) and 0 not in starts


def purge_job_play_cache(
    r,
    job_id: str,
    *,
    reason: str = "purged",
    clear_redis_progress: bool = True,
) -> dict:
    """Delete OSS player-bin/{job_id}/ and reset Redis so clients cannot see ghost frames.

    Local /cache/{job_id} should still be removed by the caller (Master/Worker).
    """
    from job_status import get_job_fields, remove_cache_job, set_job_fields

    fields = get_job_fields(r, job_id) if r is not None else {}
    nbytes = _safe_int(fields.get("cache_bytes"), 0)
    deleted = 0
    oss_error = None
    try:
        deleted = delete_job_prefix(job_id)
    except Exception as e:
        oss_error = str(e)
        log.exception("OSS purge failed for %s", job_id)

    if r is not None:
        try:
            remove_cache_job(r, job_id, nbytes)
        except Exception:
            log.exception("remove_cache_job failed for %s", job_id)
        patch: dict[str, Any] = {
            "play_base": "",
            "cache_bytes": 0,
            "cache_status": reason or "purged",
        }
        if clear_redis_progress:
            patch.update(
                {
                    "frames_done": 0,
                    "playable": 0,
                }
            )
        try:
            set_job_fields(r, job_id, **patch)
        except Exception:
            log.exception("redis reset after purge failed for %s", job_id)

    log.info(
        "purged play cache job=%s reason=%s oss_objects=%s redis_bytes=%s err=%s",
        job_id,
        reason,
        deleted,
        nbytes,
        oss_error,
    )
    return {
        "job_id": job_id,
        "reason": reason,
        "deleted_objects": deleted,
        "freed_bytes": nbytes,
        "error": oss_error,
    }


def align_manifest_to_oss(
    job_id: str,
    manifest: dict,
    *,
    segment_frames: Optional[int] = None,
    job_status: Optional[str] = None,
) -> dict:
    """Clamp manifest frames_available to contiguous OSS segments (fixes ghost tails).

    If OSS listing fails, returns manifest unchanged.
    """
    if not isinstance(manifest, dict):
        return manifest
    bundle = manifest.get("bundle")
    if not isinstance(bundle, dict):
        return manifest

    seg_n = segment_frames
    if seg_n is None:
        seg_n = _safe_int(bundle.get("gop_size"), 30) or 30
    claimed = bundle.get("frames_available")
    if claimed is None:
        claimed = bundle.get("frame_count")
    try:
        claimed_i = int(claimed) if claimed is not None else None
    except (TypeError, ValueError):
        claimed_i = None

    starts = list_oss_segment_starts(job_id)
    if starts is None:
        return manifest

    aligned = contiguous_frames_from_starts(
        starts, segment_frames=seg_n, claimed=claimed_i
    )
    hole = oss_segment_hole(starts)
    status = (job_status or manifest.get("status") or "").lower()

    # Orphaned tail (Lifecycle / cancel without wipe): never advertise early frames.
    if hole:
        bundle["frames_available"] = aligned  # usually 0
        manifest["bundle"] = bundle
        if aligned <= 0 and status in ("done", "ready", "partial"):
            # Do not look playable when nothing is contiguous from 0.
            manifest["status"] = "partial"
        manifest["cache_aligned"] = True
        manifest["cache_align_reason"] = "oss_segment_hole"
        return manifest

    if claimed_i is not None and aligned < claimed_i:
        bundle["frames_available"] = aligned
        manifest["bundle"] = bundle
        manifest["cache_aligned"] = True
        manifest["cache_align_reason"] = "oss_contiguous_clamp"
        # If nothing left on OSS, demote done → avoid FE treating as complete cache.
        if aligned <= 0 and status in ("done", "ready"):
            manifest["status"] = "partial"
        return manifest

    return manifest


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return default


def _is_protected(fields: dict, now: float) -> bool:
    status = (fields.get("status") or "").lower()
    if status in ("queued", "processing"):
        return True
    pin_until = _safe_int(fields.get("pin_until"), 0)
    if pin_until and pin_until > now:
        return True
    if CACHE_PIN_SEC > 0:
        last = _safe_int(fields.get("last_access_at"), 0)
        if last and (now - last) < CACHE_PIN_SEC:
            return True
    return False


def run_gc_once(r) -> dict:
    """Evict idle jobs then LRU until under CACHE_MAX_BYTES."""
    from job_status import (
        get_job_fields,
        list_cache_jobs,
    )

    now = time.time()
    removed_idle = 0
    removed_lru = 0
    freed = 0
    jobs = list_cache_jobs(r)
    for job_id in list(jobs):
        fields = get_job_fields(r, job_id)
        if _is_protected(fields, now):
            continue
        last = _safe_int(fields.get("last_access_at"), 0) or _safe_int(
            fields.get("finished_at"), 0
        )
        if not last:
            last = _safe_int(fields.get("updated_at"), 0)
        if last and (now - last) >= CACHE_IDLE_TTL_SEC:
            try:
                summary = purge_job_play_cache(r, job_id, reason="expired")
                removed_idle += 1
                freed += int(summary.get("freed_bytes") or 0)
            except Exception:
                log.exception("idle-evict failed for %s", job_id)

    if CACHE_MAX_BYTES <= 0:
        return {
            "removed_idle": removed_idle,
            "removed_lru": removed_lru,
            "freed_bytes": freed,
            "total_bytes": 0,
        }

    jobs = list_cache_jobs(r)
    entries = []
    total = 0
    for job_id in jobs:
        fields = get_job_fields(r, job_id)
        nbytes = _safe_int(fields.get("cache_bytes"), 0)
        last = _safe_int(fields.get("last_access_at"), 0) or _safe_int(
            fields.get("updated_at"), 0
        )
        total += nbytes
        entries.append((job_id, nbytes, last, fields))

    if total <= CACHE_MAX_BYTES:
        return {
            "removed_idle": removed_idle,
            "removed_lru": removed_lru,
            "freed_bytes": freed,
            "total_bytes": total,
        }

    entries.sort(key=lambda x: x[2] or 0)
    for job_id, nbytes, _last, fields in entries:
        if total <= CACHE_MAX_BYTES:
            break
        if _is_protected(fields, now):
            continue
        try:
            summary = purge_job_play_cache(r, job_id, reason="evicted")
            removed_lru += 1
            freed += int(summary.get("freed_bytes") or 0)
            total = max(0, total - int(summary.get("freed_bytes") or nbytes))
        except Exception:
            log.exception("lru-evict failed for %s", job_id)

    return {
        "removed_idle": removed_idle,
        "removed_lru": removed_lru,
        "freed_bytes": freed,
        "total_bytes": total,
    }


def _gc_loop(get_redis):
    log.info(
        "OSS cache GC started max_bytes=%s idle_ttl=%ss interval=%ss",
        CACHE_MAX_BYTES,
        CACHE_IDLE_TTL_SEC,
        CACHE_GC_INTERVAL_SEC,
    )
    while not _gc_stop.is_set():
        try:
            r = get_redis()
            summary = run_gc_once(r)
            if summary.get("removed_idle") or summary.get("removed_lru"):
                log.info("cache GC pass: %s", summary)
        except Exception:
            log.exception("cache GC pass failed")
        _gc_stop.wait(CACHE_GC_INTERVAL_SEC)


def start_cache_gc(get_redis, *, daemon: bool = True) -> Optional[threading.Thread]:
    """Start background GC on master (no-op if OSS_CACHE=0)."""
    global _gc_thread
    if not _env_bool("OSS_CACHE", "0"):
        log.info("OSS_CACHE disabled — cache GC not started")
        return None
    if _gc_thread and _gc_thread.is_alive():
        return _gc_thread
    if not OSS_ACCESS_KEY_ID or not OSS_ACCESS_KEY_SECRET:
        log.warning("OSS credentials missing — cache GC not started")
        return None
    _gc_stop.clear()
    _gc_thread = threading.Thread(
        target=_gc_loop,
        args=(get_redis,),
        name="sview-cache-gc",
        daemon=daemon,
    )
    _gc_thread.start()
    return _gc_thread


def stop_cache_gc() -> None:
    _gc_stop.set()
