#!/usr/bin/env python3
"""OSS 播放缓存 GC：空闲 TTL + 500GiB LRU，Lifecycle 作为运维兜底。

环境变量：
  CACHE_MAX_BYTES=536870912000   软上限（约 500 GiB）
  CACHE_IDLE_TTL_SEC=14400       last_access 超过此时长则删除
  CACHE_GC_INTERVAL_SEC=300      master 循环间隔
  CACHE_PIN_SEC=1800             保护最近碰过 / 可播放的任务（30 分钟）
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
CACHE_PIN_SEC = max(0, int(os.environ.get("CACHE_PIN_SEC", "1800") or "1800"))
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
    """删除 player-bin/{job_id}/ 下全部对象。返回删除数量。"""
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
    """返回 OSS 上已有的分段起点下标集合；列举失败返回 None。"""
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
    """分段对象完整（无空洞）时，从下标 0 起可播放的帧数。"""
    seg_n = max(1, int(segment_frames or 30))
    if not starts:
        return 0
    avail = 0
    s = 0
    while s in starts:
        avail = s + seg_n
        s += seg_n
    if claimed is not None and claimed >= 0:
        # 允许末尾短的半段：claimed 可以小于最后一个完整边界。
        if claimed < avail and (avail - seg_n) in starts and claimed > (avail - seg_n):
            return int(claimed)
        return int(min(avail, claimed))
    return int(avail)


def merge_available_ranges(
    ranges: Any,
    *,
    frame_count: Optional[int] = None,
) -> list[list[int]]:
    """归一并合并半开帧区间。"""
    cleaned: list[tuple[int, int]] = []
    try:
        parsed_limit = int(frame_count) if frame_count is not None else None
    except (TypeError, ValueError):
        parsed_limit = None
    limit = parsed_limit if parsed_limit is not None and parsed_limit >= 0 else None
    for item in ranges or []:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        try:
            start, end = int(item[0]), int(item[1])
        except (TypeError, ValueError):
            continue
        start = max(0, start)
        if limit is not None:
            start, end = min(start, limit), min(end, limit)
        if end <= start:
            continue
        cleaned.append((start, end))
    cleaned.sort()
    merged: list[list[int]] = []
    for start, end in cleaned:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return merged


def available_ranges_from_starts(
    starts: set[int],
    *,
    segment_frames: int = 30,
    frame_count: Optional[int] = None,
) -> list[list[int]]:
    """按实际存在的分段对象拼出合并后的半开区间。"""
    seg_n = max(1, int(segment_frames or 30))
    ranges = []
    for start in sorted(starts or set()):
        try:
            s = int(start)
        except (TypeError, ValueError):
            continue
        if s < 0 or s % seg_n:
            continue
        end = s + seg_n
        if frame_count is not None:
            end = min(end, max(0, int(frame_count)))
        if end > s:
            ranges.append([s, end])
    return merge_available_ranges(ranges, frame_count=frame_count)


def contiguous_prefix_from_ranges(ranges: Any) -> int:
    """返回从第 0 帧起连续覆盖的帧数。"""
    merged = merge_available_ranges(ranges)
    if not merged or merged[0][0] != 0:
        return 0
    return int(merged[0][1])


def frame_range_ready(ranges: Any, start: int, end: Optional[int] = None) -> bool:
    """一个半开帧区间是否被完全覆盖。"""
    start = int(start)
    end = int(end if end is not None else start + 1)
    return any(int(a) <= start and int(b) >= end for a, b in merge_available_ranges(ranges))


def contiguous_oss_frames(
    job_id: str,
    *,
    segment_frames: int = 30,
    claimed: Optional[int] = None,
) -> Optional[int]:
    """OSS 分段从 0 起连续可播放的帧数；列举失败返回 None。"""
    starts = list_oss_segment_starts(job_id)
    if starts is None:
        return None
    return contiguous_frames_from_starts(
        starts, segment_frames=segment_frames, claimed=claimed
    )


def filter_playable_segment_starts(
    starts: list[int],
    *,
    frames_available: Optional[int],
    segment_frames: int = 30,
    available_ranges: Any = None,
) -> tuple[list[int], list[dict[str, Any]]]:
    """只保留已公布可播放前缀里、OSS 上存在的分段起点。

    起点可播放的条件：非负，且是 segment_frames 的整数倍，
    并严格小于 frames_available（最后一个对象是
    floor((avail-1)/seg)*seg）。超出范围的起点跳过，不签名。
    """
    seg_n = max(1, int(segment_frames or 30))
    kept: list[int] = []
    skipped: list[dict[str, Any]] = []
    for s in starts:
        try:
            idx = int(s)
        except (TypeError, ValueError):
            skipped.append({"start": s, "reason": "invalid"})
            continue
        if idx < 0:
            skipped.append({"start": idx, "reason": "negative"})
        elif (idx % seg_n) != 0:
            skipped.append({"start": idx, "reason": "unaligned"})
        elif available_ranges is not None and not frame_range_ready(
            available_ranges, idx, idx + 1
        ):
            skipped.append({"start": idx, "reason": "not_ready"})
        elif available_ranges is None and frames_available is not None and idx >= int(frames_available):
            skipped.append({"start": idx, "reason": "not_ready"})
        else:
            kept.append(idx)
    return kept, skipped


def oss_segment_hole(starts: set[int]) -> bool:
    """有对象但缺分段 0 时为 True（尾部孤儿 / Lifecycle 删了一部分）。"""
    return bool(starts) and 0 not in starts


def purge_job_play_cache(
    r,
    job_id: str,
    *,
    reason: str = "purged",
    clear_redis_progress: bool = True,
) -> dict:
    """删除 OSS 上的 player-bin/{job_id}/，并重置 Redis，避免客户端看到幽灵帧。

    本地 /cache/{job_id} 仍由调用方（Master/Worker）删除。
    """
    from sview.jobs.job_status import get_job_fields, remove_cache_job, set_job_fields

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
                    "available_ranges": "[]",
                    "segments_ready_count": 0,
                }
            )
            # 稀疏任务留着 ready 的话，下次打开会被当成已编完、不再开编码线程。
            if (fields.get("publish_mode") or "").lower() == "sparse":
                patch.update({"status": "queued", "encode_status": "queued"})
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
    """把 manifest 的 frames_available 夹到 OSS 上连续的分段（修幽灵尾）。

    OSS 列举失败时置 cache_align_failed，调用方不能再公布
    未夹紧的本地/OSS 声明（Master 返回 425，而不是幽灵尾）。
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
        manifest["cache_align_failed"] = True
        manifest["cache_align_reason"] = "oss_list_failed"
        return manifest
    manifest.pop("cache_align_failed", None)

    publish_mode = str(manifest.get("publish_mode") or "progressive").lower()
    frame_count = _safe_int(bundle.get("frame_count"), 0) or None
    physical_ranges = available_ranges_from_starts(
        starts,
        segment_frames=seg_n,
        frame_count=frame_count,
    )
    if publish_mode == "sparse":
        bundle["available_ranges"] = physical_ranges
        bundle["frames_available"] = contiguous_prefix_from_ranges(physical_ranges)
        manifest["bundle"] = bundle
        manifest["cache_aligned"] = True
        manifest["cache_align_reason"] = "oss_sparse_ranges"
        if frame_count and bundle["frames_available"] >= frame_count:
            manifest["status"] = "done"
        else:
            manifest["status"] = "partial"
        return manifest

    aligned = contiguous_frames_from_starts(
        starts, segment_frames=seg_n, claimed=claimed_i
    )
    hole = oss_segment_hole(starts)
    status = (job_status or manifest.get("status") or "").lower()

    # 孤儿尾（Lifecycle / 取消时没清干净）：不要公布靠前的帧。
    if hole:
        bundle["frames_available"] = aligned  # 通常是 0
        manifest["bundle"] = bundle
        if aligned <= 0 and status in ("done", "ready", "partial"):
            # 从 0 起没有连续段时，不要看起来像能播。
            manifest["status"] = "partial"
        manifest["cache_aligned"] = True
        manifest["cache_align_reason"] = "oss_segment_hole"
        return manifest

    if claimed_i is not None and aligned < claimed_i:
        bundle["frames_available"] = aligned
        manifest["bundle"] = bundle
        manifest["cache_aligned"] = True
        manifest["cache_align_reason"] = "oss_contiguous_clamp"
        # OSS 上什么都不剩时，把 done 降级，避免前端当成完整缓存。
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
    """先淘汰空闲任务，再按 LRU 淘汰，直到低于 CACHE_MAX_BYTES。"""
    from sview.jobs.job_status import (
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
    """在 master 上启动后台 GC（OSS_CACHE=0 时什么都不做）。"""
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
