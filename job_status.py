#!/usr/bin/env python3
"""Redis job status helpers shared by HTTP API and workers."""

from __future__ import annotations

import json
import os
import time
from typing import Any, Optional, Tuple

JOB_KEY_PREFIX = os.environ.get("SVIEW_JOB_KEY_PREFIX", "sview:job:")
LOCK_KEY_PREFIX = os.environ.get("SVIEW_LOCK_KEY_PREFIX", "sview:lock:")
WORKER_KEY_PREFIX = os.environ.get("SVIEW_WORKER_KEY_PREFIX", "sview:worker:")
CACHE_INDEX_KEY = os.environ.get("SVIEW_CACHE_INDEX_KEY", "sview:cache:index")
CACHE_TOTAL_BYTES_KEY = os.environ.get("SVIEW_CACHE_TOTAL_KEY", "sview:cache:total_bytes")
REDIS_QUEUE_KEY = os.environ.get("REDIS_QUEUE_KEY", "player:queue")

SVIEW_CLUSTER = os.environ.get("SVIEW_CLUSTER", "0").lower() in ("1", "true", "yes", "on")
_RAW_POD_IP = (os.environ.get("POD_IP") or "").strip()
POD_IP = _RAW_POD_IP or "local"
SVIEW_LOCK_TTL_SEC = max(30, int(os.environ.get("SVIEW_LOCK_TTL_SEC", "120") or "120"))
WORKER_HEARTBEAT_TTL_SEC = max(30, int(os.environ.get("WORKER_HEARTBEAT_TTL_SEC", "90") or "90"))
CACHE_PIN_SEC = max(0, int(os.environ.get("CACHE_PIN_SEC", "600") or "600"))


def ensure_cluster_identity(*, fatal: bool = True) -> None:
    """SVIEW_CLUSTER=1 requires a real POD_IP; otherwise all pods share owner 'local'."""
    if not SVIEW_CLUSTER:
        return
    if _RAW_POD_IP and _RAW_POD_IP.lower() != "local":
        return
    msg = (
        "SVIEW_CLUSTER=1 requires POD_IP from the Downward API (status.podIP). "
        "Refusing to run with owner='local' (multi-pod locks/proxy would break). "
        "Set POD_IP or set SVIEW_CLUSTER=0 for single-instance."
    )
    if fatal:
        raise SystemExit(msg)
    raise RuntimeError(msg)


def job_id_from_source(source: str) -> str:
    name = source.strip().replace("\\", "/").rstrip("/").split("/")[-1]
    if name.lower().endswith(".mcap"):
        name = name[:-5]
    return name


def job_redis_key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}"


def lock_redis_key(job_id: str) -> str:
    return f"{LOCK_KEY_PREFIX}{job_id}"


def set_job_fields(r, job_id: str, **fields: Any) -> None:
    data = {k: str(v) for k, v in fields.items() if v is not None}
    data["updated_at"] = str(int(time.time()))
    key = job_redis_key(job_id)
    # Redis 4+ accepts multi-field HSET; Windows Redis 3.x only accepts one pair
    # or HMSET. Prefer multi-HSET, fall back to per-field HSET.
    try:
        r.hset(key, mapping=data)
    except Exception:
        pipe = r.pipeline()
        for k, v in data.items():
            pipe.hset(key, k, v)
        pipe.execute()


def get_job_fields(r, job_id: str) -> dict:
    raw = r.hgetall(job_redis_key(job_id)) or {}
    # redis-py may return bytes if decode_responses=False
    out = {}
    for k, v in raw.items():
        if isinstance(k, bytes):
            k = k.decode("utf-8", errors="replace")
        if isinstance(v, bytes):
            v = v.decode("utf-8", errors="replace")
        out[k] = v
    return out


def is_cancel_requested(fields: Optional[dict]) -> bool:
    if not fields:
        return False
    return str(fields.get("cancel") or "0").lower() in ("1", "true", "yes", "on")


def acquire_job_lock(r, job_id: str, owner: Optional[str] = None, ttl: Optional[int] = None) -> bool:
    """SET NX EX lock. Returns True if this owner acquired the lock."""
    owner = (owner or POD_IP).strip() or "local"
    ttl = int(ttl or SVIEW_LOCK_TTL_SEC)
    ok = r.set(lock_redis_key(job_id), owner, nx=True, ex=ttl)
    return bool(ok)


def renew_job_lock(r, job_id: str, owner: Optional[str] = None, ttl: Optional[int] = None) -> bool:
    """Refresh TTL only if we still hold the lock."""
    owner = (owner or POD_IP).strip() or "local"
    ttl = int(ttl or SVIEW_LOCK_TTL_SEC)
    key = lock_redis_key(job_id)
    cur = r.get(key)
    if isinstance(cur, bytes):
        cur = cur.decode("utf-8", errors="replace")
    if cur != owner:
        return False
    r.expire(key, ttl)
    return True


def release_job_lock(r, job_id: str, owner: Optional[str] = None, force: bool = False) -> bool:
    """Delete lock if owned by us, or if force=True."""
    owner = (owner or POD_IP).strip() or "local"
    key = lock_redis_key(job_id)
    cur = r.get(key)
    if isinstance(cur, bytes):
        cur = cur.decode("utf-8", errors="replace")
    if cur is None:
        return True
    if force or cur == owner:
        r.delete(key)
        return True
    return False


def get_lock_owner(r, job_id: str) -> Optional[str]:
    cur = r.get(lock_redis_key(job_id))
    if cur is None:
        return None
    if isinstance(cur, bytes):
        cur = cur.decode("utf-8", errors="replace")
    return cur or None


def collect_job_stats(r) -> dict:
    """Queue depth + locked/processing jobs (cluster-safe via Redis)."""
    queued = int(r.llen(REDIS_QUEUE_KEY) or 0)
    jobs: list = []
    cursor = 0
    pattern = f"{LOCK_KEY_PREFIX}*"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=100)
        for key in keys or []:
            if isinstance(key, bytes):
                key = key.decode("utf-8", errors="replace")
            if not key.startswith(LOCK_KEY_PREFIX):
                continue
            job_id = key[len(LOCK_KEY_PREFIX) :]
            if not job_id:
                continue
            owner = get_lock_owner(r, job_id) or ""
            fields = get_job_fields(r, job_id)
            status = (fields.get("status") or "processing").lower()
            playable = str(fields.get("playable") or "").lower() in ("1", "true", "yes")
            try:
                frames_done = int(fields.get("frames_done") or 0)
            except (TypeError, ValueError):
                frames_done = 0
            jobs.append(
                {
                    "job_id": job_id,
                    "status": status or "processing",
                    "owner_pod": (fields.get("owner_pod") or owner or "").strip() or owner,
                    "source": fields.get("source") or "",
                    "playable": playable,
                    "frames_done": frames_done,
                }
            )
        if cursor == 0:
            break
    jobs.sort(key=lambda j: j.get("job_id") or "")
    return {
        "queued": queued,
        "processing": len(jobs),
        "inflight": queued + len(jobs),
        "jobs": jobs,
    }


def touch_job(r, job_id: str, *, pin_sec: Optional[int] = None) -> dict:
    """Refresh last_access_at / pin_until so GC won't evict an active viewer."""
    now = int(time.time())
    pin = int(pin_sec if pin_sec is not None else CACHE_PIN_SEC)
    patch = {"last_access_at": now}
    if pin > 0:
        patch["pin_until"] = now + pin
    set_job_fields(r, job_id, **patch)
    return get_job_fields(r, job_id)


def register_cache_bytes(r, job_id: str, add_bytes: int) -> int:
    """Track OSS cache size for a job; returns new job cache_bytes."""
    add_bytes = max(0, int(add_bytes or 0))
    fields = get_job_fields(r, job_id)
    try:
        cur = int(fields.get("cache_bytes") or 0)
    except (TypeError, ValueError):
        cur = 0
    new_val = cur + add_bytes
    now = int(time.time())
    set_job_fields(
        r,
        job_id,
        cache_bytes=new_val,
        last_access_at=fields.get("last_access_at") or now,
        cache_status="hot",
    )
    r.sadd(CACHE_INDEX_KEY, job_id)
    if add_bytes:
        r.incrby(CACHE_TOTAL_BYTES_KEY, add_bytes)
    return new_val


def list_cache_jobs(r) -> list:
    raw = r.smembers(CACHE_INDEX_KEY) or set()
    out = []
    for x in raw:
        if isinstance(x, bytes):
            x = x.decode("utf-8", errors="replace")
        if x:
            out.append(x)
    return sorted(out)


def remove_cache_job(r, job_id: str, nbytes: int = 0) -> None:
    r.srem(CACHE_INDEX_KEY, job_id)
    n = max(0, int(nbytes or 0))
    if n:
        try:
            r.decrby(CACHE_TOTAL_BYTES_KEY, n)
        except Exception:
            pass
        # Avoid negative totals
        try:
            total = int(r.get(CACHE_TOTAL_BYTES_KEY) or 0)
            if total < 0:
                r.set(CACHE_TOTAL_BYTES_KEY, 0)
        except Exception:
            pass


def cache_total_bytes(r) -> int:
    try:
        return max(0, int(r.get(CACHE_TOTAL_BYTES_KEY) or 0))
    except (TypeError, ValueError):
        return 0


def worker_redis_key(worker_id: str) -> str:
    return f"{WORKER_KEY_PREFIX}{worker_id}"


def set_worker_heartbeat(
    r,
    worker_id: str,
    *,
    processing: int = 0,
    max_inflight: int = 0,
    jobs: Optional[list] = None,
    extra: Optional[dict] = None,
) -> None:
    """Worker liveness record (TTL). Used by master /metrics and ops UI."""
    payload = {
        "worker_id": worker_id,
        "pod_ip": POD_IP,
        "processing": int(processing),
        "max_inflight": int(max_inflight),
        "jobs": jobs or [],
        "updated_at": int(time.time()),
    }
    if extra:
        payload.update(extra)
    key = worker_redis_key(worker_id)
    r.set(key, json.dumps(payload, ensure_ascii=False), ex=WORKER_HEARTBEAT_TTL_SEC)


def list_workers(r) -> list:
    out = []
    cursor = 0
    pattern = f"{WORKER_KEY_PREFIX}*"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=100)
        for key in keys or []:
            if isinstance(key, bytes):
                key = key.decode("utf-8", errors="replace")
            raw = r.get(key)
            if not raw:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            try:
                out.append(json.loads(raw))
            except Exception:
                continue
        if cursor == 0:
            break
    out.sort(key=lambda w: w.get("worker_id") or "")
    return out


def prometheus_metrics(r) -> str:
    """Text exposition for HPA (Prometheus / custom metrics adapter)."""
    stats = collect_job_stats(r)
    workers = list_workers(r)
    total_cache = cache_total_bytes(r)
    lines = [
        "# HELP sview_queue_depth Redis player queue length",
        "# TYPE sview_queue_depth gauge",
        f"sview_queue_depth {stats['queued']}",
        "# HELP sview_jobs_processing Jobs holding worker locks",
        "# TYPE sview_jobs_processing gauge",
        f"sview_jobs_processing {stats['processing']}",
        "# HELP sview_jobs_inflight queued + processing",
        "# TYPE sview_jobs_inflight gauge",
        f"sview_jobs_inflight {stats['inflight']}",
        "# HELP sview_workers_alive Workers with fresh heartbeats",
        "# TYPE sview_workers_alive gauge",
        f"sview_workers_alive {len(workers)}",
        "# HELP sview_cache_bytes Tracked OSS play-cache bytes",
        "# TYPE sview_cache_bytes gauge",
        f"sview_cache_bytes {total_cache}",
    ]
    return "\n".join(lines) + "\n"


def job_progress_percent(fields: Optional[dict]) -> Optional[float]:
    if not fields:
        return None
    try:
        done = float(fields.get("frames_done") or 0)
        total = float(fields.get("frames_total") or fields.get("frame_count") or 0)
    except (TypeError, ValueError):
        return None
    if total <= 0:
        return None
    return round(min(100.0, 100.0 * done / total), 1)


def job_in_flight(fields: Optional[dict], r=None, job_id: Optional[str] = None) -> bool:
    """True when job is queued/processing and (if checkable) lock still held or just queued."""
    if not fields:
        return False
    status = (fields.get("status") or "").lower()
    if status == "queued":
        return True
    if status != "processing":
        return False
    if r is not None and job_id:
        # processing without a lock → stale; allow re-enqueue
        return get_lock_owner(r, job_id) is not None
    return True


def enqueue_source(
    r,
    source: str,
    job_id: Optional[str] = None,
    *,
    force: bool = False,
) -> Tuple[str, bool]:
    """Record queued status and RPUSH source. Returns (job_id, enqueued).

    If the same job is already queued/processing (lock alive) and force is False,
    skip RPUSH and return enqueued=False.
    """
    source = source.strip()
    jid = job_id or job_id_from_source(source)
    fields = get_job_fields(r, jid)
    if not force and job_in_flight(fields, r, jid) and not is_cancel_requested(fields):
        return jid, False

    set_job_fields(
        r,
        jid,
        status="queued",
        source=source,
        error="",
        frames_done=0,
        frames_total=0,
        cancel=0,
        playable=0,
        owner_pod="",
    )
    r.rpush(REDIS_QUEUE_KEY, source)
    return jid, True


def enqueue_bench_job(
    r,
    source: str,
    type_id: int,
    *,
    job_id: Optional[str] = None,
    transcode_size: int = 0,
    transcode_crf: int = 35,
    force_reencode: int = 0,
    type_name: str = "",
    force: bool = True,
) -> Tuple[str, bool]:
    """Enqueue a typed bench job with per-job transcode overrides (JSON queue payload).

    Returns (job_id, enqueued). Bench defaults to force=True (re-measure).
    """
    source = source.strip()
    jid = job_id or f"{job_id_from_source(source)}__t{int(type_id)}"
    fields = get_job_fields(r, jid)
    if not force and job_in_flight(fields, r, jid) and not is_cancel_requested(fields):
        return jid, False

    set_job_fields(
        r,
        jid,
        status="queued",
        source=source,
        error="",
        frames_done=0,
        frames_total=0,
        cancel=0,
        playable=0,
        owner_pod="",
        bench_type=int(type_id),
        bench_name=type_name or "",
        transcode_size=int(transcode_size),
        transcode_crf=int(transcode_crf),
        force_reencode=int(force_reencode),
        first_frame_sec="",
        full_sec="",
        started_at="",
        first_frame_at="",
        finished_at="",
    )
    payload = json.dumps(
        {
            "v": 1,
            "kind": "bench",
            "source": source,
            "job_id": jid,
            "bench_type": int(type_id),
            "bench_name": type_name or "",
            "transcode_size": int(transcode_size),
            "transcode_crf": int(transcode_crf),
            "force_reencode": int(force_reencode),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    r.rpush(REDIS_QUEUE_KEY, payload)
    return jid, True


def request_cancel(r, job_id: str, *, purge_owner: bool = False) -> dict:
    """Mark job cancelled; release lock if we own it or purge_owner."""
    set_job_fields(
        r,
        job_id,
        status="cancelled",
        cancel=1,
        playable=0,
        frames_done=0,
        error="cancelled by client",
        finished_at=f"{time.time():.6f}",
    )
    release_job_lock(r, job_id, force=purge_owner)
    return get_job_fields(r, job_id)


def parse_queue_item(raw: Any) -> dict:
    """Parse a queue item: plain source string (prod) or JSON bench payload."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    text = (raw or "").strip()
    if text.startswith("{"):
        data = json.loads(text)
        source = (data.get("source") or data.get("path") or "").strip()
        if not source:
            raise ValueError("queue JSON missing source")
        jid = (data.get("job_id") or "").strip() or job_id_from_source(source)
        return {
            "kind": (data.get("kind") or "bench").strip() or "bench",
            "source": source,
            "job_id": jid,
            "bench_type": data.get("bench_type"),
            "bench_name": data.get("bench_name") or "",
            "transcode_size": int(data.get("transcode_size", 0) or 0),
            "transcode_crf": int(data.get("transcode_crf", 35) or 35),
            "force_reencode": int(data.get("force_reencode", 0) or 0),
            "raw": text,
        }
    return {
        "kind": "job",
        "source": text,
        "job_id": job_id_from_source(text),
        "bench_type": None,
        "bench_name": "",
        "transcode_size": None,
        "transcode_crf": None,
        "force_reencode": None,
        "raw": text,
    }
