#!/usr/bin/env python3
"""供 HTTP API 与 worker 共用的 Redis 任务状态辅助。"""

from __future__ import annotations

import json
import hashlib
import os
import time
import uuid
from pathlib import Path
from typing import Any, Optional, Tuple

JOB_KEY_PREFIX = os.environ.get("SVIEW_JOB_KEY_PREFIX", "sview:job:")
LOCK_KEY_PREFIX = os.environ.get("SVIEW_LOCK_KEY_PREFIX", "sview:lock:")
WORKER_KEY_PREFIX = os.environ.get("SVIEW_WORKER_KEY_PREFIX", "sview:worker:")
CACHE_INDEX_KEY = os.environ.get("SVIEW_CACHE_INDEX_KEY", "sview:cache:index")
CACHE_TOTAL_BYTES_KEY = os.environ.get("SVIEW_CACHE_TOTAL_KEY", "sview:cache:total_bytes")
REDIS_QUEUE_KEY = os.environ.get("REDIS_QUEUE_KEY", "player:queue")
SEGMENT_PENDING_PREFIX = os.environ.get(
    "SVIEW_SEGMENT_PENDING_PREFIX", "sview:segment:pending:"
)
SEGMENT_LOCK_PREFIX = os.environ.get(
    "SVIEW_SEGMENT_LOCK_PREFIX", "sview:segment:lock:"
)
SEGMENT_SLOT_PREFIX = os.environ.get(
    "SVIEW_SEGMENT_SLOT_PREFIX", "sview:segment:slot:"
)
ENCODE_REQUEST_PREFIX = os.environ.get(
    "SVIEW_ENCODE_REQUEST_PREFIX", "sview:encode:request:"
)
SEGMENT_SUBSCRIBERS_PREFIX = os.environ.get(
    "SVIEW_SEGMENT_SUBSCRIBERS_PREFIX", "sview:segment:subscribers:"
)
JOB_ACTIVE_PREFIX = os.environ.get(
    "SVIEW_JOB_ACTIVE_PREFIX", "sview:job:active:"
)
ENCODE_REQUEST_TTL_SEC = max(
    300, int(os.environ.get("SVIEW_ENCODE_REQUEST_TTL_SEC", "3600") or "3600")
)
# 播放前沿之前预先排队的片段数。每完成一段，worker 就补这个
# 窗口，后面的片段还在编码时就可以先播。
SPARSE_FORWARD_SEGMENTS = max(
    1, min(8, int(os.environ.get("SVIEW_SPARSE_FORWARD_SEGMENTS", "3") or "3"))
)

SVIEW_CLUSTER = os.environ.get("SVIEW_CLUSTER", "0").lower() in ("1", "true", "yes", "on")
_RAW_POD_IP = (os.environ.get("POD_IP") or "").strip()
POD_IP = _RAW_POD_IP or "local"
SVIEW_LOCK_TTL_SEC = max(30, int(os.environ.get("SVIEW_LOCK_TTL_SEC", "120") or "120"))
WORKER_HEARTBEAT_TTL_SEC = max(30, int(os.environ.get("WORKER_HEARTBEAT_TTL_SEC", "90") or "90"))
CACHE_PIN_SEC = max(0, int(os.environ.get("CACHE_PIN_SEC", "1800") or "1800"))


def ensure_cluster_identity(*, fatal: bool = True) -> None:
    """开启 SVIEW_CLUSTER=1 时必须有真实 POD_IP，否则所有 pod 都会共用 owner 'local'。"""
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
    low = name.lower()
    for ext in (".mcap", ".hdf5", ".h5", ".hdf"):
        if low.endswith(ext):
            return name[: -len(ext)]
    return name


def sparse_cache_job_id(source: str, local_path: str | Path | None = None) -> str:
    """同一源版本、同一编码配置下稳定的共享缓存 id。"""
    stem = job_id_from_source(source)
    normalized = source.strip().replace("\\", "/").rstrip("/")
    version = ""
    if local_path is not None:
        try:
            stat = Path(local_path).stat()
            version = f"{stat.st_size}:{stat.st_mtime_ns}"
        except OSError:
            pass
    profile = ":".join(
        [
            os.environ.get("SVIEW_CACHE_FORMAT_VERSION", "sparse-v1"),
            os.environ.get("SEGMENT_FRAMES", "30"),
            os.environ.get("TRANSCODE_SIZE", "0"),
            os.environ.get("TRANSCODE_WIDTH", "0"),
            os.environ.get("TRANSCODE_HEIGHT", "0"),
            os.environ.get("TRANSCODE_CRF", "35"),
            os.environ.get("TRANSCODE_BITRATE", "2M"),
        ]
    )
    digest = hashlib.sha256(
        f"{normalized}\n{version}\n{profile}".encode("utf-8")
    ).hexdigest()[:12]
    # 给 API 的 job_id 上限 200 字符留出余量。
    return f"{stem[:180]}_c{digest}"


def job_redis_key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}"


def lock_redis_key(job_id: str) -> str:
    return f"{LOCK_KEY_PREFIX}{job_id}"


def segment_pending_key(job_id: str, start: int) -> str:
    return f"{SEGMENT_PENDING_PREFIX}{job_id}:{int(start)}"


def segment_lock_key(job_id: str, start: int) -> str:
    return f"{SEGMENT_LOCK_PREFIX}{job_id}:{int(start)}"


def segment_slot_key(job_id: str, start: int) -> str:
    """一个片段只占一个 worker 槽，等它的窗口都共用这一个。"""
    return f"{SEGMENT_SLOT_PREFIX}{job_id}:{int(start)}"


def encode_request_key(job_id: str, request_id: str) -> str:
    return f"{ENCODE_REQUEST_PREFIX}{job_id}:{request_id}"


def segment_subscribers_key(job_id: str, start: int) -> str:
    return f"{SEGMENT_SUBSCRIBERS_PREFIX}{job_id}:{int(start)}"


def set_job_fields(r, job_id: str, **fields: Any) -> None:
    data = {k: str(v) for k, v in fields.items() if v is not None}
    data["updated_at"] = str(int(time.time()))
    key = job_redis_key(job_id)
    # 多字段 HSET：Redis 4+ 可以一次写多个字段；Windows 的 Redis 3.x 只接受一对，
    # 或者用 HMSET。优先多字段 HSET，失败再逐字段 HSET。
    try:
        r.hset(key, mapping=data)
    except Exception:
        pipe = r.pipeline()
        for k, v in data.items():
            pipe.hset(key, k, v)
        pipe.execute()


def get_job_fields(r, job_id: str) -> dict:
    raw = r.hgetall(job_redis_key(job_id)) or {}
    # 若 decode_responses=False，redis-py 可能返回 bytes
    out = {}
    for k, v in raw.items():
        if isinstance(k, bytes):
            k = k.decode("utf-8", errors="replace")
        if isinstance(v, bytes):
            v = v.decode("utf-8", errors="replace")
        out[k] = v
    return out


def create_encode_request(
    r,
    job_id: str,
    *,
    client_id: str,
    client_epoch: int,
    target_start: int,
    starts: list[int],
    status: str = "waiting",
) -> str:
    request_id = uuid.uuid4().hex
    now = int(time.time())
    data = {
        "request_id": request_id,
        "job_id": job_id,
        "client_id": (client_id or "anonymous")[:128],
        "client_epoch": str(max(0, int(client_epoch or 0))),
        "target_start": str(int(target_start)),
        "starts": json.dumps([int(value) for value in starts], separators=(",", ":")),
        "status": status,
        "created_at": str(now),
        "updated_at": str(now),
    }
    key = encode_request_key(job_id, request_id)
    r.hset(key, mapping=data)
    r.expire(key, ENCODE_REQUEST_TTL_SEC)
    return request_id


def get_encode_request(r, job_id: str, request_id: str) -> dict:
    raw = r.hgetall(encode_request_key(job_id, request_id)) or {}
    out: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="replace")
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        out[str(key)] = value
    for key in ("client_epoch", "target_start", "created_at", "updated_at"):
        if key in out:
            try:
                out[key] = int(out[key])
            except (TypeError, ValueError):
                pass
    if isinstance(out.get("starts"), str):
        try:
            out["starts"] = json.loads(out["starts"])
        except (TypeError, ValueError, json.JSONDecodeError):
            out["starts"] = []
    return out


def set_encode_request_status(
    r,
    job_id: str,
    request_id: str,
    status: str,
    *,
    error: str = "",
) -> None:
    key = encode_request_key(job_id, request_id)
    if not r.hgetall(key):
        return
    data = {
        "status": status,
        "updated_at": str(int(time.time())),
        "error": error[:500],
    }
    r.hset(key, mapping=data)
    r.expire(key, ENCODE_REQUEST_TTL_SEC)


def subscribe_encode_request(
    r, job_id: str, request_id: str, starts: list[int]
) -> None:
    for start in starts:
        key = segment_subscribers_key(job_id, start)
        r.sadd(key, request_id)
        r.expire(key, ENCODE_REQUEST_TTL_SEC)


def cancel_encode_request(r, job_id: str, request_id: str) -> bool:
    request = get_encode_request(r, job_id, request_id)
    if not request:
        return False
    set_encode_request_status(r, job_id, request_id, "cancelled")
    for start in request.get("starts") or []:
        r.srem(segment_subscribers_key(job_id, int(start)), request_id)
    return True


def complete_segment_requests(
    r,
    job_id: str,
    start: int,
    *,
    error: str = "",
) -> None:
    key = segment_subscribers_key(job_id, start)
    request_ids = r.smembers(key) or set()
    for request_id in request_ids:
        if isinstance(request_id, bytes):
            request_id = request_id.decode("utf-8", errors="replace")
        request = get_encode_request(r, job_id, str(request_id))
        if not request or request.get("status") == "cancelled":
            continue
        target_start = request.get("target_start")
        if target_start is not None and int(target_start) == int(start):
            set_encode_request_status(
                r,
                job_id,
                str(request_id),
                "failed" if error else "ready",
                error=error,
            )
            for subscribed_start in request.get("starts") or []:
                if int(subscribed_start) != int(start):
                    r.srem(
                        segment_subscribers_key(job_id, int(subscribed_start)),
                        str(request_id),
                    )
    r.delete(key)


def is_cancel_requested(fields: Optional[dict]) -> bool:
    if not fields:
        return False
    return str(fields.get("cancel") or "0").lower() in ("1", "true", "yes", "on")


def acquire_job_lock(r, job_id: str, owner: Optional[str] = None, ttl: Optional[int] = None) -> bool:
    """用 SET NX EX 加锁。这个 owner 抢到锁时返回 True。"""
    owner = (owner or POD_IP).strip() or "local"
    ttl = int(ttl or SVIEW_LOCK_TTL_SEC)
    ok = r.set(lock_redis_key(job_id), owner, nx=True, ex=ttl)
    return bool(ok)


def renew_job_lock(r, job_id: str, owner: Optional[str] = None, ttl: Optional[int] = None) -> bool:
    """只有仍持有锁时才刷新 TTL。"""
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
    """锁属于自己，或 force=True 时，删除这把锁。"""
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
    """队列深度，以及持锁或处理中的任务（经 Redis，集群下也安全）。"""
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
    workers = list_workers(r)
    worker_processing = sum(
        max(0, int(worker.get("processing") or 0)) for worker in workers
    )
    by_job = {str(item.get("job_id") or ""): item for item in jobs}

    def _attach(job_id: str, *, worker: dict, segment: int | None = None, client_id: str = "", live: bool = False) -> None:
        item = by_job.get(job_id)
        if item is None:
            fields = get_job_fields(r, job_id)
            try:
                frames = int(fields.get("frames_done") or 0)
            except (TypeError, ValueError):
                frames = 0
            item = {
                "job_id": job_id,
                "status": fields.get("status") or "processing",
                "owner_pod": fields.get("owner_pod") or worker.get("pod_ip") or "",
                "source": fields.get("source") or "",
                "playable": str(fields.get("playable") or "").lower()
                in ("1", "true", "yes"),
                "frames_done": frames,
                "active_segments": [],
            }
            jobs.append(item)
            by_job[job_id] = item
        if live:
            item["status"] = "processing"
            # 同一个文件可以有多条编码线（每人一条）。按窗口和所在机器列出来，
            # 不要收成一个 job 后看不出来。
            session = {
                "client_id": client_id,
                "pod_ip": (worker.get("pod_ip") or ""),
                "worker_id": (worker.get("worker_id") or ""),
            }
            if segment is not None:
                session["segment"] = int(segment)
            sessions = item.setdefault("sessions", [])
            if not any(
                s.get("client_id") == session["client_id"]
                and s.get("pod_ip") == session["pod_ip"]
                and s.get("segment") == session.get("segment")
                for s in sessions
            ):
                sessions.append(session)
        if segment is not None:
            item.setdefault("active_segments", []).append(int(segment))
        if client_id:
            clients = item.setdefault("clients", [])
            if client_id not in clients:
                clients.append(client_id)

    active_ids: set[str] = set()
    for worker in workers:
        for active_id in worker.get("jobs") or []:
            text = str(active_id)
            if ":client:" in text:
                job_id, _, client_id = text.partition(":client:")
                active_ids.add(job_id)
                _attach(job_id, worker=worker, client_id=client_id, live=True)
            elif ":segment:" in text:
                job_id, _, start = text.partition(":segment:")
                active_ids.add(job_id)
                try:
                    seg = int(start or 0)
                except (TypeError, ValueError):
                    seg = 0
                _attach(job_id, worker=worker, segment=seg, live=True)
            elif text:
                active_ids.add(text)
                _attach(text, worker=worker, live=True)
    # 排队只认真正在等 Worker 的：队列里的条目，或已排了编码线还没被接走的
    # pending 标记（带 TTL）。任务 hash 里 status=queued 不算，GC 清理后的
    # 稀疏任务和从没开始过的提交都会停在 queued。
    waiting_ids: list[str] = []
    seen_waiting: set[str] = set()

    def _mark_waiting(job_id: str) -> None:
        if not job_id or job_id in active_ids or job_id in by_job or job_id in seen_waiting:
            return
        seen_waiting.add(job_id)
        waiting_ids.append(job_id)

    try:
        raw_items = r.lrange(REDIS_QUEUE_KEY, 0, -1) or []
    except Exception:
        raw_items = []
    for raw in raw_items:
        try:
            _mark_waiting(str(parse_queue_item(raw).get("job_id") or ""))
        except Exception:
            continue
    for prefix in ("sview:session:pending:", SEGMENT_PENDING_PREFIX):
        for key in _scan_keys(r, f"{prefix}*"):
            _mark_waiting(key[len(prefix):].rsplit(":", 1)[0])
    for job_id in waiting_ids:
        _attach(job_id, worker={})
        by_job[job_id]["status"] = "queued"
    waiting = len(waiting_ids)
    jobs.sort(key=lambda j: j.get("job_id") or "")
    processing = max(
        sum(1 for item in jobs if (item.get("status") or "") != "queued"),
        worker_processing,
    )
    queued = max(queued, waiting)
    return {
        "queued": queued,
        "processing": processing,
        "inflight": queued + processing,
        "jobs": jobs,
    }


def touch_job(r, job_id: str, *, pin_sec: Optional[int] = None) -> dict:
    """刷新 last_access_at / pin_until，避免 GC 清掉正在观看的人。"""
    now = int(time.time())
    pin = int(pin_sec if pin_sec is not None else CACHE_PIN_SEC)
    patch = {"last_access_at": now}
    if pin > 0:
        patch["pin_until"] = now + pin
    set_job_fields(r, job_id, **patch)
    return get_job_fields(r, job_id)


def register_cache_bytes(r, job_id: str, add_bytes: int) -> int:
    """记录某个任务的 OSS 缓存大小，并返回新的 cache_bytes。"""
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


def _scan_keys(r, pattern: str) -> list[str]:
    """返回匹配 Redis glob 的键。客户端不能 scan 时为空。"""
    if not hasattr(r, "scan"):
        return []
    found: list[str] = []
    cursor: Any = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=200)
        for key in keys or []:
            if isinstance(key, bytes):
                key = key.decode("utf-8", errors="replace")
            found.append(str(key))
        if cursor in (0, "0", b"0"):
            break
    return found


def _delete_keys(r, keys: list[str]) -> None:
    for key in keys:
        try:
            r.delete(key)
        except Exception:
            pass


def _clear_job_runtime_keys(r, job_id: str) -> None:
    """丢掉锁、会话和片段认领，保留任务 hash 本身。"""
    exact = [
        lock_redis_key(job_id),
        job_active_key(job_id),
        f"sview:frame-index:lock:{job_id}",
        f"sview:manifest:lock:{job_id}",
        f"sview:segment:end:{job_id}",
    ]
    patterns = [
        f"{SEGMENT_PENDING_PREFIX}{job_id}:*",
        f"{SEGMENT_LOCK_PREFIX}{job_id}:*",
        f"{SEGMENT_SLOT_PREFIX}{job_id}:*",
        f"{ENCODE_REQUEST_PREFIX}{job_id}:*",
        f"{SEGMENT_SUBSCRIBERS_PREFIX}{job_id}:*",
        f"sview:session:alive:{job_id}:*",
        f"sview:session:pending:{job_id}:*",
        f"sview:session:frontier:{job_id}:*",
    ]
    keys = list(exact)
    for pattern in patterns:
        keys.extend(_scan_keys(r, pattern))
    _delete_keys(r, keys)


def _drop_queued_job(r, job_id: str, source: str = "") -> None:
    """去掉会再次启动这个任务的队列项。"""
    try:
        items = r.lrange(REDIS_QUEUE_KEY, 0, -1) or []
    except Exception:
        return
    source = (source or "").strip()
    for raw in items:
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        matched = bool(source) and text.strip() == source
        if not matched and text.startswith("{"):
            try:
                data = json.loads(text)
            except (TypeError, ValueError, json.JSONDecodeError):
                data = {}
            matched = str(data.get("job_id") or "") == job_id
        if matched:
            try:
                r.lrem(REDIS_QUEUE_KEY, 1, raw)
            except Exception:
                pass


def jobs_matching_source(r, source: str) -> list[str]:
    """已保存的 source 等于该路径的那些 job_id。

    稀疏 id 带文件 stat 摘要，文件缺失时算出的 id
    和文件还在时创建的任务不一样。
    """
    source = (source or "").strip()
    if not source:
        return []
    stem = job_id_from_source(source)
    ids: list[str] = []
    for key in _scan_keys(r, f"{JOB_KEY_PREFIX}{stem}*"):
        if not key.startswith(JOB_KEY_PREFIX):
            continue
        job_id = key[len(JOB_KEY_PREFIX) :]
        if (get_job_fields(r, job_id).get("source") or "").strip() == source:
            ids.append(job_id)
    return ids


def fail_job(
    r,
    job_id: str,
    error: str,
    *,
    source: str = "",
    publish_mode: str = "",
) -> dict:
    """把一个任务标成失败，保留 error，并清掉它的 Redis 播放状态。"""
    message = (error or "failed").strip()[:500]
    _clear_job_runtime_keys(r, job_id)
    _drop_queued_job(r, job_id, source)
    fields = get_job_fields(r, job_id)
    try:
        nbytes = int(fields.get("cache_bytes") or 0)
    except (TypeError, ValueError):
        nbytes = 0
    try:
        remove_cache_job(r, job_id, nbytes)
    except Exception:
        pass
    patch: dict[str, Any] = {
        "status": "failed",
        "error": message,
        "playable": 0,
        "frames_done": 0,
        "frames_total": 0,
        "frame_count": 0,
        "available_ranges": "[]",
        "segments_ready_count": 0,
        "encode_status": "failed",
        "encode_window": "",
        "encode_frontier": "",
        "decode_start": "",
        "input_frames": "",
        "output_frames": "",
        "play_base": "",
        "cache_bytes": 0,
        "cache_status": "",
        "owner_pod": "",
        "cancel": 0,
        "finished_at": f"{time.time():.6f}",
    }
    if source:
        patch["source"] = source
    if publish_mode:
        patch["publish_mode"] = publish_mode
    set_job_fields(r, job_id, **patch)
    return get_job_fields(r, job_id)


def fail_submit(
    r,
    job_id: str,
    source: str,
    error: str,
    *,
    publish_mode: str = "",
) -> list[str]:
    """让这次提交失败，同一 source 更早的 Redis 任务也一并失败。"""
    ids: list[str] = []
    try:
        ids = jobs_matching_source(r, source)
    except Exception:
        ids = []
    if job_id and job_id not in ids:
        ids.append(job_id)
    for jid in ids:
        fail_job(r, jid, error, source=source, publish_mode=publish_mode)
    return ids


def remove_cache_job(r, job_id: str, nbytes: int = 0) -> None:
    r.srem(CACHE_INDEX_KEY, job_id)
    n = max(0, int(nbytes or 0))
    if n:
        try:
            r.decrby(CACHE_TOTAL_BYTES_KEY, n)
        except Exception:
            pass
        # 避免总量变成负数
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


def job_active_key(job_id: str) -> str:
    return f"{JOB_ACTIVE_PREFIX}{job_id}"


def claim_job_segment(
    r,
    job_id: str,
    start: int,
    owner: str,
    *,
    ttl: int = 180,
) -> bool:
    """每个共享任务同时只允许一个稀疏片段在跑。"""
    return bool(
        r.set(job_active_key(job_id), f"{int(start)}:{owner}", nx=True, ex=max(30, int(ttl)))
    )


def release_job_segment(r, job_id: str, owner: str) -> None:
    key = job_active_key(job_id)
    current = r.get(key)
    if isinstance(current, bytes):
        current = current.decode("utf-8", errors="replace")
    if current and str(current).endswith(f":{owner}"):
        r.delete(key)


def set_worker_heartbeat(
    r,
    worker_id: str,
    *,
    processing: int = 0,
    max_inflight: int = 0,
    jobs: Optional[list] = None,
    extra: Optional[dict] = None,
) -> None:
    """带 TTL 的 worker 存活记录。给 master 的 /metrics 和运维界面用。"""
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
    """给 HPA 用的文本指标（Prometheus / 自定义指标适配器）。"""
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
    """任务处于 queued 或 processing，且（查得到的话）仍持有锁，或只是刚入队时，返回 True。"""
    if not fields:
        return False
    status = (fields.get("status") or "").lower()
    if status == "queued":
        return True
    if status != "processing":
        return False
    if r is not None and job_id:
        # 状态为 processing 但没有锁，视为过期，允许重新入队
        return get_lock_owner(r, job_id) is not None
    return True


def enqueue_source(
    r,
    source: str,
    job_id: Optional[str] = None,
    *,
    force: bool = False,
    origin_sec: float = 0.0,
) -> Tuple[str, bool]:
    """记下 queued 状态，并 RPUSH source。返回 (job_id, enqueued)。

    同一任务已在 queued 或 processing（锁还在）且 force 为 False 时，
    跳过 RPUSH，并返回 enqueued=False。
    ``origin_sec`` > 0 时从该偏移开始编码，job_id 变成 ``{stem}_o{sec}``。
    """
    from sview.jobs.origin import origin_job_id, parse_start_sec

    source = source.strip()
    origin_sec = parse_start_sec(origin_sec)
    jid = job_id or origin_job_id(job_id_from_source(source), origin_sec)
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
        origin_sec=origin_sec if origin_sec > 0 else "",
    )
    if origin_sec > 0:
        payload = json.dumps(
            {
                "v": 1,
                "kind": "job",
                "source": source,
                "job_id": jid,
                "origin_sec": origin_sec,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        r.rpush(REDIS_QUEUE_KEY, payload)
    else:
        r.rpush(REDIS_QUEUE_KEY, source)
    return jid, True


def enqueue_sparse_job(
    r,
    source: str,
    *,
    job_id: Optional[str] = None,
    force: bool = False,
    segment_frames: int = 30,
) -> Tuple[str, bool]:
    """创建一个稀疏任务，并把首段入队。"""
    source = source.strip()
    jid = job_id or job_id_from_source(source)
    fields = get_job_fields(r, jid)
    if (
        not force
        and str(fields.get("publish_mode") or "").lower() == "sparse"
        and not is_cancel_requested(fields)
    ):
        try:
            ranges = json.loads(fields.get("available_ranges") or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            ranges = []
        first_ready = any(
            isinstance(value, (list, tuple))
            and len(value) >= 2
            and int(value[0]) <= 0
            and int(value[1]) >= max(1, int(segment_frames or 30))
            for value in ranges
        )
        active = any(
            str(active_id) == jid
            or str(active_id).startswith(f"{jid}:segment:")
            or str(active_id).startswith(f"{jid}:client:")
            for worker in list_workers(r)
            for active_id in (worker.get("jobs") or [])
        )
        if first_ready or active:
            return jid, False
        # 恢复 worker 认领片段后挂掉的稀疏任务。
        # 共享 OSS 仍以它为准，只重置过期的 Redis 认领。
        for start in range(0, max(1, int(segment_frames or 30)), max(1, int(segment_frames or 30))):
            r.delete(segment_pending_key(jid, start))
            r.delete(segment_lock_key(jid, start))
        return jid, True
    set_job_fields(
        r,
        jid,
        status="queued",
        source=source,
        cache_id=jid,
        publish_mode="sparse",
        error="",
        frames_done=0,
        frames_total=0,
        frame_count=0,
        segments_ready_count=0,
        available_ranges="[]",
        segment_epoch=0,
        encode_status="queued",
        encode_frontier=0,
        cancel=0,
        playable=0,
        owner_pod="",
    )
    return jid, True


def enqueue_segment_window(
    r,
    job_id: str,
    source: str,
    *,
    frame_index: int,
    prefetch_segments: int = 2,
    segment_frames: int = 30,
    client_epoch: int = 0,
    request_id: str = "",
    client_id: str = "",
) -> list[int]:
    """把缺少的稀疏片段入队，并用 Redis SET-NX 去重。

    片段用 LPUSH，这样交互拖动能压过普通的 RPUSH
    任务，窗口内部仍保持升序。
    """
    seg_n = max(1, int(segment_frames or 30))
    frame = max(0, int(frame_index or 0))
    first = (frame // seg_n) * seg_n
    count = max(1, min(16, int(prefetch_segments or 1)))
    fields = get_job_fields(r, job_id)
    try:
        total = int(fields.get("frame_count") or fields.get("frames_total") or 0)
    except (TypeError, ValueError):
        total = 0
    starts = [first + i * seg_n for i in range(count)]
    if total > 0:
        starts = [s for s in starts if s < total]
    if request_id:
        subscribe_encode_request(r, job_id, request_id, starts)

    try:
        epoch = int(r.hincrby(job_redis_key(job_id), "segment_epoch", 1))
    except Exception:
        epoch = int(fields.get("segment_epoch") or 0) + 1
        set_job_fields(r, job_id, segment_epoch=epoch)

    queued: list[int] = []
    payloads: list[str] = []
    for start in starts:
        pending = segment_pending_key(job_id, start)
        if not r.set(pending, str(epoch), nx=True, ex=1800):
            continue
        end = min(total, start + seg_n) if total > 0 else start + seg_n
        payloads.append(
            json.dumps(
                {
                    "v": 2,
                    "kind": "segment",
                    "source": source,
                    "job_id": job_id,
                    "seg_start": start,
                    "seg_end": end,
                    "segment_frames": seg_n,
                    "segment_epoch": epoch,
                    "client_epoch": int(client_epoch or 0),
                    "request_id": request_id,
                    "client_id": client_id,
                    "priority": 0 if start == first else 1,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        queued.append(start)
    # 倒序 LPUSH，让连续播放的那段留在队头。
    # 更靠后的预取片段排在它后面。
    for payload in reversed(payloads):
        r.lpush(REDIS_QUEUE_KEY, payload)
    active = (fields.get("encode_status") or "").lower()
    if queued:
        status_fields = {"encode_status": "queued"}
    elif active in ("decoding", "encoding", "locating", "reading_mcap_index", "queued"):
        status_fields = {}
    else:
        status_fields = {"encode_status": "idle"}
    set_job_fields(
        r,
        job_id,
        status="processing",
        publish_mode="sparse",
        encode_window=json.dumps(
            [first, (starts[-1] + seg_n) if starts else first],
            separators=(",", ":"),
        ),
        segment_epoch=epoch,
        cancel=0,
        error="",
        **status_fields,
    )
    return queued


def enqueue_forward_from(
    r,
    job_id: str,
    source: str,
    *,
    frame_index: int,
    segment_frames: int = 30,
    window: int = SPARSE_FORWARD_SEGMENTS,
    client_epoch: int = 0,
    request_id: str = "",
    client_id: str = "",
    set_frontier: bool = True,
    bump_epoch: bool = True,
    ranges_override: Any = None,
) -> list[int]:
    """从播放头或拖动点往前，把缺少的片段入队。

    已经在 available_ranges 里的片段会跳过。第一个缺少的
    片段推到队头。拖动会记下 encode_frontier，
    于是这个任务更早的片段不再作为播放路径。
    """
    from sview.cache.oss_cache_gc import frame_range_ready

    seg_n = max(1, int(segment_frames or 30))
    origin = max(0, int(frame_index or 0))
    origin = (origin // seg_n) * seg_n
    window_n = max(1, min(8, int(window or SPARSE_FORWARD_SEGMENTS)))
    fields = get_job_fields(r, job_id)
    try:
        total = int(fields.get("frame_count") or fields.get("frames_total") or 0)
    except (TypeError, ValueError):
        total = 0
    if ranges_override is not None:
        ranges = ranges_override
    else:
        try:
            ranges = json.loads(fields.get("available_ranges") or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            ranges = []
    if set_frontier:
        set_job_fields(r, job_id, encode_frontier=origin)
        fields = get_job_fields(r, job_id)

    starts: list[int] = []
    seg = origin
    scanned = 0
    limit = total if total > 0 else origin + window_n * seg_n
    while len(starts) < window_n and seg < limit and scanned < 10000:
        scanned += 1
        end = min(total, seg + seg_n) if total > 0 else seg + seg_n
        if end > seg and not frame_range_ready(ranges, seg, end):
            starts.append(seg)
        seg += seg_n
    if request_id and starts:
        subscribe_encode_request(r, job_id, request_id, starts[:1])

    if bump_epoch:
        try:
            epoch = int(r.hincrby(job_redis_key(job_id), "segment_epoch", 1))
        except Exception:
            epoch = int(fields.get("segment_epoch") or 0) + 1
            set_job_fields(r, job_id, segment_epoch=epoch)
    else:
        try:
            epoch = int(fields.get("segment_epoch") or 0)
        except (TypeError, ValueError):
            epoch = 0

    queued: list[int] = []
    payloads: list[str] = []
    for index, start in enumerate(starts):
        pending = segment_pending_key(job_id, start)
        if not r.set(pending, str(epoch), nx=True, ex=1800):
            continue
        end = min(total, start + seg_n) if total > 0 else start + seg_n
        payloads.append(
            json.dumps(
                {
                    "v": 2,
                    "kind": "segment",
                    "source": source,
                    "job_id": job_id,
                    "seg_start": start,
                    "seg_end": end,
                    "segment_frames": seg_n,
                    "segment_epoch": epoch,
                    "client_epoch": int(client_epoch or 0),
                    "request_id": request_id if index == 0 else "",
                    "client_id": client_id if index == 0 else "",
                    "priority": 0 if index == 0 else 1,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        queued.append(start)
    for payload in reversed(payloads):
        r.lpush(REDIS_QUEUE_KEY, payload)
    active = (fields.get("encode_status") or "").lower()
    if queued:
        status_fields = {"encode_status": "queued"}
    elif active in ("decoding", "encoding", "locating", "reading_mcap_index", "queued"):
        status_fields = {}
    else:
        status_fields = {"encode_status": "idle"}
    window_end = (queued[-1] + seg_n) if queued else origin
    set_job_fields(
        r,
        job_id,
        status="processing",
        publish_mode="sparse",
        encode_window=json.dumps([origin, window_end], separators=(",", ":")),
        **({"encode_frontier": origin} if set_frontier else {}),
        **({"segment_epoch": epoch} if bump_epoch else {}),
        cancel=0,
        error="",
        **status_fields,
    )
    return queued


def session_frontier_key(job_id: str, client_id: str) -> str:
    return f"sview:session:frontier:{job_id}:{(client_id or 'anonymous')[:128]}"


def session_alive_key(job_id: str, client_id: str) -> str:
    return f"sview:session:alive:{job_id}:{(client_id or 'anonymous')[:128]}"


def session_pending_key(job_id: str, client_id: str) -> str:
    return f"sview:session:pending:{job_id}:{(client_id or 'anonymous')[:128]}"


def read_client_frontier(r, job_id: str, client_id: str) -> int:
    raw = r.get(session_frontier_key(job_id, client_id))
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return 0


def set_client_frontier(
    r,
    job_id: str,
    client_id: str,
    frame_index: int,
    *,
    segment_frames: int = 30,
) -> int:
    """把这个窗口指到某个片段，不再另开编码线程。"""
    seg_n = max(1, int(segment_frames or 30))
    origin = (max(0, int(frame_index or 0)) // seg_n) * seg_n
    r.set(session_frontier_key(job_id, client_id), str(origin), ex=ENCODE_REQUEST_TTL_SEC)
    set_job_fields(r, job_id, encode_frontier=origin)
    return origin


def claim_segment_slot(
    r,
    job_id: str,
    start: int,
    client_id: str,
    *,
    ttl: int = 180,
) -> bool:
    """认领某个片段唯一的 worker 槽。

    同一窗口可以给自己的认领续期。别的窗口必须等
    片段发布，不能再占第二条在途线程。占着槽的窗口
    编码线程已经不在（没在跑也没在排队）时，直接接手，
    不用等槽过期。
    """
    client_id = (client_id or "anonymous")[:128]
    key = segment_slot_key(job_id, start)
    ttl = max(30, int(ttl))
    if r.set(key, client_id, nx=True, ex=ttl):
        return True
    current = _decode(r.get(key))
    if current == client_id:
        try:
            r.expire(key, ttl)
        except Exception:
            r.set(key, client_id, ex=ttl)
        return True
    if current and not slot_holder_live(r, job_id, current):
        r.set(key, client_id, ex=ttl)
        return _decode(r.get(key)) == client_id
    return False


def _decode(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


def slot_holder_live(r, job_id: str, holder: str) -> bool:
    """占槽窗口的编码线程还在跑，或者已经排进队列。"""
    if not holder:
        return False
    return bool(
        r.get(session_alive_key(job_id, holder))
        or r.get(session_pending_key(job_id, holder))
    )


def live_run_at(r, job_id: str, start: int, client_id: str, segment_frames: int = 30) -> bool:
    """别的窗口有活着的编码线程正停在 ``start`` 这一段，或刚发布完它前一段。

    线程编完一段、去认领下一段之前，槽还挂在刚发布的那段上。
    两处都看，才不会把正在往后推进的线程误判成已经停下。
    """
    seg_n = max(1, int(segment_frames or 30))
    for seg in (int(start), int(start) - seg_n):
        if seg < 0:
            continue
        holder = _decode(r.get(segment_slot_key(job_id, seg)))
        if holder and holder != client_id and slot_holder_live(r, job_id, holder):
            return True
    return False


def segment_end_key(job_id: str) -> str:
    return f"sview:segment:end:{job_id}"


def mark_decode_end(r, job_id: str, start: int) -> None:
    """记下从 ``start`` 起已经没有可解码的消息，别再给这里排编码线程。"""
    key = segment_end_key(job_id)
    try:
        current = int(_decode(r.get(key)) or 0)
    except (TypeError, ValueError):
        current = 0
    value = int(start) if current <= 0 else min(current, int(start))
    r.set(key, str(value), ex=ENCODE_REQUEST_TTL_SEC)


def decode_end(r, job_id: str) -> Optional[int]:
    try:
        raw = _decode(r.get(segment_end_key(job_id)))
        return int(raw) if raw else None
    except (TypeError, ValueError):
        return None


# 交接只替最近这么多秒内请求过的窗口排线程，早就关掉的窗口不再白编。
HANDOFF_RECENT_SEC = max(10, int(os.environ.get("SVIEW_HANDOFF_RECENT_SEC", "120") or "120"))


def hand_off_waiting_windows(
    r,
    job_id: str,
    source: str,
    *,
    exclude: str = "",
    segment_frames: int = 30,
) -> list[str]:
    """一条编码线程退出时，替还在等缺段、又没人在编的窗口排线程。

    只管请求的那一段确实还缺的窗口（正卡在这里等）。前沿那段已经好了的
    窗口在播缓存，或者早就切去别的文件，播到缺口时它自己会再请求。
    """
    from sview.cache.oss_cache_gc import frame_range_ready

    seg_n = max(1, int(segment_frames or 30))
    fields = get_job_fields(r, job_id)
    try:
        total = int(fields.get("frame_count") or fields.get("frames_total") or 0)
    except (TypeError, ValueError):
        total = 0
    try:
        ranges = json.loads(fields.get("available_ranges") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        ranges = []
    started: list[str] = []
    prefix = session_frontier_key(job_id, "")[: -len("anonymous")]
    for key in _scan_keys(r, f"{prefix}*"):
        client = key[len(prefix):]
        if not client or client == exclude:
            continue
        try:
            remaining = int(r.ttl(key))
        except Exception:
            remaining = ENCODE_REQUEST_TTL_SEC
        if remaining >= 0 and ENCODE_REQUEST_TTL_SEC - remaining > HANDOFF_RECENT_SEC:
            continue
        want = read_client_frontier(r, job_id, client)
        want_end = min(total, want + seg_n) if total > 0 else want + seg_n
        if want_end > want and frame_range_ready(ranges, want, want_end):
            continue
        if ensure_sparse_session(r, job_id, source, client, segment_frames=segment_frames):
            started.append(client)
    return started


def release_segment_slot(r, job_id: str, start: int, client_id: str) -> None:
    client_id = (client_id or "anonymous")[:128]
    key = segment_slot_key(job_id, start)
    current = r.get(key)
    if isinstance(current, bytes):
        current = current.decode("utf-8", errors="replace")
    if current == client_id:
        r.delete(key)


def first_unencoded_segment(
    r,
    job_id: str,
    client_id: str,
    segment_frames: int = 30,
) -> Optional[int]:
    """本窗口前沿及之后，第一个还缺的片段。

    返回 None 表示直到 frame_count 的片段都已发布。
    """
    from sview.cache.oss_cache_gc import frame_range_ready

    seg_n = max(1, int(segment_frames or 30))
    frontier = read_client_frontier(r, job_id, client_id)
    seg = (frontier // seg_n) * seg_n
    fields = get_job_fields(r, job_id)
    try:
        total = int(fields.get("frame_count") or fields.get("frames_total") or 0)
    except (TypeError, ValueError):
        total = 0
    try:
        ranges = json.loads(fields.get("available_ranges") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        ranges = []
    end_at = decode_end(r, job_id)
    guard = 0
    while guard < 10000 and (total <= 0 or seg < total):
        if end_at is not None and seg >= end_at:
            return None
        end = min(total, seg + seg_n) if total > 0 else seg + seg_n
        if total <= 0 or end <= seg or not frame_range_ready(ranges, seg, end):
            return seg
        seg += seg_n
        guard += 1
    return None


def sync_sparse_ranges_from_oss(
    r,
    job_id: str,
    oss_ranges: Any,
    *,
    frame_count: Optional[int] = None,
    expect_revision: Optional[str] = None,
    segment_frames: int = 30,
) -> bool:
    """Redis 声称的区间里有 OSS 上已经没有的段时，改成 OSS 的实际区间。

    worker 先 PUT 分段再写 Redis，正常情况下 OSS 只会比 Redis 多。
    Redis 多出来的段只可能是被删了（生命周期 / GC）。不改的话
    ``first_unencoded_segment`` 会以为全编完，不再开编码线程。
    ``expect_revision``：读 OSS 之前看到的 manifest_revision；列举期间
    有新段发布就放弃这次同步，避免拿旧列表盖掉刚发布的段。
    """
    from sview.cache.oss_cache_gc import (
        contiguous_prefix_from_ranges,
        frame_range_ready,
        merge_available_ranges,
    )

    fields = get_job_fields(r, job_id)
    if expect_revision is not None and str(fields.get("manifest_revision") or "") != str(
        expect_revision or ""
    ):
        return False
    try:
        claimed = json.loads(fields.get("available_ranges") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        claimed = []
    actual = merge_available_ranges(oss_ranges or [])
    stale = any(
        int(b) > int(a) and not frame_range_ready(actual, int(a), int(b))
        for a, b in merge_available_ranges(claimed)
    )
    if not stale:
        return False
    try:
        total = int(frame_count or fields.get("frame_count") or fields.get("frames_total") or 0)
    except (TypeError, ValueError):
        total = 0
    seg_n = max(1, int(segment_frames or 30))
    prefix = contiguous_prefix_from_ranges(actual)
    set_job_fields(
        r,
        job_id,
        status="ready" if total > 0 and prefix >= total else "processing",
        available_ranges=json.dumps(actual, separators=(",", ":")),
        frames_done=prefix,
        segments_ready_count=sum(
            max(0, (int(b) - int(a) + seg_n - 1) // seg_n) for a, b in actual
        ),
        playable=1 if actual else 0,
    )
    return True


def ensure_sparse_session(
    r,
    job_id: str,
    source: str,
    client_id: str,
    *,
    segment_frames: int = 30,
) -> bool:
    """为本窗口下一个缺段排一条编码线程。

    同一窗口之后再拖动，只移动该窗口的前沿。
    下一段已有编码槽的其他窗口不再占 worker 在途名额，
    而是等到该片段发布。
    """
    client_id = (client_id or "anonymous")[:128]
    if r.get(session_alive_key(job_id, client_id)):
        return False
    if r.get(session_pending_key(job_id, client_id)):
        return False
    target = first_unencoded_segment(r, job_id, client_id, segment_frames)
    if target is None:
        return False
    if not claim_segment_slot(r, job_id, target, client_id):
        return False
    if not r.set(session_pending_key(job_id, client_id), "1", nx=True, ex=120):
        return False
    r.lpush(
        REDIS_QUEUE_KEY,
        json.dumps(
            {
                "v": 2,
                "kind": "sparse_session",
                "source": source,
                "job_id": job_id,
                "client_id": client_id,
                "segment_frames": max(1, int(segment_frames or 30)),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    )
    return True


def release_segment_claim(r, job_id: str, start: int, *, owner: str = "") -> None:
    """释放一个稀疏片段的待处理标记和 worker 锁。"""
    r.delete(segment_pending_key(job_id, start))
    key = segment_lock_key(job_id, start)
    if not owner:
        r.delete(key)
        return
    cur = r.get(key)
    if isinstance(cur, bytes):
        cur = cur.decode("utf-8", errors="replace")
    if cur == owner:
        r.delete(key)


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
    """入队一个分类型基准任务，可按任务覆盖转码参数（队列负载为 JSON）。

    返回 (job_id, enqueued)。基准默认 force=True（重新计时）。
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
    """把任务标为取消；锁属于自己或带了 purge_owner 时释放锁。"""
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
    """解析队列项：普通的 source 字符串（线上），或 JSON 基准负载。"""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    text = (raw or "").strip()
    if text.startswith("{"):
        data = json.loads(text)
        source = (data.get("source") or data.get("path") or "").strip()
        if not source:
            raise ValueError("queue JSON missing source")
        jid = (data.get("job_id") or "").strip() or job_id_from_source(source)
        kind = (data.get("kind") or "").strip()
        if not kind:
            kind = "bench" if data.get("bench_type") is not None else "job"
        try:
            origin_sec = float(data.get("origin_sec") or data.get("start_sec") or 0)
        except (TypeError, ValueError):
            origin_sec = 0.0
        if origin_sec != origin_sec or origin_sec < 0:
            origin_sec = 0.0
        return {
            "kind": kind,
            "source": source,
            "job_id": jid,
            "bench_type": data.get("bench_type"),
            "bench_name": data.get("bench_name") or "",
            "transcode_size": int(data.get("transcode_size", 0) or 0),
            "transcode_crf": int(data.get("transcode_crf", 35) or 35),
            "force_reencode": int(data.get("force_reencode", 0) or 0),
            "origin_sec": origin_sec,
            "publish_mode": (data.get("publish_mode") or "").strip().lower(),
            "seg_start": int(data.get("seg_start", 0) or 0),
            "seg_end": int(data.get("seg_end", 0) or 0),
            "segment_frames": int(data.get("segment_frames", 30) or 30),
            "segment_epoch": int(data.get("segment_epoch", 0) or 0),
            "client_epoch": int(data.get("client_epoch", 0) or 0),
            "request_id": str(data.get("request_id") or ""),
            "client_id": str(data.get("client_id") or ""),
            "priority": int(data.get("priority", 1) or 0),
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
        "origin_sec": 0.0,
        "publish_mode": "",
        "seg_start": 0,
        "seg_end": 0,
        "segment_frames": 30,
        "segment_epoch": 0,
        "client_epoch": 0,
        "request_id": "",
        "client_id": "",
        "priority": 0,
        "raw": text,
    }
