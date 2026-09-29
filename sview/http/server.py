#!/usr/bin/env python3
"""
SView HTTP: local cache file server + job submit/status API.

  GET  /healthz
  POST /api/jobs          {"source":"oss://.../*.mcap"}
  POST /api/jobs          HandVis mesh:
                          {"kind":"handvis","ep_dir":"oss://.../ep1/","item_name":"clip_001"}
                          (或 "video_path"+"posehand_path"; task_id 默认 posehand 文件名)
  GET  /api/jobs/stats    queue depth + processing jobs
  GET  /api/jobs/{job_id}
  GET  /api/jobs/{job_id}/manifest.json
  GET  /api/jobs/{job_id}/index.json            # HandVis mesh catalog
  GET  /api/jobs/{job_id}/faces.u32
  GET  /api/jobs/{job_id}/video_projection.bin
  GET  /api/jobs/{job_id}/segments/000000.bin   # ~30 frames packed (preferred)
  GET  /api/jobs/{job_id}/frames/000000.bin     # single frame (compat)

  # Bench / typed scenarios (does not change /api/jobs contract)
  GET  /api/test/types
  POST /api/test/jobs     {"type":1-8,"source":"...","force":true}
  GET  /api/test/jobs/{job_id}

  # Legacy aliases (same handlers as /api/jobs/... media paths)
  GET  /{job_id}/manifest.json
  GET  /{job_id}/segments/000000.bin
  GET  /{job_id}/frames/000000.bin

Env:
  HTTP_HOST / HTTP_PORT
  LOCAL_OUTPUT_DIR (cache root)
  SEGMENT_FRAMES=30        frames per segment response (aligned to gop)
  REDIS_*  (for /api/jobs)
  SVIEW_CLUSTER=1          enable owner sticky reverse-proxy across pods
  POD_IP / POD_HTTP_PORT   Downward API identity for cluster mode
  BENCH_H264_SOURCE / BENCH_H265_SOURCE  (optional defaults for /api/test/jobs)
"""

import errno
import json
import logging
import mimetypes
import os
import re
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional, Union
from urllib.parse import parse_qs, unquote

from sview.logging_setup import setup_logging

log = setup_logging("sview_http")

# Client gone mid-request (LB / probe / browser). Not an app failure.
_CLIENT_DISCONNECT_ERRNOS = {
    errno.ECONNRESET,
    errno.EPIPE,
    getattr(errno, "ECONNABORTED", 103),
}


def _is_client_disconnect(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
        return True
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) in _CLIENT_DISCONNECT_ERRNOS:
        return True
    return False


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that does not dump stack traces for peer resets."""

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if exc is not None and _is_client_disconnect(exc):
            log.debug("HTTP client %s disconnected: %s", client_address, exc)
            return
        super().handle_error(request, client_address)

_JOB_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,200}$")
_FRAME_RE = re.compile(r"^[0-9]{6}\.bin$")
_PROXY_HEADER = "X-Sview-Proxy"

# HandVis 播放页静态托管（可选）。镜像内 /app/handvis_web；本地回退 ./handvis/app/web。
_HANDVIS_WEB_DIR = Path(os.environ.get("HANDVIS_WEB_DIR", "/app/handvis_web"))
if not _HANDVIS_WEB_DIR.is_dir():
    _local_web = Path(__file__).parent / "handvis" / "app" / "web"
    if _local_web.is_dir():
        _HANDVIS_WEB_DIR = _local_web


def _segment_frames() -> int:
    try:
        return max(1, int(os.environ.get("SEGMENT_FRAMES", "30")))
    except (TypeError, ValueError):
        return 30


def _pack_segment(start_index: int, frame_blobs: list[bytes], segment_frames: int) -> bytes:
    """Pack consecutive single-frame bundles into a segment container."""
    header = {
        "packet_schema": "playertrans.bundle_segment.v1",
        "start_index": int(start_index),
        "count": len(frame_blobs),
        "segment_frames": int(segment_frames),
    }
    header_bytes = json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    parts: list[bytes] = []
    for blob in frame_blobs:
        parts.append(struct.pack("<I", len(blob)))
        parts.append(blob)
    payload = b"".join(parts)
    return (
        struct.pack("<I", len(header_bytes))
        + header_bytes
        + struct.pack("<I", len(payload))
        + payload
    )

_redis_client = None


def _cluster_enabled() -> bool:
    return os.environ.get("SVIEW_CLUSTER", "0").lower() in ("1", "true", "yes", "on")


def _pod_ip() -> str:
    return (os.environ.get("POD_IP") or "").strip() or "local"


def _pod_http_port() -> int:
    raw = os.environ.get("POD_HTTP_PORT") or os.environ.get("HTTP_PORT") or "8080"
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 8080

def _playable_min_frames() -> int:
    try:
        return max(1, int(os.environ.get("PLAYABLE_MIN_FRAMES", "30")))
    except (TypeError, ValueError):
        return 30


def _get_redis():
    """Lazy Redis client for job API (same env as workers)."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    import redis

    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379") or "6379")
    username = os.environ.get("REDIS_USERNAME") or None
    password = os.environ.get("REDIS_PASSWORD") or None
    db = int(os.environ.get("REDIS_DB", "0") or "0")
    _redis_client = redis.Redis(
        host=host,
        port=port,
        username=username,
        password=password,
        db=db,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
    )
    _redis_client.ping()
    log.info("HTTP API Redis connected: %s:%s db=%s", host, port, db)
    return _redis_client


def _safe_job_id(job_id: str) -> bool:
    return bool(_JOB_ID_RE.match(job_id))


def _resolve_under(root: Path, *parts: str) -> Optional[Path]:
    try:
        root_r = root.resolve()
        target = root_r.joinpath(*parts).resolve()
        target.relative_to(root_r)
        return target
    except (ValueError, OSError):
        return None


def _optional_int(value) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_frame_count(*values) -> Optional[int]:
    """First positive int among values; 0/empty means total is still unknown."""
    for value in values:
        n = _optional_int(value)
        if n is not None and n > 0:
            return n
    return None


def _purge_job_play_cache(cache_root: Path, job_id: str, *, reason: str) -> dict:
    """Wipe local /cache/{job_id}, OSS player-bin/{job_id}/, and Redis progress fields."""
    import shutil

    summary: dict = {"local_cleared": False}
    stale = _resolve_under(cache_root, job_id)
    if stale is not None and stale.is_dir():
        shutil.rmtree(stale, ignore_errors=True)
        summary["local_cleared"] = True
        log.info("cleared local cache (%s) %s", reason, stale)

    if os.environ.get("OSS_CACHE", "0").lower() in ("1", "true", "yes", "on"):
        try:
            from sview.cache.oss_cache_gc import purge_job_play_cache

            summary.update(
                purge_job_play_cache(_get_redis(), job_id, reason=reason) or {}
            )
        except Exception:
            log.exception("OSS play-cache purge failed job=%s reason=%s", job_id, reason)
            summary["oss_purge_error"] = True
    return summary


def _oss_cache_on() -> bool:
    return os.environ.get("OSS_CACHE", "0").lower() in ("1", "true", "yes", "on")


def _align_manifest_dict(job_id: str, data: dict) -> dict:
    """Clamp frames_available to contiguous OSS segments when OSS_CACHE is on."""
    if not _oss_cache_on():
        return data
    try:
        from sview.cache.oss_cache_gc import align_manifest_to_oss

        job_status = None
        try:
            from sview.jobs.job_status import get_job_fields

            job_status = (get_job_fields(_get_redis(), job_id).get("status") or "").lower()
        except Exception:
            pass
        return align_manifest_to_oss(
            job_id,
            data,
            segment_frames=_segment_frames(),
            job_status=job_status,
        )
    except Exception:
        log.exception("manifest OSS align failed for %s", job_id)
        data["cache_align_failed"] = True
        data["cache_align_reason"] = "oss_align_exception"
        return data


def _read_local_manifest(cache_root: Path, job_id: str) -> Optional[dict]:
    path = _resolve_under(cache_root, job_id, "manifest.json")
    if path is None or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Failed reading local manifest for %s: %s", job_id, e)
        return None


def _read_oss_manifest(job_id: str) -> Optional[dict]:
    try:
        from sview.worker.cpu import get_oss_bucket, oss_cache_key

        key = oss_cache_key(job_id, "manifest.json")
        raw = get_oss_bucket().get_object(key).read()
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _load_play_manifest(cache_root: Path, job_id: str) -> Optional[dict]:
    """Load play manifest; with OSS_CACHE prefer the OSS object over local.

    Local progressive files can claim frames that have not been PUT yet.
    Signed playback must follow what is on OSS, then clamp to contiguous segments.
    """
    local = _read_local_manifest(cache_root, job_id)
    if _oss_cache_on():
        oss = _read_oss_manifest(job_id)
        data = oss if oss is not None else local
    else:
        data = local
    if data is None:
        return None
    return _align_manifest_dict(job_id, data)


def _read_manifest_summary(cache_root: Path, job_id: str) -> Optional[dict]:
    """Summarize manifest for job API (local file, else OSS), clamped to contiguous OSS segments.

    Returns a clean public payload:
      job_id, status, frame_count, frames_done, playable, cached, duration_sec
    Plus internal `_manifest_status` for server-side logic only (stripped before send).
    """
    data = _load_play_manifest(cache_root, job_id)
    if data is None:
        return None
    try:
        bundle = data.get("bundle") or {}
        m_status = (data.get("status") or "done").lower()
        frame_count = _optional_frame_count(bundle.get("frame_count"))
        frames_done = _optional_int(bundle.get("frames_available"))
        if frames_done is None:
            frames_done = frame_count
        duration_sec = (bundle.get("timeline") or {}).get("duration_sec")
        origin_sec = data.get("origin_sec")
        publish_mode = (data.get("publish_mode") or "progressive").lower()
        available_ranges = bundle.get("available_ranges") or []
        # Cannot confirm OSS objects — do not advertise a local/OSS claim.
        if data.get("cache_align_failed"):
            return {
                "job_id": data.get("job_id", job_id),
                "status": "processing",
                "frame_count": frame_count,
                "frames_done": None,
                "playable": False,
                "cached": False,
                "duration_sec": duration_sec,
                "origin_sec": origin_sec,
                "play_base": data.get("play_base") or None,
                "publish_mode": publish_mode,
                "available_ranges": available_ranges,
                "_manifest_status": "partial",
                "cache_aligned": False,
            }
        # Ghost cache: aligned to 0 frames → never report ready/cached.
        if frames_done is not None and frames_done <= 0 and data.get("cache_aligned"):
            return {
                "job_id": data.get("job_id", job_id),
                "status": "processing",
                "frame_count": frame_count,
                "frames_done": 0,
                "playable": False,
                "cached": False,
                "duration_sec": duration_sec,
                "origin_sec": origin_sec,
                "play_base": data.get("play_base") or None,
                "publish_mode": publish_mode,
                "available_ranges": available_ranges,
                "_manifest_status": "partial",
                "cache_aligned": True,
            }
        if m_status == "partial":
            api_status = "processing"
            playable = bool(
                frames_done is not None and frames_done >= _playable_min_frames()
            )
            cached = False
        else:
            api_status = "ready"
            playable = bool(frames_done and frames_done > 0)
            cached = playable
            if frames_done is None:
                frames_done = frame_count
        return {
            "job_id": data.get("job_id", job_id),
            "status": api_status,
            "frame_count": frame_count,
            "frames_done": frames_done,
            "playable": playable,
            "cached": cached,
            "duration_sec": (bundle.get("timeline") or {}).get("duration_sec"),
            "origin_sec": origin_sec,
            "play_base": data.get("play_base") or None,
            "publish_mode": publish_mode,
            "available_ranges": available_ranges,
            "_manifest_status": m_status,
            "cache_aligned": bool(data.get("cache_aligned")),
        }
    except Exception as e:
        log.warning("Failed summarizing manifest for %s: %s", job_id, e)
        return None


def _public_job_payload(data: dict) -> dict:
    """Strip internal keys; keep only fields documented for frontend."""
    out = {}
    for k in (
        "job_id",
        "status",
        "source",
        "error",
        "frame_count",
        "frames_done",
        "playable",
        "cached",
        "duration_sec",
        "local_path",
        "updated_at",
        "play_base",
        "percent",
        "cache_bytes",
        "last_access_at",
        "cache_status",
        "kind",
        "video_url",
        "origin_sec",
        "cache_id",
        "publish_mode",
        "available_ranges",
        "segments_ready_count",
        "segment_epoch",
        "manifest_revision",
        "encode_window",
        "encode_status",
        "encode_frontier",
        "decode_start",
        "input_frames",
        "output_frames",
    ):
        if k in data and data[k] is not None and data[k] != "":
            out[k] = data[k]
    # Always include these when present as bool/number 0
    for k in ("playable", "cached", "frames_done", "frame_count", "percent", "cache_bytes"):
        if k in data and k not in out and data[k] is not None:
            out[k] = data[k]
    if "error" in data:
        out["error"] = data["error"] or None
    for key in ("available_ranges", "encode_window"):
        if isinstance(out.get(key), str):
            try:
                out[key] = json.loads(out[key])
            except (TypeError, ValueError, json.JSONDecodeError):
                out[key] = [] if key == "available_ranges" else None
    return out


def _optional_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _public_test_job_payload(data: dict) -> dict:
    """Public payload for /api/test/* — includes bench type + timing fields."""
    out = _public_job_payload(data)
    for k in (
        "type",
        "type_name",
        "bench_type",
        "bench_name",
        "transcode_size",
        "transcode_crf",
        "force_reencode",
        "first_frame_sec",
        "full_sec",
        "started_at",
        "first_frame_at",
        "finished_at",
        "notes",
    ):
        if k in data and data[k] is not None and data[k] != "":
            out[k] = data[k]
    # Normalize type alias
    if "type" not in out and "bench_type" in out:
        out["type"] = _optional_int(out["bench_type"])
    if "type_name" not in out and data.get("bench_name"):
        out["type_name"] = data["bench_name"]
    for k in ("first_frame_sec", "full_sec", "started_at", "first_frame_at", "finished_at"):
        if k in out:
            parsed = _optional_float(out[k])
            if parsed is not None:
                out[k] = parsed
    for k in ("transcode_size", "transcode_crf", "force_reencode", "type", "bench_type"):
        if k in out:
            parsed = _optional_int(out[k])
            if parsed is not None:
                out[k] = parsed
    if "transcode" not in out and any(
        k in out for k in ("transcode_size", "transcode_crf", "force_reencode")
    ):
        out["transcode"] = {
            "size": out.get("transcode_size"),
            "crf": out.get("transcode_crf"),
            "force_reencode": out.get("force_reencode"),
        }
    return out


class SviewCacheHandler(BaseHTTPRequestHandler):
    cache_root = Path(".")

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        # Access log into the shared project log file (cwd/sview.log).
        log.info("HTTP %s - " + fmt, self.address_string(), *args)

    def handle(self):
        try:
            super().handle()
        except Exception as exc:
            if _is_client_disconnect(exc):
                log.debug(
                    "HTTP client %s disconnected during handle: %s",
                    self.client_address,
                    exc,
                )
                return
            raise

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, HEAD")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, *")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_HEAD(self):
        self._handle_get(body=False)

    def do_GET(self):
        self._handle_get(body=True)

    def do_POST(self):
        path = unquote(self.path.split("?", 1)[0])
        if path.rstrip("/") == "/api/jobs":
            self._handle_create_job()
            return
        if path.rstrip("/") == "/api/test/jobs":
            self._handle_create_test_job()
            return
        # POST /api/jobs/{job_id}/cancel
        # POST /api/jobs/{job_id}/touch
        parts = [p for p in path.split("/") if p]
        if (
            len(parts) == 6
            and parts[0] == "api"
            and parts[1] == "jobs"
            and parts[3] == "encode"
            and parts[5] == "cancel"
        ):
            job_id, request_id = parts[2], parts[4]
            if not _safe_job_id(job_id) or not _safe_job_id(request_id):
                self._send_err(400, "invalid job_id or request_id")
                return
            self._handle_cancel_encode_request(job_id, request_id)
            return
        if (
            len(parts) == 4
            and parts[0] == "api"
            and parts[1] == "jobs"
            and parts[3] in ("cancel", "touch", "encode")
        ):
            job_id = parts[2]
            if not _safe_job_id(job_id):
                self._send_err(400, "invalid job_id")
                return
            if parts[3] == "cancel":
                self._handle_cancel_job(job_id)
            elif parts[3] == "encode":
                self._handle_encode_window(job_id)
            else:
                self._handle_touch_job(job_id)
            return
        if path.rstrip("/") == "/api/cache/gc":
            self._handle_cache_gc()
            return
        self._send_err(404, "not found")

    def _read_json_body(self) -> Any:
        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0:
            return {}
        if length > 1_000_000:
            raise ValueError("body too large")
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, code: int, obj: dict):
        payload = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self._cors()
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _handle_create_job(self):
        from sview.jobs.job_status import (
            enqueue_source,
            get_job_fields,
            job_id_from_source,
            job_in_flight,
            request_cancel,
        )
        from sview.jobs.origin import origin_job_id, parse_start_sec
        from sview.path_map import source_to_local

        try:
            body = self._read_json_body()
        except Exception as e:
            self._send_json(400, {"error": f"invalid json: {e}"})
            return

        kind = (body.get("kind") or body.get("type") or "").strip().lower()
        if kind in ("handvis", "mesh", "hand", "hand_mesh") or body.get("posehand_path"):
            self._handle_create_handvis_job(body)
            return

        source = (body.get("source") or body.get("path") or "").strip()
        if not source:
            self._send_json(400, {"error": "missing source (oss://...mcap)"})
            return
        if not source.lower().endswith(".mcap") and ".mcap" not in source.lower():
            # allow without strict check but warn via message if clearly wrong
            pass

        origin_sec = parse_start_sec(
            body.get("start_sec") if body.get("start_sec") is not None else body.get("origin_sec")
        )
        job_id = origin_job_id(job_id_from_source(source), origin_sec)
        if not _safe_job_id(job_id):
            self._send_json(400, {"error": f"invalid job_id derived from source: {job_id}"})
            return

        publish_mode = (body.get("publish_mode") or "progressive").strip().lower()
        if publish_mode not in ("progressive", "sparse"):
            self._send_json(400, {"error": "publish_mode must be progressive or sparse"})
            return
        if publish_mode == "sparse":
            if origin_sec > 0:
                self._send_json(
                    400,
                    {"error": "start_sec/origin_sec cannot be combined with sparse mode"},
                )
                return
            local = source_to_local(source)
            if local.suffix.lower() in (".hdf5", ".h5", ".hdf"):
                self._send_json(
                    400,
                    {"error": "sparse seek is not supported for HDF5; use progressive"},
                )
                return
            if not local.exists():
                self._send_json(
                    404,
                    {
                        "error": "source not found on local mount",
                        "source": source,
                        "local_path": str(local),
                    },
                )
                return
            try:
                from sview.jobs.job_status import enqueue_sparse_job, sparse_cache_job_id

                job_id = sparse_cache_job_id(source, local)
                if not _safe_job_id(job_id):
                    self._send_json(
                        400,
                        {"error": f"invalid sparse cache id: {job_id}"},
                    )
                    return

                r = _get_redis()
                if not bool(body.get("force")):
                    cached_manifest = _load_play_manifest(self.cache_root, job_id)
                    cached_bundle = (cached_manifest or {}).get("bundle") or {}
                    cached_ranges = cached_bundle.get("available_ranges") or []
                    cached_count = int(cached_bundle.get("frame_count") or 0)
                    from sview.cache.oss_cache_gc import frame_range_ready

                    if (
                        (cached_manifest or {}).get("publish_mode") == "sparse"
                        and cached_count > 0
                        and frame_range_ready(
                            cached_ranges,
                            0,
                            min(_segment_frames(), cached_count),
                        )
                    ):
                        from sview.cache.oss_cache_gc import (
                            contiguous_prefix_from_ranges,
                        )
                        from sview.jobs.job_status import set_job_fields

                        prefix = contiguous_prefix_from_ranges(cached_ranges)
                        complete = (
                            (cached_manifest or {}).get("status") == "done"
                        )
                        set_job_fields(
                            r,
                            job_id,
                            status="ready" if complete else "processing",
                            source=source,
                            cache_id=job_id,
                            publish_mode="sparse",
                            frame_count=cached_count,
                            frames_total=cached_count,
                            frames_done=prefix,
                            available_ranges=json.dumps(
                                cached_ranges, separators=(",", ":")
                            ),
                            playable=1,
                            encode_status="idle",
                            cancel=0,
                        )
                        fields = get_job_fields(r, job_id)
                        self._send_json(
                            200,
                            _public_job_payload(
                                {
                                    **fields,
                                    "job_id": job_id,
                                    "cached": complete,
                                    "playable": True,
                                }
                            ),
                        )
                        return
                if bool(body.get("force")):
                    request_cancel(r, job_id, purge_owner=True)
                    _purge_job_play_cache(
                        self.cache_root, job_id, reason="force_sparse_reenqueue"
                    )
                job_id, enqueued = enqueue_sparse_job(
                    r,
                    source,
                    job_id=job_id,
                    force=bool(body.get("force")),
                    segment_frames=_segment_frames(),
                )
                from sview.jobs.job_status import (
                    ensure_sparse_session,
                    set_client_frontier,
                )

                client_id = str(body.get("client_id") or "").strip()[:128]
                if not client_id:
                    client_id = f"submit-{job_id[:80]}"
                set_client_frontier(
                    r,
                    job_id,
                    client_id,
                    0,
                    segment_frames=_segment_frames(),
                )
                if ensure_sparse_session(
                    r,
                    job_id,
                    source,
                    client_id,
                    segment_frames=_segment_frames(),
                ):
                    enqueued = True
                fields = get_job_fields(r, job_id)
            except Exception as e:
                log.exception("sparse enqueue failed")
                self._send_json(503, {"error": f"sparse enqueue failed: {e}"})
                return
            self._send_json(
                202 if enqueued else 200,
                _public_job_payload(
                    {
                        **fields,
                        "job_id": job_id,
                        "source": source,
                        "status": fields.get("status") or "queued",
                        "publish_mode": "sparse",
                        "cached": False,
                        "playable": str(fields.get("playable") or "").lower()
                        in ("1", "true", "yes"),
                    }
                ),
            )
            return

        # Already cached locally (complete)?
        summary = _read_manifest_summary(self.cache_root, job_id)
        force = bool(body.get("force"))
        if summary and summary.get("status") == "ready" and not force:
            self._send_json(
                200,
                _public_job_payload(
                    {
                        **summary,
                        "source": source,
                        "cached": True,
                        "playable": True,
                    }
                ),
            )
            return

        # Cluster: Redis ready on another pod — no local files; client may fetch via proxy.
        if not force and _cluster_enabled() and (not summary or summary.get("status") != "ready"):
            try:
                r0 = _get_redis()
                fields0 = get_job_fields(r0, job_id)
                owner = (fields0.get("owner_pod") or "").strip()
                if (
                    (fields0.get("status") or "").lower() == "ready"
                    and owner
                    and owner not in ("", "local", _pod_ip())
                ):
                    self._send_json(
                        200,
                        _public_job_payload(
                            {
                                "job_id": job_id,
                                "status": "ready",
                                "source": fields0.get("source") or source,
                                "cached": True,
                                "playable": True,
                                "frames_done": _optional_int(fields0.get("frames_done")),
                                "frame_count": _optional_int(fields0.get("frame_count"))
                                or _optional_int(fields0.get("frames_total")),
                            }
                        ),
                    )
                    return
            except Exception:
                pass

        # Optional: verify local mount path exists (helps fail fast)
        local = source_to_local(source)
        if not local.exists():
            self._send_json(
                404,
                {
                    "error": "source not found on local mount",
                    "source": source,
                    "local_path": str(local),
                    "hint": "check HEAD_CAM_MOUNT / ORIN_MOUNT PVC mounts",
                },
            )
            return

        try:
            r = _get_redis()
            if force:
                request_cancel(r, job_id, purge_owner=True)
                _purge_job_play_cache(self.cache_root, job_id, reason="force_reenqueue")

            fields_before = get_job_fields(r, job_id)
            if not force and job_in_flight(fields_before, r, job_id):
                self._send_json(
                    202,
                    _public_job_payload(
                        {
                            "job_id": job_id,
                            "status": fields_before.get("status", "queued"),
                            "source": fields_before.get("source") or source,
                            "local_path": str(local),
                            "cached": False,
                            "playable": str(fields_before.get("playable") or "").lower()
                            in ("1", "true", "yes"),
                            "frames_done": _optional_int(fields_before.get("frames_done")) or 0,
                            "frame_count": _optional_int(fields_before.get("frame_count")),
                        }
                    ),
                )
                return

            enqueue_source(r, source, job_id=job_id, force=force, origin_sec=origin_sec)
        except Exception as e:
            log.exception("enqueue failed")
            self._send_json(503, {"error": f"redis enqueue failed: {e}"})
            return

        fields = get_job_fields(r, job_id)
        self._send_json(
            202,
            _public_job_payload(
                {
                    "job_id": job_id,
                    "status": fields.get("status", "queued"),
                    "source": source,
                    "local_path": str(local),
                    "cached": False,
                    "playable": False,
                    "frames_done": _optional_int(fields.get("frames_done")) or 0,
                    "frame_count": None,
                    "origin_sec": origin_sec if origin_sec > 0 else None,
                }
            ),
        )

    def _handle_create_handvis_job(self, body: dict):
        """POST /api/jobs kind=handvis：往 handvis:jobs:queue 投递 mesh 构建。

        输入可以是 ep_dir+item_name（worker 现扫 OSS），或 video_path+posehand_path。
        task_id 默认取 posehand 文件名（去后缀），可在 body 里显式覆盖。
        """
        ep_dir = (body.get("ep_dir") or "").strip()
        item_name = (body.get("item_name") or "").strip()
        video_path = (body.get("video_path") or "").strip()
        posehand_path = (body.get("posehand_path") or "").strip()
        task_id = (body.get("task_id") or body.get("job_id") or "").strip()
        force = bool(body.get("force") or body.get("force_rebuild"))

        if not posehand_path and not (ep_dir and item_name):
            self._send_json(
                400,
                {"error": "handvis job needs posehand_path or ep_dir+item_name"},
            )
            return

        if not task_id:
            if posehand_path:
                task_id = job_id_from_source(posehand_path)
            else:
                task_id = f"{ep_dir.rstrip('/').rsplit('/', 1)[-1]}__{item_name}"
        if not _safe_job_id(task_id):
            self._send_json(400, {"error": f"invalid task_id: {task_id}"})
            return

        from sview.jobs.job_status import get_job_fields, set_job_fields

        payload = {
            "ep_dir": ep_dir,
            "item_name": item_name,
            "force_rebuild": force,
        }
        if video_path:
            payload["video_path"] = video_path
        if posehand_path:
            payload["posehand_path"] = posehand_path
        envelope = {
            "task_id": task_id,
            "type": "build_chunks",
            "payload": payload,
        }

        try:
            r = _get_redis()
            fields_before = get_job_fields(r, task_id)
            kind_before = (fields_before.get("kind") or "").lower()
            status_before = (fields_before.get("status") or "").lower()
            if not force and fields_before and kind_before == "handvis":
                # 已完成：直接返回缓存，不重复入队（重复入队会把 ready 状态冲掉）。
                if status_before == "ready":
                    self._send_json(
                        200,
                        _public_job_payload(
                            {
                                "job_id": task_id,
                                "status": "ready",
                                "source": fields_before.get("source")
                                or posehand_path
                                or ep_dir,
                                "cached": True,
                                "playable": True,
                                "frames_done": _optional_int(fields_before.get("frames_done")),
                                "frame_count": _optional_int(fields_before.get("frame_count"))
                                or _optional_int(fields_before.get("frames_total")),
                                "video_url": fields_before.get("video_url") or None,
                                "kind": "handvis",
                            }
                        ),
                    )
                    return
                if status_before in ("queued", "processing"):
                    self._send_json(
                        202,
                        _public_job_payload(
                            {
                                "job_id": task_id,
                                "status": fields_before.get("status", "queued"),
                                "source": posehand_path or ep_dir,
                                "cached": False,
                                "playable": str(fields_before.get("playable") or "").lower()
                                in ("1", "true", "yes"),
                                "frames_done": _optional_int(fields_before.get("frames_done")) or 0,
                                "kind": "handvis",
                            }
                        ),
                    )
                    return
            set_job_fields(
                r,
                task_id,
                status="queued",
                kind="handvis",
                source=posehand_path or ep_dir,
                ep_dir=ep_dir,
                item_name=item_name,
                error="",
                frames_done=0,
                frames_total=0,
                playable=0,
            )
            queue_key = os.environ.get("HANDVIS_QUEUE_KEY", "handvis:jobs:queue")
            r.rpush(queue_key, json.dumps(envelope, ensure_ascii=False))
        except Exception as e:
            log.exception("handvis enqueue failed")
            self._send_json(503, {"error": f"redis enqueue failed: {e}"})
            return

        log.info(
            "handvis job queued task_id=%s ep_dir=%s item=%s posehand=%s",
            task_id,
            ep_dir,
            item_name,
            bool(posehand_path),
        )
        self._send_json(
            202,
            _public_job_payload(
                {
                    "job_id": task_id,
                    "status": "queued",
                    "source": posehand_path or ep_dir,
                    "cached": False,
                    "playable": False,
                    "frames_done": 0,
                    "kind": "handvis",
                }
            ),
        )

    def _handle_cancel_job(self, job_id: str):
        from sview.jobs.job_status import POD_IP, get_job_fields, request_cancel
        try:
            r = _get_redis()
        except Exception as e:
            self._send_json(503, {"error": f"redis unavailable: {e}"})
            return

        fields = get_job_fields(r, job_id)
        if not fields:
            self._send_json(404, {"error": "job not found", "job_id": job_id})
            return

        owner = (fields.get("owner_pod") or "").strip()
        # Force-release lock if we own it or single-instance / no owner.
        purge = (not owner) or owner in ("local", POD_IP, _pod_ip())
        fields = request_cancel(r, job_id, purge_owner=purge)
        _purge_job_play_cache(self.cache_root, job_id, reason="cancelled")

        self._send_json(
            200,
            _public_job_payload(
                {
                    "job_id": job_id,
                    "status": "cancelled",
                    "source": fields.get("source"),
                    "error": fields.get("error") or "cancelled by client",
                    "playable": False,
                    "cached": False,
                    "frames_done": 0,
                }
            ),
        )

    def _handle_touch_job(self, job_id: str):
        from sview.jobs.job_status import get_job_fields, job_progress_percent, set_job_fields, touch_job

        try:
            r = _get_redis()
        except Exception as e:
            self._send_json(503, {"error": f"redis unavailable: {e}"})
            return
        fields = get_job_fields(r, job_id)
        if not fields:
            set_job_fields(r, job_id, status="ready")
        fields = touch_job(r, job_id)
        pct = job_progress_percent(fields)
        self._send_json(
            200,
            _public_job_payload(
                {
                    "job_id": job_id,
                    "status": fields.get("status") or "unknown",
                    "last_access_at": fields.get("last_access_at"),
                    "pin_until": fields.get("pin_until"),
                    "play_base": fields.get("play_base"),
                    "percent": pct,
                    "frames_done": _optional_int(fields.get("frames_done")),
                    "frame_count": _optional_int(fields.get("frame_count"))
                    or _optional_int(fields.get("frames_total")),
                    "playable": str(fields.get("playable") or "").lower()
                    in ("1", "true", "yes"),
                }
            ),
        )

    def _handle_encode_window(self, job_id: str):
        """Queue an idempotent sparse encode window around one global frame."""
        from sview.cache.oss_cache_gc import frame_range_ready
        from sview.jobs.job_status import (
            create_encode_request,
            ensure_sparse_session,
            get_job_fields,
            set_client_frontier,
            set_encode_request_status,
            touch_job,
        )

        try:
            body = self._read_json_body()
        except Exception as e:
            self._send_json(400, {"error": f"invalid json: {e}"})
            return
        try:
            frame_index = int(body.get("frame_index"))
            prefetch = max(1, min(16, int(body.get("prefetch_segments") or 2)))
            client_epoch = max(0, int(body.get("client_epoch") or 0))
            client_id = str(body.get("client_id") or "anonymous").strip()[:128]
        except (TypeError, ValueError):
            self._send_json(400, {"error": "frame_index must be an integer"})
            return
        if frame_index < 0:
            self._send_json(400, {"error": "frame_index must be non-negative"})
            return
        try:
            r = _get_redis()
            fields = get_job_fields(r, job_id)
        except Exception as e:
            self._send_json(503, {"error": f"redis unavailable: {e}"})
            return
        if not fields:
            self._send_json(404, {"error": "job not found", "job_id": job_id})
            return
        if (fields.get("publish_mode") or "").lower() != "sparse":
            self._send_json(409, {"error": "job is not in sparse publish mode"})
            return
        source = (fields.get("source") or "").strip()
        if not source:
            self._send_json(409, {"error": "sparse job has no source"})
            return
        frame_count = _optional_frame_count(
            fields.get("frame_count"), fields.get("frames_total")
        )
        if frame_count is not None and frame_index >= frame_count:
            self._send_json(
                416,
                {"error": "frame_index past end", "frame_count": frame_count},
            )
            return
        seg_n = _segment_frames()
        start = (frame_index // seg_n) * seg_n
        wanted_end = (
            min(frame_count, start + seg_n)
            if frame_count is not None
            else start + seg_n
        )
        man = _load_play_manifest(self.cache_root, job_id)
        ranges = ((man or {}).get("bundle") or {}).get("available_ranges")
        if ranges is None:
            raw_ranges = fields.get("available_ranges") or "[]"
            try:
                ranges = json.loads(raw_ranges)
            except (TypeError, ValueError, json.JSONDecodeError):
                ranges = []
        ready = frame_range_ready(ranges, start, wanted_end)
        request_id = create_encode_request(
            r,
            job_id,
            client_id=client_id,
            client_epoch=client_epoch,
            target_start=start,
            starts=[start],
            status="ready" if ready else "waiting",
        )
        touch_job(r, job_id)
        try:
            frontier = set_client_frontier(
                r,
                job_id,
                client_id,
                frame_index,
                segment_frames=seg_n,
            )
            session_started = ensure_sparse_session(
                r,
                job_id,
                source,
                client_id,
                segment_frames=seg_n,
            )
            fields = get_job_fields(r, job_id)
        except Exception as e:
            set_encode_request_status(
                r, job_id, request_id, "failed", error=str(e)
            )
            log.exception("sparse encode enqueue failed")
            self._send_json(503, {"error": f"encode enqueue failed: {e}"})
            return
        if ready:
            self._send_json(
                200,
                {
                    "job_id": job_id,
                    "status": "ready",
                    "publish_mode": "sparse",
                    "segment_start": start,
                    "request_id": request_id,
                    "session_started": session_started,
                    "available_ranges": ranges,
                    "segment_epoch": _optional_int(fields.get("segment_epoch")) or 0,
                    "manifest_revision": _optional_int(
                        fields.get("manifest_revision")
                    )
                    or 0,
                    "encode_frontier": frontier,
                    "encode_window": [frontier, frontier + seg_n],
                },
            )
            return
        self._send_json(
            202,
            {
                "job_id": job_id,
                "status": "queued" if session_started else "already_queued",
                "publish_mode": "sparse",
                "segment_start": start,
                "request_id": request_id,
                "session_started": session_started,
                "segment_epoch": _optional_int(fields.get("segment_epoch")) or 0,
                "encode_frontier": frontier,
                "encode_window": [frontier, frontier + seg_n],
            },
        )

    def _handle_encode_request_status(self, job_id: str, request_id: str):
        from sview.jobs.job_status import get_encode_request

        try:
            request = get_encode_request(_get_redis(), job_id, request_id)
        except Exception as e:
            self._send_json(503, {"error": f"redis unavailable: {e}"})
            return
        if not request:
            self._send_json(404, {"error": "encode request not found"})
            return
        self._send_json(200, request)

    def _handle_cancel_encode_request(self, job_id: str, request_id: str):
        from sview.jobs.job_status import cancel_encode_request, get_encode_request

        try:
            r = _get_redis()
            if not cancel_encode_request(r, job_id, request_id):
                self._send_json(404, {"error": "encode request not found"})
                return
            request = get_encode_request(r, job_id, request_id)
        except Exception as e:
            self._send_json(503, {"error": f"redis unavailable: {e}"})
            return
        self._send_json(200, request)

    def _handle_cache_gc(self):
        try:
            from sview.cache.oss_cache_gc import run_gc_once

            summary = run_gc_once(_get_redis())
            self._send_json(200, {"ok": True, **summary})
        except Exception as e:
            log.exception("manual cache GC failed")
            self._send_json(500, {"error": str(e)})

    def _handle_create_test_job(self):
        """POST /api/test/jobs — typed bench scenarios (type 1-8). Does not alter /api/jobs."""
        try:
            from sview.jobs.bench_types import bench_job_id, get_bench_type, resolve_bench_source
        except ImportError as e:
            self._send_json(
                503,
                {
                    "error": f"bench_types not available: {e}",
                    "hint": "rebuild image with sview package (see root Dockerfile COPY)",
                },
            )
            return
        from sview.jobs.job_status import enqueue_bench_job, get_job_fields
        from sview.path_map import source_to_local

        try:
            body = self._read_json_body()
        except Exception as e:
            self._send_json(400, {"error": f"invalid json: {e}"})
            return

        try:
            type_id = int(body.get("type") or body.get("bench_type") or 0)
        except (TypeError, ValueError):
            self._send_json(400, {"error": "type must be an integer 1-8"})
            return
        if type_id < 1 or type_id > 8:
            self._send_json(400, {"error": "type must be an integer 1-8"})
            return

        try:
            cfg = get_bench_type(type_id)
            source = resolve_bench_source(type_id, body.get("source") or body.get("path"))
        except Exception as e:
            self._send_json(400, {"error": str(e)})
            return

        job_id = bench_job_id(source, type_id)
        if not _safe_job_id(job_id):
            self._send_json(400, {"error": f"invalid job_id derived from source: {job_id}"})
            return

        # Default force=true so repeated type runs re-measure instead of serving stale cache.
        force = True if "force" not in body else bool(body.get("force"))
        summary = _read_manifest_summary(self.cache_root, job_id)
        if summary and summary.get("status") == "ready" and not force:
            extra = {}
            try:
                r = _get_redis()
                extra = get_job_fields(r, job_id)
            except Exception:
                pass
            self._send_json(
                200,
                _public_test_job_payload(
                    {
                        **summary,
                        **extra,
                        "source": source,
                        "cached": True,
                        "playable": True,
                        "type": type_id,
                        "type_name": cfg["name"],
                        "bench_type": type_id,
                        "bench_name": cfg["name"],
                        "transcode_size": cfg["size"],
                        "transcode_crf": cfg["crf"],
                        "force_reencode": cfg["force_reencode"],
                        "notes": cfg["notes"],
                    }
                ),
            )
            return

        # force=true: drop stale cache so status polling cannot hit an old ready manifest.
        if force:
            import shutil

            stale = _resolve_under(self.cache_root, job_id)
            if stale is not None and stale.is_dir():
                shutil.rmtree(stale, ignore_errors=True)
                log.info("cleared stale test cache %s", stale)

        local = source_to_local(source)
        if not local.exists():
            self._send_json(
                404,
                {
                    "error": "source not found on local mount",
                    "source": source,
                    "local_path": str(local),
                    "type": type_id,
                    "hint": "check HEAD_CAM_MOUNT / ORIN_MOUNT or pass a reachable source",
                },
            )
            return

        try:
            r = _get_redis()
            enqueue_bench_job(
                r,
                source,
                type_id,
                job_id=job_id,
                transcode_size=cfg["size"],
                transcode_crf=cfg["crf"],
                force_reencode=cfg["force_reencode"],
                type_name=cfg["name"],
                force=force,
            )
        except Exception as e:
            log.exception("bench enqueue failed")
            self._send_json(503, {"error": f"redis enqueue failed: {e}"})
            return

        fields = get_job_fields(r, job_id)
        self._send_json(
            202,
            _public_test_job_payload(
                {
                    "job_id": job_id,
                    "status": fields.get("status", "queued"),
                    "source": source,
                    "local_path": str(local),
                    "cached": False,
                    "playable": False,
                    "frames_done": 0,
                    "frame_count": None,
                    "type": type_id,
                    "type_name": cfg["name"],
                    "bench_type": type_id,
                    "bench_name": cfg["name"],
                    "transcode_size": cfg["size"],
                    "transcode_crf": cfg["crf"],
                    "force_reencode": cfg["force_reencode"],
                    "notes": cfg["notes"],
                }
            ),
        )

    def _handle_test_types(self):
        try:
            from sview.jobs.bench_types import list_bench_types
        except ImportError as e:
            self._send_json(
                503,
                {
                    "error": f"bench_types not available: {e}",
                    "hint": "rebuild image with sview package (see root Dockerfile COPY)",
                },
            )
            return

        self._send_json(
            200,
            {
                "types": list_bench_types(),
                "defaults": {
                    "BENCH_H264_SOURCE": os.environ.get("BENCH_H264_SOURCE") or None,
                    "BENCH_H265_SOURCE": os.environ.get("BENCH_H265_SOURCE") or None,
                },
            },
        )

    def _handle_test_job_status(self, job_id: str):
        from sview.jobs.job_status import get_job_fields

        summary = _read_manifest_summary(self.cache_root, job_id)
        fields: dict = {}
        try:
            r = _get_redis()
            fields = get_job_fields(r, job_id)
        except Exception as e:
            log.warning("redis test status read failed: %s", e)

        rstatus = (fields.get("status") or "").lower() if fields else ""

        # While Redis says the job is in-flight / failed, never short-circuit on a
        # stale on-disk ready manifest from a previous force run.
        if rstatus in ("queued", "processing", "failed"):
            frames_done = _optional_int(fields.get("frames_done"))
            frame_count = _optional_frame_count(
                fields.get("frame_count"),
                fields.get("frames_total"),
            )
            if summary and rstatus == "processing":
                avail = summary.get("frames_done")
                if avail is not None and (frames_done is None or avail > frames_done):
                    frames_done = avail
                if frame_count is None:
                    frame_count = _optional_frame_count(summary.get("frame_count"))

            playable_flag = fields.get("playable")
            playable = str(playable_flag).lower() in ("1", "true", "yes")
            if not playable and frames_done is not None:
                playable = frames_done >= _playable_min_frames()

            self._send_json(
                200,
                _public_test_job_payload(
                    {
                        **fields,
                        "job_id": job_id,
                        "status": rstatus,
                        "frames_done": frames_done,
                        "frame_count": frame_count,
                        "playable": playable if rstatus != "failed" else False,
                        "cached": False,
                        "duration_sec": (summary or {}).get("duration_sec"),
                        "type": fields.get("bench_type"),
                        "type_name": fields.get("bench_name"),
                        "error": fields.get("error"),
                    }
                ),
            )
            return

        if summary and summary.get("status") == "ready":
            self._send_json(
                200,
                _public_test_job_payload(
                    {
                        **fields,
                        **summary,
                        "cached": True,
                        "playable": True,
                        "type": fields.get("bench_type") or summary.get("type"),
                        "type_name": fields.get("bench_name"),
                    }
                ),
            )
            return

        if fields:
            self._send_json(
                200,
                _public_test_job_payload(
                    {
                        **fields,
                        "job_id": job_id,
                        "status": fields.get("status") or "queued",
                        "type": fields.get("bench_type"),
                        "type_name": fields.get("bench_name"),
                        "cached": False,
                    }
                ),
            )
            return

        if summary:
            self._send_json(200, _public_test_job_payload(summary))
            return

        self._send_json(404, {"error": "job not found", "job_id": job_id})

    def _handle_job_stats(self):
        from sview.jobs.job_status import cache_total_bytes, collect_job_stats, list_workers

        try:
            r = _get_redis()
            stats = collect_job_stats(r)
            stats["workers_alive"] = len(list_workers(r))
            stats["cache_bytes"] = cache_total_bytes(r)
        except Exception as e:
            log.exception("job stats failed")
            self._send_json(503, {"error": f"redis unavailable: {e}"})
            return
        self._send_json(200, stats)

    def _handle_metrics(self):
        try:
            from sview.jobs.job_status import prometheus_metrics

            text = prometheus_metrics(_get_redis())
            payload = text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self._cors()
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)
        except Exception as e:
            log.exception("metrics failed")
            self._send_err(503, f"metrics unavailable: {e}")

    def _handle_debug_oss(self):
        """Ops check: which OSS hosts this Master process will use (no secrets)."""
        from urllib.parse import urlparse

        import sview.worker.cpu as sw

        sample_url = ""
        sample_host = ""
        sign_err = None
        try:
            sample_url = sw.sign_oss_url("player-bin/_debug/ping.bin", expires=120)
            sample_host = urlparse(sample_url).netloc
        except Exception as e:
            sign_err = str(e)
        self._send_json(
            200,
            {
                "oss_endpoint": sw.OSS_ENDPOINT,
                "oss_public_endpoint": sw.OSS_PUBLIC_ENDPOINT,
                "oss_bucket": sw.OSS_BUCKET,
                "oss_cache": bool(sw.OSS_CACHE),
                "oss_media_redirect": os.environ.get("OSS_MEDIA_REDIRECT", "1"),
                "oss_media_local_first": os.environ.get("OSS_MEDIA_LOCAL_FIRST", "1"),
                "sample_sign_host": sample_host,
                "sample_sign_is_internal": "-internal." in sample_host,
                "sign_error": sign_err,
                "hint": (
                    "Browser cannot use *-internal* hosts. Keep OSS_MEDIA_REDIRECT=0 "
                    "(Master proxies), or fix sample_sign_host to public before enabling redirect."
                ),
            },
        )

    def _handle_workers(self):
        try:
            from sview.jobs.job_status import list_workers

            workers = list_workers(_get_redis())
            self._send_json(200, {"workers": workers, "count": len(workers)})
        except Exception as e:
            self._send_json(503, {"error": f"redis unavailable: {e}"})

    def _handle_job_events(self, job_id: str):
        """SSE stream of job progress (Master polls Redis, pushes to client)."""
        from sview.jobs.job_status import get_job_fields, job_progress_percent, touch_job

        try:
            r = _get_redis()
        except Exception as e:
            self._send_err(503, f"redis unavailable: {e}")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self._cors()
        self.end_headers()
        # Comment frame so proxies flush headers immediately (Ingress often buffers
        # until the first body bytes; EventSource then dies with readyState=2).
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
        except Exception as exc:
            if _is_client_disconnect(exc):
                return
            raise

        last_payload = ""
        last_ping = time.time()
        deadline = time.time() + 300
        try:
            while time.time() < deadline:
                try:
                    touch_job(r, job_id)
                    fields = get_job_fields(r, job_id)
                except Exception as e:
                    msg = f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
                    self.wfile.write(msg.encode("utf-8"))
                    self.wfile.flush()
                    break
                status = (fields.get("status") or "unknown").lower()
                done = _optional_int(fields.get("frames_done")) or 0
                total = _optional_frame_count(
                    fields.get("frame_count"),
                    fields.get("frames_total"),
                )
                pct = job_progress_percent(fields)
                playable = str(fields.get("playable") or "").lower() in (
                    "1",
                    "true",
                    "yes",
                )
                event = {
                    "job_id": job_id,
                    "status": status,
                    "frames_done": done,
                    "frame_count": total,
                    "percent": pct,
                    "playable": playable,
                    "play_base": fields.get("play_base") or None,
                    "video_url": fields.get("video_url") or None,
                    "error": fields.get("error") or None,
                    "publish_mode": fields.get("publish_mode") or "progressive",
                    "segments_ready_count": _optional_int(
                        fields.get("segments_ready_count")
                    ),
                    "segment_epoch": _optional_int(fields.get("segment_epoch")) or 0,
                    "encode_status": fields.get("encode_status") or None,
                    "decode_start": _optional_int(fields.get("decode_start")),
                    "input_frames": _optional_int(fields.get("input_frames")),
                    "output_frames": _optional_int(fields.get("output_frames")),
                }
                for key in ("available_ranges", "encode_window"):
                    try:
                        event[key] = json.loads(fields.get(key) or "null")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        event[key] = [] if key == "available_ranges" else None
                payload = json.dumps(event, ensure_ascii=False)
                if payload != last_payload:
                    last_payload = payload
                    last_ping = time.time()
                    chunk = f"event: progress\ndata: {payload}\n\n"
                    self.wfile.write(chunk.encode("utf-8"))
                    self.wfile.flush()
                elif time.time() - last_ping >= 15:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    last_ping = time.time()
                if status in ("ready", "failed", "cancelled"):
                    done_ev = f"event: done\ndata: {payload}\n\n"
                    self.wfile.write(done_ev.encode("utf-8"))
                    self.wfile.flush()
                    break
                time.sleep(1.0)
        except Exception as exc:
            if not _is_client_disconnect(exc):
                log.exception("SSE stream failed for %s", job_id)

    def _handle_job_status(self, job_id: str):
        from sview.jobs.job_status import get_job_fields, job_progress_percent, touch_job

        summary = _read_manifest_summary(self.cache_root, job_id)
        # Complete cache wins.
        if summary and summary.get("status") == "ready":
            try:
                touch_job(_get_redis(), job_id)
            except Exception:
                pass
            pct = None
            if summary.get("frame_count") and summary.get("frames_done") is not None:
                try:
                    pct = round(
                        min(
                            100.0,
                            100.0
                            * float(summary["frames_done"])
                            / float(summary["frame_count"]),
                        ),
                        1,
                    )
                except Exception:
                    pct = 100.0
            self._send_json(
                200,
                _public_job_payload(
                    {**summary, "cached": True, "playable": True, "percent": pct}
                ),
            )
            return

        fields: dict = {}
        try:
            r = _get_redis()
            fields = get_job_fields(r, job_id)
            if fields:
                try:
                    touch_job(r, job_id)
                    fields = get_job_fields(r, job_id)
                except Exception:
                    pass
        except Exception as e:
            log.warning("redis status read failed: %s", e)

        if fields:
            frames_done = _optional_int(fields.get("frames_done"))
            frame_count = _optional_frame_count(
                fields.get("frame_count"),
                fields.get("frames_total"),
            )
            if summary:
                avail = summary.get("frames_done")
                if avail is not None and (frames_done is None or avail > frames_done):
                    frames_done = avail
                if frame_count is None:
                    frame_count = _optional_frame_count(summary.get("frame_count"))
            if frames_done is None and frame_count is not None and summary and summary.get("status") == "ready":
                frames_done = frame_count

            playable_flag = fields.get("playable")
            if playable_flag is not None and str(playable_flag).lower() in ("1", "true", "yes"):
                playable = True
            elif summary and summary.get("playable"):
                playable = True
            else:
                playable = bool(
                    frames_done is not None and frames_done >= _playable_min_frames()
                )

            status = fields.get("status", "unknown")
            if (
                status in ("queued", "unknown")
                and summary
                and summary.get("_manifest_status") == "partial"
            ):
                status = "processing"

            duration_sec = None
            if summary:
                duration_sec = summary.get("duration_sec")

            pct = job_progress_percent(
                {
                    **fields,
                    "frames_done": frames_done,
                    "frames_total": frame_count,
                    "frame_count": frame_count,
                }
            )

            self._send_json(
                200,
                _public_job_payload(
                    {
                        "job_id": job_id,
                        "status": status,
                        "source": fields.get("source"),
                        "error": fields.get("error") or None,
                        "updated_at": fields.get("updated_at"),
                        "frame_count": frame_count,
                        "frames_done": frames_done,
                        "playable": playable,
                        "cached": False,
                        "duration_sec": duration_sec,
                        "play_base": fields.get("play_base") or None,
                        "percent": pct,
                        "cache_bytes": _optional_int(fields.get("cache_bytes")),
                        "last_access_at": fields.get("last_access_at"),
                        "publish_mode": fields.get("publish_mode") or "progressive",
                        "available_ranges": fields.get("available_ranges") or "[]",
                        "segments_ready_count": _optional_int(
                            fields.get("segments_ready_count")
                        ),
                        "segment_epoch": _optional_int(fields.get("segment_epoch")) or 0,
                        "manifest_revision": _optional_int(
                            fields.get("manifest_revision")
                        )
                        or 0,
                        "encode_window": fields.get("encode_window") or None,
                        "encode_status": fields.get("encode_status") or None,
                        "decode_start": _optional_int(fields.get("decode_start")),
                        "input_frames": _optional_int(fields.get("input_frames")),
                        "output_frames": _optional_int(fields.get("output_frames")),
                    }
                ),
            )
            return

        # No Redis — partial on-disk only
        if summary:
            self._send_json(200, _public_job_payload(summary))
            return

        self._send_json(404, {"job_id": job_id, "status": "not_found"})

    # ---- HandVis 静态页（可选，目录存在才生效） ----

    def _serve_handvis(self, path: str, body: bool) -> bool:
        web = _HANDVIS_WEB_DIR
        if not web.is_dir():
            return False
        if path in ("/handvis", "/handvis/", "/handvis/index.html"):
            # 首页的列表/评分依赖 HandVis 自己的 PG API，Master 上没有；
            # 直接转到播放页（输入 ep_dir/item_name 或视频/posehand 路径即可）。
            self.send_response(302)
            self.send_header("Location", "/handvis/playback")
            self._cors()
            self.end_headers()
            return True
        if path in ("/handvis/playback", "/handvis/playback.html"):
            return self._serve_handvis_html(web / "playback.html", body)
        if path.startswith("/handvis/assets/"):
            rel = path[len("/handvis/assets/"):]
            return self._serve_handvis_asset(web, rel, body)
        # HandVis HTML 里是绝对 /static/... 引用，映射到 assets/。
        if path.startswith("/static/"):
            rel = path[len("/static/"):]
            return self._serve_handvis_asset(web, rel, body)
        return False

    def _serve_handvis_html(self, html_path: Path, body: bool) -> bool:
        if not html_path.is_file():
            self._send_err(404, "handvis page not found in image")
            return True
        try:
            ver = os.environ.get("STATIC_ASSET_VERSION", "").strip() or str(
                int(html_path.stat().st_mtime)
            )
            html = html_path.read_text(encoding="utf-8").replace("__ASSET_VERSION__", ver)
            # 注入 Master 模式标记：播放页 JS 据此切换到 /api/jobs 链路。
            # HANDVIS_MASTER_BASE 非空时页面跨域打远端 Master（本地只起托管）。
            marker = '<script type="module"'
            mb = os.environ.get("HANDVIS_MASTER_BASE", "").strip().rstrip("/")
            mb = mb.replace('"', "").replace("<", "")
            inject = "<script>window.HANDVIS_MASTER=1;"
            if mb:
                inject += f'window.HANDVIS_MASTER_BASE="{mb}";'
            inject += "</script>"
            if marker in html and "HANDVIS_MASTER" not in html:
                html = html.replace(marker, inject + marker, 1)
            data = html.encode("utf-8")
        except OSError:
            self._send_err(404, "handvis page unreadable")
            return True
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.end_headers()
        if body:
            self.wfile.write(data)
        return True

    def _serve_handvis_asset(self, web: Path, rel: str, body: bool) -> bool:
        target = _resolve_under(web / "assets", rel)
        if target is None or not target.is_file():
            self._send_err(404, "asset not found")
            return True
        try:
            data = target.read_bytes()
        except OSError:
            self._send_err(404, "asset unreadable")
            return True
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        if target.suffix == ".js":
            ctype = "text/javascript; charset=utf-8"
        elif target.suffix == ".css":
            ctype = "text/css; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        # HTML 带 ?v= 版本号，资源可长缓存。
        self.send_header("Cache-Control", "public, max-age=86400")
        self._cors()
        self.end_headers()
        if body:
            self.wfile.write(data)
        return True

    def _handle_get(self, body: bool):
        path = unquote(self.path.split("?", 1)[0])

        if path in ("/healthz", "/health", "/"):
            payload = b'{"status":"ok"}\n'
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self._cors()
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if body:
                self.wfile.write(payload)
            return

        # HandVis 播放页托管：/handvis/playback + /static/* → handvis_web/assets。
        if path.startswith("/handvis") or path.startswith("/static/"):
            if self._serve_handvis(path, body):
                return

        parts = [p for p in path.split("/") if p]

        # GET /api/test/types
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "test" and parts[2] == "types":
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_test_types()
            return

        # GET /api/test/jobs/{job_id}
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "test" and parts[2] == "jobs":
            job_id = parts[3]
            if not _safe_job_id(job_id):
                self._send_err(400, "invalid job_id")
                return
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_test_job_status(job_id)
            return

        # GET /api/jobs/stats  (before /api/jobs/{job_id})
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "jobs" and parts[2] == "stats":
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_job_stats()
            return

        # GET /metrics or /api/metrics — Prometheus text for HPA
        if path.rstrip("/") in ("/metrics", "/api/metrics"):
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_metrics()
            return

        # GET /api/debug/oss — verify sign host (no secrets)
        if path.rstrip("/") in ("/api/debug/oss", "/debug/oss"):
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_debug_oss()
            return

        # GET /api/workers — alive worker heartbeats
        if len(parts) == 2 and parts[0] == "api" and parts[1] == "workers":
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_workers()
            return

        # GET /api/jobs/{job_id}/events — SSE progress stream
        if (
            len(parts) == 4
            and parts[0] == "api"
            and parts[1] == "jobs"
            and parts[3] == "events"
        ):
            job_id = parts[2]
            if not _safe_job_id(job_id):
                self._send_err(400, "invalid job_id")
                return
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_job_events(job_id)
            return

        # GET /api/jobs/{job_id}/signed-media — batch presigned public OSS URLs
        if (
            len(parts) == 4
            and parts[0] == "api"
            and parts[1] == "jobs"
            and parts[3] == "signed-media"
        ):
            job_id = parts[2]
            if not _safe_job_id(job_id):
                self._send_err(400, "invalid job_id")
                return
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_signed_media(job_id)
            return

        # GET /api/jobs/{job_id}/encode/{request_id}
        if (
            len(parts) == 5
            and parts[0] == "api"
            and parts[1] == "jobs"
            and parts[3] == "encode"
        ):
            job_id, request_id = parts[2], parts[4]
            if not _safe_job_id(job_id) or not _safe_job_id(request_id):
                self._send_err(400, "invalid job_id or request_id")
                return
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_encode_request_status(job_id, request_id)
            return

        # GET /api/jobs/{job_id}
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "jobs":
            job_id = parts[2]
            if not _safe_job_id(job_id):
                self._send_err(400, "invalid job_id")
                return
            if not body:
                self.send_response(200)
                self._cors()
                self.end_headers()
                return
            self._handle_job_status(job_id)
            return

        # GET /api/jobs/{job_id}/index.json  (HandVis mesh catalog; same job_id as segments)
        if (
            len(parts) == 4
            and parts[0] == "api"
            and parts[1] == "jobs"
            and parts[3] == "index.json"
        ):
            self._serve_job_sidecar(
                parts[2],
                "index.json",
                "application/json; charset=utf-8",
                body=body,
                as_json=True,
            )
            return

        # GET /api/jobs/{job_id}/faces.u32
        if (
            len(parts) == 4
            and parts[0] == "api"
            and parts[1] == "jobs"
            and parts[3] == "faces.u32"
        ):
            self._serve_job_sidecar(
                parts[2], "faces.u32", "application/octet-stream", body=body
            )
            return

        # GET /api/jobs/{job_id}/video_projection.bin
        if (
            len(parts) == 4
            and parts[0] == "api"
            and parts[1] == "jobs"
            and parts[3] == "video_projection.bin"
        ):
            self._serve_job_sidecar(
                parts[2],
                "video_projection.bin",
                "application/octet-stream",
                body=body,
            )
            return

        # GET /api/jobs/{job_id}/manifest.json  (preferred)
        # GET /{job_id}/manifest.json          (legacy alias)
        if (
            (len(parts) == 4 and parts[0] == "api" and parts[1] == "jobs"
             and parts[3] == "manifest.json")
            or (len(parts) == 2 and parts[1] == "manifest.json")
        ):
            job_id = parts[2] if parts[0] == "api" else parts[0]
            self._serve_manifest(job_id, body=body)
            return

        # GET /api/jobs/{job_id}/segments/{name}  (preferred batch path)
        # GET /{job_id}/segments/{name}           (legacy alias)
        if (
            (len(parts) == 5 and parts[0] == "api" and parts[1] == "jobs"
             and parts[3] == "segments")
            or (len(parts) == 3 and parts[1] == "segments")
        ):
            if parts[0] == "api":
                job_id, seg_name = parts[2], parts[4]
            else:
                job_id, seg_name = parts[0], parts[2]
            self._serve_segment(job_id, seg_name, body=body)
            return

        # GET /api/jobs/{job_id}/frames/{name}  (single-frame compat)
        # GET /{job_id}/frames/{name}           (legacy alias)
        if (
            (len(parts) == 5 and parts[0] == "api" and parts[1] == "jobs"
             and parts[3] == "frames")
            or (len(parts) == 3 and parts[1] == "frames")
        ):
            if parts[0] == "api":
                job_id, frame_name = parts[2], parts[4]
            else:
                job_id, frame_name = parts[0], parts[2]
            self._serve_frame(job_id, frame_name, body=body)
            return

        self._send_err(404, "not found")

    def _handle_signed_media(self, job_id: str):
        """Batch-sign player-bin segment GET URLs on the public OSS endpoint.

        Query:
          starts=0,30,60          explicit segment indices
          start=0&count=8&step=30 range form (defaults step=SEGMENT_FRAMES)
          kind=segments           only; frames are not stored on OSS
        Browser should fetch returned HTTPS URLs directly (CORS on bucket required).
        Out-of-range starts are omitted (see `skipped`); they are not signed.
        """
        if not self._oss_cache_enabled():
            self._send_json(503, {"error": "OSS_CACHE disabled"})
            return

        qs: dict = {}
        if "?" in self.path:
            qs = parse_qs(self.path.split("?", 1)[1], keep_blank_values=False)

        kind = (qs.get("kind", ["segments"])[0] or "segments").strip().lower()
        if kind == "frames":
            self._send_err(
                400,
                "kind=frames is not stored on OSS play cache; use kind=segments",
            )
            return
        if kind != "segments":
            self._send_err(400, "kind must be segments")
            return

        starts: list[int] = []
        if qs.get("starts"):
            raw = qs["starts"][0]
            try:
                starts = [int(x.strip()) for x in raw.split(",") if x.strip() != ""]
            except ValueError:
                self._send_err(400, "invalid starts")
                return
        else:
            try:
                start = int((qs.get("start") or ["0"])[0])
                count = int((qs.get("count") or ["8"])[0])
                step = int((qs.get("step") or [str(_segment_frames())])[0])
            except ValueError:
                self._send_err(400, "invalid start/count/step")
                return
            if step <= 0 or count <= 0:
                self._send_err(400, "count/step must be positive")
                return
            count = min(32, count)
            starts = [start + i * step for i in range(count)]

        if not starts:
            self._send_err(400, "no starts")
            return
        if len(starts) > 32:
            self._send_err(400, "max 32 urls per request")
            return
        for s in starts:
            if s < 0:
                self._send_err(400, "negative start")
                return

        man = _load_play_manifest(self.cache_root, job_id)
        if man is None:
            self._send_err(404, "manifest not found")
            return
        if man.get("cache_align_failed"):
            self._send_retry(425, "play cache not aligned yet", retry_after=1)
            return
        if (man.get("publish_mode") or "").lower() != "sparse" and man.get("cache_aligned") and (
            _optional_int((man.get("bundle") or {}).get("frames_available")) or 0
        ) <= 0:
            self._send_err(404, "play cache missing or not contiguous from frame 0")
            return

        from sview.cache.oss_cache_gc import filter_playable_segment_starts

        avail = _optional_int((man.get("bundle") or {}).get("frames_available"))
        kept, skipped = filter_playable_segment_starts(
            starts,
            frames_available=avail,
            segment_frames=_segment_frames(),
            available_ranges=(man.get("bundle") or {}).get("available_ranges")
            if (man.get("publish_mode") or "").lower() == "sparse"
            else None,
        )
        if not kept:
            self._send_json(
                200,
                {
                    "job_id": job_id,
                    "kind": kind,
                    "expires_in": int(os.environ.get("OSS_SIGN_EXPIRES", "7200")),
                    "items": [],
                    "skipped": skipped,
                },
            )
            return

        expires = int(os.environ.get("OSS_SIGN_EXPIRES", "7200"))
        try:
            from sview.jobs.job_status import touch_job
            from sview.worker.cpu import oss_cache_key, sign_oss_url

            try:
                touch_job(_get_redis(), job_id)
            except Exception:
                pass

            items = []
            for s in kept:
                name = f"{s:06d}.bin"
                key = oss_cache_key(job_id, "segments", name)
                url = sign_oss_url(key, expires=expires)
                if "-internal." in url:
                    self._send_json(
                        500,
                        {
                            "error": "signed URL used internal endpoint; set OSS_PUBLIC_ENDPOINT",
                            "sample": url.split("?", 1)[0],
                        },
                    )
                    return
                items.append({"start": s, "name": name, "url": url})
        except Exception as e:
            log.exception("signed-media failed for %s", job_id)
            self._send_json(503, {"error": f"sign failed: {e}"})
            return

        payload = {
            "job_id": job_id,
            "kind": kind,
            "expires_in": expires,
            "items": items,
        }
        if skipped:
            payload["skipped"] = skipped
        self._send_json(200, payload)

    def _owner_for_proxy(self, job_id: str) -> Optional[str]:
        """Return remote owner pod IP if we should reverse-proxy; else None."""
        if not _cluster_enabled():
            return None
        if (self.headers.get(_PROXY_HEADER) or "").strip() == "1":
            return None  # already a hop — serve local only
        try:
            from sview.jobs.job_status import get_job_fields, get_lock_owner

            r = _get_redis()
            fields = get_job_fields(r, job_id)
            owner = (fields.get("owner_pod") or "").strip()
            if not owner:
                owner = (get_lock_owner(r, job_id) or "").strip()
        except Exception:
            return None
        if not owner or owner in ("local", _pod_ip()):
            return None
        return owner

    def _proxy_from_owner(self, owner: str, body: bool) -> bool:
        """Reverse-proxy current request to owner pod. Returns True if handled."""
        # Preserve query flags such as ?proxy=1 across the owner hop.
        path = self.path
        url = f"http://{owner}:{_pod_http_port()}{path}"
        method = "GET" if body else "HEAD"
        req = urllib.request.Request(
            url,
            method=method,
            headers={_PROXY_HEADER: "1", "Accept": "*/*"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read() if body else b""
                status = getattr(resp, "status", 200) or 200
                ctype = resp.headers.get("Content-Type") or "application/octet-stream"
                cache = resp.headers.get("Cache-Control") or "no-store"
                retry_after = resp.headers.get("Retry-After")
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data) if body else 0))
                self.send_header("Cache-Control", cache)
                if retry_after:
                    self.send_header("Retry-After", retry_after)
                self._cors()
                self.end_headers()
                if body and data:
                    self.wfile.write(data)
                return True
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read() if body else b""
            except Exception:
                err_body = b""
            self.send_response(e.code)
            self.send_header(
                "Content-Type",
                e.headers.get("Content-Type") or "text/plain; charset=utf-8",
            )
            self.send_header("Content-Length", str(len(err_body) if body else 0))
            if e.headers.get("Retry-After"):
                self.send_header("Retry-After", e.headers.get("Retry-After"))
            self._cors()
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if body and err_body:
                self.wfile.write(err_body)
            return True
        except Exception as e:
            log.warning("owner proxy failed %s -> %s: %s", path, owner, e)
            self._send_err(503, "owner pod unavailable")
            return True

    def _oss_cache_enabled(self) -> bool:
        return _oss_cache_on()

    def _query_flag(self, name: str) -> bool:
        if "?" not in self.path:
            return False
        qs = parse_qs(self.path.split("?", 1)[1], keep_blank_values=False)
        for v in qs.get(name, []):
            if str(v).lower() in ("1", "true", "yes", "on"):
                return True
        return False

    def _redirect_oss_play(self, job_id: str, *rel_parts: str, body: bool) -> bool:
        """302 to a short-lived signed OSS URL (private bucket + browser CORS).

        Returns True if a redirect response was sent.
        Note: do NOT use this for manifest.json — browsers often fail to parse
        JSON after a cross-origin 302; serve/proxy the body instead.
        """
        if not self._oss_cache_enabled():
            return False
        try:
            from sview.worker.cpu import oss_cache_key, sign_oss_url

            key = oss_cache_key(job_id, *rel_parts)
            url = sign_oss_url(key, expires=int(os.environ.get("OSS_SIGN_EXPIRES", "7200")))
        except Exception as e:
            log.warning("oss sign redirect skip %s/%s: %s", job_id, "/".join(rel_parts), e)
            return False
        if "-internal." in url:
            # Browser cannot reach VPC-only OSS hosts; fall back to proxy path.
            log.error("signed URL still internal, skip 302: %s", url.split("?", 1)[0])
            return False
        self.send_response(302)
        self.send_header("Location", url)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Expose-Headers", "Location")
        self._cors()
        self.end_headers()
        return True

    def _proxy_oss_object(self, job_id: str, *rel_parts: str, content_type: str, body: bool) -> bool:
        """Fetch private OSS object server-side and return bytes with Master CORS."""
        if not self._oss_cache_enabled():
            return False
        try:
            from sview.worker.cpu import get_oss_bucket, oss_cache_key

            key = oss_cache_key(job_id, *rel_parts)
            result = get_oss_bucket().get_object(key)
            data = result.read()
        except Exception as e:
            log.debug("oss proxy miss %s/%s: %s", job_id, "/".join(rel_parts), e)
            return False
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.end_headers()
        if body:
            self.wfile.write(data)
        return True

    def _serve_job_sidecar(
        self,
        job_id: str,
        name: str,
        content_type: str,
        body: bool,
        as_json: bool = False,
    ):
        """Serve HandVis sidecars from local cache or OSS player-bin/{job_id}/{name}."""
        if not _safe_job_id(job_id) or name not in (
            "index.json",
            "faces.u32",
            "video_projection.bin",
        ):
            self._send_err(400, "invalid path")
            return
        owner = self._owner_for_proxy(job_id)
        if owner and self._proxy_from_owner(owner, body=body):
            return
        file_path = _resolve_under(self.cache_root, job_id, name)
        if file_path is not None and file_path.is_file():
            data = file_path.read_bytes()
            cache = "no-store" if as_json else "public, max-age=86400, immutable"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", cache)
            self._cors()
            self.end_headers()
            if body:
                self.wfile.write(data)
            return
        if as_json:
            if self._proxy_oss_object(job_id, name, content_type=content_type, body=body):
                return
        else:
            force_proxy = self._query_flag("proxy")
            prefer_oss = os.environ.get("OSS_MEDIA_REDIRECT", "1").lower() in (
                "1", "true", "yes", "on",
            )
            if prefer_oss and not force_proxy and self._redirect_oss_play(job_id, name, body=body):
                return
            if self._proxy_oss_object(job_id, name, content_type=content_type, body=body):
                return
        if self._frame_not_ready(job_id, None):
            self._send_retry(425, "asset not ready yet", retry_after=1)
            return
        self._send_err(404, f"missing {name}")

    def _serve_manifest(self, job_id: str, body: bool):
        if not _safe_job_id(job_id):
            self._send_err(400, "invalid job_id")
            return
        data = _load_play_manifest(self.cache_root, job_id)
        if (data or {}).get("publish_mode", "").lower() != "sparse":
            # Remote owner → always proxy (ignore stale local cache on this pod).
            owner = self._owner_for_proxy(job_id)
            if owner and self._proxy_from_owner(owner, body=body):
                return
            data = data or _load_play_manifest(self.cache_root, job_id)
        if data is None:
            self._send_err(404, "file not found")
            return
        if data.get("cache_align_failed"):
            self._send_retry(425, "play cache not aligned yet", retry_after=1)
            return

        # After cancel/GC/hole: nothing contiguous → 404 so FE cannot start on ghost cache.
        avail = _optional_int((data.get("bundle") or {}).get("frames_available"))
        if (
            (data.get("publish_mode") or "").lower() != "sparse"
            and data.get("cache_aligned")
            and (avail is None or avail <= 0)
        ):
            self._send_err(404, "play cache missing or not contiguous from frame 0")
            return

        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        cache = "no-store"
        if (data.get("status") or "").lower() == "done" and not data.get("cache_aligned"):
            cache = "public, max-age=60"
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", cache)
        self._cors()
        self.end_headers()
        if body:
            self.wfile.write(payload)
    def _frame_not_ready(self, job_id: str, frame_idx: Optional[int]) -> bool:
        """True if missing frame should be 425 (still encoding) rather than 404."""
        summary = _read_manifest_summary(self.cache_root, job_id)
        if summary:
            mstat = summary.get("_manifest_status")
            avail = summary.get("frames_done")
            if mstat == "partial":
                return True
            if (
                frame_idx is not None
                and avail is not None
                and frame_idx >= int(avail)
            ):
                return True
        try:
            from sview.jobs.job_status import get_job_fields

            fields = get_job_fields(_get_redis(), job_id)
            if (fields.get("status") or "").lower() == "processing":
                return True
        except Exception:
            pass
        return False

    def _serve_segment(self, job_id: str, seg_name: str, body: bool):
        """Serve SEGMENT_FRAMES consecutive frame bundles in one response."""
        if not _safe_job_id(job_id) or not _FRAME_RE.match(seg_name):
            self._send_err(400, "invalid path")
            return
        try:
            start_index = int(seg_name.split(".", 1)[0])
        except ValueError:
            self._send_err(400, "invalid path")
            return
        seg_n = _segment_frames()
        if start_index < 0 or (start_index % seg_n) != 0:
            self._send_err(400, f"segment start must be multiple of {seg_n}")
            return

        sparse_manifest = _load_play_manifest(self.cache_root, job_id)
        is_sparse = (
            (sparse_manifest or {}).get("publish_mode") or ""
        ).lower() == "sparse"
        # Sparse segments live in the shared OSS cache. Do not let a stale
        # worker owner intercept the request before the OSS path.
        if not is_sparse:
            owner = self._owner_for_proxy(job_id)
            if owner and self._proxy_from_owner(owner, body=body):
                return
        else:
            from sview.cache.oss_cache_gc import frame_range_ready

            sparse_ranges = (
                ((sparse_manifest or {}).get("bundle") or {}).get(
                    "available_ranges"
                )
                or []
            )
            if frame_range_ready(
                sparse_ranges, start_index, start_index + 1
            ):
                prefer_oss = os.environ.get("OSS_MEDIA_REDIRECT", "1").lower() in (
                    "1", "true", "yes", "on",
                )
                force_proxy = self._query_flag("proxy")
                if (
                    prefer_oss
                    and not force_proxy
                    and self._redirect_oss_play(
                        job_id, "segments", seg_name, body=body
                    )
                ):
                    return
                if self._proxy_oss_object(
                    job_id,
                    "segments",
                    seg_name,
                    content_type="application/octet-stream",
                    body=body,
                ):
                    return
                self._send_err(404, "sparse segment advertised but object missing")
                return
            self._send_retry(425, "sparse segment not encoded yet", retry_after=1)
            return

        packed_path = _resolve_under(
            self.cache_root, job_id, "segments", seg_name
        )
        if packed_path is not None and packed_path.is_file():
            self._send_file(
                packed_path,
                "application/octet-stream",
                cache="public, max-age=86400, immutable",
                body=body,
            )
            return

        # Prefer local frames when present (same-node Master/Worker). OSS redirect
        # only when local pack is unavailable — avoids CORS/parse issues on FE.
        prefer_oss = os.environ.get("OSS_MEDIA_REDIRECT", "1").lower() in (
            "1", "true", "yes", "on",
        )
        local_first = os.environ.get("OSS_MEDIA_LOCAL_FIRST", "1").lower() in (
            "1", "true", "yes", "on",
        )
        force_proxy = self._query_flag("proxy")
        # ?proxy=1 → never 302 (frontend fallback when Master still signs *-internal*).
        if (
            prefer_oss
            and not force_proxy
            and not local_first
            and self._redirect_oss_play(job_id, "segments", seg_name, body=body)
        ):
            return

        if is_sparse:
            from sview.cache.oss_cache_gc import frame_range_ready

            sparse_ranges = (
                ((sparse_manifest or {}).get("bundle") or {}).get(
                    "available_ranges"
                )
                or []
            )
            if frame_range_ready(
                sparse_ranges, start_index, start_index + 1
            ):
                if (
                    prefer_oss
                    and not force_proxy
                    and self._redirect_oss_play(
                        job_id, "segments", seg_name, body=body
                    )
                ):
                    return
                if self._proxy_oss_object(
                    job_id,
                    "segments",
                    seg_name,
                    content_type="application/octet-stream",
                    body=body,
                ):
                    return
                self._send_err(404, "sparse segment advertised but object missing")
                return
            self._send_retry(425, "sparse segment not encoded yet", retry_after=1)
            return

        summary = _read_manifest_summary(self.cache_root, job_id)
        avail = int(summary["frames_done"]) if summary and summary.get("frames_done") is not None else None
        mstat = (summary.get("_manifest_status") or "").lower() if summary else ""
        complete = mstat in ("done", "ready")

        if avail is not None and start_index >= avail:
            if complete:
                self._send_err(404, "segment past end")
            else:
                self._send_retry(425, "segment not ready yet", retry_after=1)
            return

        # Full segment unless this is the final partial chunk of a finished job.
        want = seg_n
        if avail is not None:
            remaining = avail - start_index
            if remaining < seg_n:
                if not complete:
                    self._send_retry(425, "segment not ready yet", retry_after=1)
                    return
                want = remaining
        if want <= 0:
            self._send_err(404, "segment past end")
            return

        blobs: list[bytes] = []
        for i in range(start_index, start_index + want):
            name = f"{i:06d}.bin"
            file_path = _resolve_under(self.cache_root, job_id, "frames", name)
            if file_path is None or not file_path.is_file():
                # Local miss: public 302 (unless ?proxy=1), else Master streams via internal GET.
                if (
                    prefer_oss
                    and not force_proxy
                    and self._redirect_oss_play(
                        job_id, "segments", seg_name, body=body
                    )
                ):
                    return
                if self._proxy_oss_object(
                    job_id,
                    "segments",
                    seg_name,
                    content_type="application/octet-stream",
                    body=body,
                ):
                    return
                if self._frame_not_ready(job_id, i):
                    self._send_retry(425, "segment not ready yet", retry_after=1)
                    return
                self._send_err(404, f"missing frame {name}")
                return
            try:
                blobs.append(file_path.read_bytes())
            except OSError:
                if (
                    prefer_oss
                    and not force_proxy
                    and self._redirect_oss_play(
                        job_id, "segments", seg_name, body=body
                    )
                ):
                    return
                if self._proxy_oss_object(
                    job_id,
                    "segments",
                    seg_name,
                    content_type="application/octet-stream",
                    body=body,
                ):
                    return
                if self._frame_not_ready(job_id, i):
                    self._send_retry(425, "segment not ready yet", retry_after=1)
                    return
                self._send_err(404, f"missing frame {name}")
                return

        data = _pack_segment(start_index, blobs, seg_n)
        # Full segments are immutable; final short segment of a done job too.
        cache = "public, max-age=86400, immutable"
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", cache)
        self.send_header("X-Sview-Segment-Start", str(start_index))
        self.send_header("X-Sview-Segment-Count", str(len(blobs)))
        self._cors()
        self.end_headers()
        if body:
            self.wfile.write(data)

    def _serve_frame(self, job_id: str, frame_name: str, body: bool):
        if not _safe_job_id(job_id) or not _FRAME_RE.match(frame_name):
            self._send_err(400, "invalid path")
            return
        # Remote owner → always proxy (ignore stale local cache on this pod).
        owner = self._owner_for_proxy(job_id)
        if owner and self._proxy_from_owner(owner, body=body):
            return
        file_path = _resolve_under(self.cache_root, job_id, "frames", frame_name)
        if file_path is None or not file_path.is_file():
            # Prefer 425 (retry) over 404 while job is still producing frames.
            frame_idx = None
            try:
                frame_idx = int(frame_name.split(".", 1)[0])
            except ValueError:
                pass
            if self._frame_not_ready(job_id, frame_idx):
                self._send_retry(425, "frame not ready yet", retry_after=1)
                return
            self._send_err(404, "file not found")
            return
        self._send_file(
            file_path,
            "application/octet-stream",
            cache="public, max-age=86400, immutable",
            body=body,
        )
    def _send_err(self, code: int, msg: str):
        payload = (msg + "\n").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self._cors()
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _send_retry(self, code: int, msg: str, retry_after: int = 1):
        payload = (msg + "\n").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Retry-After", str(retry_after))
        self._cors()
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _send_file(
        self,
        file_path: Optional[Path],
        content_type: str,
        cache: str,
        body: bool,
    ):
        if file_path is None or not file_path.is_file():
            self._send_err(404, "file not found")
            return
        try:
            size = file_path.stat().st_size
        except OSError:
            self._send_err(404, "file not found")
            return

        ctype = content_type or (
            mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        )
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        self.send_header("Cache-Control", cache)
        self._cors()
        self.end_headers()
        if not body:
            return
        try:
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except Exception as exc:
            if _is_client_disconnect(exc):
                log.debug("HTTP client disconnected while sending %s", file_path.name)
                return
            raise


def start_sview_http_server(
    cache_root: Union[Path, str],
    host: Optional[str] = None,
    port: Optional[int] = None,
    daemon: bool = True,
) -> Optional[ThreadingHTTPServer]:
    if host is None:
        host = os.environ.get("HTTP_HOST", "0.0.0.0")
    if port is None:
        port = int(os.environ.get("HTTP_PORT", "8080"))
    if port <= 0:
        log.info("HTTP server disabled (HTTP_PORT=%s)", port)
        return None

    from sview.jobs.job_status import POD_IP, SVIEW_CLUSTER, ensure_cluster_identity

    ensure_cluster_identity(fatal=True)
    log.info("HTTP cluster mode=%s pod_ip=%s", SVIEW_CLUSTER, POD_IP)

    root = Path(cache_root)
    root.mkdir(parents=True, exist_ok=True)

    class _Handler(SviewCacheHandler):
        cache_root = root

    server = QuietThreadingHTTPServer((host, port), _Handler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="sview-http",
        daemon=daemon,
    )
    thread.start()
    log.info("HTTP server listening on http://%s:%d  root=%s", host, port, root)
    try:
        import sview.worker.cpu as _sw

        log.info(
            "OSS media: endpoint=%s public=%s redirect=%s local_first=%s",
            _sw.OSS_ENDPOINT,
            _sw.OSS_PUBLIC_ENDPOINT,
            os.environ.get("OSS_MEDIA_REDIRECT", "1"),
            os.environ.get("OSS_MEDIA_LOCAL_FIRST", "1"),
        )
        if _sw.OSS_CACHE:
            try:
                from urllib.parse import urlparse

                _u = _sw.sign_oss_url("player-bin/_debug/ping.bin", expires=60)
                log.info("OSS sample sign host=%s", urlparse(_u).netloc)
            except Exception as e:
                log.warning("OSS sample sign failed: %s", e)
    except Exception:
        pass
    log.info("  GET  /healthz")
    log.info("  GET  /metrics")
    log.info("  GET  /api/debug/oss")
    if _HANDVIS_WEB_DIR.is_dir():
        log.info("  GET  /handvis/playback  (HandVis 3D 播放页, web=%s)", _HANDVIS_WEB_DIR)
    log.info("  POST /api/jobs")
    log.info("  GET  /api/jobs/stats")
    log.info("  GET  /api/workers")
    log.info("  GET  /api/jobs/{job_id}")
    log.info("  GET  /api/jobs/{job_id}/events  (SSE)")
    log.info("  POST /api/jobs/{job_id}/touch")
    log.info("  POST /api/jobs/{job_id}/encode  (sparse on-demand segments)")
    log.info("  GET  /api/jobs/{job_id}/encode/{request_id}")
    log.info("  POST /api/jobs/{job_id}/encode/{request_id}/cancel")
    log.info("  GET  /api/jobs/{job_id}/signed-media  (batch public OSS sign)")
    log.info("  GET  /api/jobs/{job_id}/manifest.json")
    log.info("  GET  /api/jobs/{job_id}/index.json  (HandVis)")
    log.info("  GET  /api/jobs/{job_id}/faces.u32")
    log.info("  GET  /api/jobs/{job_id}/video_projection.bin")
    log.info(
        "  GET  /api/jobs/{job_id}/segments/000000.bin  (SEGMENT_FRAMES=%d)",
        _segment_frames(),
    )
    log.info("  GET  /api/jobs/{job_id}/frames/000000.bin")
    log.info("  GET  /api/test/types")
    log.info("  POST /api/test/jobs")
    log.info("  GET  /api/test/jobs/{job_id}")
    log.info("  (legacy) GET /{job_id}/manifest.json | segments|frames/*.bin")

    # Master/all with OSS_CACHE: start idle+LRU GC loop.
    role = (os.environ.get("SVIEW_ROLE", "all") or "all").strip().lower()
    if role in ("all", "master") and os.environ.get("OSS_CACHE", "0").lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        try:
            from sview.cache.oss_cache_gc import start_cache_gc

            start_cache_gc(_get_redis)
        except Exception:
            log.exception("failed to start cache GC")

    return server


if __name__ == "__main__":
    setup_logging("sview_http")
    cache = Path(os.environ.get("LOCAL_OUTPUT_DIR", str(Path(__file__).resolve().parents[2] / "output")))
    srv = start_sview_http_server(cache, daemon=False)
    if srv is None:
        raise SystemExit("HTTP_PORT disabled")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        log.info("Shutting down HTTP server")
        srv.shutdown()
