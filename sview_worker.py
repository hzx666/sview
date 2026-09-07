#!/usr/bin/env python3
"""
SView worker – CPU transcode HEVC->H264 (libx264) + NAL AU split, Redis queue, HTTP cache.

Pairs with sview_http.py. Splits H.264 into per-frame AUs via Annex-B NAL scan
(no ffprobe). Encode path is libx264 (no CUDA/NVENC).

Input formats (auto-detected):
  - Head-cam / SSIEGO: protobuf ``/camera/*/video`` (H.264/HEVC AUs)
  - Orin teleop: ROS2 ``sensor_msgs/CompressedImage`` JPEG，或
    ``/streamer/realsense_*/h264`` / ``/camera/{f_l,f_r,left_wrist,right_wrist}/h264``
    Annex-B H.264（见 ``orin_images.py``；映射到 FR/FL/RR/RL）

Two modes:
--path <file>         Process a single mcap file.
(no --path)           Poll Redis queue for source paths / bench JSON jobs.

All config via environment variables (K8s Secret friendly).

Resolution / quality (env or CLI):
  TRANSCODE_SIZE / --scale           square resize; default 1080 (0 = keep source)
  TRANSCODE_WIDTH / --width          output width; 0 = keep source
  TRANSCODE_HEIGHT / --height        output height; 0 = keep source
  TRANSCODE_CRF / --crf              libx264 CRF 0-51 (default 28; alias TRANSCODE_CQ)
  TRANSCODE_BITRATE / --bitrate      ABR target; 0/empty = CRF+VBV (recommended)
  TRANSCODE_MAXRATE / --maxrate      VBV ceiling (default 1.5M)
  TRANSCODE_BUFSIZE / --bufsize      VBV buffer (default 3M)
  TRANSCODE_PRESET / --preset        default ultrafast (speed); veryfast for nicer quality
  TRANSCODE_FORCE_REENCODE=1         force re-encode (default on; disables H.264 passthrough)
  X264_THREADS=2                     min threads per ffmpeg (floor when the pod is busy)
  X264_THREAD_BUDGET=16              total x264 threads on the pod; live 4-cam ≈ budget/4 each
  PROGRESSIVE_PUBLISH=1              write frames to LOCAL_OUTPUT_DIR as built
  PARTIAL_MANIFEST_EVERY=30          refresh partial manifest every N frames
  PLAYABLE_MIN_FRAMES=30             API/frontend playable threshold
  MAX_INFLIGHT_JOBS=3                max concurrent process_mcap in Redis mode
  OVERLAP_AFTER_PLAYABLE=1           start next job once every active job is playable
  CPU_CONCURRENCY=12                 max concurrent ffmpeg encodes on this pod
  ENCODE_JOB_MAX_CONCURRENT=4        hard per-job ffmpeg cap (12/4 → 3 jobs fill pod)
  ENCODE_PLAYABLE_MAX_CONCURRENT=4   after playable, keep 4-cam parallel (1 = serializes, <realtime)
  ENCODE_FOREGROUND_RESERVE=0        slots kept for not-yet-playable jobs (0=auto ~25%)
  LIVE_ENCODE=1                      hold 4 encode slots for the job so cameras stay parallel
  ORIN_DIRECT=1                      Orin JPEG CRF-only (no ABR); H.264 uses TRANSCODE_*
  ORIN_CRF=23                        Orin JPEG→H.264 CRF (HD compress; raise toward 12 for near-lossless)
  LOG_FILE=sview.log                 project log under cwd (shared by all modules)
  LOG_PROGRESS_EVERY_PCT=5           job progress log step (percent); 0=every callback
  SVIEW_ROLE=all|master|worker       master=HTTP only; worker=encode; all=both
  OSS_CACHE=1                        mirror progressive publish to OSS play cache
  OSS_CACHE_PREFIX=player-bin        OSS key prefix for play cache
  OSS_PUBLIC_BASE=https://bucket.oss-cn-shanghai.aliyuncs.com
  SEGMENT_FRAMES=30                  OSS/HTTP segment size
  CACHE_MAX_BYTES=536870912000       OSS play-cache cap (~500GiB)
  CACHE_IDLE_TTL_SEC=14400           idle job eviction (4h)
  CACHE_GC_INTERVAL_SEC=300          master GC loop interval
"""

import os
import subprocess
import time
import threading
import tempfile
import sys
import argparse
import logging
from contextlib import contextmanager
from pathlib import Path
from collections import defaultdict

import redis
from mcap_protobuf_compat import (
    read_headcam_video_frame_count,
    read_protobuf_messages_compat as read_protobuf_messages,
)
import json

from sview_logging import setup_logging

log = setup_logging("sview_worker")

# ---------------------------------------------------------------------------
#  Configuration  (all from env vars)
# ---------------------------------------------------------------------------

# -- Redis --
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_USERNAME = os.environ.get("REDIS_USERNAME") or None  # Aliyun Redis ACL
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_QUEUE_KEY = os.environ.get("REDIS_QUEUE_KEY", "player:queue")
# -- Local mounts (K8s YAML mounts OSS prefixes here; see path_map.py) --
# Prefer HEAD_CAM_MOUNT / ORIN_MOUNT; LOCAL_MOUNT_PATH kept for backward compat.
from path_map import (
    HEAD_CAM_MOUNT,
    ORIN_MOUNT,
    redis_path_to_local,
    source_to_local,
    classify_source,
)

LOCAL_MOUNT_PATH = HEAD_CAM_MOUNT  # legacy alias (= /head-data by default)
REDIS_PATH_PREFIX = os.environ.get("REDIS_PATH_PREFIX", "/head-cam")

# -- OSS --
_oss_endpoint = os.environ.get("OSS_ENDPOINT", "https://oss-cn-shanghai.aliyuncs.com")
if _oss_endpoint and "://" not in _oss_endpoint:
    _oss_endpoint = "https://" + _oss_endpoint
OSS_ENDPOINT = _oss_endpoint
OSS_BUCKET = os.environ.get("OSS_BUCKET", "ss-oss-data-platform")
OSS_ACCESS_KEY_ID = os.environ.get("OSS_ACCESS_KEY_ID", "")
OSS_ACCESS_KEY_SECRET = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
OSS_OUTPUT_PREFIX = os.environ.get("OSS_OUTPUT_PREFIX", "processed_data").strip("/")
OSS_OUTPUT_SUB = os.environ.get("OSS_OUTPUT_SUB", "ego_human_data/head_cam").strip("/")
# When true, never upload finished archives to processed_data/; local-cache mode.
NO_UPLOAD = os.environ.get("NO_UPLOAD", "0").lower() in ("1", "true", "yes")
# Role: all (default) | master (HTTP+enqueue only) | worker (Redis consumer + encode)
SVIEW_ROLE = (os.environ.get("SVIEW_ROLE", "all") or "all").strip().lower()
# Mirror progressive local frames to OSS as a shared play cache (segments + manifest).
OSS_CACHE = os.environ.get("OSS_CACHE", "0").lower() in ("1", "true", "yes", "on")
OSS_CACHE_PREFIX = (os.environ.get("OSS_CACHE_PREFIX", "player-bin") or "player-bin").strip("/")
# Default off: most buckets enable Block Public Access. Playback uses Master
# local/signed redirect instead of anonymous play_base GET.
OSS_CACHE_PUBLIC = os.environ.get("OSS_CACHE_PUBLIC", "0").lower() in ("1", "true", "yes", "on")
# Latch after first "public ACL not allowed" so we do not double-PUT every object.
_oss_public_acl_blocked = False

# Browser-facing base, e.g. https://bucket.oss-cn-shanghai.aliyuncs.com
# If empty, derived from OSS_BUCKET + OSS_PUBLIC_ENDPOINT.
OSS_PUBLIC_BASE = (os.environ.get("OSS_PUBLIC_BASE") or "").rstrip("/")
_oss_public_endpoint = os.environ.get("OSS_PUBLIC_ENDPOINT", "oss-cn-shanghai.aliyuncs.com")
if _oss_public_endpoint.startswith("https://"):
    _oss_public_endpoint = _oss_public_endpoint[len("https://"):]
elif _oss_public_endpoint.startswith("http://"):
    _oss_public_endpoint = _oss_public_endpoint[len("http://"):]
OSS_PUBLIC_ENDPOINT = _oss_public_endpoint
# Segment size for OSS cache objects (align with HTTP SEGMENT_FRAMES / gop).
try:
    SEGMENT_FRAMES = max(1, int(os.environ.get("SEGMENT_FRAMES", "30") or "30"))
except (TypeError, ValueError):
    SEGMENT_FRAMES = 30

# -- Local output (used when --no-upload) --
LOCAL_OUTPUT_DIR = Path(os.environ.get("LOCAL_OUTPUT_DIR", str(Path(__file__).parent / "output")))
LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -- HTTP cache server (serves LOCAL_OUTPUT_DIR) --
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))  # 0 = disable

# -- FFmpeg & CPU encode --
_default_ffmpeg = "ffmpeg"
if sys.platform == "win32":
    _default_ffmpeg = r"C:\Users\10279\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1.2-full_build\bin\ffmpeg.exe"
FFMPEG = os.environ.get("FFMPEG_PATH", _default_ffmpeg)

CPU_CONCURRENCY = int(os.environ.get("CPU_CONCURRENCY", os.environ.get("GPU_CONCURRENCY", "12")))
# Hard per-job ffmpeg cap (e.g. 12 slots / 4 per job → up to 3 jobs saturate the pod).
try:
    ENCODE_JOB_MAX_CONCURRENT = max(
        1, int(os.environ.get("ENCODE_JOB_MAX_CONCURRENT", "4") or "4")
    )
except ValueError:
    ENCODE_JOB_MAX_CONCURRENT = 4
# After a job is playable it may yield slots to newer (not-yet-playable) jobs.
# Must stay >= 4 for quad-cam realtime: 1 slot serializes 4 ffmpeg and drops
# publish rate below ~30fps, so the playhead catches the encoder frontier.
try:
    ENCODE_PLAYABLE_MAX_CONCURRENT = max(
        1, int(os.environ.get("ENCODE_PLAYABLE_MAX_CONCURRENT", "4") or "4")
    )
except ValueError:
    ENCODE_PLAYABLE_MAX_CONCURRENT = 4
try:
    ENCODE_FOREGROUND_RESERVE = max(
        0, int(os.environ.get("ENCODE_FOREGROUND_RESERVE", "0") or "0")
    )
except ValueError:
    ENCODE_FOREGROUND_RESERVE = 0
# Publish interval after the first playable chunk. 30 = 1 GOP / 1s @ 30fps so
# the player sees a steady frontier instead of a 5s burst then a stall.
CHUNK_FRAMES = int(os.environ.get("CHUNK_FRAMES", "30"))
# Orin JPEG→H264 progressive chunk size (smaller → earlier first playable frame).
ORIN_CHUNK_FRAMES = max(1, int(os.environ.get("ORIN_CHUNK_FRAMES", "30")))
# Hold per-camera encode slots for the whole job so a playable job cannot be
# serialized down to 1 camera (which drops publish rate below ~30fps).
LIVE_ENCODE = os.environ.get("LIVE_ENCODE", "1").lower() in ("1", "true", "yes", "on")


class FairEncodeGate:
    """Pod-wide ffmpeg slot gate: fair per-job caps + throttle after playable.

    1) every job is hard-capped at ENCODE_JOB_MAX_CONCURRENT (and fair share).
    2) playable jobs are further limited to ENCODE_PLAYABLE_MAX_CONCURRENT
       (default 4 so quad-cam stays parallel / realtime) and cannot eat the
       foreground reserve.
    Waiters are granted in FIFO order among those that currently pass _grant_ok.
    """

    def __init__(
        self,
        capacity: int,
        *,
        job_max: int = 4,
        playable_max: int = 1,
        foreground_reserve: int = 0,
    ):
        import math
        from collections import deque

        self.capacity = max(1, int(capacity))
        self.job_max = max(1, int(job_max))
        self.playable_max = max(1, int(playable_max))
        self.foreground_reserve = max(0, int(foreground_reserve))
        self._math = math
        self._cv = threading.Condition()
        self._jobs: dict[str, dict] = {}
        self._waiters: deque = deque()  # (job_id, Event)

    def register(self, job_id: str) -> None:
        if not job_id:
            return
        with self._cv:
            self._jobs.setdefault(job_id, {"playable": False, "holding": 0})
            self._cv.notify_all()

    def unregister(self, job_id: str) -> None:
        if not job_id:
            return
        with self._cv:
            self._jobs.pop(job_id, None)
            self._cv.notify_all()

    def mark_playable(self, job_id: str) -> None:
        if not job_id:
            return
        with self._cv:
            st = self._jobs.get(job_id)
            if st is not None and not st["playable"]:
                st["playable"] = True
                log.info(
                    "encode-gate: job %s -> playable (cap=%d, reserve_fg=%s)",
                    job_id,
                    self.playable_max,
                    self._fg_reserve_locked(),
                )
            self._cv.notify_all()

    def _total_holding(self) -> int:
        return sum(int(j.get("holding") or 0) for j in self._jobs.values())

    def _fg_ids(self) -> list:
        return [jid for jid, j in self._jobs.items() if not j.get("playable")]

    def _fg_reserve_locked(self) -> int:
        if not self._fg_ids():
            return 0
        if self.foreground_reserve > 0:
            return min(self.capacity, self.foreground_reserve)
        # Auto: keep ~25% (at least 1) for not-yet-playable jobs.
        return max(1, self.capacity // 4)

    def _job_limit(self, job_id: str) -> int:
        n = max(1, len(self._jobs))
        fair = max(1, self._math.ceil(self.capacity / n))
        hard = min(self.capacity, self.job_max)
        st = self._jobs.get(job_id)
        if st is None:
            return min(hard, fair)
        if st.get("playable"):
            # Only throttle playable jobs when a not-yet-playable job needs slots.
            # Keep playable_max (default 4) so quad-cam stays parallel — 1 slot
            # serializes cameras and publish rate falls below realtime (~30fps).
            if self._fg_ids():
                return min(self.playable_max, hard, fair)
            return min(hard, fair)
        fg = self._fg_ids()
        # Foreground share among other foreground jobs (usually larger than bg).
        fg_fair = max(1, self._math.ceil(self.capacity / max(1, len(fg))))
        return min(hard, self.capacity, max(fair, fg_fair))

    def _grant_ok(self, job_id: str) -> bool:
        if self._total_holding() >= self.capacity:
            return False
        if job_id not in self._jobs:
            # Unregistered callers (scripts): only global capacity.
            return True
        st = self._jobs[job_id]
        if st["holding"] >= self._job_limit(job_id):
            return False
        if st.get("playable"):
            reserve = self._fg_reserve_locked()
            if reserve > 0:
                bg_hold = sum(
                    int(j["holding"])
                    for j in self._jobs.values()
                    if j.get("playable")
                )
                if bg_hold >= self.capacity - reserve:
                    return False
        return True

    def holding_count(self) -> int:
        """Currently granted ffmpeg slots (for sizing x264 threads)."""
        with self._cv:
            return self._total_holding()

    def acquire(self, job_id: str | None) -> None:
        jid = (job_id or "").strip() or "_anon_"
        ready = threading.Event()
        with self._cv:
            if jid not in self._jobs and jid != "_anon_":
                self._jobs[jid] = {"playable": False, "holding": 0}
            self._waiters.append((jid, ready))
            while True:
                # Grant to the earliest waiter that is currently allowed.
                grant_idx = None
                for i, (wj, _ev) in enumerate(self._waiters):
                    if self._grant_ok(wj):
                        grant_idx = i
                        break
                if grant_idx is not None and self._waiters[grant_idx][1] is ready:
                    self._waiters.remove((jid, ready))
                    if jid not in self._jobs:
                        self._jobs[jid] = {"playable": False, "holding": 0}
                    self._jobs[jid]["holding"] += 1
                    self._cv.notify_all()
                    return
                self._cv.wait(timeout=0.5)

    def release(self, job_id: str | None) -> None:
        jid = (job_id or "").strip() or "_anon_"
        with self._cv:
            st = self._jobs.get(jid)
            if st is not None and st["holding"] > 0:
                st["holding"] -= 1
            self._cv.notify_all()

    def slot(self, job_id: str | None = None):
        from contextlib import contextmanager

        @contextmanager
        def _cm():
            self.acquire(job_id)
            try:
                yield
            finally:
                self.release(job_id)

        return _cm()


encode_gate = FairEncodeGate(
    CPU_CONCURRENCY,
    job_max=ENCODE_JOB_MAX_CONCURRENT,
    playable_max=ENCODE_PLAYABLE_MAX_CONCURRENT,
    foreground_reserve=ENCODE_FOREGROUND_RESERVE,
)
# Back-compat alias for benches that monkeypatch cpu_encode_lock.
cpu_encode_lock = encode_gate

# -- Transcode resolution & quality (all overridable via env / CLI) --
# Resolution:
#   TRANSCODE_SIZE > 0     → square output (e.g. 720 → 720x720)
#   TRANSCODE_WIDTH/HEIGHT → rectangular output (either alone keeps other from source)
#   all 0                  → keep source camera resolution
# Quality (applied whenever ffmpeg re-encodes):
#   TRANSCODE_CRF / TRANSCODE_CQ, TRANSCODE_BITRATE, TRANSCODE_MAXRATE,
#   TRANSCODE_BUFSIZE, TRANSCODE_PRESET
# TRANSCODE_FORCE_REENCODE=1 → even if H.264 + no resize, still re-encode so quality
#   knobs take effect (default 0 = H.264 passthrough when size unchanged).

def _env_bool(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: str = "0") -> int:
    raw = os.environ.get(name, default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


# Orin JPEG→H.264 only. Orin H.264 uses the same TRANSCODE_* knobs as head-cam.
# ORIN_DIRECT=1 still skips bitrate crush on JPEG; scale applies when SIZE/W/H set.
ORIN_DIRECT = _env_bool("ORIN_DIRECT", "1")
ORIN_CRF = max(1, min(51, _env_int("ORIN_CRF", "23")))

# HD compress defaults: 1080 square + CRF28 + force re-encode (no H.264 passthrough).
# Set TRANSCODE_SIZE=0 to keep source resolution; TRANSCODE_BITRATE=0 → CRF+VBV (no ABR).
TRANSCODE_SIZE = max(0, _env_int("TRANSCODE_SIZE", "1080"))
TRANSCODE_WIDTH = max(0, _env_int("TRANSCODE_WIDTH", "0"))
TRANSCODE_HEIGHT = max(0, _env_int("TRANSCODE_HEIGHT", "0"))
# Prefer TRANSCODE_CRF; fall back to TRANSCODE_CQ for CLI/env parity with GPU version.
TRANSCODE_CRF = max(0, min(51, _env_int("TRANSCODE_CRF", os.environ.get("TRANSCODE_CQ", "28"))))
TRANSCODE_BITRATE = os.environ.get("TRANSCODE_BITRATE", "0")  # 0/empty = CRF mode
TRANSCODE_MAXRATE = os.environ.get("TRANSCODE_MAXRATE", "1.5M")
TRANSCODE_BUFSIZE = os.environ.get("TRANSCODE_BUFSIZE", "3M")
TRANSCODE_PRESET = os.environ.get("TRANSCODE_PRESET", "ultrafast")  # libx264 preset
TRANSCODE_FORCE_REENCODE = _env_bool("TRANSCODE_FORCE_REENCODE", "1")
# Cap threads per ffmpeg so N concurrent encodes do not oversubscribe the pod.
try:
    X264_THREADS = max(1, int(os.environ.get("X264_THREADS", "2") or "2"))
except ValueError:
    X264_THREADS = 2
# Soft budget for *all* in-flight x264 threads. Live 4-cam (holding=4) gets
# budget/4 each so a single 边转边播 job can use the whole pod instead of 2×4=8.
try:
    X264_THREAD_BUDGET = max(
        X264_THREADS,
        int(os.environ.get("X264_THREAD_BUDGET", "0") or "0"),
    )
except ValueError:
    X264_THREAD_BUDGET = 0
if X264_THREAD_BUDGET <= 0:
    X264_THREAD_BUDGET = max(CPU_CONCURRENCY, X264_THREADS * 4)

# -- Progressive publish (边转边播 path A) --
# When NO_UPLOAD + PROGRESSIVE_PUBLISH: write frames/manifest into LOCAL_OUTPUT_DIR
# as soon as each frame is built so HTTP can serve them before job is ready.
PROGRESSIVE_PUBLISH = _env_bool("PROGRESSIVE_PUBLISH", "1")
PARTIAL_MANIFEST_EVERY = max(1, _env_int("PARTIAL_MANIFEST_EVERY", "30"))
# Frontend may start once Redis/API reports at least this many frames available.
PLAYABLE_MIN_FRAMES = max(1, _env_int("PLAYABLE_MIN_FRAMES", "30"))

# -- Redis worker concurrency (overlap after first job is playable) --
MAX_INFLIGHT_JOBS = max(1, _env_int("MAX_INFLIGHT_JOBS", "3"))
OVERLAP_AFTER_PLAYABLE = _env_bool("OVERLAP_AFTER_PLAYABLE", "1")
# Progress log: emit when percent crosses this step (5 → 0/5/10/…/100).
# Set 0 to log on every progress_cb (can be noisy).
LOG_PROGRESS_EVERY_PCT = max(0.0, float(os.environ.get("LOG_PROGRESS_EVERY_PCT", "5") or "5"))


def _progress_pct(done: int, total: int) -> float:
    if total and int(total) > 0:
        return min(100.0, 100.0 * float(done) / float(total))
    return 0.0


class _JobProgressLogger:
    """Throttle per-job progress lines so logs stay readable under concurrency."""

    def __init__(self, label: str, step_pct: float | None = None):
        self.label = label
        self.step = LOG_PROGRESS_EVERY_PCT if step_pct is None else float(step_pct)
        self._last_bucket = -1
        self._t0 = time.time()
        self._logged_playable = False

    def update(self, done: int, total: int) -> None:
        done_i = int(done or 0)
        total_i = int(total or 0)
        pct = _progress_pct(done_i, total_i)
        at_start = done_i <= 0
        at_end = total_i > 0 and done_i >= total_i
        crossed_playable = (
            done_i >= PLAYABLE_MIN_FRAMES and not self._logged_playable
        )
        if crossed_playable:
            self._logged_playable = True

        if self.step <= 0:
            should_log = True
            bucket = int(pct)
        else:
            bucket = int(pct // self.step)
            should_log = (
                at_start
                or at_end
                or crossed_playable
                or bucket > self._last_bucket
            )

        if not should_log:
            return
        if self.step > 0 and bucket > self._last_bucket:
            self._last_bucket = bucket
        if at_end:
            self._last_bucket = max(self._last_bucket, int(100 // max(self.step, 1)))

        elapsed = time.time() - self._t0
        flags = []
        if crossed_playable and not at_end:
            flags.append("playable")
        if at_end:
            flags.append("done")
        suffix = f" [{' '.join(flags)}]" if flags else ""
        total_disp = total_i if total_i > 0 else "?"
        log.info(
            "[%s] progress %5.1f%% (%s/%s frames) elapsed=%.1fs%s",
            self.label,
            pct,
            done_i,
            total_disp,
            elapsed,
            suffix,
        )


# Per-thread overrides for bench jobs (safe with concurrent process_mcap).
_tls = threading.local()


def _tc_size() -> int:
    o = getattr(_tls, "override", None)
    return int(o["size"]) if o and "size" in o else TRANSCODE_SIZE


def _tc_width() -> int:
    o = getattr(_tls, "override", None)
    return int(o["width"]) if o and "width" in o else TRANSCODE_WIDTH


def _tc_height() -> int:
    o = getattr(_tls, "override", None)
    return int(o["height"]) if o and "height" in o else TRANSCODE_HEIGHT


def _tc_crf() -> int:
    o = getattr(_tls, "override", None)
    return int(o["crf"]) if o and "crf" in o else TRANSCODE_CRF


def _tc_force_reencode() -> bool:
    o = getattr(_tls, "override", None)
    return bool(o["force_reencode"]) if o and "force_reencode" in o else TRANSCODE_FORCE_REENCODE


def _resize_requested() -> bool:
    """True when env/CLI/thread override asks to change output resolution."""
    return _tc_size() > 0 or _tc_width() > 0 or _tc_height() > 0


def _transcode_size_desc() -> str:
    size = _tc_size()
    width = _tc_width()
    height = _tc_height()
    if size > 0:
        return f"{size}x{size}"
    if width > 0 or height > 0:
        w = width if width > 0 else "src"
        h = height if height > 0 else "src"
        return f"{w}x{h}"
    return "source"


@contextmanager
def temporary_transcode_settings(
    size: int | None = None,
    crf: int | None = None,
    force_reencode: int | bool | None = None,
):
    """Apply per-job transcode knobs for the current thread only."""
    prev = getattr(_tls, "override", None)
    cur = dict(prev) if prev else {}
    try:
        if size is not None:
            cur["size"] = max(0, int(size))
        if crf is not None:
            cur["crf"] = max(0, min(51, int(crf)))
        if force_reencode is not None:
            cur["force_reencode"] = bool(int(force_reencode))
        _tls.override = cur or None
        yield
    finally:
        _tls.override = prev


# -- Temp working directory --
TEMP_DIR = Path(os.environ.get("TEMP_DIR", tempfile.gettempdir())) / "sview_worker"
TEMP_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
#  OSS
# ---------------------------------------------------------------------------

_oss_bucket = None
_oss_sign_bucket = None

def get_oss_bucket():
    """SDK client for Pod↔OSS (may use VPC internal endpoint)."""
    global _oss_bucket
    if _oss_bucket is not None:
        return _oss_bucket
    import oss2
    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    _oss_bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)
    _oss_bucket.get_bucket_info()
    log.info("OSS connected: %s / %s", OSS_ENDPOINT, OSS_BUCKET)
    return _oss_bucket


def get_oss_sign_bucket():
    """SDK client for browser-facing signed URLs (must use public endpoint)."""
    global _oss_sign_bucket
    if _oss_sign_bucket is not None:
        return _oss_sign_bucket
    import oss2

    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    ep = (OSS_PUBLIC_ENDPOINT or "").strip()
    if not ep or "internal" in ep.lower():
        ep = "oss-cn-shanghai.aliyuncs.com"
    if "://" not in ep:
        ep = "https://" + ep
    _oss_sign_bucket = oss2.Bucket(auth, ep, OSS_BUCKET)
    log.info("OSS sign endpoint (browser): %s / %s", ep, OSS_BUCKET)
    return _oss_sign_bucket


def upload_to_oss(local_path: Path, oss_key: str):
    bucket = get_oss_bucket()
    bucket.put_object_from_file(oss_key, str(local_path))
    log.info("  -> uploaded  oss://%s/%s  (%.1f MB)", OSS_BUCKET, oss_key, local_path.stat().st_size / 1e6)


def oss_play_base(job_id: str) -> str:
    """HTTPS prefix browsers use to fetch manifest/segments from OSS cache."""
    base = OSS_PUBLIC_BASE
    if not base:
        pub = OSS_PUBLIC_ENDPOINT
        if not pub or "internal" in pub.lower():
            pub = "oss-cn-shanghai.aliyuncs.com"
        base = f"https://{OSS_BUCKET}.{pub}"
    if "-internal." in base:
        base = base.replace("-internal.aliyuncs.com", ".aliyuncs.com")
    return f"{base.rstrip('/')}/{OSS_CACHE_PREFIX}/{job_id}"


def oss_cache_key(job_id: str, *parts: str) -> str:
    rel = "/".join(p.strip("/") for p in parts if p)
    return f"{OSS_CACHE_PREFIX}/{job_id}/{rel}".replace("//", "/")


def upload_bytes_to_oss(oss_key: str, data: bytes, *, content_type: str = "application/octet-stream", job_id: str | None = None):
    import oss2

    global _oss_public_acl_blocked
    bucket = get_oss_bucket()
    headers = {"Content-Type": content_type}
    want_public = (
        OSS_CACHE
        and OSS_CACHE_PUBLIC
        and not _oss_public_acl_blocked
        and oss_key.startswith(OSS_CACHE_PREFIX + "/")
    )
    if want_public:
        headers["x-oss-object-acl"] = oss2.OBJECT_ACL_PUBLIC_READ
    try:
        bucket.put_object(oss_key, data, headers=headers)
    except Exception as e:
        if want_public and "public" in str(e).lower():
            _oss_public_acl_blocked = True
            headers.pop("x-oss-object-acl", None)
            bucket.put_object(oss_key, data, headers=headers)
            log.warning(
                "OSS public-read ACL blocked by bucket; further player-bin uploads stay private "
                "(Master local / signed redirect). first_key=%s",
                oss_key,
            )
        else:
            raise
    log.info(
        "  -> oss-cache  oss://%s/%s  (%.1f KB)",
        OSS_BUCKET,
        oss_key,
        len(data) / 1024.0,
    )
    if job_id and OSS_CACHE:
        try:
            from job_status import register_cache_bytes

            register_cache_bytes(get_redis(), job_id, len(data))
        except Exception:
            log.exception("[%s] failed to register cache_bytes", job_id)


def sign_oss_url(oss_key: str, expires: int = 7200) -> str:
    """Presigned HTTPS GET for browsers — always public endpoint, never *-internal*.

    Important: must sign with the public host. Rewriting host after signing can
    invalidate the signature depending on OSS auth version.
    """
    bucket = get_oss_sign_bucket()
    url = bucket.sign_url("GET", oss_key, expires, slash_safe=True)
    if "-internal." in url:
        raise RuntimeError(
            f"refusing browser signed URL on internal endpoint: {url.split('?', 1)[0]}"
        )
    return url


def _pack_segment_bytes(start_index: int, frame_blobs: list[bytes], segment_frames: int) -> bytes:
    import struct

    header = {
        "packet_schema": "playertrans.bundle_segment.v1",
        "start_index": int(start_index),
        "count": len(frame_blobs),
        "segment_frames": int(segment_frames),
    }
    header_bytes = json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
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


def _oss_cache_upload_segment(job_id: str, start: int, end: int, frame_dir: Path) -> None:
    """Upload frames [start, end) as one segment object (end exclusive)."""
    if end <= start:
        return
    blobs: list[bytes] = []
    for i in range(start, end):
        path = frame_dir / f"{i:06d}.bin"
        if not path.is_file():
            log.warning("[%s] oss-cache skip segment %d: missing %s", job_id, start, path.name)
            return
        blobs.append(path.read_bytes())
    data = _pack_segment_bytes(start, blobs, SEGMENT_FRAMES)
    key = oss_cache_key(job_id, "segments", f"{start:06d}.bin")
    upload_bytes_to_oss(key, data, job_id=job_id)


def _oss_cache_upload_manifest(job_id: str, manifest: dict) -> None:
    body = dict(manifest)
    body["play_base"] = oss_play_base(job_id)
    payload = json.dumps(body, indent=2, ensure_ascii=False).encode("utf-8")
    key = oss_cache_key(job_id, "manifest.json")
    upload_bytes_to_oss(key, payload, content_type="application/json; charset=utf-8", job_id=job_id)


def _oss_cache_maybe_flush(
    job_id: str,
    frames_done: int,
    min_frames: int,
    frame_dir: Path,
    manifest: dict | None,
) -> None:
    """After local progressive write, mirror completed segments (+ manifest) to OSS."""
    if not OSS_CACHE:
        return
    if frames_done <= 0:
        return
    try:
        get_oss_bucket()
    except Exception:
        log.exception("[%s] oss-cache connect failed", job_id)
        return

    start = ((frames_done - 1) // SEGMENT_FRAMES) * SEGMENT_FRAMES
    end = frames_done
    try:
        _oss_cache_upload_segment(job_id, start, end, frame_dir)
    except Exception:
        log.exception("[%s] oss-cache segment upload failed [%d:%d)", job_id, start, end)

    if manifest is not None:
        try:
            _oss_cache_upload_manifest(job_id, manifest)
        except Exception:
            log.exception("[%s] oss-cache manifest upload failed", job_id)


# ---------------------------------------------------------------------------
#  Redis
# ---------------------------------------------------------------------------

_redis_client = None

def get_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    _redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
    )
    _redis_client.ping()
    log.info("Redis connected: %s:%s db=%s user=%s",
            REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_USERNAME or "-")
    return _redis_client


# ---------------------------------------------------------------------------
#  Path mapping  (OSS URI / legacy queue path -> local mount; see path_map.py)
# ---------------------------------------------------------------------------

def local_path_to_oss_base(local_path: Path) -> str:
    """Derive output key prefix from local source path (for optional OSS upload)."""
    lp = str(local_path).replace("\\", "/")
    rel = None
    for mount in (HEAD_CAM_MOUNT, ORIN_MOUNT, LOCAL_MOUNT_PATH):
        mount_s = str(mount).replace("\\", "/").rstrip("/") + "/"
        if lp.startswith(mount_s):
            rel = lp[len(mount_s):]
            break
    if rel is None:
        rel = local_path.stem
    if rel.endswith(".mcap"):
        rel = rel[:-5]
    # .../{job_id}/sview  — frames + manifest live under this prefix
    return f"{OSS_OUTPUT_PREFIX}/{OSS_OUTPUT_SUB}/{rel}/sview"


# ---------------------------------------------------------------------------
#  CPU transcoding (libx264)
# ---------------------------------------------------------------------------

def _rate_control_empty(value: str | None) -> bool:
    v = (value or "").strip().lower()
    return (not v) or v in ("0", "none", "crf", "-")


def _x264_threads_now() -> int:
    """Fair-share x264 threads across granted ffmpeg slots.

    Floor is X264_THREADS. A lone 4-cam live job (holding≈4) gets about
    X264_THREAD_BUDGET/4, so 边转边播 can use idle cores the gate reserved
    for overlap jobs that are not running yet.
    """
    floor = max(1, int(X264_THREADS))
    try:
        holding = int(encode_gate.holding_count() or 0)
    except Exception:
        holding = 0
    holding = max(1, holding)
    share = max(floor, int(X264_THREAD_BUDGET) // holding)
    return min(8, share)


def _ffmpeg_fast_input_flags() -> list[str]:
    """Skip demux probing; format is already forced with -f hevc/h264/mjpeg.

    Do not put ``-flags low_delay`` here: before ``-i`` it applies to the
    *decoder*. HEVC/H.264 with B-frames would then emit decode order, and
    the re-encoded AUs would play out of presentation order.
    """
    return [
        "-fflags", "nobuffer",
        "-probesize", "32",
        "-analyzeduration", "0",
    ]


def _ffmpeg_annexb_input_flags() -> list[str]:
    """Probe enough of a raw Annex-B pipe to see SPS/PPS.

    ``_ffmpeg_fast_input_flags`` uses probesize=32. An H.264 SPS NAL is often
    already >32 bytes, so ffmpeg starts the decoder before PPS and fails with
    ``non-existing PPS 0 referenced``. The whole chunk is already in stdin.
    """
    return [
        "-fflags", "+genpts+discardcorrupt",
        "-probesize", "1048576",
        "-analyzeduration", "2000000",
    ]


def _x264_encode_flags(*, threads: int | None = None) -> list[str]:
    """libx264 flags for live progressive encode (single-slice, no B-frames)."""
    n = int(threads) if threads is not None else _x264_threads_now()
    n = max(1, n)
    # sliced_threads=0 is required so each picture is one VCL NAL (AU split).
    params = (
        f"threads={n}:sliced_threads=0:rc-lookahead=0:sync-lookahead=0:"
        "aq-mode=0:mbtree=0:weightp=0:8x8dct=0"
    )
    return [
        "-c:v", "libx264",
        "-threads", str(n),
        "-flags", "+low_delay",
        "-x264-params", params,
        "-profile:v", "baseline",
        "-preset", TRANSCODE_PRESET,
        "-tune", "zerolatency",
        "-bf", "0",
        "-pix_fmt", "yuv420p",
    ]


def _ffmpeg_x264_rate_args(*, crf: int | None = None) -> list[str]:
    """Build libx264 rate-control args.

    Default HD path: CRF + optional VBV (-maxrate/-bufsize). Avoid -b:v unless an
    explicit ABR bitrate is configured — ABR largely overrides CRF and balloons size.
    """
    use_crf = _tc_crf() if crf is None else max(0, min(51, int(crf)))
    args = ["-crf", str(use_crf)]
    if not _rate_control_empty(TRANSCODE_BITRATE):
        args += ["-b:v", TRANSCODE_BITRATE]
        if not _rate_control_empty(TRANSCODE_MAXRATE):
            args += ["-maxrate", TRANSCODE_MAXRATE]
        if not _rate_control_empty(TRANSCODE_BUFSIZE):
            args += ["-bufsize", TRANSCODE_BUFSIZE]
        return args
    if not _rate_control_empty(TRANSCODE_MAXRATE):
        args += ["-maxrate", TRANSCODE_MAXRATE]
        buf = TRANSCODE_BUFSIZE if not _rate_control_empty(TRANSCODE_BUFSIZE) else TRANSCODE_MAXRATE
        args += ["-bufsize", buf]
    return args


def cpu_transcode_chunk(raw_data: bytes, output_path: Path,
                        scale_w: int = None, scale_h: int = None,
                        codec: str = "hevc", job_id: str | None = None,
                        acquire_gate: bool = True) -> float:
    """Transcode raw video -> H.264 using libx264 (CPU), resize to (scale_w, scale_h).

    codec: source codec ('hevc'/'h265' or 'h264'/'avc'); selects ffmpeg -f input.
    Quality knobs: TRANSCODE_CRF / BITRATE / MAXRATE / BUFSIZE / PRESET.
    Output: baseline, GOP 30, dump_extra on keyframes (AU split needs sliced_threads=0).
    """
    if not scale_w:
        scale_w = _tc_size() if _tc_size() > 0 else (_tc_width() or 2096)
    if not scale_h:
        scale_h = _tc_size() if _tc_size() > 0 else (_tc_height() or 2096)
    t0 = time.time()
    # fast_bilinear: much cheaper than default bicubic for live progressive encode.
    scale_filter = f"scale={scale_w}:{scale_h}:flags=fast_bilinear"
    in_format = "hevc" if codec.lower() in ("hevc", "h265", "h.265") else "h264"
    jid = job_id if job_id is not None else getattr(_tls, "encode_job_id", None)

    def _run():
        n_threads = _x264_threads_now()
        cmd = [
            FFMPEG, "-y", "-hide_banner", "-loglevel", "error",
            *_ffmpeg_annexb_input_flags(),
            # Decoder threads (HEVC decode is often the live-path bottleneck).
            "-threads", str(n_threads),
            "-f", in_format, "-r", "30", "-i", "pipe:0",
            "-vf", scale_filter,
            *_x264_encode_flags(threads=n_threads),
            *_ffmpeg_x264_rate_args(),
            "-g", "30",
            "-keyint_min", "30",
            "-sc_threshold", "0",
            "-bsf:v", "dump_extra=freq=keyframe",
            "-f", "h264",
            str(output_path),
        ]
        subprocess.run(
            cmd,
            input=raw_data,
            capture_output=True,
            timeout=600,
            check=True,
        )

    try:
        if acquire_gate:
            lock = cpu_encode_lock
            if hasattr(lock, "slot"):
                with lock.slot(jid):
                    _run()
            else:
                with lock:
                    _run()
        else:
            _run()
    except subprocess.CalledProcessError as e:
        log.error(
            "FFmpeg failed (exit %d):\nSTDERR:\n%s\nSTDOUT:\n%s",
            e.returncode,
            e.stderr.decode(errors="replace")[-3000:] if e.stderr else "(none)",
            e.stdout.decode(errors="replace")[-2000:] if e.stdout else "(none)",
        )
        raise
    return time.time() - t0


def _transcode_chunk_worker(ch_name, chunk_idx, frames_slice, work_dir,
                            scale_w=None, scale_h=None, codec="hevc",
                            job_id: str | None = None):
    if not scale_w:
        scale_w = _tc_size() if _tc_size() > 0 else (_tc_width() or 2096)
    if not scale_h:
        scale_h = _tc_size() if _tc_size() > 0 else (_tc_height() or 2096)
    chunk_path = work_dir / f"{ch_name}_chunk_{chunk_idx:04d}.bin"
    raw_data = b"".join(frames_slice)
    log.debug(
        "[%s chunk %d] %s raw: %d frames, %.1f KB input",
        ch_name,
        chunk_idx,
        codec,
        len(frames_slice),
        len(raw_data) / 1024,
    )
    if not raw_data:
        log.warning("[%s chunk %d] Empty %s data, skipping transcode", ch_name, chunk_idx, codec)
        return ch_name, chunk_idx, str(chunk_path), 0.0
    # Fast path: H.264 + no resize + not forcing quality re-encode → copy bytes
    if (
        codec.lower() in ("h264", "avc")
        and not _resize_requested()
        and not _tc_force_reencode()
    ):
        t0 = time.time()
        with open(chunk_path, "wb") as f:
            f.write(raw_data)
        elapsed = time.time() - t0
        log.debug("[%s chunk %d] h264 copy: %d frames, %.1f KB -> %d bytes",
                    ch_name, chunk_idx, len(frames_slice), len(raw_data) / 1024,
                    chunk_path.stat().st_size)
        return ch_name, chunk_idx, str(chunk_path), elapsed
    jid = job_id if job_id is not None else getattr(_tls, "encode_job_id", None)
    elapsed = cpu_transcode_chunk(
        raw_data, chunk_path, scale_w, scale_h, codec, job_id=jid
    )
    log.debug("[%s chunk %d] Output: %d bytes -> %s",
                ch_name, chunk_idx, chunk_path.stat().st_size if chunk_path.exists() else -1, chunk_path.name)
    return ch_name, chunk_idx, str(chunk_path), elapsed


def _concat_h264_chunks(chunk_paths: list, output_path: Path):
    if len(chunk_paths) == 1:
        Path(chunk_paths[0]).rename(output_path)
        return
    with open(output_path, "wb") as out:
        for p in chunk_paths:
            with open(p, "rb") as inf:
                out.write(inf.read())


def _encode_rate_suffix(n_frames: int, elapsed: float, fps: float = 30.0) -> str:
    """Log helper: ` (41.7 fps, 1.39x realtime)`."""
    if elapsed <= 0 or n_frames <= 0:
        return ""
    out_fps = n_frames / elapsed
    rt = out_fps / max(1.0, float(fps) or 30.0)
    return f" ({out_fps:.1f} fps, {rt:.2f}x realtime)"


def _iter_annexb_nals(data: bytes):
    """Yield Annex-B NAL units including their start codes."""
    if not data:
        return
    starts = []
    pos = 0
    dlen = len(data)
    while pos < dlen - 3:
        pos4 = data.find(b"\x00\x00\x00\x01", pos)
        pos3 = data.find(b"\x00\x00\x01", pos)
        if pos4 < 0 and pos3 < 0:
            break
        if pos4 < 0:
            np, sc = pos3, 3
        elif pos3 < 0:
            np, sc = pos4, 4
        else:
            np, sc = (pos4, 4) if pos4 <= pos3 else (pos3, 3)
        if np + sc >= dlen:
            break
        starts.append(np)
        pos = np + sc + 1
    if not starts:
        return
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else dlen
        yield data[start:end]


def _annexb_nal_header_byte(nal: bytes) -> int | None:
    if len(nal) >= 5 and nal.startswith(b"\x00\x00\x00\x01"):
        return nal[4]
    if len(nal) >= 4 and nal.startswith(b"\x00\x00\x01"):
        return nal[3]
    return None


def _collect_annexb_param_sets(data: bytes, *, hevc: bool) -> dict:
    """Map NAL type → latest parameter-set NAL (with start code)."""
    want = (32, 33, 34) if hevc else (7, 8)
    found = {}
    for nal in _iter_annexb_nals(data):
        hdr = _annexb_nal_header_byte(nal)
        if hdr is None:
            continue
        ntype = ((hdr >> 1) & 0x3F) if hevc else (hdr & 0x1F)
        if ntype in want:
            found[ntype] = nal
    return found


def _param_sets_bytes(ps: dict, *, hevc: bool) -> bytes:
    order = (32, 33, 34) if hevc else (7, 8)
    return b"".join(ps[t] for t in order if t in ps)


def _has_required_param_sets(ps: dict, *, hevc: bool) -> bool:
    if hevc:
        return 33 in ps and 34 in ps  # SPS + PPS (VPS optional but usual)
    return 7 in ps and 8 in ps


def _annexb_has_leading_ps(data: bytes, *, hevc: bool) -> bool:
    """True if SPS+PPS appear before the first VCL NAL."""
    skip = {35, 39, 6} if hevc else {6, 9}
    vcl = {0, 1, 2, 3, 4, 5, 16, 17, 18, 19, 20, 21} if hevc else {1, 5}
    seen: list[int] = []
    for nal in _iter_annexb_nals(data):
        hdr = _annexb_nal_header_byte(nal)
        if hdr is None:
            continue
        ntype = ((hdr >> 1) & 0x3F) if hevc else (hdr & 0x1F)
        if ntype in skip:
            continue
        seen.append(ntype)
        if ntype in vcl:
            break
        if len(seen) >= 8:
            break
    if hevc:
        return 33 in seen and 34 in seen
    return 7 in seen and 8 in seen


class LiveX264Encoder:
    """Holds one encode-gate slot for the whole job; each chunk is a fresh ffmpeg.

    After playable, per-chunk acquire used to serialize 4 cameras below realtime.
    Each chunk is prefixed with cached VPS/SPS/PPS so HEVC P-frame chunks still decode.
    """

    def __init__(
        self,
        *,
        in_args: list,
        scale_w,
        scale_h,
        job_id: str | None,
        rate_args: list,
        gop: int = 30,
        codec: str = "hevc",
        kind: str = "annexb",
    ):
        self._job_id = (job_id or "").strip() or "_anon_"
        self._in_args = list(in_args)
        self._scale_w = scale_w
        self._scale_h = scale_h
        self._rate_args = list(rate_args or [])
        self._gop = max(1, int(gop))
        self._kind = kind
        self._codec = (codec or "hevc").lower()
        self._hevc = self._codec in ("hevc", "h265", "h.265")
        self._ps: dict = {}
        self._sniffed = False
        self._closed = False
        self._acquired = False
        encode_gate.acquire(self._job_id)
        self._acquired = True

    def seed_param_sets(self, ps: dict | bytes | None) -> None:
        """Prime SPS/PPS (and HEVC VPS) from a later keyframe / full-bag scan."""
        if not ps:
            return
        if isinstance(ps, (bytes, bytearray)):
            found = _collect_annexb_param_sets(bytes(ps), hevc=self._hevc)
            if found:
                self._ps.update(found)
            return
        self._ps.update(ps)

    @classmethod
    def annexb(cls, *, codec: str, scale_w, scale_h, job_id: str | None, fps: float = 30.0):
        in_fmt = "hevc" if (codec or "").lower() in ("hevc", "h265", "h.265") else "h264"
        fps_i = int(fps) if fps and fps > 0 else 30
        in_args = ["-f", in_fmt, "-r", str(fps_i), "-i", "pipe:0"]
        return cls(
            in_args=in_args,
            scale_w=scale_w,
            scale_h=scale_h,
            job_id=job_id,
            rate_args=_ffmpeg_x264_rate_args(),
            gop=30,
            codec=in_fmt,
            kind="annexb",
        )

    @classmethod
    def jpeg(
        cls,
        *,
        scale_w,
        scale_h,
        job_id: str | None,
        fps: float = 30.0,
        crf: int = 23,
        direct: bool = True,
    ):
        fps_f = float(fps) if fps and fps > 0 else 30.0
        in_args = [
            "-f", "image2pipe",
            "-framerate", str(fps_f),
            "-vcodec", "mjpeg",
            "-i", "pipe:0",
        ]
        use_crf = max(1, min(51, int(crf))) if direct else max(0, min(51, int(crf)))
        if direct:
            rate_args = ["-crf", str(use_crf)]
        else:
            rate_args = _ffmpeg_x264_rate_args(crf=use_crf)
        return cls(
            in_args=in_args,
            scale_w=scale_w,
            scale_h=scale_h,
            job_id=job_id,
            rate_args=rate_args,
            gop=30,
            codec="mjpeg",
            kind="jpeg",
        )

    def _release(self) -> None:
        if not self._acquired:
            return
        try:
            encode_gate.release(self._job_id)
        finally:
            self._acquired = False

    def _sniff_codec(self, raw: bytes) -> None:
        if self._sniffed:
            return
        self._sniffed = True
        # Explicit H.264 must not be reclassified: NAL header 0x41 is a
        # non-IDR slice, but (0x41 >> 1) & 0x3F == 32 looks like HEVC VPS.
        if not self._hevc and self._codec in ("h264", "avc"):
            return
        for nal in _iter_annexb_nals(raw):
            hdr = _annexb_nal_header_byte(nal)
            if hdr is None:
                continue
            h264_t = hdr & 0x1F
            if h264_t in (5, 7, 8):
                self._hevc = False
                self._codec = "h264"
                return
            hevc_t = (hdr >> 1) & 0x3F
            if hevc_t in (32, 33, 34) and hdr not in (0x41, 0x21):
                self._hevc = True
                self._codec = "hevc"
                return
            if h264_t in (1, 9):
                self._hevc = False
                self._codec = "h264"
                return
            if h264_t in (1, 6, 9) or hevc_t in (1, 6, 9, 35, 39):
                continue
            continue

    def seed_param_sets(self, ps: dict) -> None:
        if ps:
            self._ps.update(ps)

    def _with_param_sets(self, raw: bytes) -> bytes:
        found = _collect_annexb_param_sets(raw, hevc=self._hevc)
        if found:
            self._ps.update(found)
        if _annexb_has_leading_ps(raw, hevc=self._hevc):
            return raw
        prefix = _param_sets_bytes(self._ps, hevc=self._hevc)
        if not prefix:
            return raw
        return prefix + raw

    def _jpeg_cmd(self, n_frames: int, output_path: Path) -> list:
        gop = min(self._gop, max(1, int(n_frames)))
        n_threads = _x264_threads_now()
        cmd = [
            FFMPEG, "-y", "-hide_banner", "-loglevel", "error",
            *_ffmpeg_fast_input_flags(),
            "-threads", str(n_threads),
            *self._in_args,
        ]
        if self._scale_w and self._scale_h and int(self._scale_w) > 0 and int(self._scale_h) > 0:
            cmd += ["-vf", f"scale={int(self._scale_w)}:{int(self._scale_h)}:flags=fast_bilinear"]
        cmd += [
            "-an",
            *_x264_encode_flags(threads=n_threads),
            *self._rate_args,
            "-g", str(gop),
            "-keyint_min", str(gop),
            "-sc_threshold", "0",
            "-bsf:v", "dump_extra=freq=keyframe",
            "-f", "h264",
            str(output_path),
        ]
        return cmd

    def encode_frames(self, frames: list, timeout: float | None = None) -> list:
        n = len(frames)
        if n <= 0:
            return []
        if self._closed:
            raise RuntimeError("live encoder already closed")
        raw = b"".join(frames)
        if not raw:
            return [b""] * n
        wait_s = timeout if timeout is not None else max(60.0, 2.0 * n)
        fd, out_s = tempfile.mkstemp(prefix="sview_live_", suffix=".h264")
        os.close(fd)
        out_p = Path(out_s)
        try:
            if self._kind == "jpeg":
                try:
                    subprocess.run(
                        self._jpeg_cmd(n, out_p),
                        input=raw,
                        capture_output=True,
                        timeout=wait_s,
                        check=True,
                    )
                except subprocess.CalledProcessError as e:
                    err = (e.stderr or b"").decode(errors="replace")[-2000:]
                    raise RuntimeError(
                        f"ffmpeg live-chunk failed (exit {e.returncode}): {err}"
                    ) from e
                except subprocess.TimeoutExpired as e:
                    raise TimeoutError(
                        f"ffmpeg live-chunk timeout after {wait_s:.0f}s ({n} frames)"
                    ) from e
            else:
                self._sniff_codec(raw)
                raw = self._with_param_sets(raw)
                try:
                    cpu_transcode_chunk(
                        raw,
                        out_p,
                        int(self._scale_w) if self._scale_w else None,
                        int(self._scale_h) if self._scale_h else None,
                        "hevc" if self._hevc else "h264",
                        job_id=self._job_id,
                        acquire_gate=False,
                    )
                except subprocess.CalledProcessError as e:
                    err = (e.stderr or b"").decode(errors="replace")[-2000:]
                    raise RuntimeError(
                        f"ffmpeg live-chunk failed (exit {e.returncode}): {err}"
                    ) from e
            blob = out_p.read_bytes() if out_p.is_file() else b""
            return _split_h264_access_units(blob, None)
        finally:
            try:
                out_p.unlink()
            except OSError:
                pass

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._release()


def _close_live_encoders(encoders: dict | None) -> None:
    if not encoders:
        return
    for ch, enc in list(encoders.items()):
        try:
            enc.close()
        except Exception:
            log.exception("live encoder close failed (%s)", ch)
    encoders.clear()



def _start_live_annexb_encoders(channels, camera_metas: dict, job_id: str, scale_w, scale_h) -> dict:
    ovr = getattr(_tls, "override", None)
    encoders: dict = {}

    def _one(ch: str):
        codec = (camera_metas.get(ch, {}) or {}).get("codec") or "hevc"
        fps = float((camera_metas.get(ch, {}) or {}).get("fps") or 30)
        return ch, LiveX264Encoder.annexb(
            codec=str(codec),
            scale_w=scale_w,
            scale_h=scale_h,
            job_id=job_id,
            fps=fps,
        )

    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        with ThreadPoolExecutor(max_workers=max(1, len(channels))) as pool:
            futs = [pool.submit(_tls_call, ovr, _one, ch) for ch in channels]
            for fut in as_completed(futs):
                ch, enc = fut.result()
                encoders[ch] = enc
        return encoders
    except Exception:
        _close_live_encoders(encoders)
        raise


def _start_live_jpeg_encoders(
    channels,
    camera_metas: dict,
    job_id: str,
    scale_w,
    scale_h,
    *,
    crf: int,
    direct: bool,
) -> dict:
    ovr = getattr(_tls, "override", None)
    encoders: dict = {}

    def _one(ch: str):
        fps = float((camera_metas.get(ch, {}) or {}).get("fps") or 30)
        return ch, LiveX264Encoder.jpeg(
            scale_w=scale_w,
            scale_h=scale_h,
            job_id=job_id,
            fps=fps,
            crf=crf,
            direct=direct,
        )

    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        with ThreadPoolExecutor(max_workers=max(1, len(channels))) as pool:
            futs = [pool.submit(_tls_call, ovr, _one, ch) for ch in channels]
            for fut in as_completed(futs):
                ch, enc = fut.result()
                encoders[ch] = enc
        return encoders
    except Exception:
        _close_live_encoders(encoders)
        raise


def _is_h264_passthrough(camera_metas: dict, channels) -> bool:
    """True when we can emit camera Annex-B AUs without ffmpeg re-encode."""
    if _resize_requested() or _tc_force_reencode():
        return False
    chs = list(channels)
    if not chs or not camera_metas:
        return False
    for ch in chs:
        codec = (camera_metas.get(ch, {}).get("codec") or "").lower()
        if codec not in ("h264", "avc"):
            return False
    return True


def _headcam_chunk_len(chunk_idx: int) -> int:
    """First chunk is small so large files become playable sooner."""
    if chunk_idx <= 0:
        return PLAYABLE_MIN_FRAMES
    return max(1, CHUNK_FRAMES)


# SSIEGO / SView always expose these four logical cameras.
HEADCAM_CHANNELS = ("RR", "FR", "RL", "FL")
HEADCAM_CHANNEL_SET = frozenset(HEADCAM_CHANNELS)


def _tls_call(override, fn, *args, **kwargs):
    """Run fn on a worker thread with the caller's transcode overrides."""
    prev = getattr(_tls, "override", None)
    _tls.override = override
    try:
        return fn(*args, **kwargs)
    finally:
        _tls.override = prev


# ---------------------------------------------------------------------------
#  Processing
# ---------------------------------------------------------------------------


def _write_bundle_range(
    f_start: int,
    f_end: int,
    *,
    channel_aus: dict,
    video_timestamps: dict,
    imu_sorted: list,
    imu_idx: int,
    output_frame_dir: Path,
    manifest_path: Path,
    job_id: str,
    camera_metas: dict,
    min_frames: int,
    key_frame_indices: list,
    progressive: bool,
    progress_cb=None,
) -> int:
    """Write frame bins for [f_start, f_end). Returns updated IMU scan index."""
    for fidx in range(f_start, f_end):
        streams_data = {}
        for ch in sorted(channel_aus.keys()):
            aus = channel_aus[ch]
            streams_data[ch] = aus[fidx] if fidx < len(aus) else b""

        is_keyframe = True
        for _ch, data in streams_data.items():
            if not data:
                continue
            if not _h264_payload_has_idr(data):
                is_keyframe = False
                break
        if is_keyframe:
            key_frame_indices.append(fidx)

        all_ts = []
        all_next_ts = []
        for ch in sorted(channel_aus.keys()):
            tl = video_timestamps.get(ch, [])
            if fidx < len(tl):
                all_ts.append(tl[fidx])
            if fidx + 1 < len(tl):
                all_next_ts.append(tl[fidx + 1])
        ts_ns = min(all_ts) if all_ts else 0
        tw_start = ts_ns
        if all_next_ts:
            tw_end = min(all_next_ts)
        else:
            tw_end = tw_start + int(1e9 / 30)

        imu_samples = []
        while imu_idx < len(imu_sorted) and imu_sorted[imu_idx]["log_time_ns"] < tw_start:
            imu_idx += 1
        j = imu_idx
        while j < len(imu_sorted) and imu_sorted[j]["log_time_ns"] < tw_end:
            r = imu_sorted[j]
            entry = {"sample_index": r["sample_index"]}
            if r.get("sensor_type"):
                entry["sensor_type"] = int(r["sensor_type"])
            entry.update({
                "monotonic_raw_ns": r["monotonic_ns"],
                "wall_ns": r["wall_ns"],
                "x": r["x"], "y": r["y"], "z": r["z"],
                "temp_c": r["temp_c"],
                "unit": r["unit"],
                "scale": r["scale"],
                "log_time_ns": r["log_time_ns"],
            })
            imu_samples.append(entry)
            j += 1

        bundle = _build_bundle_frame(
            frame_idx=fidx,
            streams_data=streams_data,
            timestamp_ns=ts_ns,
            time_window_ns=[tw_start, tw_end],
            is_keyframe=is_keyframe,
            imu_json={"samples": imu_samples},
            events_json={"records": []},
        )
        _write_bytes_atomic(output_frame_dir / f"{fidx:06d}.bin", bundle)

        frames_done = fidx + 1
        published_manifest = None
        if progressive and (
            frames_done == 1
            or frames_done % PARTIAL_MANIFEST_EVERY == 0
            or frames_done == min_frames
        ):
            tl_start, tl_end = _estimate_timeline(
                video_timestamps, channel_aus, min_frames,
            )
            published_manifest = _build_manifest_dict(
                job_id=job_id,
                camera_metas=camera_metas,
                frame_count=min_frames,
                frames_available=frames_done,
                key_frame_indices=key_frame_indices,
                timeline_start_ns=tl_start,
                timeline_end_ns=tl_end,
                status="partial" if frames_done < min_frames else "done",
            )
            _publish_manifest(manifest_path, published_manifest)

        if progressive and OSS_CACHE and (
            frames_done % SEGMENT_FRAMES == 0 or frames_done == min_frames
        ):
            # Prefer freshly built manifest; otherwise rebuild a light one for OSS.
            man = published_manifest
            if man is None and (frames_done % PARTIAL_MANIFEST_EVERY == 0 or frames_done == min_frames):
                tl_start, tl_end = _estimate_timeline(
                    video_timestamps, channel_aus, min_frames,
                )
                man = _build_manifest_dict(
                    job_id=job_id,
                    camera_metas=camera_metas,
                    frame_count=min_frames,
                    frames_available=frames_done,
                    key_frame_indices=key_frame_indices,
                    timeline_start_ns=tl_start,
                    timeline_end_ns=tl_end,
                    status="partial" if frames_done < min_frames else "done",
                )
            elif man is None and manifest_path.is_file():
                try:
                    man = json.loads(manifest_path.read_text(encoding="utf-8"))
                    man["bundle"]["frames_available"] = frames_done
                except Exception:
                    man = None
            _oss_cache_maybe_flush(
                job_id, frames_done, min_frames, output_frame_dir, man,
            )

        # Must fire exactly at PLAYABLE_MIN_FRAMES so the Redis scheduler can
        # overlap the next job. Throttling every 10 frames alone can skip it
        # (e.g. first chunk ends at frame 30 when min_frames is the full length).
        if progress_cb and (
            fidx == f_start
            or fidx % 10 == 0
            or frames_done == min_frames
            or frames_done == PLAYABLE_MIN_FRAMES
            or frames_done == f_end
        ):
            try:
                progress_cb(frames_done, min_frames)
            except Exception:
                log.exception("progress_cb failed at frame %s", fidx)

    return imu_idx


def _trim_pad_aus(aus: list, expect: int) -> list:
    if len(aus) > expect:
        return aus[:expect]
    while len(aus) < expect:
        aus.append(b"")
    return aus


def _publish_headcam_chunk(
    *,
    fname: str,
    channels: list,
    start: int,
    end: int,
    cidx: int,
    video_frames: dict,
    video_timestamps: dict,
    camera_metas: dict,
    channel_aus: dict,
    work_dir: Path,
    output_frame_dir: Path,
    manifest_path: Path,
    job_id: str,
    min_frames: int,
    key_frame_indices: list,
    progressive: bool,
    passthrough: bool,
    imu_sorted: list,
    imu_idx: int,
    progress_cb=None,
    live_encoders: dict | None = None,
) -> int:
    """Encode/passthrough one aligned chunk and write frame bins immediately."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    expect = end - start
    if expect <= 0:
        return imu_idx
    t_chunk = time.time()
    sw = _scale_dim(camera_metas, "w")
    sh = _scale_dim(camera_metas, "h")
    live = (
        (not passthrough)
        and live_encoders
        and all(ch in live_encoders for ch in channels)
    )

    if passthrough:
        for ch in channels:
            if ch not in channel_aus:
                channel_aus[ch] = []
            aus = [bytes(x) for x in video_frames[ch][start:end]]
            aus = _trim_pad_aus(aus, expect)
            while len(channel_aus[ch]) < start:
                channel_aus[ch].append(b"")
            channel_aus[ch].extend(aus)
    elif live:
        def _live_one(ch: str):
            sl = video_frames[ch][start:end]
            t0 = time.time()
            aus = live_encoders[ch].encode_frames(sl)
            return ch, _trim_pad_aus(aus, expect), time.time() - t0

        with ThreadPoolExecutor(max_workers=min(4, len(channels))) as pool:
            futs = [pool.submit(_live_one, ch) for ch in channels]
            for fut in as_completed(futs):
                ch_name, aus, elapsed = fut.result()
                if ch_name not in channel_aus:
                    channel_aus[ch_name] = []
                while len(channel_aus[ch_name]) < start:
                    channel_aus[ch_name].append(b"")
                if len(channel_aus[ch_name]) > start:
                    channel_aus[ch_name] = channel_aus[ch_name][:start]
                channel_aus[ch_name].extend(aus)
                log.debug(
                    "[%s]  %s chunk %d [%d:%d) %d AUs %.2fs [live]",
                    fname, ch_name, cidx, start, end, len(aus), elapsed,
                )
    else:
        _ovr = getattr(_tls, "override", None)
        _jid = job_id or getattr(_tls, "encode_job_id", None)
        with ThreadPoolExecutor(max_workers=min(4, len(channels))) as pool:
            futs = [
                pool.submit(
                    _tls_call,
                    _ovr,
                    _transcode_chunk_worker,
                    ch,
                    cidx,
                    video_frames[ch][start:end],
                    work_dir,
                    sw,
                    sh,
                    camera_metas.get(ch, {}).get("codec", "hevc") or "hevc",
                    _jid,
                )
                for ch in channels
            ]
            for fut in as_completed(futs):
                ch_name, _cidx, path, elapsed = fut.result()
                if ch_name not in channel_aus:
                    channel_aus[ch_name] = []
                raw = Path(path).read_bytes() if Path(path).is_file() else b""
                aus = _split_h264_access_units(raw, work_dir)
                aus = _trim_pad_aus(aus, expect)
                while len(channel_aus[ch_name]) < start:
                    channel_aus[ch_name].append(b"")
                if len(channel_aus[ch_name]) > start:
                    channel_aus[ch_name] = channel_aus[ch_name][:start]
                channel_aus[ch_name].extend(aus)
                log.debug(
                    "[%s]  %s chunk %d [%d:%d) %d AUs %.2fs",
                    fname, ch_name, cidx, start, end, len(aus), elapsed,
                )
                try:
                    os.unlink(path)
                except OSError:
                    pass

    # Drop raw packet bytes for this range (keep list length for indexing).
    for ch in channels:
        frames = video_frames[ch]
        for i in range(start, min(end, len(frames))):
            frames[i] = b""

    imu_idx = _write_bundle_range(
        start,
        end,
        channel_aus=channel_aus,
        video_timestamps=video_timestamps,
        imu_sorted=imu_sorted,
        imu_idx=imu_idx,
        output_frame_dir=output_frame_dir,
        manifest_path=manifest_path,
        job_id=job_id,
        camera_metas=camera_metas,
        min_frames=min_frames,
        key_frame_indices=key_frame_indices,
        progressive=progressive,
        progress_cb=progress_cb,
    )
    elapsed = time.time() - t_chunk
    tag = " [passthrough]" if passthrough else (" [live]" if live else "")
    log.info(
        "[%s]  Head-cam chunk %d frames [%d:%d) %.1f%% published in %.2fs%s%s",
        fname,
        cidx,
        start,
        end,
        _progress_pct(end, min_frames),
        elapsed,
        _encode_rate_suffix(expect, elapsed),
        tag,
    )
    return imu_idx


def process_mcap(source_path, no_upload=False, progress_cb=None, job_id=None):
    """CPU transcode HEVC->H264, upload to OSS (or save locally if no_upload).

    progress_cb: optional callable(frames_done: int, frames_total: int) invoked
    while writing per-frame bins (throttled). Used for Redis progress polling.
    job_id: optional output/cache id (default: source file stem). Bench types use
    distinct ids (e.g. stem__t3) so scenarios do not overwrite each other.
    """
    local_path = Path(source_path)
    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {source_path}")

    fname = local_path.name
    out_job_id = (job_id or local_path.stem).strip() or local_path.stem
    t_start = time.time()
    total_size = local_path.stat().st_size
    progress_log = _JobProgressLogger(out_job_id)
    user_progress_cb = progress_cb

    def _progress_with_log(done, total):
        progress_log.update(done, total)
        if user_progress_cb is not None:
            user_progress_cb(done, total)

    progress_cb = _progress_with_log

    log.info("[%s]  %.0f MB  start  job_id=%s", fname, total_size / 1e6, out_job_id)
    log.info(
        "[%s]  Transcode: size=%s crf=%s bitrate=%s maxrate=%s bufsize=%s "
        "preset=%s force_reencode=%s",
        fname, _transcode_size_desc(), _tc_crf(),
        TRANSCODE_BITRATE, TRANSCODE_MAXRATE, TRANSCODE_BUFSIZE,
        TRANSCODE_PRESET, _tc_force_reencode(),
    )

    oss_base = local_path_to_oss_base(local_path)
    work_dir = TEMP_DIR / f"job_{local_path.stem}_{int(time.time())}"
    work_dir.mkdir(parents=True, exist_ok=True)

    # Accumulators
    video_frames = defaultdict(list)  # ch_name -> [bytes, ...]
    video_timestamps = defaultdict(list)  # ch_name -> [log_time_ns of each frame]
    imu_rows = []          # all raw IMU samples
    camera_metas = {}      # ch_name -> {width, height, fps, position}
    futures = []
    channel_found = set()
    chunk_idx = 0
    total_imu = 0
    channel_aus = {}  # ch_name -> list of AU byte arrays (filled below)
    # h264_ps_cache: 非 live 模式下按通道缓存 SPS/PPS，给缺参数集的后续 chunk 补头。
    h264_ps_cache: dict = {}

    def _h264_prefix_cached_ps(raw: bytes, cache: dict) -> bytes:
        found = _collect_annexb_param_sets(raw, hevc=False)
        if found:
            cache.update(found)
        if _annexb_has_leading_ps(raw, hevc=False):
            return raw
        prefix = _param_sets_bytes(cache, hevc=False)
        if not prefix:
            return raw
        return prefix + raw

    orin_published = False  # Orin path writes bins while encoding chunks
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _prev_encode_jid = getattr(_tls, "encode_job_id", None)
    _tls.encode_job_id = out_job_id
    live_encoders: dict = {}
    try:
        from orin_images import (
            jpeg_sequence_to_h264,
            orin_h264_channels_from_summary,
            orin_stream_kind,
            parse_orin_jpeg_streams,
            iter_orin_h264_annexb,
            sview_channel_for_topic,
            _h264_sps_size,
        )
        from hdf5_images import is_hdf5_file, parse_hdf5_image_streams

        # Orin teleop bags: ROS2 CompressedImage JPEG 或 H.264 Annex-B。
        # SSIEGO / head-cam protobuf 必须走 else；误判成 Orin 时官方
        # read_ros2_messages 会在 protobuf 通道上抛 DecoderNotFoundError。
        # Astribot 等 HDF5 不是 MCAP，不能走 protobuf reader。
        orin_kind = None
        orin_ok = False
        if is_hdf5_file(local_path):
            log.info(
                "[%s]  Format=HDF5 camera images (JPEG→H.264 progressive, chunk=%d)",
                fname, ORIN_CHUNK_FRAMES,
            )
            frame_blobs, video_timestamps, camera_metas = parse_hdf5_image_streams(
                local_path,
            )
            orin_kind = "jpeg"
            orin_ok = True
        else:
            orin_kind = orin_stream_kind(local_path)
            if orin_kind:
                try:
                    if orin_kind == "h264":
                        log.info(
                            "[%s]  Format=Orin ROS2 H.264 streams "
                            "(stream, same TRANSCODE_* as head-cam, chunk=%d)",
                            fname, ORIN_CHUNK_FRAMES,
                        )
                        # Parsed incrementally in the encode loop so the first
                        # GOP is published without waiting to scan the whole bag.
                    else:
                        log.info(
                            "[%s]  Format=Orin ROS2 CompressedImage (JPEG→H.264 progressive, chunk=%d)",
                            fname, ORIN_CHUNK_FRAMES,
                        )
                        try:
                            frame_blobs, video_timestamps, camera_metas = parse_orin_jpeg_streams(
                                local_path,
                            )
                        except ValueError as e:
                            # ACS 旧镜像会把 format=h264 的 CompressedImage 当 JPEG。
                            # 新代码 peek 后应走 h264；这里再兜底一次，避免误分类。
                            log.warning(
                                "[%s]  JPEG parse failed (%s); retry as Orin H.264",
                                fname, e,
                            )
                            orin_kind = "h264"
                    orin_ok = True
                except Exception as exc:
                    log.warning(
                        "[%s]  Orin parse failed (%s); falling back to head-cam protobuf",
                        fname, exc,
                    )
                    orin_kind = None
        if orin_ok and orin_kind == "h264":
            try:
                jpeg_frames = defaultdict(list)
                idr_seen: dict = {}
                expected_ch = sorted(orin_h264_channels_from_summary(local_path))
                summary_n = read_headcam_video_frame_count(local_path)
                job_id = out_job_id
                progressive = bool(no_upload and PROGRESSIVE_PUBLISH)
                if progressive:
                    publish_dir = _prepare_publish_dir(job_id)
                    output_frame_dir = publish_dir / "frames"
                    log.info("[%s]  Progressive publish -> %s", fname, publish_dir)
                else:
                    publish_dir = None
                    output_frame_dir = work_dir / "frames"
                    output_frame_dir.mkdir(parents=True, exist_ok=True)
                manifest_path = (
                    (publish_dir / "manifest.json") if publish_dir else (work_dir / "manifest.json")
                )
                key_frame_indices = []
                channels: list = []
                channel_aus: dict = {}
                live_encoders = {}
                min_frames = summary_n or 0
                start = 0
                cidx = 0
                sw = sh = None
                passthrough = False
                if summary_n:
                    log.info(
                        "[%s]  Orin H.264 frame_count from mcap summary: %d (channels=%s)",
                        fname, summary_n, ",".join(expected_ch) or "?",
                    )
                if progress_cb:
                    try:
                        progress_cb(0, min_frames or 0)
                    except Exception:
                        log.exception("progress_cb failed at start")

                def _aligned_n() -> int:
                    if not channels:
                        return 0
                    return min(len(jpeg_frames.get(ch, [])) for ch in channels)

                def _lock_channels(final: bool = False) -> bool:
                    nonlocal channels, channel_aus, live_encoders, passthrough, sw, sh
                    if channels:
                        return True
                    have = [ch for ch, rows in jpeg_frames.items() if rows]
                    if expected_ch:
                        ready = [ch for ch in expected_ch if jpeg_frames.get(ch)]
                        if not ready:
                            return False
                        if len(ready) < len(expected_ch) and not final:
                            return False
                        channels = ready
                    elif final:
                        if not have:
                            return False
                        channels = sorted(have)
                    elif len(have) >= 4:
                        channels = sorted(have)
                    else:
                        return False
                    # Same knobs as head-cam: TRANSCODE_SIZE/CRF/BITRATE/FORCE_REENCODE.
                    passthrough = _is_h264_passthrough(camera_metas, channels)
                    sw = _scale_dim(camera_metas, "w")
                    sh = _scale_dim(camera_metas, "h")
                    if _resize_requested():
                        for ch in camera_metas:
                            camera_metas[ch]["width"] = sw
                            camera_metas[ch]["height"] = sh
                    channel_aus = {ch: [] for ch in channels}
                    log.info(
                        "[%s]  Orin H.264 progressive (%s), cams=%s "
                        "size=%s crf=%s bitrate=%s maxrate=%s force_reencode=%s",
                        fname,
                        "h264 passthrough" if passthrough else "libx264",
                        ",".join(channels),
                        _transcode_size_desc(),
                        _tc_crf(),
                        TRANSCODE_BITRATE,
                        TRANSCODE_MAXRATE,
                        int(_tc_force_reencode()),
                    )
                    if (not passthrough) and LIVE_ENCODE:
                        live_encoders = _start_live_annexb_encoders(
                            channels, camera_metas, job_id, sw, sh,
                        )
                        log.info(
                            "[%s]  Live libx264 sessions x%d (hold encode slots for job)",
                            fname, len(live_encoders),
                        )
                        for ch, enc in live_encoders.items():
                            enc.seed_param_sets(h264_ps_cache.get(ch) or {})
                    return True

                def _enc_one(ch: str, cidx_i: int, f0: int, f1: int):
                    want = f1 - f0
                    if passthrough:
                        aus = _trim_pad_aus(
                            [bytes(x) for x in jpeg_frames[ch][f0:f1]], want,
                        )
                        return ch, aus, 0.0, sum(len(a) for a in aus)
                    enc = live_encoders.get(ch) if live_encoders else None
                    origin, extra, lead = f0, 0, 0
                    last_idr = _h264_last_idr_index(jpeg_frames[ch], f0)
                    if last_idr < 0:
                        rel = _h264_first_idr_index(jpeg_frames[ch][f0:f1])
                        last_idr = (f0 + rel) if rel >= 0 else -1
                    if last_idr >= 0:
                        if last_idr < f0:
                            origin, extra = last_idr, f0 - last_idr
                        elif last_idr > f0:
                            origin, lead = last_idr, last_idr - f0
                    if extra and any(not b for b in jpeg_frames[ch][origin:f0]):
                        origin, extra, lead = f0, 0, 0
                    frames_seg = jpeg_frames[ch][origin:f1]

                    def _fit_aus(aus_all: list) -> list:
                        aus = list(aus_all[extra:] if extra else aus_all)
                        if lead:
                            aus = [b""] * lead + aus
                        if len(aus) > want:
                            aus = aus[:want]
                        while len(aus) < want:
                            aus.append(b"")
                        return aus

                    if enc is not None:
                        t0 = time.time()
                        aus = _fit_aus(enc.encode_frames(frames_seg))
                        elapsed = time.time() - t0
                        nbytes = sum(len(a) for a in aus)
                        return ch, aus, elapsed, nbytes
                    out_p = work_dir / f"{ch}_orin_c{cidx_i:04d}.h264"
                    lock = cpu_encode_lock
                    gate = lock.slot(job_id) if hasattr(lock, "slot") else lock
                    with gate:
                        raw_in = _h264_prefix_cached_ps(
                            b"".join(frames_seg), h264_ps_cache[ch],
                        )
                        elapsed = cpu_transcode_chunk(
                            raw_in, out_p, sw, sh, codec="h264",
                            job_id=job_id, acquire_gate=False,
                        )
                    raw = out_p.read_bytes()
                    aus = _split_h264_access_units(raw, work_dir)
                    try:
                        out_p.unlink()
                    except OSError:
                        pass
                    return ch, _fit_aus(aus), elapsed, len(raw)

                def _emit_chunks(final: bool = False) -> None:
                    nonlocal start, cidx, min_frames
                    if not _lock_channels(final=final):
                        return
                    n = _aligned_n()
                    if summary_n:
                        min_frames = summary_n
                    else:
                        min_frames = max(min_frames, n)
                    while start < n:
                        step = (
                            PLAYABLE_MIN_FRAMES if cidx == 0
                            else max(1, ORIN_CHUNK_FRAMES)
                        )
                        if not final and n < start + step:
                            return
                        end = min(start + step, n)
                        t_chunk = time.time()
                        _ovr = getattr(_tls, "override", None)
                        with ThreadPoolExecutor(max_workers=min(4, len(channels))) as pool:
                            futs = [
                                pool.submit(_tls_call, _ovr, _enc_one, ch, cidx, start, end)
                                for ch in channels
                            ]
                            for fut in as_completed(futs):
                                ch, aus, elapsed, nbytes = fut.result()
                                channel_aus[ch].extend(aus)
                        chunk_len = end - start
                        for ch in channels:
                            have = len(channel_aus[ch])
                            expect = start + chunk_len
                            if have > expect:
                                channel_aus[ch] = channel_aus[ch][:expect]
                            while len(channel_aus[ch]) < expect:
                                channel_aus[ch].append(b"")
                            origin = _h264_last_idr_index(jpeg_frames[ch], start)
                            if passthrough or origin < 0:
                                origin = start
                            for i in range(0, origin):
                                jpeg_frames[ch][i] = b""
                        _write_bundle_range(
                            start,
                            end,
                            channel_aus=channel_aus,
                            video_timestamps=video_timestamps,
                            imu_sorted=[],
                            imu_idx=0,
                            output_frame_dir=output_frame_dir,
                            manifest_path=manifest_path,
                            job_id=job_id,
                            camera_metas=camera_metas,
                            min_frames=max(min_frames, end),
                            key_frame_indices=key_frame_indices,
                            progressive=progressive,
                            progress_cb=progress_cb,
                        )
                        elapsed = time.time() - t_chunk
                        log.info(
                            "[%s]  Orin chunk %d frames [%d:%d) %.1f%% published in %.2fs%s%s",
                            fname,
                            cidx,
                            start,
                            end,
                            _progress_pct(end, max(min_frames, end)),
                            elapsed,
                            _encode_rate_suffix(end - start, elapsed),
                            " [passthrough]" if passthrough else (
                                " [live]" if live_encoders else ""
                            ),
                        )
                        cidx += 1
                        start = end

                for ch, data, ts_ns, meta in iter_orin_h264_annexb(local_path):
                    if not idr_seen.get(ch):
                        if not _h264_payload_has_idr(data):
                            continue
                        idr_seen[ch] = True
                    jpeg_frames[ch].append(data)
                    video_timestamps[ch].append(ts_ns)
                    if ch not in camera_metas:
                        camera_metas[ch] = dict(meta)
                    elif meta.get("width") and meta.get("height"):
                        camera_metas[ch]["width"] = meta["width"]
                        camera_metas[ch]["height"] = meta["height"]
                    ps = h264_ps_cache.setdefault(ch, {})
                    if not _has_required_param_sets(ps, hevc=False):
                        found = _collect_annexb_param_sets(data, hevc=False)
                        if found:
                            ps.update(found)
                    if not channels:
                        _emit_chunks(final=False)
                    else:
                        need = (
                            PLAYABLE_MIN_FRAMES if cidx == 0
                            else max(1, ORIN_CHUNK_FRAMES)
                        )
                        if _aligned_n() >= start + need:
                            _emit_chunks(final=False)
                _emit_chunks(final=True)
                if start < 1:
                    raise ValueError(f"Orin MCAP has no h264 frames: {local_path}")
                min_frames = start
                for ch in channels:
                    video_frames[ch] = jpeg_frames.get(ch, [])
                log.info(
                    "[%s]  Built %d frame bundles (%d keyframes)%s [orin h264 stream]",
                    fname, min_frames, len(key_frame_indices),
                    " [progressive]" if progressive else "",
                )
                tl_start, tl_end = _estimate_timeline(
                    video_timestamps, channel_aus, min_frames,
                )
                _publish_manifest(
                    manifest_path,
                    _build_manifest_dict(
                        job_id=job_id,
                        camera_metas=camera_metas,
                        frame_count=min_frames,
                        frames_available=min_frames,
                        key_frame_indices=key_frame_indices,
                        timeline_start_ns=tl_start,
                        timeline_end_ns=tl_end,
                        status="done",
                    ),
                )
                if progress_cb:
                    try:
                        progress_cb(min_frames, min_frames)
                    except Exception:
                        log.exception("progress_cb failed at end")
                orin_published = True
            except Exception as exc:
                log.warning(
                    "[%s]  Orin H.264 stream encode failed (%s); "
                    "falling back to head-cam protobuf",
                    fname, exc,
                )
                orin_kind = None
                orin_ok = False
        elif orin_ok:
            jpeg_frames = frame_blobs
            if orin_kind == "h264":
                for ch in list(jpeg_frames.keys()):
                    idx = _h264_first_idr_index(jpeg_frames[ch])
                    if idx > 0:
                        log.info(
                            "[%s]  %s drop %d leading P-frames before first IDR",
                            fname, ch, idx,
                        )
                        jpeg_frames[ch] = jpeg_frames[ch][idx:]
                        video_timestamps[ch] = list(video_timestamps[ch][idx:])
                    elif idx < 0:
                        log.warning(
                            "[%s]  %s has no IDR NAL; decode may fail",
                            fname, ch,
                        )
                    ps: dict = {}
                    for blob in jpeg_frames[ch]:
                        found = _collect_annexb_param_sets(blob, hevc=False)
                        if found:
                            ps.update(found)
                        if _has_required_param_sets(ps, hevc=False):
                            break
                    h264_ps_cache[ch] = ps
                    if not _has_required_param_sets(ps, hevc=False):
                        log.warning("[%s]  %s missing in-band SPS/PPS", fname, ch)
                    elif 7 in ps:
                        w, h = _h264_sps_size(ps[7])
                        if w > 0 and h > 0 and ch in camera_metas:
                            camera_metas[ch]["width"] = w
                            camera_metas[ch]["height"] = h
            n = min(len(v) for v in jpeg_frames.values())
            if n < 1:
                raise ValueError(f"Orin MCAP has no {orin_kind} frames: {local_path}")
            for ch in jpeg_frames:
                video_timestamps[ch] = list(video_timestamps[ch][:n])
                video_frames[ch] = jpeg_frames[ch][:n]

            # Direct = CRF-only (no ABR crush). Still honor TRANSCODE_SIZE/W/H for HD compress.
            if ORIN_DIRECT:
                if _resize_requested():
                    sw = _scale_dim(camera_metas, "w")
                    sh = _scale_dim(camera_metas, "h")
                    for ch in camera_metas:
                        camera_metas[ch]["width"] = sw
                        camera_metas[ch]["height"] = sh
                    log.info(
                        "[%s]  Orin HD encode (CRF %d, scale %dx%d, no bitrate cap)",
                        fname, ORIN_CRF, sw, sh,
                    )
                else:
                    sw, sh = None, None
                    log.info(
                        "[%s]  Orin direct encode (CRF %d, no bitrate cap, no scale)",
                        fname, ORIN_CRF,
                    )
            else:
                sw = _scale_dim(camera_metas, "w") if _resize_requested() else None
                sh = _scale_dim(camera_metas, "h") if _resize_requested() else None
                if sw and sh:
                    for ch in camera_metas:
                        camera_metas[ch]["width"] = sw
                        camera_metas[ch]["height"] = sh

            channels = sorted(jpeg_frames.keys())
            channel_aus = {ch: [] for ch in channels}
            job_id = out_job_id
            progressive = bool(no_upload and PROGRESSIVE_PUBLISH)
            if progressive:
                publish_dir = _prepare_publish_dir(job_id)
                output_frame_dir = publish_dir / "frames"
                log.info("[%s]  Progressive publish -> %s", fname, publish_dir)
            else:
                publish_dir = None
                output_frame_dir = work_dir / "frames"
                output_frame_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = (
                (publish_dir / "manifest.json") if publish_dir else (work_dir / "manifest.json")
            )
            key_frame_indices = []
            min_frames = n
            if progress_cb:
                try:
                    progress_cb(0, min_frames)
                except Exception:
                    log.exception("progress_cb failed at start")

            if LIVE_ENCODE:
                if orin_kind == "h264":
                    live_encoders = _start_live_annexb_encoders(
                        channels,
                        camera_metas,
                        job_id,
                        sw,
                        sh,
                    )
                    log.info(
                        "[%s]  Live H.264→H.264 sessions x%d (re-encode, hold encode slots for job)",
                        fname, len(live_encoders),
                    )
                    for ch, enc in live_encoders.items():
                        enc.seed_param_sets(h264_ps_cache.get(ch) or {})
                else:
                    live_encoders = _start_live_jpeg_encoders(
                        channels,
                        camera_metas,
                        job_id,
                        sw,
                        sh,
                        crf=ORIN_CRF if ORIN_DIRECT else _tc_crf(),
                        direct=ORIN_DIRECT,
                    )
                    log.info(
                        "[%s]  Live JPEG→H.264 sessions x%d (hold encode slots for job)",
                        fname, len(live_encoders),
                    )
            for ch in channels:
                h264_ps_cache.setdefault(ch, {})
                if orin_kind == "h264":
                    for blob in jpeg_frames[ch]:
                        found = _collect_annexb_param_sets(blob, hevc=False)
                        if found:
                            h264_ps_cache[ch].update(found)
                    enc = live_encoders.get(ch) if live_encoders else None
                    if enc is not None:
                        enc.seed_param_sets(h264_ps_cache[ch])

            def _enc_one(ch: str, cidx: int, start: int, end: int):
                want = end - start
                enc = live_encoders.get(ch) if live_encoders else None
                origin = start
                extra = 0
                lead = 0
                if orin_kind == "h264":
                    last_idr = _h264_last_idr_index(jpeg_frames[ch], start)
                    if last_idr < 0:
                        rel = _h264_first_idr_index(jpeg_frames[ch][start:end])
                        last_idr = (start + rel) if rel >= 0 else -1
                    if last_idr >= 0:
                        if last_idr < start:
                            origin, extra = last_idr, start - last_idr
                        elif last_idr > start:
                            origin, lead = last_idr, last_idr - start
                    if extra and any(not b for b in jpeg_frames[ch][origin:start]):
                        origin, extra, lead = start, 0, 0
                frames_seg = jpeg_frames[ch][origin:end]

                def _fit_aus(aus_all: list) -> list:
                    aus = list(aus_all[extra:] if extra else aus_all)
                    if lead:
                        aus = [b""] * lead + aus
                    if len(aus) > want:
                        aus = aus[:want]
                    while len(aus) < want:
                        aus.append(b"")
                    return aus

                if enc is not None:
                    t0 = time.time()
                    aus = _fit_aus(enc.encode_frames(frames_seg))
                    elapsed = time.time() - t0
                    nbytes = sum(len(a) for a in aus)
                    return ch, aus, elapsed, nbytes
                fps = float(camera_metas.get(ch, {}).get("fps") or 30)
                out_p = work_dir / f"{ch}_orin_c{cidx:04d}.h264"
                lock = cpu_encode_lock
                gate = lock.slot(job_id) if hasattr(lock, "slot") else lock
                with gate:
                    if orin_kind == "h264":
                        raw_in = _h264_prefix_cached_ps(
                            b"".join(frames_seg), h264_ps_cache[ch]
                        )
                        elapsed = cpu_transcode_chunk(
                            raw_in,
                            out_p,
                            sw,
                            sh,
                            codec="h264",
                            job_id=job_id,
                            acquire_gate=False,
                        )
                    elif ORIN_DIRECT:
                        elapsed = jpeg_sequence_to_h264(
                            jpeg_frames[ch][start:end],
                            out_p,
                            fps=fps,
                            scale_w=sw,
                            scale_h=sh,
                            ffmpeg=FFMPEG,
                            crf=ORIN_CRF,
                            preset=TRANSCODE_PRESET,
                            bitrate=None,
                            maxrate=None,
                            bufsize=None,
                            gop=len(jpeg_frames[ch][start:end]),
                            direct=True,
                            threads=_x264_threads_now(),
                        )
                    else:
                        br = None if _rate_control_empty(TRANSCODE_BITRATE) else TRANSCODE_BITRATE
                        mr = None if _rate_control_empty(TRANSCODE_MAXRATE) else TRANSCODE_MAXRATE
                        bs = None if _rate_control_empty(TRANSCODE_BUFSIZE) else TRANSCODE_BUFSIZE
                        elapsed = jpeg_sequence_to_h264(
                            jpeg_frames[ch][start:end],
                            out_p,
                            fps=fps,
                            scale_w=sw,
                            scale_h=sh,
                            ffmpeg=FFMPEG,
                            crf=_tc_crf(),
                            preset=TRANSCODE_PRESET,
                            bitrate=br,
                            maxrate=mr,
                            bufsize=bs,
                            gop=len(jpeg_frames[ch][start:end]),
                            direct=False,
                            threads=_x264_threads_now(),
                        )
                raw = out_p.read_bytes()
                aus = _split_h264_access_units(raw, work_dir)
                try:
                    out_p.unlink()
                except OSError:
                    pass
                aus = _fit_aus(aus)
                return ch, aus, elapsed, len(raw)

            cidx = 0
            start = 0
            while start < n:
                # First chunk uses PLAYABLE_MIN_FRAMES for earlier playable.
                step = (
                    PLAYABLE_MIN_FRAMES if cidx == 0
                    else max(1, ORIN_CHUNK_FRAMES)
                )
                if orin_kind == "h264":
                    end = _idr_aligned_chunk_end(
                        jpeg_frames, channels, start, n, step
                    )
                else:
                    end = min(start + step, n)
                t_chunk = time.time()
                _ovr = getattr(_tls, "override", None)
                with ThreadPoolExecutor(max_workers=min(4, len(channels))) as pool:
                    futs = [
                        pool.submit(_tls_call, _ovr, _enc_one, ch, cidx, start, end)
                        for ch in channels
                    ]
                    for fut in as_completed(futs):
                        ch, aus, elapsed, nbytes = fut.result()
                        channel_aus[ch].extend(aus)
                        log.debug(
                            "[%s]  %s chunk %d [%d:%d] %d AUs %.1fKB %.2fs",
                            fname, ch, cidx, start, end, len(aus), nbytes / 1024, elapsed,
                        )
                # Align this chunk across channels (defensive).
                chunk_len = end - start
                for ch in channels:
                    have = len(channel_aus[ch])
                    expect = start + chunk_len
                    if have > expect:
                        channel_aus[ch] = channel_aus[ch][:expect]
                    while len(channel_aus[ch]) < expect:
                        channel_aus[ch].append(b"")
                    if orin_kind == "h264":
                        # Keep the current GOP (from last IDR) for the next fresh-ffmpeg chunk.
                        origin = _h264_last_idr_index(jpeg_frames[ch], start)
                        if origin < 0:
                            origin = start
                        for i in range(0, origin):
                            jpeg_frames[ch][i] = b""
                    else:
                        for i in range(start, end):
                            jpeg_frames[ch][i] = b""

                _write_bundle_range(
                    start,
                    end,
                    channel_aus=channel_aus,
                    video_timestamps=video_timestamps,
                    imu_sorted=[],
                    imu_idx=0,
                    output_frame_dir=output_frame_dir,
                    manifest_path=manifest_path,
                    job_id=job_id,
                    camera_metas=camera_metas,
                    min_frames=min_frames,
                    key_frame_indices=key_frame_indices,
                    progressive=progressive,
                    progress_cb=progress_cb,
                )
                elapsed = time.time() - t_chunk
                log.info(
                    "[%s]  Orin chunk %d frames [%d:%d) %.1f%% published in %.2fs%s%s",
                    fname,
                    cidx,
                    start,
                    end,
                    _progress_pct(end, min_frames),
                    elapsed,
                    _encode_rate_suffix(end - start, elapsed),
                    " [live]" if live_encoders else "",
                )
                cidx += 1
                start = end

            log.info(
                "[%s]  Built %d frame bundles (%d keyframes)%s [orin progressive]",
                fname, min_frames, len(key_frame_indices),
                " [progressive]" if progressive else "",
            )
            tl_start, tl_end = _estimate_timeline(video_timestamps, channel_aus, min_frames)
            _publish_manifest(
                manifest_path,
                _build_manifest_dict(
                    job_id=job_id,
                    camera_metas=camera_metas,
                    frame_count=min_frames,
                    frames_available=min_frames,
                    key_frame_indices=key_frame_indices,
                    timeline_start_ns=tl_start,
                    timeline_end_ns=tl_end,
                    status="done",
                ),
            )
            orin_published = True
        else:
            # Head-cam: stream-read → per-chunk encode/passthrough → publish immediately
            # (same progressive model as Orin; first chunk = PLAYABLE_MIN_FRAMES).
            size_guess = max(1, int(total_size * 0.60 / (4 * 30000)))
            summary_frames = read_headcam_video_frame_count(local_path)
            job_id = out_job_id
            progressive = bool(no_upload and PROGRESSIVE_PUBLISH)
            publish_dir = None
            if progressive:
                publish_dir = _prepare_publish_dir(job_id)
                output_frame_dir = publish_dir / "frames"
                log.info("[%s]  Progressive publish -> %s", fname, publish_dir)
            else:
                output_frame_dir = work_dir / "frames"
                output_frame_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = (
                (publish_dir / "manifest.json") if publish_dir else (work_dir / "manifest.json")
            )
            key_frame_indices = []
            published_upto = 0
            chunk_idx = 0
            imu_idx = 0
            ts_rebased = False
            ts_wall_offset = 0
            passthrough = None  # decided once metas+channels are ready
            channels = []
            if summary_frames and summary_frames > 0:
                declared_total = summary_frames
                total_from_summary = True
                log.info(
                    "[%s]  frame_count from mcap summary: %d",
                    fname, summary_frames,
                )
            else:
                declared_total = size_guess
                total_from_summary = False
                log.info(
                    "[%s]  frame_count estimated from file size: %d (no mcap summary)",
                    fname, declared_total,
                )

            if progress_cb:
                try:
                    progress_cb(0, declared_total)
                except Exception:
                    log.exception("progress_cb failed at start")

            def _append_video_ts(ch: str, log_time_ns: int):
                t = int(log_time_ns)
                if ts_rebased and ts_wall_offset:
                    t -= ts_wall_offset
                video_timestamps[ch].append(t)

            def _try_rebase_timeline():
                nonlocal ts_rebased, ts_wall_offset
                if ts_rebased or not imu_rows:
                    return
                # Peek whether wall-domain rebase is needed (same rules as helper).
                offsets = []
                for row in imu_rows:
                    mono = _parse_ns(row.get("monotonic_ns"))
                    wall = _parse_ns(row.get("wall_ns"))
                    if mono is None or wall is None or mono <= 0 or wall <= 0:
                        continue
                    offsets.append(wall - mono)
                if not offsets:
                    # Still normalize IMU log_time when possible.
                    if _rebase_timestamps_to_monotonic(video_timestamps, imu_rows):
                        log.info("[%s]  Rebased timestamps onto monotonic_raw_ns", fname)
                    ts_rebased = True
                    ts_wall_offset = 0
                    return
                offsets.sort()
                offset = offsets[len(offsets) // 2]
                probe = next(
                    (
                        row for row in imu_rows
                        if _parse_ns(row.get("monotonic_ns"))
                        and _parse_ns(row.get("wall_ns"))
                    ),
                    imu_rows[0],
                )
                mono0 = _parse_ns(probe.get("monotonic_ns"))
                wall0 = _parse_ns(probe.get("wall_ns"))
                log0 = int(probe["log_time_ns"])
                wall_domain = (
                    mono0 is not None
                    and wall0 is not None
                    and abs(log0 - wall0) <= abs(log0 - mono0)
                )
                if wall_domain:
                    ts_wall_offset = offset
                    for ch, ts_list in video_timestamps.items():
                        video_timestamps[ch] = [int(t) - offset for t in ts_list]
                    log.info(
                        "[%s]  Wall→monotonic rebase: offset=%d ns",
                        fname, offset,
                    )
                for row in imu_rows:
                    mono = _parse_ns(row.get("monotonic_ns"))
                    if mono is None or mono <= 0:
                        continue
                    if int(row["log_time_ns"]) != mono:
                        row["log_time_ns"] = mono
                ts_rebased = True

            def _channels_ready(final: bool = False) -> bool:
                if not channel_found:
                    return False
                if final:
                    return True
                # Lock progressive publish only after the full SView quad is present.
                # Waiting on camera_metas alone is unsafe: a single early /meta
                # (often FL) would freeze channel_aus to one cam and the other
                # three streams stay empty forever.
                return HEADCAM_CHANNEL_SET.issubset(channel_found)

            def _select_channels(final: bool = False) -> list:
                if HEADCAM_CHANNEL_SET.issubset(channel_found):
                    return list(HEADCAM_CHANNELS)
                if final:
                    # Non-standard bag: publish whatever cams actually appeared.
                    return sorted(channel_found)
                return []

            def _flush_ready_chunks(final: bool = False):
                nonlocal published_upto, chunk_idx, imu_idx, passthrough
                nonlocal declared_total, channels, channel_aus, live_encoders
                if not _channels_ready(final=final):
                    return
                selected = _select_channels(final=final)
                if not selected:
                    return
                if passthrough is None:
                    channels = selected
                    passthrough = _is_h264_passthrough(camera_metas, channels)
                    channel_aus = {ch: [] for ch in channels}
                    log.info(
                        "[%s]  Head-cam progressive (%s), cams=%s first_chunk=%d chunk=%d",
                        fname,
                        "h264 passthrough" if passthrough else "libx264",
                        ",".join(channels),
                        PLAYABLE_MIN_FRAMES,
                        CHUNK_FRAMES,
                    )
                    if (not passthrough) and LIVE_ENCODE and not live_encoders:
                        sw = _scale_dim(camera_metas, "w")
                        sh = _scale_dim(camera_metas, "h")
                        live_encoders = _start_live_annexb_encoders(
                            channels, camera_metas, job_id, sw, sh,
                        )
                        log.info(
                            "[%s]  Live libx264 sessions x%d (hold encode slots for job)",
                            fname, len(live_encoders),
                        )
                else:
                    # Channel set is immutable after the first progressive lock.
                    channels = [ch for ch in channels if ch in channel_aus]
                    if not channels:
                        return
                min_count = min(len(video_frames[ch]) for ch in channels)
                while True:
                    if final:
                        next_end = min_count
                        if next_end <= published_upto:
                            break
                    else:
                        next_end = published_upto + _headcam_chunk_len(chunk_idx)
                        if min_count < next_end:
                            break
                    _try_rebase_timeline()
                    # Summary count is the full-job total; only grow if we publish past it.
                    # Size guess is a floor when Summary is missing.
                    if total_from_summary:
                        declared_total = max(declared_total, next_end)
                    else:
                        declared_total = max(declared_total, next_end, size_guess)
                    imu_sorted = sorted(imu_rows, key=lambda r: r["log_time_ns"])
                    imu_idx = _publish_headcam_chunk(
                        fname=fname,
                        channels=channels,
                        start=published_upto,
                        end=next_end,
                        cidx=chunk_idx,
                        video_frames=video_frames,
                        video_timestamps=video_timestamps,
                        camera_metas=camera_metas,
                        channel_aus=channel_aus,
                        work_dir=work_dir,
                        output_frame_dir=output_frame_dir,
                        manifest_path=manifest_path,
                        job_id=job_id,
                        min_frames=declared_total,
                        key_frame_indices=key_frame_indices,
                        progressive=progressive,
                        passthrough=bool(passthrough),
                        imu_sorted=imu_sorted,
                        imu_idx=imu_idx,
                        progress_cb=progress_cb,
                        live_encoders=live_encoders,
                    )
                    published_upto = next_end
                    chunk_idx += 1
                    if final:
                        break

            for msg in read_protobuf_messages(str(local_path)):
                topic = msg.topic

                if "/meta" in topic and "camera" in topic.lower():
                    ch = sview_channel_for_topic(topic)
                    if ch and ch not in camera_metas:
                        try:
                            p = msg.proto_msg
                            camera_metas[ch] = {
                                "width": int(p.width),
                                "height": int(p.height),
                                "fps": int(getattr(p, "fps", 30)),
                                "position": str(getattr(p, "position", ch)),
                                "codec": str(getattr(p, "codec", "") or "").lower(),
                            }
                        except Exception:
                            pass

                elif "/video" in topic or "/h264" in topic.lower():
                    ch_name = sview_channel_for_topic(topic)
                    if not ch_name:
                        continue
                    channel_found.add(ch_name)
                    video_frames[ch_name].append(msg.proto_msg.data)
                    _append_video_ts(ch_name, msg.log_time_ns)
                    _flush_ready_chunks(final=False)

                elif topic == "/imu/sample":
                    p = msg.proto_msg
                    row = {
                        "log_time_ns": msg.log_time_ns,
                        "monotonic_ns": str(p.monotonic_raw_ns),
                        "wall_ns": str(p.wall_ns),
                        "unit": p.unit,
                        "x": p.x,
                        "y": p.y,
                        "z": p.z,
                        "temp_c": p.temp_c,
                        "scale": p.scale,
                        "sample_index": int(getattr(p, "sample_index", total_imu)),
                        "sensor_type": int(getattr(p, "sensor_type", 0) or 0),
                    }
                    if ts_rebased:
                        mono = _parse_ns(row.get("monotonic_ns"))
                        if mono is not None and mono > 0:
                            row["log_time_ns"] = mono
                    imu_rows.append(row)
                    total_imu += 1

            min_total = (
                min(len(video_frames[ch]) for ch in channels)
                if channels else (
                    min(len(f) for f in video_frames.values()) if video_frames else 0
                )
            )
            log.info(
                "[%s]  Parsed: %d video frames x %d ch,  %d IMU (published=%d)",
                fname, min_total, len(video_frames), total_imu, published_upto,
            )
            if channels:
                for ch in channels:
                    video_timestamps[ch] = list(video_timestamps[ch][:min_total])
                    video_frames[ch] = list(video_frames[ch][:min_total])
            _flush_ready_chunks(final=True)
            if not ts_rebased:
                if _rebase_timestamps_to_monotonic(video_timestamps, imu_rows):
                    log.info("[%s]  Rebased timestamps onto monotonic_raw_ns", fname)
                ts_rebased = True

            min_frames = published_upto
            declared_total = min_frames
            if min_frames < 1:
                raise ValueError(f"Head-cam MCAP has no aligned video frames: {local_path}")

            # Apply output size to camera metas for manifest (resize path).
            if _resize_requested():
                sw = _scale_dim(camera_metas, "w")
                sh = _scale_dim(camera_metas, "h")
                for ch in camera_metas:
                    camera_metas[ch]["width"] = sw
                    camera_metas[ch]["height"] = sh

            log.info(
                "[%s]  Built %d frame bundles (%d keyframes)%s [head-cam progressive]",
                fname, min_frames, len(key_frame_indices),
                " [progressive]" if progressive else "",
            )
            tl_start, tl_end = _estimate_timeline(video_timestamps, channel_aus, min_frames)
            _publish_manifest(
                manifest_path,
                _build_manifest_dict(
                    job_id=job_id,
                    camera_metas=camera_metas,
                    frame_count=min_frames,
                    frames_available=min_frames,
                    key_frame_indices=key_frame_indices,
                    timeline_start_ns=tl_start,
                    timeline_end_ns=tl_end,
                    status="done",
                ),
            )
            if progress_cb:
                try:
                    progress_cb(min_frames, min_frames)
                except Exception:
                    log.exception("progress_cb failed at end")
            orin_published = True  # frames already on disk (reuse Orin post-path)

        # --- Save/upload outputs ---
        if no_upload:
            import shutil as _shutil
            for cb in work_dir.glob("*_concat.bin"):
                try:
                    cb.unlink()
                except OSError:
                    pass
            if progressive:
                # Frames + manifest already under LOCAL_OUTPUT_DIR/{job_id}
                log.info("  -> progressive cache  %s  (%d frames)", publish_dir, min_frames)
            else:
                out_dir = LOCAL_OUTPUT_DIR / job_id
                _shutil.copytree(str(work_dir), str(out_dir), dirs_exist_ok=True)
                log.info("  -> saved  %s  (%d frames)", out_dir, min_frames)
        else:
            # Upload each frame bin + manifest under .../{job_id}/sview/
            if manifest_path.exists():
                oss_key = f"{oss_base}/manifest.json"
                upload_to_oss(manifest_path, oss_key)
            for bin_file in sorted(output_frame_dir.glob("*.bin")):
                oss_key = f"{oss_base}/frames/{bin_file.name}"
                upload_to_oss(bin_file, oss_key)


        total_elapsed = round(time.time() - t_start, 1)
        # Ensure a final 100% line even if the last progress_cb was throttled away.
        try:
            progress_log.update(min_frames, min_frames)
        except Exception:
            pass
        log.info(
            "[%s]  Done in %.1fs  %d video ch + %d IMU  job_id=%s",
            fname,
            total_elapsed,
            len(video_frames),
            total_imu,
            out_job_id,
        )
    finally:
        _close_live_encoders(live_encoders)
        _tls.encode_job_id = _prev_encode_jid
        import shutil
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass




# ---------------------------------------------------------------------------
#  Timestamp rebase (wall / MCAP log_time → monotonic_raw_ns)
# ---------------------------------------------------------------------------

def _parse_ns(value):
    """Parse a nanosecond timestamp from int/str; return None if missing/invalid."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rebase_timestamps_to_monotonic(video_timestamps: dict, imu_rows: list) -> bool:
    """Use monotonic_raw_ns as the shared timeline (same as local_full_mcap_flow_verify).

    Production MCAPs often stamp msg.log_time with wall-clock ns (~1e18), which
    exceeds JS Number.MAX_SAFE_INTEGER and breaks SView IMU plots. Verify-style
    recordings already use monotonic log_time (~1e11).

    - If MCAP log_time is in the wall domain: subtract median(wall - mono) from
        video timestamps, and set each IMU log_time_ns = monotonic_raw_ns.
    - If already monotonic: only normalize IMU log_time_ns to monotonic_raw_ns.

    Mutates video_timestamps and imu_rows in place. Returns True if any change.
    """
    if not imu_rows:
        return False

    offsets = []
    for row in imu_rows:
        mono = _parse_ns(row.get("monotonic_ns"))
        wall = _parse_ns(row.get("wall_ns"))
        if mono is None or wall is None or mono <= 0 or wall <= 0:
            continue
        offsets.append(wall - mono)

    changed = False

    if offsets:
        offsets.sort()
        offset = offsets[len(offsets) // 2]
        probe = next(
            (
                row for row in imu_rows
                if _parse_ns(row.get("monotonic_ns"))
                and _parse_ns(row.get("wall_ns"))
            ),
            imu_rows[0],
        )
        mono0 = _parse_ns(probe.get("monotonic_ns"))
        wall0 = _parse_ns(probe.get("wall_ns"))
        log0 = int(probe["log_time_ns"])
        wall_domain = (
            mono0 is not None
            and wall0 is not None
            and abs(log0 - wall0) <= abs(log0 - mono0)
        )
        if wall_domain:
            for ch, ts_list in video_timestamps.items():
                video_timestamps[ch] = [int(t) - offset for t in ts_list]
            changed = True
            log.info(
                "Wall→monotonic rebase: offset=%d ns (median of %d IMU pairs)",
                offset, len(offsets),
            )

    for row in imu_rows:
        mono = _parse_ns(row.get("monotonic_ns"))
        if mono is None or mono <= 0:
            continue
        if int(row["log_time_ns"]) != mono:
            row["log_time_ns"] = mono
            changed = True

    return changed


# ---------------------------------------------------------------------------
#  Per-frame H264 access unit splitting & bundle serialization
# ---------------------------------------------------------------------------

def _split_h264_access_units(data: bytes, work_dir: Path) -> list:
    """Split raw H.264 Annex B into per-frame AUs by NAL scan (no ffprobe).

    Correct access-unit grouping for this encode path (single-slice libx264):

      - Each primary picture is one VCL NAL (type 1 or 5).
      - Non-VCL that *follow* a VCL (SEI/SPS/PPS/AUD) start the *next* AU —
        they prefix the following picture (H.264 AU order).
      - Consecutive VCLs with nothing between them each start a new AU.

    Result for dump_extra=freq=keyframe bitstreams::

      [SPS][PPS][SEI][IDR] | [P] | ... | [P] | [SPS][PPS][SEI][IDR] | ...

    Keyframes keep their parameter sets (better for seek/decode than ffprobe's
    habit of attaching SPS/PPS to the previous P packet). Do not re-inject
    SPS/PPS — that previously wrote corrupt 1-byte parameter sets.

    work_dir is unused; kept for call-site parity with server_cpu.py.
    """
    del work_dir  # API parity with ffprobe variant
    if not data:
        return []

    nal_starts = []  # [(offset, start_code_len, nal_type), ...]
    pos = 0
    dlen = len(data)
    while pos < dlen - 3:
        pos4 = data.find(b"\x00\x00\x00\x01", pos)
        pos3 = data.find(b"\x00\x00\x01", pos)
        if pos4 < 0 and pos3 < 0:
            break
        if pos4 < 0:
            np, sc = pos3, 3
        elif pos3 < 0:
            np, sc = pos4, 4
        else:
            np, sc = (pos4, 4) if pos4 <= pos3 else (pos3, 3)
        if np + sc >= dlen:
            break
        nal_starts.append((np, sc, data[np + sc] & 0x1f))
        pos = np + sc + 1

    if not nal_starts:
        return [data]

    aus = []
    au_start = nal_starts[0][0]
    seen_vcl = False

    for nal_start, _, nal_type in nal_starts:
        if nal_type in (1, 5):
            if seen_vcl:
                # Next primary picture with no prefix NALs between VCLs
                aus.append(data[au_start:nal_start])
                au_start = nal_start
            seen_vcl = True
        elif seen_vcl and nal_type in (6, 7, 8, 9):
            # SEI / SPS / PPS / AUD after a picture → prefix of the next AU
            aus.append(data[au_start:nal_start])
            au_start = nal_start
            seen_vcl = False

    aus.append(data[au_start:])
    return aus


def _h264_payload_has_idr(data: bytes) -> bool:
    """Check if H.264 payload contains an IDR NAL unit (type 5)."""
    pos = 0
    while pos < len(data) - 4:
        if data[pos:pos+4] == b'\x00\x00\x00\x01':
            if (data[pos+4] & 0x1f) == 5:
                return True
            pos += 5
        elif data[pos:pos+3] == b'\x00\x00\x01':
            if (data[pos+3] & 0x1f) == 5:
                return True
            pos += 4
        else:
            pos += 1
    return False


def _idr_aligned_chunk_end(
    frames_by_ch: dict, channels: list, start: int, n: int, step: int
) -> int:
    """End index for a fresh-ffmpeg H.264 chunk: next shared IDR (or n).

    Each live chunk is a new ffmpeg process. Cutting on a P-frame makes the
    next chunk fail (no reference pictures) even if SPS/PPS are prefixed.
    """
    end = min(start + max(1, step), n)
    if end >= n:
        return n
    j = end
    while j < n:
        if all(_h264_payload_has_idr(frames_by_ch[ch][j]) for ch in channels):
            return j
        j += 1
    return n


def _h264_first_idr_index(frames: list) -> int:
    for i, blob in enumerate(frames):
        if blob and _h264_payload_has_idr(blob):
            return i
    return -1


def _h264_last_idr_index(frames: list, at_or_before: int) -> int:
    if not frames:
        return -1
    i = min(int(at_or_before), len(frames) - 1)
    while i >= 0:
        blob = frames[i]
        if blob and _h264_payload_has_idr(blob):
            return i
        i -= 1
    return -1

def _write_bytes_atomic(target_path: Path, payload: bytes):
    """Write bytes atomically: write to .tmp first, then rename to target."""
    tmp_path = target_path.with_suffix(target_path.suffix + '.tmp')
    with open(tmp_path, 'wb') as f:
        f.write(payload)
    tmp_path.replace(target_path)


def _estimate_timeline(video_timestamps: dict, channel_aus: dict, upto_frame: int):
    """Timeline start/end for `upto_frame` frames (1-based count).

    Prefer full timestamp span when enough samples exist; otherwise fall back to
    last-interval extrapolation (spec 4.1). If ``upto_frame`` is the full-job
    count but only a prefix of timestamps is in memory, extrapolate end_ns so
    duration_sec stays stable during progressive publish.
    """
    if upto_frame <= 0 or not channel_aus or not video_timestamps:
        return 0, 0
    first_ch = sorted(channel_aus.keys())[0]
    ts_list = video_timestamps.get(first_ch, [])[:upto_frame]
    if len(ts_list) >= 2:
        start_ns = ts_list[0]
        last_start_ns = ts_list[-1]
        have = len(ts_list)
        avg = (last_start_ns - start_ns) / (have - 1)
        if avg > 0 and upto_frame > have:
            end_ns = start_ns + int(avg * (upto_frame - 1))
        else:
            previous_start_ns = ts_list[-2]
            end_ns = last_start_ns + (last_start_ns - previous_start_ns)
        return start_ns, end_ns
    if len(ts_list) == 1:
        return ts_list[0], ts_list[0] + 33_333_333
    return 0, 0


def _duration_sec(
    timeline_start_ns: int,
    timeline_end_ns: int,
    frame_count: int,
    camera_metas: dict,
) -> float:
    """Stable total duration for manifest (never shrink/grow during progressive)."""
    if timeline_start_ns and timeline_end_ns and timeline_end_ns > timeline_start_ns:
        return round((timeline_end_ns - timeline_start_ns) / 1e9, 6)
    fps = 30
    for ch in ("RR", "FR", "RL", "FL"):
        if ch in camera_metas and camera_metas[ch].get("fps"):
            try:
                fps = max(1, int(camera_metas[ch]["fps"]))
                break
            except (TypeError, ValueError):
                pass
    if fps == 30:
        for m in camera_metas.values():
            if m.get("fps"):
                try:
                    fps = max(1, int(m["fps"]))
                    break
                except (TypeError, ValueError):
                    pass
    if frame_count <= 0:
        return 0.0
    return round(max(0, frame_count - 1) / float(fps), 6)


def _build_manifest_dict(
    job_id: str,
    camera_metas: dict,
    frame_count: int,
    frames_available: int,
    key_frame_indices: list,
    timeline_start_ns: int,
    timeline_end_ns: int,
    status: str,
) -> dict:
    """status: 'partial' while publishing, 'done' when complete.

    timeline/duration_sec always reflect the *full* job (frame_count), not the
    currently published prefix — so progressive clients get a stable seek range.
    """
    manifest = {
        "job_id": job_id,
        "format": "head_cam",
        "status": status,
        "bundle": {
            "frame_count": frame_count,
            "frames_available": frames_available,
            "gop_size": 30,
            "key_frame_indices": list(key_frame_indices),
            "timeline": {
                "start_ns": timeline_start_ns,
                "end_ns": timeline_end_ns,
                "duration_sec": _duration_sec(
                    timeline_start_ns, timeline_end_ns, frame_count, camera_metas,
                ),
            },
        },
        "streams": _build_streams_manifest(
            camera_metas,
            _scale_dim(camera_metas, "w"),
            _scale_dim(camera_metas, "h"),
        ),
    }
    if OSS_CACHE:
        manifest["play_base"] = oss_play_base(job_id)
    return manifest


def _publish_manifest(manifest_path: Path, manifest: dict) -> None:
    payload = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    _write_bytes_atomic(manifest_path, payload)


def _prepare_publish_dir(job_id: str) -> Path:
    """Create/clear LOCAL_OUTPUT_DIR/{job_id} for progressive frame publish."""
    import shutil as _shutil
    out_dir = LOCAL_OUTPUT_DIR / job_id
    if out_dir.exists():
        _shutil.rmtree(out_dir, ignore_errors=True)
    frames = out_dir / "frames"
    frames.mkdir(parents=True, exist_ok=True)
    return out_dir


def _build_bundle_frame(frame_idx: int, streams_data: dict, timestamp_ns: int,
                        time_window_ns: list, is_keyframe: bool,
                        imu_json: dict, events_json: dict) -> bytes:
    """Build a single bundle frame bin file.

    Format: header_length(uint32 LE) | header_json(UTF-8) |
            payload_length(uint32 LE) | payload(RR|FR|RL|FL|IMU|Events)

    streams_data: dict like {'RR': bytes, 'FR': bytes, 'RL': bytes, 'FL': bytes}
    """
    import struct
    offset = 0
    streams_hdr = {}
    combined = bytearray()

    # Video streams in RR/FR/RL/FL order
    for ch in ("RR", "FR", "RL", "FL"):
        raw = streams_data.get(ch, b"")
        streams_hdr[ch.lower()] = {
            "payload_offset": offset,
            "payload_length": len(raw),
            "is_null": not raw,
        }
        combined.extend(raw)
        offset += len(raw)

    # IMU section
    imu_str = json.dumps(imu_json, ensure_ascii=False)
    imu_bytes = imu_str.encode("utf-8")
    imu_hdr = {"payload_offset": offset, "payload_length": len(imu_bytes)}
    combined.extend(imu_bytes)
    offset += len(imu_bytes)

    # Events section
    evt_str = json.dumps(events_json, ensure_ascii=False)
    evt_bytes = evt_str.encode("utf-8")
    evt_hdr = {"payload_offset": offset, "payload_length": len(evt_bytes)}
    combined.extend(evt_bytes)
    offset += len(evt_bytes)

    hdr = {
        "packet_schema": "playertrans.bundle_frame.v1",
        "frame_index": frame_idx,
        "timestamp_ns": timestamp_ns,
        "time_window_ns": time_window_ns,
        "key_frame": is_keyframe,
        "streams": streams_hdr,
        "imu": imu_hdr,
        "events": evt_hdr,
    }
    hdr_json = json.dumps(hdr, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    payload = bytes(combined)
    return struct.pack("<I", len(hdr_json)) + hdr_json + struct.pack("<I", len(payload)) + payload


def _resolve_path(raw_path: str) -> Path:
    p = Path(raw_path)
    if p.exists():
        return p
    mapped = source_to_local(raw_path)
    if mapped.exists():
        log.info("Mapped %s -> %s", raw_path, mapped)
        return mapped
    raise FileNotFoundError(f"Neither '{raw_path}' nor '{mapped}' exists")


# ---------------------------------------------------------------------------
#  Redis queue loop
# ---------------------------------------------------------------------------

class JobCancelled(Exception):
    """Raised from progress_cb when Redis cancel flag is set."""


class LostJobLock(Exception):
    """Raised when another worker stole / expired our job lock."""


def _clear_job_cache(job_id: str, *, purge_oss: bool = True) -> None:
    import shutil as _shutil

    out_dir = LOCAL_OUTPUT_DIR / job_id
    if out_dir.is_dir():
        _shutil.rmtree(out_dir, ignore_errors=True)
        log.info("Cleared local cache for job %s", job_id)
    if purge_oss and OSS_CACHE:
        try:
            from oss_cache_gc import delete_job_prefix

            n = delete_job_prefix(job_id)
            log.info("Cleared OSS play cache for job %s objects=%s", job_id, n)
        except Exception:
            log.exception("OSS play-cache clear failed for %s", job_id)


class _JobLockRenewer:
    """Background renew so long ffmpeg chunks cannot let the lock expire."""

    def __init__(self, r, job_id: str):
        from job_status import SVIEW_LOCK_TTL_SEC, renew_job_lock

        self._r = r
        self._job_id = job_id
        self._renew = renew_job_lock
        self._interval = max(10, int(SVIEW_LOCK_TTL_SEC) // 3)
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"sview-lock-{job_id[:40]}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def lost(self) -> bool:
        return self._lost.is_set()

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                if not self._renew(self._r, self._job_id):
                    log.error("[%s] background renew failed — lock lost", self._job_id)
                    self._lost.set()
                    return
            except Exception:
                log.exception("[%s] background renew error", self._job_id)
                self._lost.set()
                return


def _run_one_queue_job(r, item: dict, on_playable=None) -> None:
    """Process a single dequeued Redis item (runs in a worker thread)."""
    from job_status import (
        POD_IP,
        SVIEW_CLUSTER,
        acquire_job_lock,
        get_job_fields,
        is_cancel_requested,
        release_job_lock,
        renew_job_lock,
        set_job_fields,
    )
    from job_status import REDIS_QUEUE_KEY as _QKEY

    redis_path = item["source"]
    job_id = item["job_id"]
    kind, _mount, _rel = classify_source(redis_path)
    local_path = redis_path_to_local(redis_path)
    is_bench = item.get("kind") == "bench" or item.get("bench_type") is not None
    log.info(
        "Dequeued [%s/%s]: %s  ->  %s  job_id=%s owner=%s cluster=%s",
        kind, item.get("kind"), redis_path, local_path, job_id, POD_IP, SVIEW_CLUSTER,
    )

    # Cancelled before start → skip (do not steal lock).
    try:
        if is_cancel_requested(get_job_fields(r, job_id)):
            set_job_fields(
                r, job_id,
                status="cancelled",
                cancel=1,
                error="cancelled before start",
                finished_at=f"{time.time():.6f}",
            )
            log.info("Skip cancelled job %s", job_id)
            if on_playable is not None:
                on_playable(job_id)
            return
    except Exception:
        log.exception("Cancel pre-check failed for %s", job_id)

    if not acquire_job_lock(r, job_id):
        # Another worker holds the lock — avoid double-transcode.
        if SVIEW_CLUSTER:
            time.sleep(0.2)
            # Re-queue only if lock vanished (winner crashed before claiming).
            from job_status import get_lock_owner
            if get_lock_owner(r, job_id) is None and not is_cancel_requested(
                get_job_fields(r, job_id)
            ):
                r.rpush(_QKEY, item.get("raw") or redis_path)
                log.warning("Job %s lock free after miss — re-queued", job_id)
            else:
                log.warning("Job %s lock held elsewhere — drop duplicate", job_id)
        else:
            log.warning("Job %s lock not acquired — drop", job_id)
        if on_playable is not None:
            on_playable(job_id)
        return

    lock_held = True
    renewer = _JobLockRenewer(r, job_id)
    renewer.start()
    started_at = time.time()
    encode_gate.register(job_id)
    _tls.encode_job_id = job_id
    # Always start from a clean play cache so cancel/Lifecycle orphans cannot leave a hole.
    try:
        _clear_job_cache(job_id, purge_oss=True)
    except Exception:
        log.exception("[%s] pre-encode cache wipe failed", job_id)
    try:
        fields = {
            "status": "processing",
            "source": redis_path,
            "error": "",
            "frames_done": 0,
            "frames_total": 0,
            "playable": 0,
            "cancel": 0,
            "owner_pod": POD_IP,
            "started_at": f"{started_at:.6f}",
            "first_frame_at": "",
            "first_frame_sec": "",
            "finished_at": "",
            "full_sec": "",
            "cache_bytes": 0,
            "cache_status": "writing",
        }
        if OSS_CACHE:
            fields["play_base"] = oss_play_base(job_id)
        if is_bench:
            fields.update({
                "bench_type": item.get("bench_type") or "",
                "bench_name": item.get("bench_name") or "",
                "transcode_size": item.get("transcode_size"),
                "transcode_crf": item.get("transcode_crf"),
                "force_reencode": item.get("force_reencode"),
            })
        set_job_fields(r, job_id, **fields)
    except Exception:
        log.exception("Failed to set processing status for %s", job_id)

    t_first_wall = [None]
    playable_notified = [False]

    try:
        def _progress(done, total):
            try:
                if renewer.lost():
                    raise LostJobLock("job lock lost")
                if is_cancel_requested(get_job_fields(r, job_id)):
                    raise JobCancelled("cancel requested")
                if not renew_job_lock(r, job_id):
                    raise LostJobLock("job lock lost during progress")
                now = time.time()
                playable = 1 if done >= PLAYABLE_MIN_FRAMES else 0
                patch = {
                    "status": "processing",
                    "source": redis_path,
                    "frames_done": done,
                    "frames_total": total,
                    "playable": playable,
                    "owner_pod": POD_IP,
                    "error": "",
                }
                if OSS_CACHE:
                    patch["play_base"] = oss_play_base(job_id)
                if done >= 1 and t_first_wall[0] is None:
                    t_first_wall[0] = now
                    patch["first_frame_at"] = f"{now:.6f}"
                    patch["first_frame_sec"] = f"{(now - started_at):.3f}"
                set_job_fields(r, job_id, **patch)
                if playable and not playable_notified[0]:
                    playable_notified[0] = True
                    encode_gate.mark_playable(job_id)
                    log.info(
                        "[%s] playable %.1f%% (%s/%s frames) — unlock overlap scheduler",
                        job_id,
                        _progress_pct(done, total),
                        done,
                        total,
                    )
                    if on_playable is not None:
                        on_playable(job_id)
            except (JobCancelled, LostJobLock):
                raise
            except Exception:
                log.exception("Failed to update progress for %s", job_id)

        with temporary_transcode_settings(
            size=item.get("transcode_size") if is_bench else None,
            crf=item.get("transcode_crf") if is_bench else None,
            force_reencode=item.get("force_reencode") if is_bench else None,
        ):
            if is_bench:
                log.info(
                    "[%s] bench type=%s size=%s crf=%s force=%s",
                    job_id,
                    item.get("bench_type"),
                    _tc_size(),
                    _tc_crf(),
                    int(_tc_force_reencode()),
                )
            process_mcap(
                str(local_path),
                no_upload=NO_UPLOAD,
                progress_cb=_progress,
                job_id=job_id,
            )

        if renewer.lost():
            raise LostJobLock("job lock lost")
        # Final cancel check after encode returns.
        if is_cancel_requested(get_job_fields(r, job_id)):
            raise JobCancelled("cancel requested")

        finished_at = time.time()
        full_sec = finished_at - started_at
        first_sec = (
            (t_first_wall[0] - started_at) if t_first_wall[0] is not None else full_sec
        )
        frame_count = ""
        try:
            import json as _json
            mf = LOCAL_OUTPUT_DIR / job_id / "manifest.json"
            if mf.is_file():
                frame_count = str((_json.loads(mf.read_text(encoding="utf-8"))
                                   .get("bundle") or {}).get("frame_count") or "")
        except Exception:
            pass
        try:
            set_job_fields(
                r, job_id,
                status="ready",
                source=redis_path,
                frame_count=frame_count,
                frames_done=frame_count or 0,
                frames_total=frame_count or 0,
                playable=1,
                cancel=0,
                owner_pod=POD_IP,
                error="",
                finished_at=f"{finished_at:.6f}",
                full_sec=f"{full_sec:.3f}",
                first_frame_sec=f"{first_sec:.3f}",
                first_frame_at=(
                    f"{t_first_wall[0]:.6f}" if t_first_wall[0] is not None else ""
                ),
                **({"play_base": oss_play_base(job_id)} if OSS_CACHE else {}),
            )
        except Exception:
            log.exception("Failed to set ready status for %s", job_id)
        if not playable_notified[0]:
            playable_notified[0] = True
            encode_gate.mark_playable(job_id)
            if on_playable is not None:
                on_playable(job_id)
    except JobCancelled:
        try:
            _clear_job_cache(job_id, purge_oss=True)
            try:
                from oss_cache_gc import purge_job_play_cache

                purge_job_play_cache(r, job_id, reason="cancelled")
            except Exception:
                log.exception("purge redis/oss after cancel failed for %s", job_id)
            set_job_fields(
                r, job_id,
                status="cancelled",
                cancel=1,
                playable=0,
                frames_done=0,
                play_base="",
                cache_bytes=0,
                source=redis_path,
                error="cancelled by client",
                finished_at=f"{time.time():.6f}",
            )
        except Exception:
            log.exception("Failed to finalize cancel for %s", job_id)
        log.info("Job cancelled: %s", job_id)
        if on_playable is not None and not playable_notified[0]:
            on_playable(job_id)
    except LostJobLock:
        try:
            _clear_job_cache(job_id)
            set_job_fields(
                r, job_id,
                status="failed",
                playable=0,
                source=redis_path,
                error="lost job lock (another owner or TTL expired)",
                finished_at=f"{time.time():.6f}",
            )
        except Exception:
            log.exception("Failed to finalize lost-lock for %s", job_id)
        log.error("Lost job lock, stopped: %s", job_id)
        lock_held = False  # do not delete another pod's lock
        if on_playable is not None and not playable_notified[0]:
            on_playable(job_id)
    except Exception as exc:
        try:
            set_job_fields(
                r, job_id,
                status="failed",
                source=redis_path,
                error=str(exc)[:500],
                finished_at=f"{time.time():.6f}",
            )
        except Exception:
            pass
        log.exception("Failed processing: %s", redis_path)
        # Unblock scheduler so a failed pre-playable job does not stall the queue.
        if on_playable is not None and not playable_notified[0]:
            on_playable(job_id)
    finally:
        renewer.stop()
        try:
            encode_gate.unregister(job_id)
        except Exception:
            log.exception("[%s] encode-gate unregister failed", job_id)
        try:
            if getattr(_tls, "encode_job_id", None) == job_id:
                _tls.encode_job_id = None
        except Exception:
            pass
        if lock_held:
            try:
                release_job_lock(r, job_id)
            except Exception:
                log.exception("Failed to release lock for %s", job_id)


def run_redis_loop():
    r = get_redis()
    if (not NO_UPLOAD) or OSS_CACHE:
        get_oss_bucket()
    log.info("Redis queue mode: listening on '%s'  (head_cam=%s  orin=%s)",
            REDIS_QUEUE_KEY, HEAD_CAM_MOUNT, ORIN_MOUNT)
    log.info(
        "Concurrency: MAX_INFLIGHT_JOBS=%d OVERLAP_AFTER_PLAYABLE=%s "
        "CPU_CONCURRENCY=%d ENCODE_JOB_MAX=%d ENCODE_PLAYABLE_MAX=%d "
        "X264_THREADS>=%d budget=%d (live 4-cam ≈%d threads/ffmpeg)",
        MAX_INFLIGHT_JOBS,
        OVERLAP_AFTER_PLAYABLE,
        CPU_CONCURRENCY,
        ENCODE_JOB_MAX_CONCURRENT,
        ENCODE_PLAYABLE_MAX_CONCURRENT,
        X264_THREADS,
        X264_THREAD_BUDGET,
        max(X264_THREADS, X264_THREAD_BUDGET // 4),
    )
    if ENCODE_PLAYABLE_MAX_CONCURRENT < 4:
        log.warning(
            "ENCODE_PLAYABLE_MAX_CONCURRENT=%d < 4: 4-cam jobs may fall below "
            "realtime after playable (cameras encode one-at-a-time)",
            ENCODE_PLAYABLE_MAX_CONCURRENT,
        )
    log.info(
        "Role=%s OSS_CACHE=%s prefix=%s",
        SVIEW_ROLE, int(OSS_CACHE), OSS_CACHE_PREFIX,
    )
    if NO_UPLOAD:
        log.info("Output: local cache %s  (NO_UPLOAD=1, HTTP :%s)", LOCAL_OUTPUT_DIR, HTTP_PORT)
    else:
        log.info("Output: oss://%s/%s/%s/", OSS_BUCKET, OSS_OUTPUT_PREFIX, OSS_OUTPUT_SUB)
    if OSS_CACHE:
        log.info("OSS play cache: %s", oss_play_base("{job_id}"))

    from job_status import POD_IP as _POD, parse_queue_item, set_worker_heartbeat

    lock = threading.Lock()
    active: dict = {}  # job_id -> {"playable": bool, "thread": Thread}
    wake = threading.Event()
    worker_id = f"{_POD}-{os.getpid()}"
    last_hb = [0.0]

    def _heartbeat():
        now = time.time()
        if now - last_hb[0] < 15:
            return
        last_hb[0] = now
        with lock:
            jobs = list(active.keys())
            n = len(active)
        try:
            set_worker_heartbeat(
                r,
                worker_id,
                processing=n,
                max_inflight=MAX_INFLIGHT_JOBS,
                jobs=jobs,
                extra={"role": SVIEW_ROLE, "oss_cache": int(OSS_CACHE)},
            )
        except Exception:
            log.exception("worker heartbeat failed")

    def _on_playable(job_id: str):
        with lock:
            info = active.get(job_id)
            if info is not None:
                info["playable"] = True
            playable_n = sum(1 for i in active.values() if i.get("playable"))
            inflight = len(active)
        log.info(
            "Overlap unlock: job %s playable (playable=%d inflight=%d/%d)",
            job_id, playable_n, inflight, MAX_INFLIGHT_JOBS,
        )
        wake.set()

    def _reap_finished():
        with lock:
            done = [
                jid for jid, info in active.items()
                if not info["thread"].is_alive()
            ]
            for jid in done:
                active.pop(jid, None)
            return len(done)

    def _can_start_more() -> bool:
        with lock:
            # Drop dead threads first so a finished job cannot block the queue.
            dead = [jid for jid, info in active.items() if not info["thread"].is_alive()]
            for jid in dead:
                active.pop(jid, None)
            n = len(active)
            if n >= MAX_INFLIGHT_JOBS:
                return False
            if n == 0:
                return True
            if not OVERLAP_AFTER_PLAYABLE:
                return False
            # Start next once *every* currently running job is already playable
            # (or only one running and it is playable). Avoids piling a 3rd job
            # while an early job is still warming up.
            return all(info.get("playable") for info in active.values())

    while True:
        try:
            _reap_finished()
            _heartbeat()
            if _can_start_more():
                result = r.blpop(REDIS_QUEUE_KEY, timeout=1)
                if result is None:
                    continue

                _, raw_item = result
                try:
                    item = parse_queue_item(raw_item)
                except Exception:
                    log.exception("Invalid queue item: %r", raw_item)
                    continue

                job_id = item["job_id"]

                # Same job_id already running: drop duplicate so we do not
                # overwrite active[job_id] / steal the slot.
                with lock:
                    dup = job_id in active and active[job_id]["thread"].is_alive()
                if dup:
                    log.warning(
                        "Job %s already inflight — drop duplicate queue item",
                        job_id,
                    )
                    continue

                def _thread_main(it=item, jid=job_id):
                    try:
                        _run_one_queue_job(r, it, on_playable=_on_playable)
                    finally:
                        with lock:
                            active.pop(jid, None)
                        wake.set()

                t = threading.Thread(
                    target=_thread_main,
                    name=f"sview-job-{job_id[:48]}",
                    daemon=True,
                )
                with lock:
                    active[job_id] = {"playable": False, "thread": t}
                    inflight_now = len(active)
                log.info(
                    "Started job thread %s (inflight=%d/%d)",
                    job_id, inflight_now, MAX_INFLIGHT_JOBS,
                )
                t.start()
            else:
                # Wait for playable/finished; timed wake keeps reaping healthy.
                wake.wait(timeout=0.5)
                wake.clear()

        except redis.exceptions.ConnectionError:
            log.warning("Redis connection lost, retrying in 10s...")
            time.sleep(10)
            try:
                global _redis_client
                _redis_client = None
                r = get_redis()
            except Exception:
                pass
        except KeyboardInterrupt:
            log.info("Shutting down.")
            break
        except Exception:
            log.exception("Unexpected error in Redis loop")
            time.sleep(10)


# ---------------------------------------------------------------------------
#  Entry point
# ---------------------------------------------------------------------------

def _start_http_server():
    """Serve LOCAL_OUTPUT_DIR on HTTP_HOST:HTTP_PORT (see sview_http.py)."""
    from sview_http import start_sview_http_server
    return start_sview_http_server(LOCAL_OUTPUT_DIR, host=HTTP_HOST, port=HTTP_PORT)


def main():
    parser = argparse.ArgumentParser(
        description="SView worker (CPU libx264 + NAL AU split, Redis queue, HTTP cache)")
    parser.add_argument("--path", help="Process a single mcap file")
    parser.add_argument("--no-upload", action="store_true", dest="no_upload",
                        help="Skip OSS upload; save output files to local output/ directory")
    parser.add_argument("--serve", action="store_true",
                        help="Start HTTP cache server (default on in Redis queue mode)")
    parser.add_argument("--no-serve", action="store_true",
                        help="Do not start HTTP cache server")
    parser.add_argument("--serve-only", action="store_true",
                        help="Only run HTTP cache server (no Redis / no --path processing)")
    parser.add_argument("--scale", type=int, default=None,
                        help="Square output size, e.g. 720/480. "
                             "0 keeps source (env TRANSCODE_SIZE). Overrides WIDTH/HEIGHT.")
    parser.add_argument("--width", type=int, default=None,
                        help="Output width; 0 = keep source (env TRANSCODE_WIDTH)")
    parser.add_argument("--height", type=int, default=None,
                        help="Output height; 0 = keep source (env TRANSCODE_HEIGHT)")
    parser.add_argument("--cq", type=int, default=None,
                        help="libx264 CRF 0-51 (alias of --crf; env TRANSCODE_CRF / TRANSCODE_CQ)")
    parser.add_argument("--crf", type=int, default=None,
                        help="libx264 CRF 0-51, higher = lower quality / smaller (env TRANSCODE_CRF)")
    parser.add_argument("--bitrate", default=None,
                        help="Target bitrate, e.g. 1M/2M (env TRANSCODE_BITRATE)")
    parser.add_argument("--maxrate", default=None,
                        help="Max bitrate, e.g. 2M/3M (env TRANSCODE_MAXRATE)")
    parser.add_argument("--bufsize", default=None,
                        help="VBV buffer size, e.g. 4M (env TRANSCODE_BUFSIZE)")
    parser.add_argument("--preset", default=None,
                        help="libx264 preset, e.g. ultrafast/veryfast/medium (env TRANSCODE_PRESET)")
    parser.add_argument("--force-reencode", action="store_true", default=None,
                        help="Force libx264 re-encode even for H.264 + no resize "
                             "(env TRANSCODE_FORCE_REENCODE=1)")
    parser.add_argument("--no-force-reencode", action="store_true",
                        help="Allow H.264 passthrough when resolution unchanged")
    args = parser.parse_args()

    global TRANSCODE_SIZE, TRANSCODE_WIDTH, TRANSCODE_HEIGHT
    global TRANSCODE_CRF, TRANSCODE_BITRATE, TRANSCODE_MAXRATE, TRANSCODE_BUFSIZE
    global TRANSCODE_PRESET, TRANSCODE_FORCE_REENCODE
    if args.scale is not None:
        TRANSCODE_SIZE = max(0, args.scale)
    if args.width is not None:
        TRANSCODE_WIDTH = max(0, args.width)
    if args.height is not None:
        TRANSCODE_HEIGHT = max(0, args.height)
    crf_val = args.crf if args.crf is not None else args.cq
    if crf_val is not None:
        TRANSCODE_CRF = max(0, min(51, crf_val))
    if args.bitrate is not None:
        TRANSCODE_BITRATE = args.bitrate
    if args.maxrate is not None:
        TRANSCODE_MAXRATE = args.maxrate
        # Keep bufsize roughly in line with maxrate when overridden via CLI
        if args.bitrate and not os.environ.get("TRANSCODE_BUFSIZE") and args.bufsize is None:
            TRANSCODE_BUFSIZE = args.maxrate
    if args.bufsize is not None:
        TRANSCODE_BUFSIZE = args.bufsize
    if args.preset is not None:
        TRANSCODE_PRESET = args.preset
    if args.no_force_reencode:
        TRANSCODE_FORCE_REENCODE = False
    elif args.force_reencode:
        TRANSCODE_FORCE_REENCODE = True

    log.info(
        "Effective transcode config: size=%s crf=%s bitrate=%s maxrate=%s "
        "bufsize=%s preset=%s force_reencode=%s",
        _transcode_size_desc(), TRANSCODE_CRF, TRANSCODE_BITRATE,
        TRANSCODE_MAXRATE, TRANSCODE_BUFSIZE, TRANSCODE_PRESET,
        TRANSCODE_FORCE_REENCODE,
    )

    from job_status import POD_IP, SVIEW_CLUSTER, ensure_cluster_identity

    ensure_cluster_identity(fatal=True)
    log.info("Cluster mode=%s pod_ip=%s role=%s oss_cache=%s",
             SVIEW_CLUSTER, POD_IP, SVIEW_ROLE, int(OSS_CACHE))

    role = SVIEW_ROLE
    if args.serve_only:
        role = "master"

    if role not in ("all", "master", "worker"):
        raise SystemExit(f"Invalid SVIEW_ROLE={SVIEW_ROLE!r}; use all|master|worker")

    if role == "master":
        # API / enqueue only — workers consume Redis.
        if _start_http_server() is None:
            raise SystemExit("HTTP server disabled (set HTTP_PORT>0)")
        log.info("Master mode: HTTP API only (no encode). Queue=%s", REDIS_QUEUE_KEY)
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            log.info("Shutting down.")
        return

    if args.serve_only:
        if _start_http_server() is None:
            raise SystemExit("HTTP server disabled (set HTTP_PORT>0)")
        log.info("Serve-only mode; Ctrl+C to exit")
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            log.info("Shutting down.")
        return

    # Worker or all: Redis consumer. HTTP optional.
    env_off = os.environ.get("HTTP_ENABLED", "1").lower() in ("0", "false", "no")
    if role == "worker":
        # Encode-only by default; set HTTP_ENABLED=1 or --serve to also expose HTTP.
        want_http = (not args.no_serve) and (
            args.serve
            or os.environ.get("HTTP_ENABLED", "0").lower() in ("1", "true", "yes", "on")
        )
    else:
        want_http = (not args.no_serve) and (not env_off) and (args.serve or not args.path)

    if want_http:
        _start_http_server()

    if args.path:
        local_path = _resolve_path(args.path)
        process_mcap(str(local_path), no_upload=args.no_upload)
        if args.serve and not args.no_serve:
            log.info("Processing done; HTTP still serving %s  Ctrl+C to exit", LOCAL_OUTPUT_DIR)
            try:
                threading.Event().wait()
            except KeyboardInterrupt:
                log.info("Shutting down.")
    else:
        if role == "worker":
            log.info("Worker mode: consuming Redis queue (encode → local + OSS cache)")
        run_redis_loop()


def _scale_dim(metas: dict, which: str) -> int:
    """Output width/height for transcode + manifest.

    Priority:
      1. TRANSCODE_SIZE > 0 → square (SIZE x SIZE)
      2. TRANSCODE_WIDTH / TRANSCODE_HEIGHT if set for that axis
      3. Source camera meta
      4. Fallback 2096
    """
    size = _tc_size()
    if size > 0:
        return size
    if which == "w" and _tc_width() > 0:
        return _tc_width()
    if which == "h" and _tc_height() > 0:
        return _tc_height()
    key = "width" if which == "w" else "height"
    for ch in ("RR", "FR", "RL", "FL"):
        if ch in metas and metas[ch].get(key):
            return int(metas[ch][key])
    # Fallback when meta not yet available
    return 2096


def _build_streams_manifest(metas: dict, fallback_w: int, fallback_h: int) -> dict:
    """Build streams section of manifest; width/height are the transcoded output size."""
    result = {}
    for ch in ("RR", "FR", "RL", "FL"):
        key = f"camera_{ch.lower()}"
        if ch in metas:
            m = metas[ch]
            result[key] = {
                "position": m.get("position", ch),
                "width": fallback_w,
                "height": fallback_h,
                "fps": m["fps"],
            }
        else:
            result[key] = {
                "position": ch,
                "width": fallback_w,
                "height": fallback_h,
                "fps": 30,
            }
    return result


if __name__ == "__main__":
    main()
