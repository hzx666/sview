#!/usr/bin/env python3
"""
用 CPU 跑的 SView worker：libx264 把 HEVC 转成 H.264，再按 NAL 切 AU，带 Redis 队列和 HTTP 缓存。

配合 sview_http.py。用 Annex-B NAL 扫描把 H.264 切成逐帧 AU
（不用 ffprobe）。编码走 libx264（无 CUDA/NVENC）。

输入格式（先看路径族，再嗅探编码）：
  - ``head_cam`` / SSIEGO：protobuf ``/camera/*/video``（H.264/HEVC AU）
  - ``teleoperation/{Orin,Agilex,Astribot,Franka,Unitree}``：ROS2 JPEG / H.264
    或 HDF5（Astribot）。bundle_frame.v2 里用原始 topic 名作流名。
    不会回退到头显 protobuf。
  - 未知路径（CLI 拷贝）：先探测 Orin，再试 protobuf。

两种模式：
--path <file>         处理单个 mcap。
(no --path)           轮询 Redis 队列，取源路径或 bench JSON 任务。

配置全部来自环境变量（方便挂 K8s Secret）。

分辨率 / 质量（环境变量或 CLI）：
  TRANSCODE_SIZE / --scale           正方形缩放；默认 1080（0 = 保持源分辨率）
  TRANSCODE_WIDTH / --width          输出宽；0 = 保持源
  TRANSCODE_HEIGHT / --height        输出高；0 = 保持源
  TRANSCODE_CRF / --crf              libx264 CRF 0-51（默认 28；别名 TRANSCODE_CQ）
  TRANSCODE_BITRATE / --bitrate      ABR 目标；0/空 = CRF+VBV（推荐）
  TRANSCODE_MAXRATE / --maxrate      VBV 上限（默认 1.5M）
  TRANSCODE_BUFSIZE / --bufsize      VBV 缓冲（默认 3M）
  TRANSCODE_PRESET / --preset        默认 ultrafast（要速度）；veryfast 画质更好
  TRANSCODE_FORCE_REENCODE=1         强制重编码（默认开；关掉 H.264 直通）
  X264_THREADS=2                     每个 ffmpeg 最少线程（Pod 忙时的下限）
  X264_THREAD_BUDGET=16              Pod 上 x264 线程总数；四路直播约 budget/4
  PROGRESSIVE_PUBLISH=1              帧一生成就写入 LOCAL_OUTPUT_DIR
  PARTIAL_MANIFEST_EVERY=30          每 N 帧刷新一次 partial 清单
  PLAYABLE_MIN_FRAMES=30             API/前端判定可播的帧数门槛
  MAX_INFLIGHT_JOBS=3                Redis 模式下同时跑的 process_mcap 上限
  OVERLAP_AFTER_PLAYABLE=1           每个在跑任务都可播后再开下一个
  CPU_CONCURRENCY=12                 本 Pod 同时跑的 ffmpeg 上限
  ENCODE_JOB_MAX_CONCURRENT=4        单任务 ffmpeg 硬上限（12/4 → 3 个任务占满 Pod）
  ENCODE_PLAYABLE_MAX_CONCURRENT=4   可播后仍保持四路并行（1 = 串行，低于实时）
  ENCODE_FOREGROUND_RESERVE=0        留给还没可播任务的槽（0=自动约 25%）
  LIVE_ENCODE=1                      每路相机一个常驻 ffmpeg；槽占到任务结束
  ORIN_DIRECT=1                      JPEG 仅 CRF、不加 ABR（默认 0，与头显同一套 TRANSCODE_*）
  LOG_FILE=sview.log                 工作目录下的项目日志（各模块共用）
  LOG_PROGRESS_EVERY_PCT=5           任务进度日志步长（百分比）；0=每次回调都打
  SVIEW_ROLE=all|master|worker       master=只 HTTP；worker=编码；all=两者都做
  OSS_CACHE=1                        边转边播镜像到 OSS 播放缓存
  OSS_CACHE_PREFIX=player-bin        播放缓存的 OSS key 前缀
  OSS_PUBLIC_BASE=https://bucket.oss-cn-shanghai.aliyuncs.com
  SEGMENT_FRAMES=30                  OSS/HTTP 分段大小
  CACHE_MAX_BYTES=536870912000       OSS 播放缓存上限（约 500GiB）
  CACHE_IDLE_TTL_SEC=14400           空闲任务淘汰（4 小时）
  CACHE_GC_INTERVAL_SEC=300          master 的 GC 循环间隔
"""

import os
import subprocess
import time
import threading
import tempfile
import sys
import argparse
import logging
import struct
from contextlib import contextmanager
from pathlib import Path
from collections import defaultdict

import redis
from sview.ingest.mcap_protobuf_compat import (
    read_headcam_video_frame_count,
    read_protobuf_messages_compat as read_protobuf_messages,
)
import json

from sview.logging_setup import setup_logging
from sview.ingest.stream_ids import stream_file_slug
from sview.jobs.bundle_frame import (
    HEADCAM_CHANNEL_SET,
    HEADCAM_CHANNELS,
    build_streams_manifest,
    manifest_format_for,
    pack_bundle_frame,
)

log = setup_logging("sview_worker")

# ---------------------------------------------------------------------------
#  配置（全部来自环境变量）
# ---------------------------------------------------------------------------

# -- Redis --
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_USERNAME = os.environ.get("REDIS_USERNAME") or None  # 阿里云 Redis ACL
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_QUEUE_KEY = os.environ.get("REDIS_QUEUE_KEY", "player:queue")
# -- 本地挂载（K8s YAML 把 OSS 前缀挂到这里；见 path_map.py） --
# 优先 HEAD_CAM_MOUNT / ORIN_MOUNT；LOCAL_MOUNT_PATH 只为兼容旧配置。
from sview.path_map import (
    HEAD_CAM_MOUNT,
    ORIN_MOUNT,
    redis_path_to_local,
    source_to_local,
    classify_source,
    ingest_family,
    teleop_robot,
)

LOCAL_MOUNT_PATH = HEAD_CAM_MOUNT  # 旧别名（默认 /head-data）
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
# 为 true 时不把成品上传到 processed_data/，只走本地缓存。
NO_UPLOAD = os.environ.get("NO_UPLOAD", "0").lower() in ("1", "true", "yes")
# 角色：all（默认）| master（只 HTTP + 入队）| worker（消费 Redis + 编码）
SVIEW_ROLE = (os.environ.get("SVIEW_ROLE", "all") or "all").strip().lower()
# 把本地边转边播的帧镜像到 OSS，作为共享播放缓存（分段 + 清单）。
OSS_CACHE = os.environ.get("OSS_CACHE", "0").lower() in ("1", "true", "yes", "on")
OSS_CACHE_PREFIX = (os.environ.get("OSS_CACHE_PREFIX", "player-bin") or "player-bin").strip("/")
# 默认关闭：多数桶开了 Block Public Access。播放走 Master
# 的本地或签名跳转，不走匿名的 play_base GET。
OSS_CACHE_PUBLIC = os.environ.get("OSS_CACHE_PUBLIC", "0").lower() in ("1", "true", "yes", "on")
# 第一次出现 "public ACL not allowed" 后闩住，避免每个对象重复 PUT。
_oss_public_acl_blocked = False

# 浏览器访问的根地址，例如 https://bucket.oss-cn-shanghai.aliyuncs.com
# 为空时由 OSS_BUCKET + OSS_PUBLIC_ENDPOINT 拼出来。
OSS_PUBLIC_BASE = (os.environ.get("OSS_PUBLIC_BASE") or "").rstrip("/")
_oss_public_endpoint = os.environ.get("OSS_PUBLIC_ENDPOINT", "oss-cn-shanghai.aliyuncs.com")
if _oss_public_endpoint.startswith("https://"):
    _oss_public_endpoint = _oss_public_endpoint[len("https://"):]
elif _oss_public_endpoint.startswith("http://"):
    _oss_public_endpoint = _oss_public_endpoint[len("http://"):]
OSS_PUBLIC_ENDPOINT = _oss_public_endpoint
# 缓存分段大小（OSS 对象，与 HTTP 的 SEGMENT_FRAMES / gop 对齐）。
try:
    SEGMENT_FRAMES = max(1, int(os.environ.get("SEGMENT_FRAMES", "30") or "30"))
except (TypeError, ValueError):
    SEGMENT_FRAMES = 30

# -- 本地输出（--no-upload 时使用） --
LOCAL_OUTPUT_DIR = Path(os.environ.get("LOCAL_OUTPUT_DIR", str(Path(__file__).parent / "output")))
LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -- HTTP 缓存服务（对外提供 LOCAL_OUTPUT_DIR） --
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))  # 0 = 关闭

# -- ffmpeg 与 CPU 编码 --
_default_ffmpeg = "ffmpeg"
if sys.platform == "win32":
    _default_ffmpeg = r"C:\Users\10279\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1.2-full_build\bin\ffmpeg.exe"
FFMPEG = os.environ.get("FFMPEG_PATH", _default_ffmpeg)

CPU_CONCURRENCY = int(os.environ.get("CPU_CONCURRENCY", os.environ.get("GPU_CONCURRENCY", "12")))
# 单任务 ffmpeg 硬上限（例如 12 槽 / 每任务 4 → 最多 3 个任务占满 Pod）。
try:
    ENCODE_JOB_MAX_CONCURRENT = max(
        1, int(os.environ.get("ENCODE_JOB_MAX_CONCURRENT", "4") or "4")
    )
except ValueError:
    ENCODE_JOB_MAX_CONCURRENT = 4
# 任务可播后，可以把槽让给更新的、还没可播的任务。
# 四路实时必须保持 >= 4：只给 1 个槽会把 4 路 ffmpeg 串起来，发布速率掉到
# ~30fps 以下，播放头就会追上编码前沿。
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
# 第一个可播块之后的发布间隔。30 就是 30fps 下 1 个 GOP / 1 秒，
# 播放端看到的前沿是稳的，不会先冲 5 秒再卡住。
CHUNK_FRAMES = int(os.environ.get("CHUNK_FRAMES", "30"))
# 边转边播块大小（Orin JPEG→H.264，越小首帧可播越早）。
ORIN_CHUNK_FRAMES = max(1, int(os.environ.get("ORIN_CHUNK_FRAMES", "30")))
# 整个任务都占着每路相机的编码槽，免得可播之后被
# 收成 1 路串行（发布速率会掉到 ~30fps 以下）。
LIVE_ENCODE = os.environ.get("LIVE_ENCODE", "1").lower() in ("1", "true", "yes", "on")


class FairEncodeGate:
    """全 Pod 的 ffmpeg 槽门闩：单任务有上限，可播后再限流。

    1) 每个任务硬顶 ENCODE_JOB_MAX_CONCURRENT（再加均分）。
    2) 已可播任务再限到 ENCODE_PLAYABLE_MAX_CONCURRENT
       （默认 4，四路保持并行、跟得上实时），也不能占掉
       前台预留。
    当前能通过 _grant_ok 的等待者，按 FIFO 发放。
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
        # 自动：给还没可播的任务留约 25%（至少 1 个）。
        return max(1, self.capacity // 4)

    def _job_limit(self, job_id: str) -> int:
        n = max(1, len(self._jobs))
        fair = max(1, self._math.ceil(self.capacity / n))
        hard = min(self.capacity, self.job_max)
        st = self._jobs.get(job_id)
        if st is None:
            return min(hard, fair)
        if st.get("playable"):
            # 只有还没可播的任务要槽时，才限流已经可播的任务。
            # 保持 playable_max（默认 4），四路继续并行 — 只给 1 个槽
            # 会把各路串行，发布速率低于实时（~30fps）。
            if self._fg_ids():
                return min(self.playable_max, hard, fair)
            return min(hard, fair)
        fg = self._fg_ids()
        # 前台任务之间的份额（通常比后台大）。
        fg_fair = max(1, self._math.ceil(self.capacity / max(1, len(fg))))
        return min(hard, self.capacity, max(fair, fg_fair))

    def _grant_ok(self, job_id: str) -> bool:
        if self._total_holding() >= self.capacity:
            return False
        if job_id not in self._jobs:
            # 未登记的调用方（脚本）：只受全局容量限制。
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
        """当前已发放的 ffmpeg 槽数（用来算 x264 线程）。"""
        with self._cv:
            return self._total_holding()

    def acquire(self, job_id: str | None, timeout: float | None = None) -> None:
        jid = (job_id or "").strip() or "_anon_"
        ready = threading.Event()
        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        with self._cv:
            if jid not in self._jobs and jid != "_anon_":
                self._jobs[jid] = {"playable": False, "holding": 0}
            self._waiters.append((jid, ready))
            try:
                while True:
                    # 发给当前允许运行、排队最早的等待者。
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
                    remaining = None
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError(
                                f"encode gate wait exceeded {float(timeout):.0f}s"
                            )
                    self._cv.wait(
                        timeout=0.5 if remaining is None else min(0.5, remaining)
                    )
            except BaseException:
                try:
                    self._waiters.remove((jid, ready))
                except ValueError:
                    pass
                raise

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
# 兼容旧压测：它们会 monkeypatch cpu_encode_lock。
cpu_encode_lock = encode_gate

# -- 转码分辨率与质量（环境变量 / CLI 都可覆盖） --
# 分辨率：
#   正方形输出：TRANSCODE_SIZE > 0（例如 720 → 720x720）
#   矩形输出：TRANSCODE_WIDTH/HEIGHT（只设一边时，另一边跟源）
#   全为 0                 → 保持相机源分辨率
# 质量（ffmpeg 重编码时生效）：
#   参数有 TRANSCODE_CRF / TRANSCODE_CQ、TRANSCODE_BITRATE、TRANSCODE_MAXRATE、
#   还有 TRANSCODE_BUFSIZE、TRANSCODE_PRESET
# 即使已是 H.264 且不缩放，TRANSCODE_FORCE_REENCODE=1 仍重编码，质量
#   参数才会生效（默认 0 = 尺寸不变时 H.264 直通）。

def _env_bool(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: str = "0") -> int:
    raw = os.environ.get(name, default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


# 与头显 / Orin H.264 一样，Orin 的 JPEG→H.264 也用这套 TRANSCODE_*。
# 设 ORIN_DIRECT=1 时，JPEG 只走 CRF（不加 ABR），仍应用 TRANSCODE_SIZE + TRANSCODE_CRF。
ORIN_DIRECT = _env_bool("ORIN_DIRECT", "0")
ORIN_CRF = max(1, min(51, _env_int("ORIN_CRF", "23")))  # 未使用；留给环境变量兼容

# 高清压缩默认：1080 正方形 + CRF28 + 强制重编码（不走 H.264 直通）。
# 设 TRANSCODE_SIZE=0 保持源分辨率；TRANSCODE_BITRATE=0 → CRF+VBV（不加 ABR）。
TRANSCODE_SIZE = max(0, _env_int("TRANSCODE_SIZE", "1080"))
TRANSCODE_WIDTH = max(0, _env_int("TRANSCODE_WIDTH", "0"))
TRANSCODE_HEIGHT = max(0, _env_int("TRANSCODE_HEIGHT", "0"))
# 优先 TRANSCODE_CRF；没有再用 TRANSCODE_CQ，与 GPU 版的 CLI / 环境变量对齐。
TRANSCODE_CRF = max(0, min(51, _env_int("TRANSCODE_CRF", os.environ.get("TRANSCODE_CQ", "28"))))
TRANSCODE_BITRATE = os.environ.get("TRANSCODE_BITRATE", "0")  # 0/空 = CRF 模式
TRANSCODE_MAXRATE = os.environ.get("TRANSCODE_MAXRATE", "1.5M")
TRANSCODE_BUFSIZE = os.environ.get("TRANSCODE_BUFSIZE", "3M")
TRANSCODE_PRESET = os.environ.get("TRANSCODE_PRESET", "ultrafast")  # 预设（libx264）
TRANSCODE_FORCE_REENCODE = _env_bool("TRANSCODE_FORCE_REENCODE", "1")
# 限制每个 ffmpeg 的线程，避免 N 路同时编码把 Pod 打满。
try:
    X264_THREADS = max(1, int(os.environ.get("X264_THREADS", "2") or "2"))
except ValueError:
    X264_THREADS = 2
# 所有在途 x264 线程的软预算。四路直播（holding=4）
# 各分 budget/4，单个边转边播任务能吃满 Pod，不必停在 2×4=8。
try:
    X264_THREAD_BUDGET = max(
        X264_THREADS,
        int(os.environ.get("X264_THREAD_BUDGET", "0") or "0"),
    )
except ValueError:
    X264_THREAD_BUDGET = 0
if X264_THREAD_BUDGET <= 0:
    X264_THREAD_BUDGET = max(CPU_CONCURRENCY, X264_THREADS * 4)

# -- 边转边播（路径 A） --
# 同时开 NO_UPLOAD 和 PROGRESSIVE_PUBLISH：帧和清单写入 LOCAL_OUTPUT_DIR，
# 每帧生成后 HTTP 就能读，不必等任务完成。
PROGRESSIVE_PUBLISH = _env_bool("PROGRESSIVE_PUBLISH", "1")
PARTIAL_MANIFEST_EVERY = max(1, _env_int("PARTIAL_MANIFEST_EVERY", "30"))
# 可用帧数达到这个值（Redis/API 报出）后，前端就可以起播。
PLAYABLE_MIN_FRAMES = max(1, _env_int("PLAYABLE_MIN_FRAMES", "30"))

# -- Redis worker 并发（首个任务可播后重叠） --
MAX_INFLIGHT_JOBS = max(1, _env_int("MAX_INFLIGHT_JOBS", "3"))
OVERLAP_AFTER_PLAYABLE = _env_bool("OVERLAP_AFTER_PLAYABLE", "1")
# 进度日志：百分比跨过这个步长才打（5 → 0/5/10/…/100）。
# 设 0 则每次 progress_cb 都打（会很吵）。
LOG_PROGRESS_EVERY_PCT = max(0.0, float(os.environ.get("LOG_PROGRESS_EVERY_PCT", "5") or "5"))


def _progress_pct(done: int, total: int) -> float:
    if total and int(total) > 0:
        return min(100.0, 100.0 * float(done) / float(total))
    return 0.0


class _JobProgressLogger:
    """按任务节流进度日志，并发时才读得下去。"""

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


# 压测任务的线程级覆盖（并发 process_mcap 时互不影响）。
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
    """环境变量、CLI 或线程覆盖要求改输出分辨率时为真。"""
    return _tc_size() > 0 or _tc_width() > 0 or _tc_height() > 0


def _apply_output_scale(camera_metas: dict) -> tuple[int | None, int | None]:
    """把 TRANSCODE_SIZE/W/H 写进 camera_metas；返回 (w, h)，没有则 (None, None)。"""
    if not _resize_requested() or not camera_metas:
        return None, None
    sw = _scale_dim(camera_metas, "w")
    sh = _scale_dim(camera_metas, "h")
    for ch in camera_metas:
        camera_metas[ch]["width"] = sw
        camera_metas[ch]["height"] = sh
    return sw, sh


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
    """只给当前线程套上本任务的转码参数。"""
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


# -- 临时工作目录 --
TEMP_DIR = Path(os.environ.get("TEMP_DIR", tempfile.gettempdir())) / "sview_worker"
TEMP_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
#  OSS
# ---------------------------------------------------------------------------

_oss_bucket = None
_oss_sign_bucket = None

def get_oss_bucket():
    """给 Pod 访问 OSS 用的 SDK 客户端（可以用 VPC 内网端点）。"""
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
    """给浏览器签 URL 的 SDK 客户端（必须用公网端点）。"""
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
    """浏览器拉清单和分段用的 HTTPS 前缀，指向 OSS 缓存。"""
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
            from sview.jobs.job_status import register_cache_bytes

            register_cache_bytes(get_redis(), job_id, len(data))
        except Exception:
            log.exception("[%s] failed to register cache_bytes", job_id)


def sign_oss_url(oss_key: str, expires: int = 7200) -> str:
    """给浏览器的预签名 HTTPS GET — 永远用公网端点，不用 *-internal*。

    必须用公网主机签名。签完再改主机，按 OSS 的鉴权版本
    可能把签名签废。
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


def _oss_cache_upload_segment(job_id: str, start: int, end: int, frame_dir: Path) -> bool:
    """把帧 [start, end) 作为一个分段上传。只有 OSS PUT 成功才返回 True。"""
    if end <= start:
        return False
    blobs: list[bytes] = []
    for i in range(start, end):
        path = frame_dir / f"{i:06d}.bin"
        if not path.is_file():
            log.warning("[%s] oss-cache skip segment %d: missing %s", job_id, start, path.name)
            return False
        blobs.append(path.read_bytes())
    data = _pack_segment_bytes(start, blobs, SEGMENT_FRAMES)
    key = oss_cache_key(job_id, "segments", f"{start:06d}.bin")
    upload_bytes_to_oss(key, data, job_id=job_id)
    return True


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
    """本地分段完成后镜像到 OSS，再写进清单。

    分段 PUT 成功之后才上传清单，这样
    frames_available 不会报 OSS 上还没有的帧。
    """
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
    uploaded = False
    try:
        uploaded = _oss_cache_upload_segment(job_id, start, end, frame_dir)
    except Exception:
        log.exception("[%s] oss-cache segment upload failed [%d:%d)", job_id, start, end)
        uploaded = False

    if manifest is None:
        return
    if not uploaded:
        log.warning(
            "[%s] oss-cache skip manifest: segment [%d:%d) not on OSS",
            job_id,
            start,
            end,
        )
        return

    man = dict(manifest)
    bundle = dict(man.get("bundle") or {})
    bundle["frames_available"] = int(end)
    man["bundle"] = bundle

    for attempt in range(2):
        try:
            _oss_cache_upload_manifest(job_id, man)
            return
        except Exception:
            log.exception(
                "[%s] oss-cache manifest upload failed (attempt %d)",
                job_id,
                attempt + 1,
            )


def _oss_cache_publish_done(
    job_id: str,
    frames_done: int,
    frame_dir: Path,
    manifest: dict | None,
) -> None:
    """本地 status=done 之后，推最后一段（可能不满）和清单。

    循环里只在 SEGMENT_FRAMES 边界，或
    frames_done == min_frames 时才刷。若 min_frames 用的是 mcap 摘要帧数，
    实际发布更少，尾巴就一直留在 worker 上。
    """
    if not OSS_CACHE or frames_done <= 0 or manifest is None:
        return
    _oss_cache_maybe_flush(job_id, frames_done, frames_done, frame_dir, manifest)


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
#  路径映射（OSS URI / 旧队列路径 -> 本地挂载；见 path_map.py）
# ---------------------------------------------------------------------------

def local_path_to_oss_base(local_path: Path) -> str:
    """从本地源路径推出输出 key 前缀（给可选的 OSS 上传用）。"""
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
    # .../{job_id}/sview  — 帧和清单都在这个前缀下
    return f"{OSS_OUTPUT_PREFIX}/{OSS_OUTPUT_SUB}/{rel}/sview"


# ---------------------------------------------------------------------------
#  转码（CPU / libx264）
# ---------------------------------------------------------------------------

def _rate_control_empty(value: str | None) -> bool:
    v = (value or "").strip().lower()
    return (not v) or v in ("0", "none", "crf", "-")


def _x264_threads_now() -> int:
    """按已发放的 ffmpeg 槽均分 x264 线程。

    下限是 X264_THREADS。单独一个四路直播任务（holding≈4）大约分到
    X264_THREAD_BUDGET/4，边转边播就能用上门闩留给
    还没跑起来的重叠任务的空闲核。
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
    """跳过解复用探测；格式已经用 -f hevc/h264/mjpeg 指定。

    这里不要加 ``-flags low_delay``：写在 ``-i`` 前面会作用到
    *解码器*。带 B 帧的 HEVC/H.264 会按解码序吐出，
    重编码后的 AU 播放顺序就乱了。
    """
    return [
        "-fflags", "nobuffer",
        "-probesize", "32",
        "-analyzeduration", "0",
    ]


def _ffmpeg_annexb_input_flags() -> list[str]:
    """原始 Annex-B 管道多探一点，才能看到 SPS/PPS。

    ``_ffmpeg_fast_input_flags`` 用 probesize=32。H.264 的 SPS NAL 经常
    已经超过 32 字节，ffmpeg 会在 PPS 之前启动解码，报
    ``non-existing PPS 0 referenced``。整块数据其实已经在 stdin 里。
    """
    return [
        "-fflags", "+genpts+discardcorrupt",
        "-probesize", "1048576",
        "-analyzeduration", "2000000",
    ]


def _x264_encode_flags(*, threads: int | None = None, sliced_threads: bool = False) -> list[str]:
    """直播边转边播用的 libx264 参数（无 B 帧）。

    一次性 Annex-B 路径关掉 sliced_threads，这样每幅图像只有一个
    VCL NAL，`_split_h264_access_units` 才能切帧。常驻
    管道打开切片线程：帧线程会把末尾图像攥到
    ffmpeg 退出，而 FLV tag 已经把一幅图像的
    各个切片收成一帧。
    """
    n = int(threads) if threads is not None else _x264_threads_now()
    n = max(1, n)
    sliced = 1 if sliced_threads else 0
    params = (
        f"threads={n}:sliced_threads={sliced}:rc-lookahead=0:sync-lookahead=0:"
        "aq-mode=0:mbtree=0:weightp=0:8x8dct=0"
    )
    # 参数必须写在 preset/tune 后面，它们会把 x264 默认值重置掉，
    # 这样 lookahead 才能保持 0，直播管道才会每幅图像都刷出来。
    return [
        "-c:v", "libx264",
        "-threads", str(n),
        "-flags", "+low_delay",
        "-profile:v", "baseline",
        "-preset", TRANSCODE_PRESET,
        "-tune", "zerolatency",
        "-bf", "0",
        "-pix_fmt", "yuv420p",
        "-x264-params", params,
    ]


def _ffmpeg_x264_rate_args(*, crf: int | None = None) -> list[str]:
    """拼 libx264 的码控参数。

    默认高清路径：CRF，可选 VBV（-maxrate/-bufsize）。除非显式配了
    ABR 码率，否则不要加 -b:v — ABR 会盖过 CRF，体积也会胀。
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
                        acquire_gate: bool = True,
                        output_skip: int = 0) -> float:
    """用 libx264（CPU）把原始视频转成 H.264，缩放到 (scale_w, scale_h)。

    codec：源编码（'hevc'/'h265' 或 'h264'/'avc'），决定 ffmpeg 的 -f。
    质量参数：TRANSCODE_CRF / BITRATE / MAXRATE / BUFSIZE / PRESET。
    输出：baseline，GOP 30，关键帧 dump_extra（切 AU 需要 sliced_threads=0）。
    """
    if not scale_w:
        scale_w = _tc_size() if _tc_size() > 0 else (_tc_width() or 2096)
    if not scale_h:
        scale_h = _tc_size() if _tc_size() > 0 else (_tc_height() or 2096)
    t0 = time.time()
    # 缩放用 fast_bilinear：边转边播比默认 bicubic 便宜很多。
    scale_filter = f"scale={scale_w}:{scale_h}:flags=fast_bilinear"
    skip = max(0, int(output_skip or 0))
    # 滤镜 select 在解码之后、x264 之前，GOP 预热帧不会被编码。
    video_filter = (
        f"select='gte(n\\,{skip})',setpts=N/30/TB,{scale_filter}"
        if skip
        else scale_filter
    )
    in_format = "hevc" if codec.lower() in ("hevc", "h265", "h.265") else "h264"
    jid = job_id if job_id is not None else getattr(_tls, "encode_job_id", None)

    def _run():
        n_threads = _x264_threads_now()
        cmd = [
            FFMPEG, "-y", "-hide_banner", "-loglevel", "error",
            *_ffmpeg_annexb_input_flags(),
            # 解码线程（HEVC 解码常常是直播路径的瓶颈）。
            "-threads", str(n_threads),
            "-f", in_format, "-r", "30", "-i", "pipe:0",
            "-vf", video_filter,
            *_x264_encode_flags(threads=n_threads),
            *_ffmpeg_x264_rate_args(),
            "-g", "30",
            "-keyint_min", "30",
            "-sc_threshold", "0",
            "-bsf:v", "dump_extra=freq=keyframe",
            "-f", "h264",
            str(output_path),
        ]
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        def _feed() -> None:
            try:
                if proc.stdin is not None:
                    proc.stdin.write(raw_data)
                    proc.stdin.close()
            except Exception:
                try:
                    if proc.stdin is not None:
                        proc.stdin.close()
                except Exception:
                    pass

        feeder = threading.Thread(target=_feed, daemon=True)
        feeder.start()
        try:
            stdout, stderr = proc.communicate(timeout=120)
        except subprocess.TimeoutExpired as e:
            proc.kill()
            try:
                if proc.stdin is not None:
                    proc.stdin.close()
            except Exception:
                pass
            try:
                stdout, stderr = proc.communicate(timeout=5)
            except Exception:
                stdout, stderr = b"", b""
            raise TimeoutError("ffmpeg chunk exceeded 120s") from e
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode or 1, cmd, output=stdout, stderr=stderr
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
                            job_id: str | None = None, output_skip: int = 0):
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
    # 快路径：H.264 且不缩放、也不强制按质量重编码 → 直接拷贝字节
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
        raw_data, chunk_path, scale_w, scale_h, codec, job_id=jid,
        output_skip=output_skip,
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
    """日志辅助：` (41.7 fps, 1.39x realtime)`。"""
    if elapsed <= 0 or n_frames <= 0:
        return ""
    out_fps = n_frames / elapsed
    rt = out_fps / max(1.0, float(fps) or 30.0)
    return f" ({out_fps:.1f} fps, {rt:.2f}x realtime)"


def _iter_annexb_nals(data: bytes):
    """逐个产出 Annex-B NAL（含起始码）。"""
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
    """按 NAL 类型记下最新的参数集（含起始码）。"""
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
        return 33 in ps and 34 in ps  # 参数集是 SPS + PPS（VPS 可选，但通常有）
    return 7 in ps and 8 in ps


def _annexb_has_leading_ps(data: bytes, *, hevc: bool) -> bool:
    """第一个 VCL NAL 之前已经有 SPS+PPS 则为真。"""
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


# 解复用 Annex-B 时，要等下一个起始码才吐出图像。末尾
# 放一个 AUD 就能收掉最后一个 VCL，而且不多一幅图像，当前块
# 不用等下一块（或 EOF）就能读回来。
_H264_AUD_CLOSER = b"\x00\x00\x00\x01\x09\xf0"
_HEVC_AUD_CLOSER = b"\x00\x00\x00\x01\x46\x01\x50"


def _live_worker_threads(cameras: int) -> int:
    """一路常驻相机 ffmpeg 用的线程数。

    算法和原来的按块编码器一样：``X264_THREAD_BUDGET`` 分给
    一起拉起的各路相机（加上已经占着的槽），至少
    ``X264_THREADS``，最多 8。解码和编码都用这个数做
    切片线程，这样不退出进程也能吐出图像。
    """
    floor = max(1, int(X264_THREADS))
    cameras = max(1, int(cameras))
    try:
        holding = int(encode_gate.holding_count() or 0)
    except Exception:
        holding = 0
    denom = max(cameras, holding, 1)
    share = max(floor, int(X264_THREAD_BUDGET) // denom)
    return min(8, share)


class _FlvH264Reader:
    """从 FLV 字节流里抽出完整的 H.264 图像。

    每个视频 tag 自带载荷长度，所以一块的最后一幅
    不用等下一帧也能收完。
    """

    def __init__(self) -> None:
        self._buf = bytearray()
        self._pos = 0
        self._header = False
        self._nal_len = 4
        self._sps = b""
        self._pps = b""

    def feed(self, data: bytes) -> None:
        if data:
            self._buf.extend(data)

    def pop_pictures(self) -> list[bytes]:
        pics: list[bytes] = []
        while True:
            pic = self._next_picture()
            if pic is None:
                break
            if pic:
                pics.append(pic)
        if self._pos:
            del self._buf[: self._pos]
            self._pos = 0
        return pics

    def _need(self, n: int) -> bool:
        return self._pos + n <= len(self._buf)

    def _next_picture(self) -> bytes | None:
        if not self._header:
            if not self._need(13):
                return None
            sig = bytes(self._buf[self._pos : self._pos + 3])
            if sig != b"FLV":
                raise RuntimeError("live ffmpeg output is not FLV")
            header_size = int.from_bytes(self._buf[self._pos + 5 : self._pos + 9], "big")
            if header_size < 9 or not self._need(header_size + 4):
                return None
            self._pos += header_size + 4
            self._header = True
        if not self._need(11):
            return None
        tag_type = self._buf[self._pos]
        size = int.from_bytes(self._buf[self._pos + 1 : self._pos + 4], "big")
        total = 11 + size + 4
        if not self._need(total):
            return None
        payload = bytes(self._buf[self._pos + 11 : self._pos + 11 + size])
        self._pos += total
        if tag_type != 9 or len(payload) < 5 or (payload[0] & 0x0F) != 7:
            return b""
        packet_type = payload[1]
        if packet_type == 0:
            self._take_avcc(payload[5:])
            return b""
        if packet_type != 1:
            return b""
        return self._annexb_picture(payload)

    def _take_avcc(self, body: bytes) -> None:
        if len(body) < 7:
            return
        self._nal_len = (body[4] & 0x03) + 1
        n_sps = body[5] & 0x1F
        p = 6
        sps: list[bytes] = []
        for _ in range(n_sps):
            if p + 2 > len(body):
                return
            ln = int.from_bytes(body[p : p + 2], "big")
            p += 2
            if p + ln > len(body):
                return
            sps.append(body[p : p + ln])
            p += ln
        if p >= len(body):
            return
        n_pps = body[p]
        p += 1
        pps: list[bytes] = []
        for _ in range(n_pps):
            if p + 2 > len(body):
                return
            ln = int.from_bytes(body[p : p + 2], "big")
            p += 2
            if p + ln > len(body):
                return
            pps.append(body[p : p + ln])
            p += ln
        self._sps = b"".join(b"\x00\x00\x00\x01" + n for n in sps)
        self._pps = b"".join(b"\x00\x00\x00\x01" + n for n in pps)

    def _annexb_picture(self, payload: bytes) -> bytes:
        body = payload[5:]
        nals: list[bytes] = []
        p = 0
        width = self._nal_len
        while p + width <= len(body):
            ln = int.from_bytes(body[p : p + width], "big")
            p += width
            if ln < 0 or p + ln > len(body):
                break
            if ln:
                nals.append(body[p : p + ln])
            p += ln
        annex = b"".join(b"\x00\x00\x00\x01" + n for n in nals)
        key = (payload[0] >> 4) == 1
        has_ps = any((n[0] & 0x1F) in (7, 8) for n in nals if n)
        if key and not has_ps and (self._sps or self._pps):
            annex = self._sps + self._pps + annex
        return annex


class LiveX264Encoder:
    """整个任务共用一个 ffmpeg，一块一块地喂。

    有空闲编码槽就占到 close()；任务上限已经
    满了也照样启动，免得别的相机干等。
    解码和 x264 都把原来的按块线程份额用作切片线程。
    帧线程会把最后几幅图像留在进程里，直到进程
    退出。Annex-B 输入用 AUD 收尾，这样本块的图像
    在下一块到来之前就能读到。
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
        threads: int | None = None,
    ):
        self._job_id = (job_id or "").strip() or "_anon_"
        self._threads = max(1, int(threads)) if threads else 0
        self._in_args = list(in_args)
        self._scale_w = scale_w
        self._scale_h = scale_h
        self._rate_args = list(rate_args or [])
        self._gop = max(1, int(gop))
        self._kind = kind
        self._codec = (codec or "hevc").lower()
        self._hevc = self._codec in ("hevc", "h265", "h.265")
        self._fps = 30.0
        self._ps: dict = {}
        self._sniffed = False
        self._closed = False
        self._acquired = False
        self._proc: subprocess.Popen | None = None
        self._flv = _FlvH264Reader()
        self._out_buf = bytearray()
        self._out_cv = threading.Condition()
        self._stderr = bytearray()
        self._stdout_eof = False
        self._pending: list[bytes] = []
        self._jpeg_hold = False

    def seed_param_sets(self, ps: dict | bytes | None) -> None:
        """从后面的关键帧或整包扫描里补上 SPS/PPS（HEVC 还有 VPS）。"""
        if not ps:
            return
        if isinstance(ps, (bytes, bytearray)):
            found = _collect_annexb_param_sets(bytes(ps), hevc=self._hevc)
            if found:
                self._ps.update(found)
            return
        self._ps.update(ps)

    @classmethod
    def annexb(cls, *, codec: str, scale_w, scale_h, job_id: str | None, fps: float = 30.0, gop: int = 30, threads: int | None = None):
        in_fmt = "hevc" if (codec or "").lower() in ("hevc", "h265", "h.265") else "h264"
        fps_i = int(fps) if fps and fps > 0 else 30
        in_args = ["-f", in_fmt, "-r", str(fps_i), "-i", "pipe:0"]
        enc = cls(
            in_args=in_args,
            scale_w=scale_w,
            scale_h=scale_h,
            job_id=job_id,
            rate_args=_ffmpeg_x264_rate_args(),
            gop=max(1, int(gop)),
            codec=in_fmt,
            kind="annexb",
            threads=threads,
        )
        enc._fps = float(fps_i)
        return enc

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
        threads: int | None = None,
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
        enc = cls(
            in_args=in_args,
            scale_w=scale_w,
            scale_h=scale_h,
            job_id=job_id,
            rate_args=rate_args,
            gop=30,
            codec="mjpeg",
            kind="jpeg",
            threads=threads,
        )
        enc._fps = fps_f
        return enc

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
        # 已经标明是 H.264 就不能再改判：NAL 头 0x41 是
        # 非 IDR 切片，但 (0x41 >> 1) & 0x3F == 32 看着像 HEVC VPS。
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

    def _scale_pair(self) -> tuple[int, int]:
        w = int(self._scale_w) if self._scale_w else 0
        h = int(self._scale_h) if self._scale_h else 0
        if w <= 0:
            w = _tc_size() if _tc_size() > 0 else (_tc_width() or 2096)
        if h <= 0:
            h = _tc_size() if _tc_size() > 0 else (_tc_height() or 2096)
        return max(2, w), max(2, h)

    def _video_filter(self, output_skip: int) -> str:
        w, h = self._scale_pair()
        scale = f"scale={w}:{h}:flags=fast_bilinear"
        skip = max(0, int(output_skip or 0))
        if skip <= 0:
            return scale
        fps = max(1, int(round(self._fps or 30)))
        # 滤镜 select 在解码之后、x264 之前，预热帧不会被编码，
        # 留下的第一幅图像是 IDR。
        return f"select='gte(n\\,{skip})',setpts=N/{fps}/TB,{scale}"

    def _worker_threads(self) -> int:
        if self._threads > 0:
            return self._threads
        return _live_worker_threads(1)

    def _command(self, probesize: int, output_skip: int) -> list[str]:
        n_threads = self._worker_threads()
        fps_i = max(1, int(round(self._fps or 30)))
        # 每个 JPEG 块用一幅额外图像收尾，这幅会被丢掉。
        # 全 I 帧避免这幅图像变成 P 帧的参考。
        gop = 1 if self._kind == "jpeg" else self._gop
        cmd = [
            FFMPEG, "-hide_banner", "-loglevel", "error", "-nostats",
            "-probesize", str(max(32, int(probesize))),
            "-analyzeduration", "0",
        ]
        if self._kind == "jpeg":
            # 解复用 image2pipe 要等下一个 SOI 才吐出图像。调用方
            # 会重复最后一张 JPEG，当前块才能放行，这份拷贝
            # 留在缓冲里。输入侧不加 -framerate：加了会囤住多张图。
            cmd += [
                "-f", "image2pipe",
                "-vcodec", "mjpeg",
                "-i", "pipe:0",
            ]
        else:
            in_fmt = "hevc" if self._hevc else "h264"
            cmd += [
                "-threads", str(n_threads),
                "-thread_type", "slice",
                "-fflags", "+genpts+discardcorrupt",
                "-f", in_fmt, "-r", str(fps_i), "-i", "pipe:0",
            ]
        cmd += [
            "-an",
            "-vf", self._video_filter(output_skip),
            *_x264_encode_flags(threads=n_threads, sliced_threads=True),
            *self._rate_args,
            "-g", str(gop),
            "-keyint_min", str(gop),
            "-sc_threshold", "0",
            *(
                []
                if self._kind == "jpeg"
                else ["-force_key_frames", f"expr:not(mod(n,{gop}))"]
            ),
            "-bsf:v", "dump_extra=freq=keyframe",
            "-flush_packets", "1",
            "-f", "flv", "pipe:1",
        ]
        return cmd

    def _hold_slot(self) -> None:
        """有空闲槽时把这个 ffmpeg 记进去。

        四路在整个分段里每路一个进程，这
        四个必须一起启动。若死等任务上限（常常是 2，另外两路
        已经占着槽），120 秒后这段会超时。
        """
        if self._acquired:
            return
        try:
            encode_gate.acquire(self._job_id, timeout=0)
        except TimeoutError:
            log.info(
                "live ffmpeg starting without encode slot job=%s",
                self._job_id,
            )
            return
        self._acquired = True

    def _stderr_text(self) -> str:
        with self._out_cv:
            err = bytes(self._stderr)
        return err.decode(errors="replace")[-2000:]

    def _start_proc(self, payload_len: int, output_skip: int) -> None:
        self._hold_slot()
        # 原始 H.264/HEVC 探测会读到 probesize 或 EOF。常驻 stdin
        # 不会 EOF，读取一旦超过已经写入的字节就会
        # 堵住。probesize 必须严格小于第一次写入。
        if self._kind == "jpeg":
            # 解复用 image2pipe 只要 SOI 标记；probesize 太大时
            # 会去等超出本块已有的字节。
            probe = 32
        else:
            probe = min(8192, max(32, (int(payload_len) * 3) // 4))
        cmd = self._command(probe, output_skip)
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
        except Exception:
            self._release()
            raise
        threading.Thread(target=self._drain_stdout, name="sview-live-out", daemon=True).start()
        threading.Thread(target=self._drain_stderr, name="sview-live-err", daemon=True).start()
        log.info(
            "live ffmpeg persistent pid=%s job=%s codec=%s threads=%s sliced=1",
            self._proc.pid,
            self._job_id,
            self._codec,
            self._worker_threads(),
        )

    def _drain_stdout(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        try:
            while True:
                chunk = proc.stdout.read(65536)
                if not chunk:
                    break
                with self._out_cv:
                    self._out_buf.extend(chunk)
                    self._out_cv.notify_all()
        finally:
            with self._out_cv:
                self._stdout_eof = True
                self._out_cv.notify_all()

    def _drain_stderr(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        try:
            while True:
                chunk = proc.stderr.read(4096)
                if not chunk:
                    break
                with self._out_cv:
                    self._stderr.extend(chunk)
                    if len(self._stderr) > 8000:
                        del self._stderr[:-4000]
        except Exception:
            return

    def _read_pictures(self, count: int, timeout: float) -> list[bytes]:
        if count <= 0:
            return []
        deadline = time.monotonic() + max(0.1, float(timeout))
        pics: list[bytes] = list(self._pending)
        self._pending.clear()
        while len(pics) < count:
            with self._out_cv:
                if self._out_buf:
                    self._flv.feed(bytes(self._out_buf))
                    self._out_buf.clear()
                pics.extend(self._flv.pop_pictures())
                if len(pics) >= count:
                    break
                proc = self._proc
                dead = proc is None or proc.poll() is not None or self._stdout_eof
                if dead and not self._out_buf:
                    err = bytes(self._stderr).decode(errors="replace")[-2000:]
                    raise RuntimeError(
                        f"ffmpeg live session ended before {count} pictures "
                        f"(got {len(pics)}): {err}"
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    err = bytes(self._stderr).decode(errors="replace")[-2000:]
                    raise TimeoutError(
                        f"ffmpeg live session timed out waiting for {count} pictures "
                        f"(got {len(pics)}): {err}"
                    )
                self._out_cv.wait(timeout=min(0.2, remaining))
        extra = pics[count:]
        if extra:
            self._pending.extend(extra)
        return pics[:count]

    def _kill_proc(self) -> None:
        proc = self._proc
        if proc is None:
            return
        try:
            if proc.stdin is not None:
                proc.stdin.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=2)
            except Exception:
                pass
        self._proc = None

    def encode_frames(
        self,
        frames: list,
        timeout: float | None = None,
        *,
        output_skip: int = 0,
    ) -> list:
        n = len(frames)
        if n <= 0:
            return []
        if self._closed:
            raise RuntimeError("live encoder already closed")
        raw = b"".join(frames)
        if not raw:
            return [b""] * n
        wait_s = timeout if timeout is not None else max(90.0, 3.0 * n)
        skip = max(0, int(output_skip or 0))
        first = self._proc is None
        if self._kind != "jpeg":
            if first:
                self._sniff_codec(raw)
                raw = self._with_param_sets(raw)
            closer = _HEVC_AUD_CLOSER if self._hevc else _H264_AUD_CLOSER
            payload = raw + closer
            release_flush = 0
        else:
            # 重复最后一张 JPEG。合成的小 JPEG 不够，
            # 得是真实 SOI，image2pipe 才放行上一幅。
            payload = raw + frames[-1]
            # 这幅重复图像会在下一块开头吐出。
            release_flush = 1 if self._jpeg_hold else 0
            self._jpeg_hold = True
        # 第一次调用把预热写进滤镜图（只解码，不编码）。
        # 之后解码器已有状态，改为丢掉已编码图像。
        baked_skip = skip if first else 0
        drop_after = 0 if first else skip
        to_read = n - baked_skip
        if to_read < 0:
            to_read = 0
        try:
            if first:
                self._start_proc(len(payload), baked_skip)
            proc = self._proc
            if proc is None or proc.stdin is None or proc.poll() is not None:
                raise RuntimeError(
                    f"ffmpeg live session is not running: {self._stderr_text()}"
                )
            proc.stdin.write(payload)
            proc.stdin.flush()
            if to_read <= 0 and release_flush <= 0:
                return []
            want = (to_read if drop_after <= 0 else n) + release_flush
            pics = self._read_pictures(want, wait_s)
            if release_flush:
                pics = pics[release_flush:]
        except Exception:
            self._kill_proc()
            self._closed = True
            self._release()
            raise
        if drop_after > 0:
            pics = pics[drop_after:]
        return pics

    def close(self) -> None:
        if self._closed and self._proc is None:
            self._release()
            return
        self._closed = True
        self._kill_proc()
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

    n_threads = _live_worker_threads(len(channels))

    def _one(ch: str):
        codec = (camera_metas.get(ch, {}) or {}).get("codec") or "hevc"
        fps = float((camera_metas.get(ch, {}) or {}).get("fps") or 30)
        return ch, LiveX264Encoder.annexb(
            codec=str(codec),
            scale_w=scale_w,
            scale_h=scale_h,
            job_id=job_id,
            fps=fps,
            threads=n_threads,
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

    n_threads = _live_worker_threads(len(channels))

    def _one(ch: str):
        fps = float((camera_metas.get(ch, {}) or {}).get("fps") or 30)
        return ch, LiveX264Encoder.jpeg(
            scale_w=scale_w,
            scale_h=scale_h,
            job_id=job_id,
            fps=fps,
            crf=crf,
            direct=direct,
            threads=n_threads,
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
    """可以不经 ffmpeg 重编码、直接吐出相机 Annex-B AU 时为真。"""
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
    """第一块切小一点，大文件能更早可播。"""
    if chunk_idx <= 0:
        return PLAYABLE_MIN_FRAMES
    return max(1, CHUNK_FRAMES)


def _annexb_vcl_offsets(data: bytes) -> list[int]:
    """主图像的字节偏移（H.264 NAL 类型 1 和 5）。"""
    offsets: list[int] = []
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
        if data[np + sc] & 0x1F in (1, 5):
            offsets.append(np)
        pos = np + sc + 1
    return offsets


def _drop_leading_annexb_pictures(data: bytes, count: int) -> bytes:
    """编码输出从第 count 幅主图像开始保留。"""
    skip = max(0, int(count or 0))
    if skip <= 0 or not data:
        return data
    offsets = _annexb_vcl_offsets(data)
    if len(offsets) <= skip:
        return b""
    return data[offsets[skip]:]


def _cam_pool_workers(channels) -> int:
    return max(1, min(8, len(list(channels))))


def _encode_stem(ch: str) -> str:
    return stream_file_slug(ch)


def _tls_call(override, fn, *args, **kwargs):
    """在工作线程里跑 fn，并带上调用方的转码覆盖参数。"""
    prev = getattr(_tls, "override", None)
    _tls.override = override
    try:
        return fn(*args, **kwargs)
    finally:
        _tls.override = prev


# ---------------------------------------------------------------------------
#  处理
# ---------------------------------------------------------------------------


def _known_frame_total(value) -> int | None:
    """整个任务的正 frame_count；还不知道则为 None。"""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _progressive_frame_count(
    summary_n: int | None,
    aligned_n: int,
    *,
    final: bool,
) -> int | None:
    """边转边播用的稳定总帧数；还不知道则为 None。

    编码期间，seek 上限用 mcap 摘要（各视频 topic 帧数的最小值）。
    没有摘要时，不要把正在增长的已发布前缀当成
    总数。完成时的清单始终用对齐后的实际帧数。
    """
    have = int(aligned_n or 0)
    if final:
        return have if have > 0 else None
    declared = _known_frame_total(summary_n)
    if declared is None:
        return None
    return max(declared, have) if have > 0 else declared


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
    min_frames: int | None,
    key_frame_indices: list,
    progressive: bool,
    progress_cb=None,
    progress_base: int | None = None,
) -> int:
    """写 [f_start, f_end) 的帧 bin。返回更新后的 IMU 扫描下标。"""
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
            stream_codecs={
                ch: str((camera_metas.get(ch) or {}).get("stored_codec") or "h264")
                for ch in streams_data
            },
        )
        _write_bytes_atomic(output_frame_dir / f"{fidx:06d}.bin", bundle)

        frames_done = fidx + 1
        known_total = _known_frame_total(min_frames)
        job_complete = known_total is not None and frames_done >= known_total
        timeline_upto = known_total if known_total is not None else frames_done
        published_manifest = None
        if progressive and (
            frames_done == 1
            or frames_done % PARTIAL_MANIFEST_EVERY == 0
            or job_complete
        ):
            tl_start, tl_end = _estimate_timeline(
                video_timestamps, channel_aus, timeline_upto,
            )
            published_manifest = _build_manifest_dict(
                job_id=job_id,
                camera_metas=camera_metas,
                frame_count=known_total,
                frames_available=frames_done,
                key_frame_indices=key_frame_indices,
                timeline_start_ns=tl_start,
                timeline_end_ns=tl_end,
                status="done" if job_complete else "partial",
            )
            _publish_manifest(manifest_path, published_manifest)

        if progressive and OSS_CACHE and (
            frames_done % SEGMENT_FRAMES == 0 or job_complete
        ):
            # 优先用刚生成的清单；否则为 OSS 重做一份轻量清单。
            man = published_manifest
            if man is None and (
                frames_done % PARTIAL_MANIFEST_EVERY == 0 or job_complete
            ):
                tl_start, tl_end = _estimate_timeline(
                    video_timestamps, channel_aus, timeline_upto,
                )
                man = _build_manifest_dict(
                    job_id=job_id,
                    camera_metas=camera_metas,
                    frame_count=known_total,
                    frames_available=frames_done,
                    key_frame_indices=key_frame_indices,
                    timeline_start_ns=tl_start,
                    timeline_end_ns=tl_end,
                    status="done" if job_complete else "partial",
                )
            elif man is None and manifest_path.is_file():
                try:
                    man = json.loads(manifest_path.read_text(encoding="utf-8"))
                    man["bundle"]["frames_available"] = frames_done
                except Exception:
                    man = None
            _oss_cache_maybe_flush(
                job_id, frames_done, known_total or 0, output_frame_dir, man,
            )

        # 必须正好在 PLAYABLE_MIN_FRAMES 触发，Redis 调度才能
        # 叠上下一个任务。光按每 10 帧节流可能把它跳过
        # （比如首块停在第 30 帧，而 min_frames 是全长）。
        if progress_cb and (
            fidx == f_start
            or fidx % 10 == 0
            or job_complete
            or frames_done == PLAYABLE_MIN_FRAMES
            or frames_done == f_end
        ):
            try:
                reported = frames_done
                if progress_base is not None:
                    reported = max(0, frames_done - int(progress_base))
                progress_cb(reported, known_total)
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
    min_frames: int | None,
    key_frame_indices: list,
    progressive: bool,
    passthrough: bool,
    imu_sorted: list,
    imu_idx: int,
    progress_cb=None,
    live_encoders: dict | None = None,
    decode_only_prefix: int = 0,
    progress_origin: int = 0,
) -> int:
    """编码或直通一块对齐后的数据，并马上写帧 bin。

    decode_only_prefix 那些帧送进同一个 ffmpeg 解码，把目标
    帧重建出来，再丢掉它们的编码图像。x264
    的输出因此从目标分段开始。
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    expect = end - start
    if expect <= 0:
        return imu_idx
    warmup = max(0, min(expect, int(decode_only_prefix or 0)))
    expect -= warmup
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
            aus = _trim_pad_aus(aus, end - start)
            while len(channel_aus[ch]) < start:
                channel_aus[ch].append(b"")
            channel_aus[ch].extend(aus)
    elif live:
        def _live_one(ch: str):
            sl = video_frames[ch][start:end]
            t0 = time.time()
            aus = live_encoders[ch].encode_frames(sl, output_skip=warmup)
            return ch, _trim_pad_aus(aus, expect), time.time() - t0

        with ThreadPoolExecutor(max_workers=_cam_pool_workers(channels)) as pool:
            futs = [pool.submit(_live_one, ch) for ch in channels]
            for fut in as_completed(futs):
                ch_name, aus, elapsed = fut.result()
                if ch_name not in channel_aus:
                    channel_aus[ch_name] = []
                while len(channel_aus[ch_name]) < start + warmup:
                    channel_aus[ch_name].append(b"")
                if len(channel_aus[ch_name]) > start + warmup:
                    channel_aus[ch_name] = channel_aus[ch_name][:start + warmup]
                channel_aus[ch_name].extend(aus)
                log.debug(
                    "[%s]  %s chunk %d [%d:%d) %d AUs %.2fs [live]",
                    fname, ch_name, cidx, start, end, len(aus), elapsed,
                )
    else:
        _ovr = getattr(_tls, "override", None)
        _jid = job_id or getattr(_tls, "encode_job_id", None)
        with ThreadPoolExecutor(max_workers=_cam_pool_workers(channels)) as pool:
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
                    warmup,
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
                while len(channel_aus[ch_name]) < start + warmup:
                    channel_aus[ch_name].append(b"")
                if len(channel_aus[ch_name]) > start + warmup:
                    channel_aus[ch_name] = channel_aus[ch_name][:start + warmup]
                channel_aus[ch_name].extend(aus)
                log.debug(
                    "[%s]  %s chunk %d [%d:%d) %d AUs %.2fs",
                    fname, ch_name, cidx, start, end, len(aus), elapsed,
                )
                try:
                    os.unlink(path)
                except OSError:
                    pass

    # 丢掉这段的原始包字节（列表长度保留，方便按下标取）。
    for ch in channels:
        frames = video_frames[ch]
        for i in range(start, min(end, len(frames))):
            frames[i] = b""

    imu_idx = _write_bundle_range(
        start if passthrough else start + warmup,
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
        progress_base=None if passthrough else progress_origin,
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


def _raise_teleop_no_video(local_path: Path, *, robot: str | None = None) -> None:
    """遥操作包不能按头显 protobuf 来读。"""
    from sview.ingest.orin_images import mcap_channel_overview

    overview = mcap_channel_overview(local_path)
    robot_s = f" robot={robot}" if robot else ""
    raise ValueError(
        "Teleop MCAP has no supported video streams"
        f"{robot_s}: {local_path} "
        f"encodings={overview.get('encodings') or {}} "
        f"schemas={overview.get('schemas') or []} "
        f"topics={overview.get('topics') or []}"
    )


def process_mcap(
    source_path,
    no_upload=False,
    progress_cb=None,
    job_id=None,
    origin_sec: float = 0.0,
    sparse_anchors: dict[str, int] | None = None,
    sparse_encode_offset: int = 0,
    sparse_encode_count: int = 0,
):
    """用 CPU 把 HEVC 转成 H.264，上传 OSS（no_upload 时存本地）。

    progress_cb：可选，签名 callable(frames_done: int, frames_total: int)，
    写逐帧 bin 时调用（有节流）。给 Redis 进度轮询用。
    job_id：可选的输出/缓存 id（默认源文件名主干）。压测类型用
    不同 id（如 stem__t3），场景之间不会互相覆盖。
    origin_sec：从包起点跳过的秒数（从 T 开始编码）。
    sparse_encode_offset：x264 开始前只解码的帧数。
    sparse_encode_count：这段前导之后要编码的帧数。
    """
    from sview.jobs.origin import (
        cut_log_time_ns,
        estimate_remaining_frames,
        mcap_log_time_range,
        parse_start_sec,
    )

    local_path = Path(source_path)
    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {source_path}")

    fname = local_path.name
    out_job_id = (job_id or local_path.stem).strip() or local_path.stem
    origin_sec = parse_start_sec(origin_sec)
    bag_t0, bag_t1 = mcap_log_time_range(local_path)
    cut_ns = cut_log_time_ns(local_path, origin_sec) if origin_sec > 0 else None
    origin_first_ts = [None]

    sparse_anchors = {
        str(k): int(v) for k, v in (sparse_anchors or {}).items()
    }
    sparse_floor = min(sparse_anchors.values()) if sparse_anchors else None
    if sparse_floor is not None:
        cut_ns = sparse_floor
    # 稀疏窗口只有几十帧。不给结束时间的话，窗口填不满时
    # 读取会把包里剩下的都扫完，
    # 整次扫描 output_frames 一直是 0。
    sparse_end_ns = None
    if int(sparse_encode_count or 0) > 0 and sparse_anchors:
        slack_s = max(5.0, int(sparse_encode_count) / 30.0)
        sparse_end_ns = max(int(v) for v in sparse_anchors.values()) + int(slack_s * 1e9)

    def _after_origin(ts_ns, channel: str | None = None) -> bool:
        if origin_sec <= 0 and not sparse_anchors:
            return True
        try:
            t = int(ts_ns)
        except (TypeError, ValueError):
            return True
        if sparse_anchors:
            anchor = sparse_anchors.get(str(channel)) if channel is not None else None
            return t >= int(anchor if anchor is not None else sparse_floor or 0)
        if cut_ns is not None:
            return t >= cut_ns
        if origin_first_ts[0] is None:
            origin_first_ts[0] = t
        return t >= origin_first_ts[0] + int(origin_sec * 1e9)

    def _scale_summary_n(n):
        return estimate_remaining_frames(n, origin_sec, bag_t0, bag_t1)
    t_start = time.time()
    total_size = local_path.stat().st_size
    progress_log = _JobProgressLogger(out_job_id)
    user_progress_cb = progress_cb

    def _progress_with_log(done, total):
        progress_log.update(done, total)
        if user_progress_cb is not None:
            user_progress_cb(done, total)

    progress_cb = _progress_with_log

    family = ingest_family(str(local_path))
    robot = teleop_robot(str(local_path))
    log.info("[%s]  %.0f MB  start  job_id=%s", fname, total_size / 1e6, out_job_id)
    log.info("[%s]  ingest_family=%s robot=%s", fname, family, robot or "-")
    if origin_sec > 0:
        log.info(
            "[%s]  origin_sec=%.3f cut_log_time_ns=%s (encode-from-T)",
            fname, origin_sec, cut_ns if cut_ns is not None else "filter-on-first-ts",
        )
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

    # 累加器
    video_frames = defaultdict(list)  # 每通道的 bytes 列表，键为 ch_name
    video_timestamps = defaultdict(list)  # 每帧 log_time_ns 的列表，键为 ch_name
    imu_rows = []          # 全部原始 IMU 采样
    camera_metas = {}      # 含 width、height、fps、position，键为 ch_name
    futures = []
    channel_found = set()
    chunk_idx = 0
    total_imu = 0
    channel_aus = {}  # 下面填充的 AU 字节数组列表，键为 ch_name
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

    orin_published = False  # 走 Orin 路径时，编码块的同时写 bin
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _prev_encode_jid = getattr(_tls, "encode_job_id", None)
    _prev_origin_sec = getattr(_tls, "encode_origin_sec", 0.0)
    _tls.encode_job_id = out_job_id
    _tls.encode_origin_sec = origin_sec
    live_encoders: dict = {}
    try:
        from sview.ingest.orin_images import (
            jpeg_sequence_to_h264,
            orin_h264_channels_from_summary,
            orin_jpeg_channels_from_summary,
            orin_stream_kind,
            iter_orin_h264_annexb,
            iter_orin_jpeg_frames,
            read_orin_video_frame_count,
            sview_channel_for_topic,
            _h264_sps_size,
        )
        from sview.ingest.hdf5_images import is_hdf5_file, parse_hdf5_image_streams

# 先认路径族：head_cam 不用 ROS2/HDF5 读取器；
# 也不会让 teleoperation/* 掉进 protobuf。
        orin_kind = None
        orin_ok = False
        if family == "head_cam":
            if is_hdf5_file(local_path):
                raise ValueError(
                    f"Head-cam path is HDF5, expected protobuf MCAP: {local_path}"
                )
            log.info("[%s]  Format=head-cam protobuf (path family)", fname)
        elif is_hdf5_file(local_path):
            log.info(
                "[%s]  Format=HDF5 camera images (JPEG→H.264 progressive, chunk=%d)",
                fname, ORIN_CHUNK_FRAMES,
            )
            frame_blobs, video_timestamps, camera_metas = parse_hdf5_image_streams(
                local_path,
            )
            if origin_sec > 0:
                skip = int(round(origin_sec * 30))
                log.info("[%s]  HDF5 origin skip first %d frames (%.1fs)", fname, skip, origin_sec)
                for ch in list(frame_blobs):
                    frame_blobs[ch] = list(frame_blobs[ch][skip:])
                    video_timestamps[ch] = list(video_timestamps.get(ch, [])[skip:])
                    if not frame_blobs[ch]:
                        frame_blobs.pop(ch, None)
                        video_timestamps.pop(ch, None)
            orin_kind = "jpeg"
            orin_ok = True
        else:
            orin_kind = orin_stream_kind(
                local_path,
                skip_headcam_guard=(family == "teleop"),
            )
            if orin_kind:
                try:
                    if orin_kind == "h264":
                        log.info(
                            "[%s]  Format=Orin ROS2 H.264 streams "
                            "(stream, same TRANSCODE_* as head-cam, chunk=%d)",
                            fname, ORIN_CHUNK_FRAMES,
                        )
                        # 在编码循环里增量解析，第一个 GOP
                        # 不用等整包扫完就能发出去。
                    else:
                        log.info(
                            "[%s]  Format=Orin ROS2 CompressedImage JPEG "
                            "(stream encode, first GOP without full-bag scan, chunk=%d)",
                            fname, ORIN_CHUNK_FRAMES,
                        )
                        # 在下面的 JPEG 流循环里增量解析。
                    orin_ok = True
                except Exception as exc:
                    if family == "teleop":
                        raise
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
                summary_n = _scale_summary_n(read_orin_video_frame_count(local_path, "h264"))
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
                min_frames = _known_frame_total(summary_n)
                start = 0
                cidx = 0
                sw = sh = None
                passthrough = False
                if summary_n:
                    log.info(
                        "[%s]  Orin H.264 frame_count from mcap summary: %d (channels=%s)",
                        fname, summary_n, ",".join(expected_ch) or "?",
                    )
                else:
                    log.info(
                        "[%s]  Orin H.264 frame_count unknown until aligned publish "
                        "(no matching mcap summary)",
                        fname,
                    )
                if progress_cb:
                    try:
                        progress_cb(0, min_frames)
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
                    else:
                        return False
                    # 与头显同一套：TRANSCODE_SIZE/CRF/BITRATE/FORCE_REENCODE。
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
                            "[%s]  Live libx264 sessions x%d",
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
                        # 这个会话已经解过更早的图像。再从
                        # 上一个源 IDR 喂进去，会把它们重新编码一遍。
                        aus = list(enc.encode_frames(jpeg_frames[ch][f0:f1]))
                        if len(aus) > want:
                            aus = aus[:want]
                        while len(aus) < want:
                            aus.append(b"")
                        elapsed = time.time() - t0
                        nbytes = sum(len(a) for a in aus)
                        return ch, aus, elapsed, nbytes
                    out_p = work_dir / f"{_encode_stem(ch)}_orin_c{cidx_i:04d}.h264"
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
                    min_frames = _progressive_frame_count(
                        summary_n, n, final=final,
                    )
                    if final and summary_n and n and n != summary_n:
                        log.info(
                            "[%s]  mcap summary frame_count=%d actual aligned=%d; "
                            "done manifest uses actual",
                            fname, summary_n, n,
                        )
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
                        with ThreadPoolExecutor(max_workers=_cam_pool_workers(channels)) as pool:
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
                            " [passthrough]" if passthrough else (
                                " [live]" if live_encoders else ""
                            ),
                        )
                        cidx += 1
                        start = end

                for ch, data, ts_ns, meta in iter_orin_h264_annexb(
                    local_path, start_time_ns=cut_ns,
                ):
                    if not _after_origin(ts_ns, ch):
                        continue
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
                done_man = _build_manifest_dict(
                    job_id=job_id,
                    camera_metas=camera_metas,
                    frame_count=min_frames,
                    frames_available=min_frames,
                    key_frame_indices=key_frame_indices,
                    timeline_start_ns=tl_start,
                    timeline_end_ns=tl_end,
                    status="done",
                )
                _publish_manifest(manifest_path, done_man)
                _oss_cache_publish_done(job_id, min_frames, output_frame_dir, done_man)
                if progress_cb:
                    try:
                        progress_cb(min_frames, min_frames)
                    except Exception:
                        log.exception("progress_cb failed at end")
                orin_published = True
            except Exception as exc:
                if family == "teleop":
                    raise
                log.warning(
                    "[%s]  Orin H.264 stream encode failed (%s); "
                    "falling back to head-cam protobuf",
                    fname, exc,
                )
                orin_kind = None
                orin_ok = False
        if (
            (not orin_published)
            and orin_ok
            and orin_kind == "jpeg"
            and not is_hdf5_file(local_path)
        ):
            try:
                jpeg_frames = defaultdict(list)
                expected_ch = sorted(orin_jpeg_channels_from_summary(local_path))
                summary_n = _scale_summary_n(read_orin_video_frame_count(local_path, "jpeg"))
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
                min_frames = _known_frame_total(summary_n)
                start = 0
                cidx = 0
                sw = sh = None
                if summary_n:
                    log.info(
                        "[%s]  Orin JPEG frame_count from mcap summary: %d",
                        fname, summary_n,
                    )
                else:
                    log.info(
                        "[%s]  Orin JPEG frame_count unknown until aligned publish "
                        "(no matching mcap summary)",
                        fname,
                    )
                if progress_cb:
                    try:
                        progress_cb(0, min_frames)
                    except Exception:
                        log.exception("progress_cb failed at start")

                def _aligned_n() -> int:
                    if not channels:
                        return 0
                    return min(len(jpeg_frames.get(ch, [])) for ch in channels)

                def _lock_channels(final: bool = False) -> bool:
                    nonlocal channels, channel_aus, live_encoders, sw, sh
                    if channels:
                        return True
                    have = [ch for ch, rows in jpeg_frames.items() if rows]
                    if expected_ch:
                        ready = [ch for ch in expected_ch if jpeg_frames.get(ch)]
                        if not ready:
                            return False
                        if len(ready) < len(expected_ch) and not final:
                            return False
                        pick = ready
                    elif final:
                        pick = sorted(have)
                    else:
                        return False
                    if not pick:
                        return False
                    channels = pick
                    sw, sh = _apply_output_scale(camera_metas)
                    channel_aus = {ch: [] for ch in channels}
                    log.info(
                        "[%s]  Orin JPEG progressive libx264, cams=%s "
                        "size=%s crf=%s bitrate=%s maxrate=%s direct=%s",
                        fname,
                        ",".join(channels),
                        _transcode_size_desc(),
                        _tc_crf(),
                        TRANSCODE_BITRATE,
                        TRANSCODE_MAXRATE,
                        int(ORIN_DIRECT),
                    )
                    if LIVE_ENCODE:
                        live_encoders = _start_live_jpeg_encoders(
                            channels,
                            camera_metas,
                            job_id,
                            sw,
                            sh,
                            crf=_tc_crf(),
                            direct=ORIN_DIRECT,
                        )
                        log.info(
                            "[%s]  Live JPEG→H.264 sessions x%d (hold encode slots for job)",
                            fname, len(live_encoders),
                        )
                    return True

                def _enc_one(ch: str, cidx_i: int, f0: int, f1: int):
                    want = f1 - f0
                    enc = live_encoders.get(ch) if live_encoders else None
                    frames_seg = jpeg_frames[ch][f0:f1]

                    def _fit_aus(aus_all: list) -> list:
                        aus = list(aus_all)
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
                    out_p = work_dir / f"{_encode_stem(ch)}_orin_c{cidx_i:04d}.h264"
                    lock = cpu_encode_lock
                    gate = lock.slot(job_id) if hasattr(lock, "slot") else lock
                    with gate:
                        br = mr = bs = None
                        if not ORIN_DIRECT:
                            br = None if _rate_control_empty(TRANSCODE_BITRATE) else TRANSCODE_BITRATE
                            mr = None if _rate_control_empty(TRANSCODE_MAXRATE) else TRANSCODE_MAXRATE
                            bs = None if _rate_control_empty(TRANSCODE_BUFSIZE) else TRANSCODE_BUFSIZE
                        elapsed = jpeg_sequence_to_h264(
                            frames_seg,
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
                            gop=len(frames_seg),
                            direct=ORIN_DIRECT,
                            threads=_x264_threads_now(),
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
                    min_frames = _progressive_frame_count(
                        summary_n, n, final=final,
                    )
                    if final and summary_n and n and n != summary_n:
                        log.info(
                            "[%s]  mcap summary frame_count=%d actual aligned=%d; "
                            "done manifest uses actual",
                            fname, summary_n, n,
                        )
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
                        with ThreadPoolExecutor(max_workers=_cam_pool_workers(channels)) as pool:
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

                for ch, data, ts_ns, meta in iter_orin_jpeg_frames(
                    local_path, start_time_ns=cut_ns,
                ):
                    if not _after_origin(ts_ns, ch):
                        continue
                    jpeg_frames[ch].append(data)
                    video_timestamps[ch].append(ts_ns)
                    if ch not in camera_metas:
                        camera_metas[ch] = dict(meta)
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
                    raise ValueError(f"Orin MCAP has no jpeg frames: {local_path}")
                min_frames = start
                for ch in channels:
                    video_frames[ch] = jpeg_frames.get(ch, [])
                log.info(
                    "[%s]  Built %d frame bundles (%d keyframes)%s [orin jpeg stream]",
                    fname, min_frames, len(key_frame_indices),
                    " [progressive]" if progressive else "",
                )
                tl_start, tl_end = _estimate_timeline(
                    video_timestamps, channel_aus, min_frames,
                )
                done_man = _build_manifest_dict(
                    job_id=job_id,
                    camera_metas=camera_metas,
                    frame_count=min_frames,
                    frames_available=min_frames,
                    key_frame_indices=key_frame_indices,
                    timeline_start_ns=tl_start,
                    timeline_end_ns=tl_end,
                    status="done",
                )
                _publish_manifest(manifest_path, done_man)
                _oss_cache_publish_done(job_id, min_frames, output_frame_dir, done_man)
                if progress_cb:
                    try:
                        progress_cb(min_frames, min_frames)
                    except Exception:
                        log.exception("progress_cb failed at end")
                orin_published = True
            except Exception as exc:
                if family == "teleop":
                    raise
                log.warning(
                    "[%s]  Orin JPEG stream encode failed (%s); "
                    "falling back to head-cam protobuf",
                    fname, exc,
                )
                orin_kind = None
                orin_ok = False
        if (not orin_published) and orin_ok:
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

            sw, sh = _apply_output_scale(camera_metas)
            jpeg_crf = _tc_crf()
            log.info(
                "[%s]  JPEG→H.264 encode CRF %d scale=%s bitrate=%s maxrate=%s "
                "direct=%s (same TRANSCODE_* as head-cam)",
                fname,
                jpeg_crf,
                f"{sw}x{sh}" if sw and sh else "source",
                TRANSCODE_BITRATE,
                TRANSCODE_MAXRATE,
                int(ORIN_DIRECT),
            )

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
                        crf=_tc_crf(),
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
                    # 常驻解码器状态取代每块单独的 IDR 前缀。
                    aus = list(enc.encode_frames(jpeg_frames[ch][start:end]))
                    if len(aus) > want:
                        aus = aus[:want]
                    while len(aus) < want:
                        aus.append(b"")
                    elapsed = time.time() - t0
                    nbytes = sum(len(a) for a in aus)
                    return ch, aus, elapsed, nbytes
                fps = float(camera_metas.get(ch, {}).get("fps") or 30)
                out_p = work_dir / f"{_encode_stem(ch)}_orin_c{cidx:04d}.h264"
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
                    else:
                        br = None
                        mr = None
                        bs = None
                        if not ORIN_DIRECT:
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
                            direct=ORIN_DIRECT,
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
                # 首块用 PLAYABLE_MIN_FRAMES，好更早可播。
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
                with ThreadPoolExecutor(max_workers=_cam_pool_workers(channels)) as pool:
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
                # 各通道对齐这一块（防御性）。
                chunk_len = end - start
                for ch in channels:
                    have = len(channel_aus[ch])
                    expect = start + chunk_len
                    if have > expect:
                        channel_aus[ch] = channel_aus[ch][:expect]
                    while len(channel_aus[ch]) < expect:
                        channel_aus[ch].append(b"")
                    if orin_kind == "h264":
                        # 无状态 ffmpeg 仍从最近的源 IDR 重新解码。
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
            done_man = _build_manifest_dict(
                job_id=job_id,
                camera_metas=camera_metas,
                frame_count=min_frames,
                frames_available=min_frames,
                key_frame_indices=key_frame_indices,
                timeline_start_ns=tl_start,
                timeline_end_ns=tl_end,
                status="done",
            )
            _publish_manifest(manifest_path, done_man)
            _oss_cache_publish_done(job_id, min_frames, output_frame_dir, done_man)
            orin_published = True
        if not orin_published:
            if family == "teleop":
                _raise_teleop_no_video(local_path, robot=robot)
            # 头显：流式读 → 按块编码或直通 → 马上发布
            # （和 Orin 一样边转边播；首块 = PLAYABLE_MIN_FRAMES）。
            summary_frames = _scale_summary_n(read_headcam_video_frame_count(local_path))
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
            passthrough = None  # 等 metas 和通道齐了再定
            channels = []
            declared_total = _known_frame_total(summary_frames)
            if declared_total:
                log.info(
                    "[%s]  frame_count from mcap summary: %d",
                    fname, declared_total,
                )
            else:
                log.info(
                    "[%s]  frame_count unknown until aligned publish (no mcap summary)",
                    fname,
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
                # 先看要不要把墙钟域换算过来（规则和辅助函数一样）。
                offsets = []
                for row in imu_rows:
                    mono = _parse_ns(row.get("monotonic_ns"))
                    wall = _parse_ns(row.get("wall_ns"))
                    if mono is None or wall is None or mono <= 0 or wall <= 0:
                        continue
                    offsets.append(wall - mono)
                if not offsets:
                    # 能归一化时，仍把 IMU 的 log_time 对齐。
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
                # 四路 SView 都到齐后才锁定边转边播。
                # 只等 camera_metas 不安全：过早的单个 /meta
                # （常常是 FL）会把 channel_aus 冻成一路，另外
                # 三路就永远是空的。
                return HEADCAM_CHANNEL_SET.issubset(channel_found)

            def _select_channels(final: bool = False) -> list:
                if HEADCAM_CHANNEL_SET.issubset(channel_found):
                    return list(HEADCAM_CHANNELS)
                if final:
                    # 非标准包：实际出现哪些相机就发布哪些。
                    return sorted(channel_found)
                return []

            def _flush_ready_chunks(final: bool = False):
                nonlocal published_upto, chunk_idx, imu_idx, passthrough
                nonlocal declared_total, channels, channel_aus, live_encoders
                encode_offset = max(0, int(sparse_encode_offset or 0))
                encode_count = max(0, int(sparse_encode_count or 0))
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
                else:
                    # 第一次边转边播锁定之后，通道集合不再变。
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
                        if encode_count > 0:
                            # 源 GOP 和目标分段放在同一个 ffmpeg
                            # 进程里。只做预热的块会在 x264 开始前
                            # 丢掉解码器状态。
                            target_end = encode_offset + encode_count
                            if published_upto >= target_end or min_count < target_end:
                                break
                            next_end = target_end
                        else:
                            next_end = published_upto + _headcam_chunk_len(chunk_idx)
                            if min_count < next_end:
                                break
                    if next_end <= published_upto:
                        break
                    warmup = max(0, min(next_end, encode_offset) - published_upto)
                    output_start = published_upto + warmup
                    if warmup and progress_cb is not None:
                        try:
                            progress_cb(0, encode_count or None)
                        except Exception:
                            log.exception("progress_cb failed during decode warmup")
                    if (
                        (not passthrough)
                        and LIVE_ENCODE
                        and not live_encoders
                        and next_end > output_start
                    ):
                        sw = _scale_dim(camera_metas, "w")
                        sh = _scale_dim(camera_metas, "h")
                        live_encoders = _start_live_annexb_encoders(
                            channels, camera_metas, job_id, sw, sh,
                        )
                        log.info(
                            "[%s]  Live libx264 sessions x%d",
                            fname, len(live_encoders),
                        )
                    _try_rebase_timeline()
                    declared_total = _progressive_frame_count(
                        summary_frames, next_end, final=final,
                    )
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
                        decode_only_prefix=warmup,
                        progress_origin=encode_offset,
                    )
                    published_upto = next_end
                    chunk_idx += 1
                    if final:
                        break

            sparse_target = (
                max(0, int(sparse_encode_offset or 0)) + max(0, int(sparse_encode_count or 0))
                if int(sparse_encode_count or 0) > 0
                else 0
            )
            for msg in read_protobuf_messages(
                str(local_path),
                start_time_ns=cut_ns,
                end_time_ns=sparse_end_ns,
            ):
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
                    if not _after_origin(msg.log_time_ns, ch_name):
                        continue
                    channel_found.add(ch_name)
                    video_frames[ch_name].append(msg.proto_msg.data)
                    _append_video_ts(ch_name, msg.log_time_ns)
                    _flush_ready_chunks(final=False)
                    if sparse_target and published_upto >= sparse_target:
                        break

                elif topic == "/imu/sample":
                    if not _after_origin(msg.log_time_ns):
                        continue
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

            # 把输出尺寸写进相机 meta，供清单使用（缩放路径）。
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
            done_man = _build_manifest_dict(
                job_id=job_id,
                camera_metas=camera_metas,
                frame_count=min_frames,
                frames_available=min_frames,
                key_frame_indices=key_frame_indices,
                timeline_start_ns=tl_start,
                timeline_end_ns=tl_end,
                status="done",
            )
            _publish_manifest(manifest_path, done_man)
            _oss_cache_publish_done(job_id, min_frames, output_frame_dir, done_man)
            if progress_cb:
                try:
                    progress_cb(min_frames, min_frames)
                except Exception:
                    log.exception("progress_cb failed at end")
            orin_published = True  # 帧已经在盘上（复用 Orin 的后处理）

        # --- 保存/上传输出 ---
        if no_upload:
            import shutil as _shutil
            for cb in work_dir.glob("*_concat.bin"):
                try:
                    cb.unlink()
                except OSError:
                    pass
            if progressive:
                # 帧和清单已经在 LOCAL_OUTPUT_DIR/{job_id} 下
                log.info("  -> progressive cache  %s  (%d frames)", publish_dir, min_frames)
            else:
                out_dir = LOCAL_OUTPUT_DIR / job_id
                _shutil.copytree(str(work_dir), str(out_dir), dirs_exist_ok=True)
                log.info("  -> saved  %s  (%d frames)", out_dir, min_frames)
        else:
            # 把每帧 bin 和清单上传到 .../{job_id}/sview/
            if manifest_path.exists():
                oss_key = f"{oss_base}/manifest.json"
                upload_to_oss(manifest_path, oss_key)
            for bin_file in sorted(output_frame_dir.glob("*.bin")):
                oss_key = f"{oss_base}/frames/{bin_file.name}"
                upload_to_oss(bin_file, oss_key)


        total_elapsed = round(time.time() - t_start, 1)
        # 即使最后一次 progress_cb 被节流丢掉，也补一行 100%。
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
        _tls.encode_origin_sec = _prev_origin_sec
        import shutil
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass




# ---------------------------------------------------------------------------
#  时间戳换算（墙钟 / mcap log_time → monotonic_raw_ns）
# ---------------------------------------------------------------------------

def _parse_ns(value):
    """从 int/str 解析纳秒时间戳；缺失或非法则返回 None。"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rebase_timestamps_to_monotonic(video_timestamps: dict, imu_rows: list) -> bool:
    """用 monotonic_raw_ns 做统一时间轴（与 local_full_mcap_flow_verify 相同）。

    线上 mcap 经常把 msg.log_time 打成墙钟纳秒（约 1e18），超过
    JS Number.MAX_SAFE_INTEGER，SView 的 IMU 曲线会坏。校验风格的
    录制已经用单调 log_time（约 1e11）。

    - mcap log_time 在墙钟域：视频时间戳减去 median(wall - mono)，
        每条 IMU 的 log_time_ns 设为 monotonic_raw_ns。
    - 已经是单调时间：只把 IMU 的 log_time_ns 归一成 monotonic_raw_ns。

    就地改 video_timestamps 和 imu_rows。有改动则返回 True。
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
#  逐帧 H.264 AU 切分与 bundle 序列化
# ---------------------------------------------------------------------------

def _split_h264_access_units(data: bytes, work_dir: Path) -> list:
    """按 NAL 扫描把原始 H.264 Annex-B 切成逐帧 AU（不用 ffprobe）。

    这条编码路径的 AU 分组（单切片 libx264）：

      - 每幅主图像是一个 VCL NAL（类型 1 或 5）。
      - 跟在 VCL 后面的非 VCL（SEI/SPS/PPS/AUD）算 *下一个* AU 的开头 —
        它们是后一幅图像的前缀（H.264 的 AU 顺序）。
      - 中间没有别的 NAL 的连续 VCL，各自新开一个 AU。

    dump_extra=freq=keyframe 码流的结果::

      [SPS][PPS][SEI][IDR] | [P] | ... | [P] | [SPS][PPS][SEI][IDR] | ...

    关键帧留着自己的参数集（比 ffprobe 把 SPS/PPS
    贴到前一个 P 包上更利于跳转和解码）。不要再注入
    SPS/PPS — 以前这样写出会坏掉的 1 字节参数集。

    work_dir 没用；留着是为了和 server_cpu.py 的调用点对齐。
    """
    del work_dir  # 与 ffprobe 那一版的 API 对齐
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
                # 下一幅主图像，VCL 之间没有前缀 NAL
                aus.append(data[au_start:nal_start])
                au_start = nal_start
            seen_vcl = True
        elif seen_vcl and nal_type in (6, 7, 8, 9):
            # 图像后面的 SEI / SPS / PPS / AUD → 下一个 AU 的前缀
            aus.append(data[au_start:nal_start])
            au_start = nal_start
            seen_vcl = False

    aus.append(data[au_start:])
    return aus


def _frames_before_random_access(frames: list) -> int:
    """窗口里第一张 IDR 之前有多少帧。没有 IDR 时整段都算解不出。"""
    for i, frame in enumerate(frames):
        if frame and _au_has_random_access(frame):
            return i
    return len(frames)


def _au_has_random_access(data: bytes) -> bool:
    """是否为 H.264 IDR 或 HEVC 的 IDR/CRA。源的 seek 必须从这里开始。"""
    if _h264_payload_has_idr(data):
        return True
    pos = 0
    raw = data or b""
    while pos + 4 < len(raw):
        if raw[pos:pos + 4] == b"\x00\x00\x00\x01":
            hdr = raw[pos + 4:pos + 6]
            pos += 4
        elif raw[pos:pos + 3] == b"\x00\x00\x01":
            hdr = raw[pos + 3:pos + 5]
            pos += 3
        else:
            pos += 1
            continue
        if len(hdr) < 2:
            continue
        hevc_type = (hdr[0] >> 1) & 0x3F
        if hevc_type in (19, 20, 21):
            return True
    return False


def _h264_payload_has_idr(data: bytes) -> bool:
    """检查 H.264 载荷里有没有 IDR NAL（类型 5）。"""
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
    """新开 ffmpeg 时，H.264 块的结束下标：下一个共用 IDR（或 n）。

    每个直播块都是新的 ffmpeg 进程。切在 P 帧上，下一块
    会失败（没有参考图像），即使前面补了 SPS/PPS 也一样。
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
    """原子写字节：先写到 .tmp，再重命名到目标。"""
    tmp_path = target_path.with_suffix(target_path.suffix + '.tmp')
    with open(tmp_path, 'wb') as f:
        f.write(payload)
    tmp_path.replace(target_path)


def _estimate_timeline(video_timestamps: dict, channel_aus: dict, upto_frame: int):
    """给出 upto_frame 帧的时间轴起止（计数从 1 开始）。

    样本够就用完整时间戳跨度；否则退回
    最后间隔外推（规格 4.1）。若 ``upto_frame`` 已是全任务
    帧数，但内存里只有一段前缀时间戳，就外推 end_ns，让
    边转边播期间 duration_sec 保持稳定。
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
    frame_count: int | None,
    camera_metas: dict,
) -> float:
    """清单用的稳定总时长（边转边播期间不缩也不涨）。

    frame_count 还不知道时，时长跟着已发布前缀走。
    """
    if timeline_start_ns and timeline_end_ns and timeline_end_ns > timeline_start_ns:
        return round((timeline_end_ns - timeline_start_ns) / 1e9, 6)
    fps = 30
    for ch in HEADCAM_CHANNELS:
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
    if not frame_count or frame_count <= 0:
        return 0.0
    return round(max(0, frame_count - 1) / float(fps), 6)


def _build_manifest_dict(
    job_id: str,
    camera_metas: dict,
    frame_count: int | None,
    frames_available: int,
    key_frame_indices: list,
    timeline_start_ns: int,
    timeline_end_ns: int,
    status: str,
    origin_sec: float = 0.0,
) -> dict:
    """发布中 status 为 'partial'，完成时为 'done'。

    timeline/duration_sec 始终表示 *整个* 任务（frame_count），不是
    当前已发布前缀 — 边转边播的客户端才能拿到稳定的跳转范围。
    还没有 mcap 摘要、frame_count 未知时，JSON 里是 null。
    """
    manifest = {
        "job_id": job_id,
        "format": manifest_format_for(camera_metas.keys()),
        "status": status,
        "bundle": {
            "frame_count": frame_count,
            "frames_available": frames_available,
            # 传输分段大小，Master 和网页播放器都用它。
            "gop_size": SEGMENT_FRAMES,
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
    try:
        origin = float(origin_sec or 0) or float(getattr(_tls, "encode_origin_sec", 0) or 0)
    except (TypeError, ValueError):
        origin = 0.0
    if origin > 0:
        manifest["origin_sec"] = round(origin, 3)
    if OSS_CACHE:
        manifest["play_base"] = oss_play_base(job_id)
    return manifest


def _publish_manifest(manifest_path: Path, manifest: dict) -> None:
    payload = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    _write_bytes_atomic(manifest_path, payload)


def _prepare_publish_dir(job_id: str) -> Path:
    """创建或清空 LOCAL_OUTPUT_DIR/{job_id}，供边转边播写帧。"""
    import shutil as _shutil
    out_dir = LOCAL_OUTPUT_DIR / job_id
    if out_dir.exists():
        _shutil.rmtree(out_dir, ignore_errors=True)
    frames = out_dir / "frames"
    frames.mkdir(parents=True, exist_ok=True)
    return out_dir


def _build_bundle_frame(frame_idx: int, streams_data: dict, timestamp_ns: int,
                        time_window_ns: list, is_keyframe: bool,
                        imu_json: dict, events_json: dict,
                        stream_codecs: dict | None = None) -> bytes:
    """打一个 bundle 帧 bin（v1 四路或 v2 命名流）。"""
    return pack_bundle_frame(
        frame_idx,
        streams_data,
        timestamp_ns,
        time_window_ns,
        is_keyframe,
        imu_json,
        events_json,
        stream_codecs=stream_codecs,
    )


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
#  队列循环（Redis）
# ---------------------------------------------------------------------------

class JobCancelled(Exception):
    """在 progress_cb 里看到 Redis 取消标记时抛出。"""


class LostJobLock(Exception):
    """别的 worker 抢走或让我们的任务锁过期时抛出。"""


class SparseSegmentComplete(BaseException):
    """内部用的非 Exception 哨兵，用来停掉有界稀疏编码。"""


class SparseSegmentAborted(BaseException):
    """停掉这个稀疏分段，并且不发布它。"""


class McapEnded(Exception):
    """打开的 mcap 在分段填满之前就没有更多消息了。"""


def _rewrite_bundle_global_index(blob: bytes, frame_index: int) -> bytes:
    """只改 bundle header.frame_index，编码后的载荷字节保持不动。"""
    if len(blob) < 8:
        raise ValueError("bundle too small")
    header_len = struct.unpack_from("<I", blob, 0)[0]
    header_end = 4 + header_len
    if header_end + 4 > len(blob):
        raise ValueError("invalid bundle header")
    header = json.loads(blob[4:header_end].decode("utf-8"))
    header["frame_index"] = int(frame_index)
    encoded = json.dumps(
        header, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return struct.pack("<I", len(encoded)) + encoded + blob[header_end:]


def _bundle_all_streams_keyframe(blob: bytes) -> bool:
    """稀疏分段第一帧里，每个非空流都必须是 IDR。"""
    if len(blob) < 8:
        return False
    header_len = struct.unpack_from("<I", blob, 0)[0]
    header_end = 4 + header_len
    if header_end + 4 > len(blob):
        return False
    header = json.loads(blob[4:header_end].decode("utf-8"))
    payload_len = struct.unpack_from("<I", blob, header_end)[0]
    payload = blob[header_end + 4 : header_end + 4 + payload_len]
    found = False
    for meta in (header.get("streams") or {}).values():
        if meta.get("is_null") or int(meta.get("payload_length") or 0) <= 0:
            continue
        start = int(meta.get("payload_offset") or 0)
        end = start + int(meta.get("payload_length") or 0)
        found = True
        if not _h264_payload_has_idr(payload[start:end]):
            return False
    return found


def _load_sparse_manifest(job_id: str) -> dict | None:
    # 多 Pod 的 worker 池里以 OSS 为准。先读本 Pod
    # 过期的本地清单，会盖掉别的 Pod 已经发布的区间。
    if OSS_CACHE:
        try:
            key = oss_cache_key(job_id, "manifest.json")
            return json.loads(get_oss_bucket().get_object(key).read().decode("utf-8"))
        except Exception:
            pass
    local = LOCAL_OUTPUT_DIR / job_id / "manifest.json"
    if local.is_file():
        try:
            return json.loads(local.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None


def _published_sparse_ranges(r, job_id: str, bundle: dict, frame_count: int) -> list:
    """合并新段之前，已经发布的区间以 OSS 上实际存在的段文件为准。

    清单原文里的区间可能过期：段被生命周期或 GC 删掉后，清单每次发布都会
    被重写而留下来。照着它合并会把 Redis 写成整片都有，编码线以为编完了就退出。
    """
    if not OSS_CACHE:
        return list(bundle.get("available_ranges") or [])
    from sview.cache.oss_cache_gc import (
        available_ranges_from_starts,
        list_oss_segment_starts,
    )

    starts = list_oss_segment_starts(job_id)
    if starts is not None:
        return available_ranges_from_starts(
            starts, segment_frames=SEGMENT_FRAMES, frame_count=frame_count
        )
    # 列举失败：退回 Redis（Master 提交、/encode 时按 OSS 校正过），不用清单原文。
    try:
        from sview.jobs.job_status import get_job_fields

        return json.loads(get_job_fields(r, job_id).get("available_ranges") or "[]")
    except Exception:
        return []


def _publish_sparse_segment(
    r,
    *,
    job_id: str,
    source: str,
    start: int,
    end: int,
    frame_blobs: list[bytes],
    base_manifest: dict,
    frame_index_data: dict,
    epoch: int,
    upload_index: bool = True,
) -> dict:
    """先 PUT 分段，再在锁里合并并发布稀疏清单。"""
    from sview.cache.oss_cache_gc import (
        contiguous_prefix_from_ranges,
        merge_available_ranges,
    )
    from sview.jobs.job_status import (
        POD_IP,
        complete_segment_requests,
        get_job_fields,
        job_active_key,
        job_redis_key,
        set_job_fields,
    )

    if not frame_blobs or end <= start:
        raise ValueError("empty sparse segment")
    if not _bundle_all_streams_keyframe(frame_blobs[0]):
        raise ValueError(f"sparse segment {start} does not start with IDR on every stream")

    packed = _pack_segment_bytes(start, frame_blobs, SEGMENT_FRAMES)
    if OSS_CACHE:
        upload_bytes_to_oss(
            oss_cache_key(job_id, "segments", f"{start:06d}.bin"),
            packed,
            job_id=job_id,
        )

    out_dir = LOCAL_OUTPUT_DIR / job_id
    (out_dir / "segments").mkdir(parents=True, exist_ok=True)
    _write_bytes_atomic(out_dir / "segments" / f"{start:06d}.bin", packed)

    lock_key = f"sview:manifest:lock:{job_id}"
    owner = f"{POD_IP}:{os.getpid()}:{threading.get_ident()}"
    deadline = time.time() + 30
    while not r.set(lock_key, owner, nx=True, ex=30):
        if time.time() >= deadline:
            raise TimeoutError("sparse manifest lock timeout")
        time.sleep(0.1)
    try:
        current = _load_sparse_manifest(job_id) or {}
        manifest = dict(base_manifest)
        manifest.update({k: v for k, v in current.items() if k != "bundle"})
        manifest["job_id"] = job_id
        manifest["publish_mode"] = "sparse"
        current_epoch = int(current.get("segment_epoch") or 0)
        fields = get_job_fields(r, job_id)
        current_epoch = max(
            current_epoch, int(fields.get("segment_epoch") or 0), int(epoch)
        )
        manifest["segment_epoch"] = current_epoch
        revision = int(r.hincrby(job_redis_key(job_id), "manifest_revision", 1))
        manifest["manifest_revision"] = revision
        bundle = dict(base_manifest.get("bundle") or {})
        bundle.update(current.get("bundle") or {})
        frame_count = int(frame_index_data["frame_count"])
        ranges = merge_available_ranges(
            [*_published_sparse_ranges(r, job_id, bundle, frame_count), [start, end]],
            frame_count=frame_count,
        )
        bundle["frame_count"] = frame_count
        bundle["gop_size"] = SEGMENT_FRAMES
        bundle["available_ranges"] = ranges
        bundle["frames_available"] = contiguous_prefix_from_ranges(ranges)
        bundle["key_frame_indices"] = sorted({int(a) for a, _b in ranges})
        timestamps = frame_index_data.get("timestamps_ns") or []
        if timestamps:
            first = int(timestamps[0])
            last = int(timestamps[-1])
            step = int((last - first) / max(1, len(timestamps) - 1)) if len(timestamps) > 1 else 33_333_333
            bundle["timeline"] = {
                "start_ns": first,
                "end_ns": last + max(1, step),
                "duration_sec": round((last + max(1, step) - first) / 1e9, 6),
            }
        manifest["bundle"] = bundle
        manifest["status"] = (
            "done" if bundle["frames_available"] >= frame_count else "partial"
        )
        _publish_manifest(out_dir / "manifest.json", manifest)
        if OSS_CACHE:
            _oss_cache_upload_manifest(job_id, manifest)
            if upload_index:
                try:
                    upload_bytes_to_oss(
                        oss_cache_key(job_id, "frame_index.json"),
                        json.dumps(frame_index_data, separators=(",", ":")).encode("utf-8"),
                        content_type="application/json; charset=utf-8",
                        job_id=job_id,
                    )
                except Exception:
                    log.exception("[%s] frame index upload failed", job_id)
        ready_count = sum(
            max(0, (int(b) - int(a) + SEGMENT_FRAMES - 1) // SEGMENT_FRAMES)
            for a, b in ranges
        )
        active = r.get(job_active_key(job_id))
        if isinstance(active, bytes):
            active = active.decode("utf-8", errors="replace")
        still_encoding = bool(active) and not str(active).startswith(f"{int(start)}:")
        set_job_fields(
            r,
            job_id,
            status="ready" if manifest["status"] == "done" else "processing",
            publish_mode="sparse",
            source=source,
            frame_count=frame_count,
            frames_total=frame_count,
            frames_done=bundle["frames_available"],
            playable=1 if ranges else 0,
            available_ranges=json.dumps(ranges, separators=(",", ":")),
            segments_ready_count=ready_count,
            segment_epoch=current_epoch,
            manifest_revision=revision,
            **(
                {}
                if still_encoding
                else {
                    "encode_status": "idle",
                    "encode_window": "",
                    "decode_start": "",
                    "input_frames": end - start,
                    "output_frames": end - start,
                    "owner_pod": "",
                }
            ),
            error="",
            **({"play_base": oss_play_base(job_id)} if OSS_CACHE else {}),
        )
        try:
            complete_segment_requests(r, job_id, start)
        except Exception:
            log.exception("[%s] segment request notification failed", job_id)
        return manifest
    finally:
        try:
            cur = r.get(lock_key)
            if isinstance(cur, bytes):
                cur = cur.decode("utf-8", errors="replace")
            if cur == owner:
                r.delete(lock_key)
        except Exception:
            pass


class _RedisValueLockRenewer:
    """持有者还对得上时，给任意 Redis 值锁续期。"""

    def __init__(self, r, key: str, owner: str, *, ttl: int, also=None):
        self._r = r
        self._key = key
        self._owner = owner
        self._ttl = max(30, int(ttl))
        self._also = list(also or [])
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"sview-lock-renew-{key[-32:]}",
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
        interval = max(10, self._ttl // 3)
        while not self._stop.wait(interval):
            try:
                current = self._r.get(self._key)
                if isinstance(current, bytes):
                    current = current.decode("utf-8", errors="replace")
                if current != self._owner:
                    self._lost.set()
                    return
                self._r.expire(self._key, self._ttl)
                for extra_key, expected in self._also:
                    extra = self._r.get(extra_key)
                    if isinstance(extra, bytes):
                        extra = extra.decode("utf-8", errors="replace")
                    if extra == expected:
                        self._r.expire(extra_key, self._ttl)
            except Exception:
                self._lost.set()
                return


def _sparse_job_busy(r, job_id: str) -> bool:
    """这个共享 Job 已经有一段在播的稀疏分段时为真。

    锁的 worker 心跳里已经没有这个 Job，就算过期，删掉。
    """
    from sview.jobs.job_status import job_active_key, list_workers

    key = job_active_key(job_id)
    current = r.get(key)
    if not current:
        return False
    if isinstance(current, bytes):
        current = current.decode("utf-8", errors="replace")
    text = str(current or "")
    _start, _, owner = text.partition(":")
    pod = owner.split(":", 1)[0]
    try:
        workers = list_workers(r)
    except Exception:
        return True
    live = any(
        (worker.get("pod_ip") or "") == pod
        and any(
            str(active).startswith(f"{job_id}:segment:")
            for active in (worker.get("jobs") or [])
        )
        for worker in workers
    )
    if live:
        return True
    try:
        ttl = int(r.ttl(key))
    except Exception:
        return True
    # 新认领可能抢在下一次心跳之前。在那之前先别动。
    if ttl > 160:
        return True
    log.info("[%s] clearing stale sparse job lock %s", job_id, text)
    r.delete(key)
    return False


def _drop_segment_behind_frontier(r, item: dict) -> bool:
    """丢掉被更新的 seek 把前沿推过去的排队分段。"""
    from sview.jobs.job_status import get_job_fields, segment_pending_key

    try:
        frontier = int(get_job_fields(r, item["job_id"]).get("encode_frontier") or 0)
    except (TypeError, ValueError):
        frontier = 0
    start = int(item.get("seg_start") or 0)
    if start >= frontier:
        return False
    r.delete(segment_pending_key(item["job_id"], start))
    log.info(
        "[%s] drop segment %d behind encode frontier %d",
        item["job_id"],
        start,
        frontier,
    )
    return True


def _schedule_forward_after_segment(
    r,
    job_id: str,
    source: str,
    start: int,
    end: int,
    segment_frames: int,
) -> None:
    """播放前沿之后，继续编后面还缺的分段。"""
    from sview.jobs.job_status import (
        SPARSE_FORWARD_SEGMENTS,
        enqueue_forward_from,
        get_job_fields,
    )

    try:
        frontier = int(get_job_fields(r, job_id).get("encode_frontier") or 0)
    except (TypeError, ValueError):
        frontier = 0
    if int(end) <= frontier:
        return
    try:
        queued = enqueue_forward_from(
            r,
            job_id,
            source,
            frame_index=max(int(end), frontier),
            segment_frames=segment_frames,
            window=SPARSE_FORWARD_SEGMENTS,
            set_frontier=False,
            bump_epoch=False,
        )
    except Exception:
        log.exception("[%s] forward segment enqueue failed", job_id)
        return
    if queued:
        log.info("[%s] queued forward segments %s", job_id, queued)


def _run_one_segment_job(
    r,
    item: dict,
    on_playable=None,
    *,
    session_client: str = "",
    bound_frontier: int | None = None,
) -> None:
    """编码并原子发布一个可以独立解码的稀疏分段。"""
    import shutil

    from sview.ingest.frame_index import (
        SparseIndexUnavailable,
        find_sparse_decode_start,
        load_or_build_frame_index,
        sparse_anchor_map,
        sparse_origin_seconds,
    )
    from sview.jobs.job_status import (
        POD_IP,
        claim_job_segment,
        complete_segment_requests,
        get_job_fields,
        is_cancel_requested,
        job_active_key,
        release_job_segment,
        release_segment_claim,
        segment_lock_key,
        segment_pending_key,
        segment_subscribers_key,
        read_client_frontier,
        set_job_fields,
    )
    from sview.jobs.origin import mcap_log_time_range

    source = item["source"]
    job_id = item["job_id"]
    start = max(0, int(item.get("seg_start") or 0))
    seg_n = max(1, int(item.get("segment_frames") or SEGMENT_FRAMES))
    epoch = int(item.get("segment_epoch") or 0)
    request_id = str(item.get("request_id") or "")
    if request_id and not (r.smembers(segment_subscribers_key(job_id, start)) or set()):
        r.delete(segment_pending_key(job_id, start))
        log.info("[%s] sparse segment %d has no viewers; skip queued work", job_id, start)
        return
    owner = f"{POD_IP}:{os.getpid()}:{threading.get_ident()}"
    in_session = bool(session_client)

    def _seek_moved() -> bool:
        if not in_session or bound_frontier is None:
            return False
        return read_client_frontier(r, job_id, session_client) != int(bound_frontier)

    if _seek_moved():
        log.info("[%s] window %s seek moved before segment %d", job_id, session_client, start)
        return
    if not in_session:
        if not claim_job_segment(r, job_id, start, owner):
            r.rpush(REDIS_QUEUE_KEY, item.get("raw") or json.dumps(item, ensure_ascii=False))
            log.info("[%s] sparse segment %d deferred; job already encoding", job_id, start)
            return
    lock_key = segment_lock_key(job_id, start)
    if not r.set(lock_key, owner, nx=True, ex=180):
        if not in_session:
            release_job_segment(r, job_id, owner)
        log.info("[%s] sparse segment %d already owned", job_id, start)
        return "busy"
    lock_renewer = _RedisValueLockRenewer(
        r,
        lock_key,
        owner,
        ttl=180,
        also=[(job_active_key(job_id), f"{start}:{owner}")],
    )
    lock_renewer.start()

    local_path = redis_path_to_local(source)
    tmp_job = f"__sparse_{job_id}_{start}_{os.getpid()}_{threading.get_ident()}"
    tmp_dir = LOCAL_OUTPUT_DIR / tmp_job
    try:
        if is_cancel_requested(get_job_fields(r, job_id)):
            return
        from sview.cache.oss_cache_gc import frame_range_ready

        if local_path.suffix.lower() in (".hdf5", ".h5", ".hdf"):
            raise ValueError("sparse seek is not supported for HDF5")
        index_path = LOCAL_OUTPUT_DIR / job_id / "frame_index.json"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        set_job_fields(
            r,
            job_id,
            status="processing",
            publish_mode="sparse",
            encode_status="reading_mcap_index",
            owner_pod=POD_IP,
            error="",
        )
        index_lock_key = f"sview:frame-index:lock:{job_id}"
        index_owner = False
        index_deadline = time.time() + 7200
        while not index_path.is_file():
            if OSS_CACHE:
                try:
                    raw_index = get_oss_bucket().get_object(
                        oss_cache_key(job_id, "frame_index.json")
                    ).read()
                    tmp_index = index_path.with_name(
                        f".{index_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
                    )
                    tmp_index.write_bytes(raw_index)
                    os.replace(tmp_index, index_path)
                    break
                except Exception:
                    pass
            if r.set(index_lock_key, owner, nx=True, ex=7200):
                index_owner = True
                break
            if time.time() >= index_deadline:
                raise TimeoutError("sparse frame-index build timeout")
            time.sleep(0.25)
        try:
            frame_index_data = load_or_build_frame_index(local_path, index_path)
            if OSS_CACHE:
                upload_bytes_to_oss(
                    oss_cache_key(job_id, "frame_index.json"),
                    json.dumps(frame_index_data, separators=(",", ":")).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                    job_id=job_id,
                )
        finally:
            if index_owner:
                try:
                    cur = r.get(index_lock_key)
                    if isinstance(cur, bytes):
                        cur = cur.decode("utf-8", errors="replace")
                    if cur == owner:
                        r.delete(index_lock_key)
                except Exception:
                    pass
        frame_count = int(frame_index_data["frame_count"])
        if start >= frame_count:
            raise ValueError(f"segment start {start} past frame_count {frame_count}")
        end = min(frame_count, int(item.get("seg_end") or start + seg_n))
        existing = _load_sparse_manifest(job_id)
        existing_ranges = ((existing or {}).get("bundle") or {}).get(
            "available_ranges"
        )
        if frame_range_ready(existing_ranges, start, end):
            log.info("[%s] sparse segment %d already published", job_id, start)
            try:
                complete_segment_requests(r, job_id, start)
            except Exception:
                log.exception("[%s] segment request notification failed", job_id)
            if not in_session:
                _schedule_forward_after_segment(r, job_id, source, start, end, seg_n)
            return

        if _seek_moved():
            log.info("[%s] window %s seek moved before locating segment %d", job_id, session_client, start)
            return

        # 分段起点来自索引表，不扫载荷。
        decode_start = find_sparse_decode_start(
            local_path,
            frame_index_data,
            target_frame=start,
            segment_frames=seg_n,
        )
        offset = start - decode_start
        anchors = sparse_anchor_map(frame_index_data, decode_start)
        bag_t0, _bag_t1 = mcap_log_time_range(local_path)
        origin_sec = sparse_origin_seconds(
            frame_index_data, decode_start, int(bag_t0 or 0)
        )

        def _bounded(done, _total):
            if lock_renewer.lost():
                raise SparseSegmentAborted()
            if is_cancel_requested(get_job_fields(r, job_id)):
                raise SparseSegmentAborted()
            if _seek_moved():
                raise SparseSegmentAborted()
            produced = int(done or 0)
            if produced <= 0 and offset > 0:
                stage = "decoding"
            else:
                stage = "encoding"
            set_job_fields(
                r,
                job_id,
                encode_status=stage,
                input_frames=offset + produced,
                output_frames=produced,
            )
            if produced >= end - start:
                raise SparseSegmentComplete()

        if _seek_moved():
            log.info("[%s] window %s seek moved before encoding segment %d", job_id, session_client, start)
            return
        set_job_fields(
            r,
            job_id,
            status="processing",
            publish_mode="sparse",
            encode_status="decoding" if offset else "encoding",
            encode_window=json.dumps([start, end], separators=(",", ":")),
            decode_start=decode_start,
            input_frames=0,
            output_frames=0,
            segment_epoch=epoch,
            owner_pod=POD_IP,
            error="",
        )
        try:
            with temporary_transcode_settings(force_reencode=1):
                process_mcap(
                    str(local_path),
                    no_upload=True,
                    progress_cb=_bounded,
                    job_id=tmp_job,
                    origin_sec=origin_sec,
                    sparse_anchors=anchors,
                    sparse_encode_offset=offset,
                    sparse_encode_count=end - start,
                )
        except SparseSegmentComplete:
            pass
        except SparseSegmentAborted:
            return

        frame_dir = tmp_dir / "frames"
        selected: list[bytes] = []
        for local_idx, global_idx in enumerate(range(start, end)):
            path = frame_dir / f"{offset + local_idx:06d}.bin"
            if not path.is_file():
                raise FileNotFoundError(f"sparse frame missing: {path.name}")
            selected.append(_rewrite_bundle_global_index(path.read_bytes(), global_idx))
        temp_manifest_path = tmp_dir / "manifest.json"
        if not temp_manifest_path.is_file():
            raise FileNotFoundError("sparse temp manifest missing")
        base_manifest = json.loads(temp_manifest_path.read_text(encoding="utf-8"))
        _publish_sparse_segment(
            r,
            job_id=job_id,
            source=source,
            start=start,
            end=end,
            frame_blobs=selected,
            base_manifest=base_manifest,
            frame_index_data=frame_index_data,
            epoch=epoch,
        )
        log.info("[%s] sparse segment published [%d:%d)", job_id, start, end)
        if not in_session:
            _schedule_forward_after_segment(r, job_id, source, start, end, seg_n)
        if on_playable is not None:
            on_playable(job_id)
    except SparseIndexUnavailable as exc:
        try:
            complete_segment_requests(r, job_id, start, error=str(exc))
        except Exception:
            log.exception("[%s] segment failure notification failed", job_id)
        set_job_fields(
            r,
            job_id,
            status="failed",
            publish_mode="sparse",
            encode_status="unsupported",
            error=str(exc)[:500],
        )
        log.warning("[%s] sparse segment %d unsupported: %s", job_id, start, exc)
        return "error"
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        try:
            complete_segment_requests(r, job_id, start, error=str(exc))
        except Exception:
            log.exception("[%s] segment failure notification failed", job_id)
        set_job_fields(
            r,
            job_id,
            status="processing",
            publish_mode="sparse",
            encode_status="failed",
            error=str(exc)[:500],
        )
        log.exception("[%s] sparse segment %d failed", job_id, start)
        return "error"
    finally:
        lock_renewer.stop()
        shutil.rmtree(tmp_dir, ignore_errors=True)
        if OSS_CACHE:
            try:
                from sview.cache.oss_cache_gc import delete_job_prefix

                delete_job_prefix(tmp_job)
            except Exception:
                log.debug("temporary sparse OSS cleanup failed", exc_info=True)
        try:
            release_job_segment(r, job_id, owner)
            release_segment_claim(r, job_id, start, owner=owner)
        except Exception:
            log.exception("[%s] release segment claim failed", job_id)


def _clear_job_cache(job_id: str, *, purge_oss: bool = True) -> None:
    import shutil as _shutil

    out_dir = LOCAL_OUTPUT_DIR / job_id
    if out_dir.is_dir():
        _shutil.rmtree(out_dir, ignore_errors=True)
        log.info("Cleared local cache for job %s", job_id)
    if purge_oss and OSS_CACHE:
        try:
            from sview.cache.oss_cache_gc import delete_job_prefix

            n = delete_job_prefix(job_id)
            log.info("Cleared OSS play cache for job %s objects=%s", job_id, n)
        except Exception:
            log.exception("OSS play-cache clear failed for %s", job_id)


class _JobLockRenewer:
    """后台续期，免得长 ffmpeg 块把锁拖过期。"""

    def __init__(self, r, job_id: str):
        from sview.jobs.job_status import SVIEW_LOCK_TTL_SEC, renew_job_lock

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


def _next_session_segment(
    frontier: int,
    total: int,
    ranges,
    seg_n: int,
    *,
    locked_fn,
    cooling_fn,
) -> tuple[int | None, bool, list[int]]:
    """取前沿处或之后的下一个未发布分段。

    第一段缺口就是播放正在等的那段。它要是
    锁着或在冷却，就先不返回分段，让调用方等着重试
    这一段。跳到更后面会开一个播放头过不去的洞。
    ``blocked`` 表示这段在，但现在还拿不到。
    """
    from sview.cache.oss_cache_gc import frame_range_ready

    nxt = None
    blocked = False
    skipped_locked: list[int] = []
    seg = max(0, int(frontier))
    seg_n = max(1, int(seg_n))
    guard = 0
    while guard < 10000 and (total <= 0 or seg < total):
        end = min(total, seg + seg_n) if total > 0 else seg + seg_n
        missing = total <= 0 or (
            end > seg and not frame_range_ready(ranges, seg, end)
        )
        # 播放头过不了空洞。这段还锁着或在冷却时去编后面的分段，
        # 播放会卡在这个缺口上。
        if missing and locked_fn(seg):
            return None, True, [seg]
        if missing and cooling_fn(seg):
            return None, True, []
        if missing:
            nxt = seg
            break
        seg += seg_n
        guard += 1
    return nxt, blocked, skipped_locked


def _same_forward_cursor(cursor: int | None, nxt: int) -> bool:
    """下一段能接上已经打开的读取和 ffmpeg 时为真。"""
    return cursor is not None and int(cursor) == int(nxt)


def _frontier_left_forward_run(
    frontier: int,
    opened: int,
    total: int,
    ranges,
    seg_n: int,
    reading: int | None = None,
) -> bool:
    """客户端现在要的缺失分段已经变了则为真。

    编码去补下一个洞时，播放头还停在已发布帧里，
    所以前沿落在 ``opened`` 后面是正常的。前沿指向
    更早的洞，或新的 seek 要的是当前正在读的
    另一段时，才中止。
    """
    current = int(opened if reading is None else reading)
    nxt, _blocked, _skipped = _next_session_segment(
        frontier,
        total,
        ranges,
        seg_n,
        locked_fn=lambda _seg: False,
        cooling_fn=lambda _seg: False,
    )
    if nxt is None:
        return int(frontier) < int(opened)
    return int(nxt) != current


def _forward_read_floor(anchors: dict) -> int:
    """各通道里最早的正时间戳。0 不能把位置倒回字节 0。"""
    vals: list[int] = []
    for raw in anchors.values():
        try:
            ts = int(raw)
        except (TypeError, ValueError):
            continue
        if ts > 0:
            vals.append(ts)
    return min(vals) if vals else 0


def _headcam_forward_topic(topic: str) -> bool:
    """视频、相机 meta 和 IMU。其他 protobuf topic 不读。"""
    from sview.ingest.orin_images import sview_channel_for_topic

    raw = (topic or "").strip()
    if raw == "/imu/sample":
        return True
    if "/meta" in raw and "camera" in raw.lower():
        return sview_channel_for_topic(raw) is not None
    if "/video" in raw or "/h264" in raw.lower():
        return sview_channel_for_topic(raw) is not None
    return False


def _headcam_forward_topics(path: Path) -> list[str]:
    """一次头显前向读取的 topic 白名单。包含 IMU。"""
    from mcap.reader import make_reader

    found: list[str] = []
    try:
        with Path(path).open("rb") as fh:
            summary = make_reader(fh).get_summary()
        if summary is not None:
            for channel in summary.channels.values():
                topic = (channel.topic or "").strip()
                if topic and _headcam_forward_topic(topic) and topic not in found:
                    found.append(topic)
    except Exception:
        log.exception("head-cam topic scan failed for %s", path)
    if found:
        return found
    return [
        "/camera/RR/video",
        "/camera/FR/video",
        "/camera/RL/video",
        "/camera/FL/video",
        "/camera/RR/meta",
        "/camera/FR/meta",
        "/camera/RL/meta",
        "/camera/FL/meta",
        "/imu/sample",
    ]


class _HeadcamForwardRun:
    """一次前向播放：一个打开的 mcap，一组常驻 ffmpeg。

    在 seek 处打开。后面连续的分段继续喂这个读取器和
    这些编码器。往后跳、遇到洞、或会话结束就关掉。
    """

    def __init__(self, *, local_path: Path, job_id: str, source: str, seg_n: int):
        self.path = local_path
        self.job_id = job_id
        self.source = source
        self.seg_n = max(1, int(seg_n))
        self.next_frame: int | None = None
        self.open_frame = 0
        self.index: dict = {}
        self.metas: dict = {}
        self.channels: list[str] = []
        self.frames: dict[str, list] = defaultdict(list)
        self.timestamps: dict[str, list] = defaultdict(list)
        self.imu: list[dict] = []
        self.encoders: dict | None = None
        self._messages = None
        self._anchors: dict[str, int] = {}
        self._seg_anchors: dict[str, int] = {}
        self._primed = False
        # 这一路的解码器是否已经吃到过关键帧（之后的 P 帧都能解）。
        self._decodable: dict[str, bool] = {}
        # 上一段只喂了关键帧之后的尾巴，ffmpeg 的关键帧计数错位了：
        # 下一段先重开这一路，用这些帧预热解码再对齐。
        self._realign: dict[str, list] = {}
        self._enc_args: tuple = (None, None, 0)
        self._floor = 0
        self._seek_check_at = 0.0
        self._index_uploaded = False
        self.closed = False

    def continues(self, nxt: int) -> bool:
        return (not self.closed) and _same_forward_cursor(self.next_frame, nxt)

    def close(self) -> None:
        self.closed = True
        self.next_frame = None
        _close_live_encoders(self.encoders)
        self.encoders = None
        gen = self._messages
        self._messages = None
        if gen is not None:
            try:
                gen.close()
            except Exception:
                pass

    def open_at(self, start: int, frame_index: dict) -> None:
        from sview.ingest.frame_index import sparse_anchor_map

        self.close()
        self.closed = False
        self.index = frame_index
        total = int(frame_index.get("frame_count") or 0)
        if total and start >= total:
            raise ValueError(f"segment start {start} past frame_count {total}")
        # 从上一个源 IDR 打开，而不是分段边界。
        # 边界如果是 P 帧就解不出，30 个输入会少吐图像。
        lookback = min(int(start), 120)
        origin = int(start) - lookback
        self._seg_anchors = sparse_anchor_map(frame_index, start)
        origin_anchors = (
            sparse_anchor_map(frame_index, origin)
            if origin != int(start)
            else self._seg_anchors
        )
        self._anchors = origin_anchors
        self._floor = _forward_read_floor(origin_anchors)
        self._primed = False
        self._decodable = {}
        self._realign = {}
        self._seek_check_at = 0.0
        self.open_frame = int(start)
        self.next_frame = int(start)
        self.frames = defaultdict(list)
        self.timestamps = defaultdict(list)
        self.imu = []
        self.channels = []
        self.metas = {}
        topics = _headcam_forward_topics(self.path)
        self._messages = read_protobuf_messages(
            str(self.path),
            start_time_ns=self._floor or None,
            topics=topics,
        )
        log.info(
            "[%s] forward run opened at frame %d floor=%d topics=%d (video, meta, imu)",
            self.job_id,
            start,
            self._floor,
            len(topics),
        )

    def _accept_ts(self, ts_ns: int, channel: str | None) -> bool:
        try:
            t = int(ts_ns)
        except (TypeError, ValueError):
            return True
        if channel is not None and channel in self._anchors:
            return t >= int(self._anchors[channel])
        return t >= int(self._floor or 0)

    def _pull_until(self, need: int, seek_back) -> None:
        if not self.channels:
            self.channels = list(HEADCAM_CHANNELS)
        while min(len(self.frames[ch]) for ch in self.channels) < need:
            self._read_one(seek_back)

    def _read_one(self, seek_back) -> None:
        from sview.ingest.orin_images import sview_channel_for_topic

        now = time.monotonic()
        if now - self._seek_check_at >= 0.1:
            self._seek_check_at = now
            if seek_back():
                raise SparseSegmentAborted()
        try:
            msg = next(self._messages)
        except StopIteration as exc:
            raise McapEnded(
                f"mcap ended before aligned frames at {self.next_frame}"
            ) from exc
        topic = msg.topic or ""
        if "/meta" in topic and "camera" in topic.lower():
            ch = sview_channel_for_topic(topic)
            if ch and ch not in self.metas:
                try:
                    p = msg.proto_msg
                    self.metas[ch] = {
                        "width": int(p.width),
                        "height": int(p.height),
                        "fps": int(getattr(p, "fps", 30)),
                        "position": str(getattr(p, "position", ch)),
                        "codec": str(getattr(p, "codec", "") or "").lower(),
                    }
                except Exception:
                    pass
            return
        if "/video" in topic or "/h264" in topic.lower():
            ch = sview_channel_for_topic(topic)
            if not ch or ch not in HEADCAM_CHANNEL_SET:
                return
            if not self._accept_ts(msg.log_time_ns, ch):
                return
            self.frames[ch].append(msg.proto_msg.data)
            self.timestamps[ch].append(int(msg.log_time_ns))
            return
        if topic == "/imu/sample" and self._accept_ts(msg.log_time_ns, None):
            try:
                p = msg.proto_msg
                self.imu.append({
                    "log_time_ns": int(msg.log_time_ns),
                    "monotonic_ns": str(p.monotonic_raw_ns),
                    "wall_ns": str(p.wall_ns),
                    "x": p.x, "y": p.y, "z": p.z,
                    "temp_c": p.temp_c,
                    "unit": p.unit,
                    "scale": p.scale,
                    "sample_index": int(getattr(p, "sample_index", len(self.imu))),
                })
            except Exception:
                pass

    def _ensure_encoders(self) -> None:
        if self.encoders is not None:
            return
        if _resize_requested():
            _apply_output_scale(self.metas)
        self._enc_args = (
            _scale_dim(self.metas, "w"),
            _scale_dim(self.metas, "h"),
            _live_worker_threads(len(self.channels)),
        )
        n_threads = self._enc_args[2]
        ovr = getattr(_tls, "override", None)
        encoders: dict = {}

        def _one(ch: str):
            return ch, self._make_encoder(ch)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        try:
            with ThreadPoolExecutor(max_workers=max(1, len(self.channels))) as pool:
                futs = [pool.submit(_tls_call, ovr, _one, ch) for ch in self.channels]
                for fut in as_completed(futs):
                    ch, enc = fut.result()
                    encoders[ch] = enc
            self.encoders = encoders
            log.info(
                "[%s] forward run ffmpeg x%d gop=%d threads=%d",
                self.job_id, len(encoders), self.seg_n, n_threads,
            )
        except Exception:
            _close_live_encoders(encoders)
            raise

    def _make_encoder(self, ch: str):
        sw, sh, n_threads = self._enc_args
        meta = self.metas.get(ch) or {}
        return LiveX264Encoder.annexb(
            codec=str(meta.get("codec") or "hevc"),
            scale_w=sw,
            scale_h=sh,
            job_id=self.job_id,
            fps=float(meta.get("fps") or 30),
            gop=self.seg_n,
            threads=n_threads,
        )

    def _restart_encoder(self, ch: str):
        """单独重开这一路 ffmpeg，让关键帧计数从 0 重新对齐分段边界。"""
        old = (self.encoders or {}).pop(ch, None)
        if old is not None:
            _close_live_encoders({ch: old})
        enc = self._make_encoder(ch)
        self.encoders[ch] = enc
        return enc

    def _segment_ready(self, need: int) -> bool:
        if not self.channels:
            return False
        for ch in self.channels:
            anchor = None if self._primed else self._seg_anchors.get(ch)
            if anchor is None:
                if len(self.frames[ch]) < need:
                    return False
                continue
            cut = next(
                (i for i, t in enumerate(self.timestamps[ch]) if int(t) >= int(anchor)),
                None,
            )
            if cut is None or len(self.frames[ch]) < cut + need:
                return False
        return True

    def _pull_segment(self, need: int, seek_back) -> None:
        # 一次一条消息。旧辅助函数把各通道之和
        # 当成了单通道目标，最后一段就会读到 EOF，
        # 这段其实已经齐了也会失败。
        if not self.channels:
            self.channels = list(HEADCAM_CHANNELS)
        while not self._segment_ready(need):
            try:
                self._read_one(seek_back)
            except McapEnded:
                if self._segment_ready(need):
                    return
                raise

    def _encode_blobs(self, need: int) -> list[bytes]:
        assert self.encoders is not None
        from concurrent.futures import ThreadPoolExecutor, as_completed

        ovr = getattr(_tls, "override", None)
        aus_by_ch: dict[str, list] = {}
        ts_by_ch: dict[str, list] = {}
        first = not self._primed

        def _one(ch: str):
            frames = self.frames[ch]
            ts = self.timestamps[ch]
            cut = 0
            skip = 0
            if first:
                anchor = self._seg_anchors.get(ch)
                if anchor is not None:
                    for i, t in enumerate(ts):
                        if int(t) >= int(anchor):
                            cut = i
                            break
                idr = cut
                for i in range(min(cut, len(frames) - 1), -1, -1):
                    if frames[i] and _au_has_random_access(frames[i]):
                        idr = i
                        break
                skip = max(0, cut - idr)
                if skip:
                    log.info(
                        "[%s] %s warmup %d frames from source IDR before frame %s",
                        self.job_id, ch, skip, self.next_frame,
                    )
            window = list(frames[cut:cut + need])
            warm = self._realign.pop(ch, None)
            if warm is not None:
                enc = self._restart_encoder(ch)
                aus = enc.encode_frames(warm + window, output_skip=len(warm))
                self._decodable[ch] = True
            elif skip:
                aus = self.encoders[ch].encode_frames(
                    frames[cut - skip:cut + need], output_skip=skip
                )
                self._decodable[ch] = True
            elif (
                getattr(self, "kind", "") == "jpeg"
                or self._decodable.get(ch)
                or (window and _au_has_random_access(window[0] or b""))
            ):
                aus = self.encoders[ch].encode_frames(window)
                self._decodable[ch] = True
            else:
                # 第一张 IDR 之前的 P 帧解不出：这些位置留空不发，
                # 只编 IDR 及之后的帧，放回原来的帧号上。
                lead = _frames_before_random_access(window)
                tail = window[lead:]
                aus = [b""] * lead
                if tail:
                    aus += self.encoders[ch].encode_frames(tail)
                    self._decodable[ch] = True
                    self._realign[ch] = tail
                log.info(
                    "[%s] %s leaves %d frames empty before first IDR at segment frame %s",
                    self.job_id, ch, lead, self.next_frame,
                )
            if len(aus) > need:
                aus = aus[:need]
            if len(aus) < need:
                aus.extend([b""] * (need - len(aus)))
            return ch, aus, cut + need, ts[cut:cut + need]

        with ThreadPoolExecutor(max_workers=max(1, len(self.channels))) as pool:
            futs = [pool.submit(_tls_call, ovr, _one, ch) for ch in self.channels]
            for fut in as_completed(futs):
                ch, aus, drop, ts_out = fut.result()
                aus_by_ch[ch] = aus
                ts_by_ch[ch] = ts_out
                self.frames[ch] = self.frames[ch][drop:]
                self.timestamps[ch] = self.timestamps[ch][drop:]
        blobs: list[bytes] = []
        codecs = {
            ch: str((self.metas.get(ch) or {}).get("stored_codec") or "h264")
            for ch in self.channels
        }
        imu_at = 0
        imu_drop = 0
        for i in range(need):
            streams = {ch: aus_by_ch[ch][i] for ch in self.channels}
            ts_vals = [ts_by_ch[ch][i] for ch in self.channels if i < len(ts_by_ch.get(ch, []))]
            ts = min(ts_vals) if ts_vals else 0
            nxt_vals = [
                ts_by_ch[ch][i + 1]
                for ch in self.channels
                if i + 1 < len(ts_by_ch.get(ch, []))
            ]
            tw_end = min(nxt_vals) if nxt_vals else ts + 33_333_333
            samples = []
            while imu_at < len(self.imu) and int(self.imu[imu_at]["log_time_ns"]) < ts:
                imu_at += 1
            j = imu_at
            while j < len(self.imu) and int(self.imu[j]["log_time_ns"]) < tw_end:
                samples.append(self.imu[j])
                j += 1
            imu_drop = j
            is_key = all(
                (not streams[ch]) or _h264_payload_has_idr(streams[ch])
                for ch in self.channels
            )
            blobs.append(_build_bundle_frame(
                i,
                streams,
                ts,
                [ts, tw_end],
                is_key,
                {"samples": samples},
                {"records": []},
                codecs,
            ))
        self._primed = True
        self.imu = self.imu[imu_drop:]
        return blobs

    def publish_segment(
        self,
        r,
        start: int,
        end: int,
        *,
        owner: str,
        epoch: int,
        seek_back,
        on_playable=None,
    ) -> str:
        """在已经打开的这一轮上编码并发布 [start, end)。"""
        from sview.jobs.job_status import (
            POD_IP,
            job_active_key,
            release_segment_claim,
            segment_lock_key,
            set_job_fields,
        )

        if self.closed or self.next_frame != start:
            raise RuntimeError(f"forward run is not positioned at {start}")
        lock_key = segment_lock_key(self.job_id, start)
        if not r.set(lock_key, owner, nx=True, ex=180):
            log.info("[%s] sparse segment %d already owned", self.job_id, start)
            return "busy"
        renewer = _RedisValueLockRenewer(
            r, lock_key, owner, ttl=180,
            also=[(job_active_key(self.job_id), f"{start}:{owner}")],
        )
        renewer.start()
        try:
            if seek_back():
                raise SparseSegmentAborted()
            set_job_fields(
                r,
                self.job_id,
                status="processing",
                publish_mode="sparse",
                encode_status="encoding",
                encode_window=json.dumps([start, end], separators=(",", ":")),
                decode_start=start,
                input_frames=0,
                output_frames=0,
                segment_epoch=epoch,
                owner_pod=POD_IP,
                error="",
            )
            with temporary_transcode_settings(force_reencode=1):
                need = end - start
                self._pull_segment(need, seek_back)
                self._ensure_encoders()
                blobs = self._encode_blobs(need)
            manifest = _build_manifest_dict(
                job_id=self.job_id,
                camera_metas=self.metas,
                frame_count=int(self.index.get("frame_count") or 0) or None,
                frames_available=0,
                key_frame_indices=[start],
                timeline_start_ns=0,
                timeline_end_ns=0,
                status="partial",
            )
            _publish_sparse_segment(
                r,
                job_id=self.job_id,
                source=self.source,
                start=start,
                end=end,
                frame_blobs=blobs,
                base_manifest=manifest,
                frame_index_data=self.index,
                epoch=epoch,
                upload_index=not self._index_uploaded,
            )
            self._index_uploaded = True
            self.next_frame = end
            log.info("[%s] forward run published [%d:%d)", self.job_id, start, end)
            if on_playable is not None:
                on_playable(self.job_id)
            return "ok"
        except SparseSegmentAborted:
            log.info("[%s] forward run dropped; seek left frame %d", self.job_id, self.open_frame)
            self.close()
            return "seek"
        except McapEnded:
            counts = {ch: len(self.frames.get(ch) or []) for ch in self.channels}
            log.info(
                "[%s] mcap ended before frame %d; buffered=%s",
                self.job_id, start, counts,
            )
            self.close()
            return "end"
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            log.exception("[%s] forward segment %d failed", self.job_id, start)
            try:
                set_job_fields(
                    r,
                    self.job_id,
                    status="processing",
                    publish_mode="sparse",
                    encode_status="failed",
                    error=str(exc)[:500],
                )
            except Exception:
                pass
            self.close()
            return "error"
        finally:
            renewer.stop()
            try:
                release_segment_claim(r, self.job_id, start, owner=owner)
            except Exception:
                log.exception("[%s] release segment claim failed", self.job_id)


class _TeleopForwardRun(_HeadcamForwardRun):
    """遥操作 ROS2 的前向读取。文件和 ffmpeg 与头显一样常驻。

    HDF5 不走这条路径。只解码视频、相机信息和 IMU；
    IMU 采样打进每个分段 bin。
    """

    def __init__(self, *, kind: str, local_path: Path, job_id: str, source: str, seg_n: int):
        super().__init__(
            local_path=local_path,
            job_id=job_id,
            source=source,
            seg_n=seg_n,
        )
        self.kind = (kind or "").strip().lower()

    def open_at(self, start: int, frame_index: dict) -> None:
        from sview.ingest.frame_index import sparse_anchor_map
        from sview.ingest.orin_images import iter_teleop_forward

        self.close()
        self.closed = False
        self.index = frame_index
        total = int(frame_index.get("frame_count") or 0)
        if total and start >= total:
            raise ValueError(f"segment start {start} past frame_count {total}")
        lookback = 0 if self.kind == "jpeg" else min(int(start), 120)
        origin = int(start) - lookback
        self._seg_anchors = sparse_anchor_map(frame_index, start)
        origin_anchors = (
            sparse_anchor_map(frame_index, origin)
            if origin != int(start)
            else self._seg_anchors
        )
        self._anchors = origin_anchors
        self._floor = _forward_read_floor(origin_anchors)
        self._primed = False
        self._decodable = {}
        self._realign = {}
        self._seek_check_at = 0.0
        self.open_frame = int(start)
        self.next_frame = int(start)
        self.frames = defaultdict(list)
        self.timestamps = defaultdict(list)
        self.imu = []
        self.metas = {}
        indexed = list((frame_index.get("channels") or {}).keys())
        self.channels = [ch for ch in indexed if ch in self._seg_anchors] or list(self._seg_anchors)
        self._messages = iter_teleop_forward(
            self.path,
            self.kind,
            start_time_ns=self._floor or None,
        )
        log.info(
            "[%s] teleop forward run opened at frame %d kind=%s floor=%d channels=%d",
            self.job_id,
            start,
            self.kind,
            self._floor,
            len(self.channels),
        )

    def _pull_until(self, need: int, seek_back) -> None:
        if not self.channels:
            self._read_one(seek_back)
            return
        while min(len(self.frames[ch]) for ch in self.channels) < need:
            self._read_one(seek_back)

    def _read_one(self, seek_back) -> None:
        now = time.monotonic()
        if now - self._seek_check_at >= 0.1:
            self._seek_check_at = now
            if seek_back():
                raise SparseSegmentAborted()
        try:
            event = next(self._messages)
        except StopIteration as exc:
            raise McapEnded(
                f"mcap ended before aligned frames at {self.next_frame}"
            ) from exc
        tag = event[0]
        if tag == "imu":
            row = dict(event[1])
            if not self._accept_ts(row.get("log_time_ns"), None):
                return
            row["sample_index"] = len(self.imu)
            self.imu.append(row)
            return
        if tag != "video":
            return
        _tag, ch, data, ts, meta = event
        if self.channels and ch not in self.channels:
            return
        if not self.channels:
            self.channels = [ch]
        if ch not in self.metas and meta:
            self.metas[ch] = dict(meta)
        if not self._accept_ts(ts, ch):
            return
        self.frames[ch].append(data)
        self.timestamps[ch].append(int(ts))

    def _ensure_encoders(self) -> None:
        if self.encoders is not None:
            return
        if _resize_requested():
            _apply_output_scale(self.metas)
        self._enc_args = (
            _scale_dim(self.metas, "w"),
            _scale_dim(self.metas, "h"),
            _live_worker_threads(len(self.channels) or 1),
        )
        n_threads = self._enc_args[2]
        ovr = getattr(_tls, "override", None)
        encoders: dict = {}

        def _one(ch: str):
            return ch, self._make_encoder(ch)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        try:
            with ThreadPoolExecutor(max_workers=max(1, len(self.channels))) as pool:
                futs = [pool.submit(_tls_call, ovr, _one, ch) for ch in self.channels]
                for fut in as_completed(futs):
                    ch, enc = fut.result()
                    encoders[ch] = enc
            self.encoders = encoders
            log.info(
                "[%s] teleop forward ffmpeg x%d kind=%s gop=%d threads=%d",
                self.job_id, len(encoders), self.kind, self.seg_n, n_threads,
            )
        except Exception:
            _close_live_encoders(encoders)
            raise

    def _make_encoder(self, ch: str):
        sw, sh, n_threads = self._enc_args
        meta = self.metas.get(ch) or {}
        fps = float(meta.get("fps") or 30)
        if self.kind == "jpeg":
            return LiveX264Encoder.jpeg(
                scale_w=sw,
                scale_h=sh,
                job_id=self.job_id,
                fps=fps,
                crf=_tc_crf(),
                direct=ORIN_DIRECT,
                threads=n_threads,
            )
        return LiveX264Encoder.annexb(
            codec=str(meta.get("codec") or "h264"),
            scale_w=sw,
            scale_h=sh,
            job_id=self.job_id,
            fps=fps,
            gop=self.seg_n,
            threads=n_threads,
        )


def _run_sparse_session(r, item: dict, on_playable=None) -> None:
    """一个播放窗口一条编码线程。seek 只移动它的前沿。"""
    from sview.jobs.job_status import (
        POD_IP,
        claim_segment_slot,
        fail_job,
        first_unencoded_segment,
        get_job_fields,
        hand_off_waiting_windows,
        live_run_at,
        mark_decode_end,
        read_client_frontier,
        release_segment_slot,
        segment_lock_key,
        segment_slot_key,
        session_alive_key,
        session_pending_key,
        set_job_fields,
    )

    job_id = item["job_id"]
    client_id = (item.get("client_id") or "anonymous")[:128]
    source = item["source"]
    seg_n = max(1, int(item.get("segment_frames") or SEGMENT_FRAMES))
    owner = f"{POD_IP}:{os.getpid()}:{threading.get_ident()}"
    alive = session_alive_key(job_id, client_id)
    if not r.set(alive, owner, nx=True, ex=60):
        r.delete(session_pending_key(job_id, client_id))
        log.info("[%s] encode session already running for window %s", job_id, client_id)
        return
    r.delete(session_pending_key(job_id, client_id))
    target = first_unencoded_segment(r, job_id, client_id, seg_n)
    if target is None or not claim_segment_slot(r, job_id, target, client_id):
        # 另一个窗口已经占着这段唯一的 worker 槽。
        current = r.get(alive)
        if isinstance(current, bytes):
            current = current.decode("utf-8", errors="replace")
        if current == owner:
            r.delete(alive)
        log.info(
            "[%s] window %s waits for segment %s without taking a worker slot",
            job_id,
            client_id,
            target,
        )
        return
    log.info(
        "[%s] encode session started for window %s segment %d",
        job_id, client_id, target,
    )
    held = target
    renewer = _RedisValueLockRenewer(
        r,
        alive,
        owner,
        ttl=60,
        also=[(segment_slot_key(job_id, held), client_id)],
    )
    renewer.start()
    fail_counts: dict[int, int] = {}
    retry_after: dict[int, float] = {}
    forward: _HeadcamForwardRun | None = None
    try:
        try:
            local_path = redis_path_to_local(source)
            family = ingest_family(source)
            from sview.ingest.hdf5_images import is_hdf5_file

            teleop_kind = ""
            use_forward = family == "head_cam" and not is_hdf5_file(local_path)
            if family == "teleop" and not is_hdf5_file(local_path):
                from sview.ingest.orin_images import orin_stream_kind

                if get_job_fields(r, job_id).get("status") in ("", "queued"):
                    set_job_fields(
                        r,
                        job_id,
                        status="processing",
                        publish_mode="sparse",
                        encode_status="probing_streams",
                        owner_pod=POD_IP,
                        error="",
                    )
                teleop_kind = orin_stream_kind(local_path, skip_headcam_guard=True) or ""
                use_forward = teleop_kind in ("h264", "jpeg")
        except Exception as exc:
            log.exception("[%s] sparse session probe failed", job_id)
            fail_job(r, job_id, f"probe failed: {exc}", source=source, publish_mode="sparse")
            return
        idle = 0
        while idle < 10:
            try:
                r.expire(alive, 60)
            except Exception:
                pass
            frontier = read_client_frontier(r, job_id, client_id)
            fields = get_job_fields(r, job_id)
            try:
                total = int(fields.get("frame_count") or fields.get("frames_total") or 0)
            except (TypeError, ValueError):
                total = 0
            try:
                ranges = json.loads(fields.get("available_ranges") or "[]")
            except (TypeError, ValueError, json.JSONDecodeError):
                ranges = []
            now = time.time()
            nxt, blocked, skipped_locked = _next_session_segment(
                frontier,
                total,
                ranges,
                seg_n,
                locked_fn=lambda seg: bool(r.get(segment_lock_key(job_id, seg))),
                cooling_fn=lambda seg: now < retry_after.get(int(seg), 0),
            )
            for skipped in skipped_locked:
                log.info(
                    "[%s] window %s waits; segment %d is already being encoded",
                    job_id,
                    client_id,
                    skipped,
                )
            if (
                nxt is None
                and blocked
                and skipped_locked
                and any(
                    not claim_segment_slot(r, job_id, seg, client_id)
                    for seg in skipped_locked
                )
            ):
                log.info(
                    "[%s] window %s leaves; segment %d already has an encode slot",
                    job_id,
                    client_id,
                    skipped_locked[0],
                )
                break
            if nxt is None or read_client_frontier(r, job_id, client_id) != frontier:
                if nxt is None:
                    if blocked:
                        idle = 0
                        time.sleep(1.0)
                    else:
                        idle += 1
                        time.sleep(0.4)
                continue
            if nxt > held + seg_n and live_run_at(r, job_id, nxt, client_id, seg_n):
                # 编到了别人已经编好的区间，区间末尾还有活着的线程在往后推，
                # 两条线在这里接上：关掉这一条，后面的帧用对方的缓存。
                log.info(
                    "[%s] window %s joins the run at segment %d; closing this encoder",
                    job_id,
                    client_id,
                    nxt,
                )
                break
            if not claim_segment_slot(r, job_id, nxt, client_id):
                # 别人已经在编的分段，这个窗口不再另开一条
                # 在途线程。播放等待
                # 已发布的分段。
                log.info(
                    "[%s] window %s leaves; segment %d already has an encode slot",
                    job_id,
                    client_id,
                    nxt,
                )
                break
            if held != nxt:
                release_segment_slot(r, job_id, held, client_id)
                held = nxt
                renewer._also = [(segment_slot_key(job_id, held), client_id)]
            idle = 0
            seg_end = min(total, nxt + seg_n) if total > 0 else nxt + seg_n
            if use_forward:
                opening = False
                try:
                    if forward is None or not forward.continues(nxt):
                        opening = True
                        if forward is not None:
                            log.info(
                                "[%s] forward run reopen at %d (cursor was %s)",
                                job_id, nxt, forward.next_frame,
                            )
                            forward.close()
                            forward = None
                        from sview.ingest.frame_index import load_or_build_frame_index

                        index_path = LOCAL_OUTPUT_DIR / job_id / "frame_index.json"
                        index_path.parent.mkdir(parents=True, exist_ok=True)
                        if not index_path.exists():
                            set_job_fields(
                                r,
                                job_id,
                                publish_mode="sparse",
                                encode_status="reading_mcap_index",
                                owner_pod=POD_IP,
                            )
                        frame_index = load_or_build_frame_index(local_path, index_path)
                        if family == "teleop":
                            forward = _TeleopForwardRun(
                                kind=teleop_kind,
                                local_path=local_path,
                                job_id=job_id,
                                source=source,
                                seg_n=seg_n,
                            )
                        else:
                            forward = _HeadcamForwardRun(
                                local_path=local_path,
                                job_id=job_id,
                                source=source,
                                seg_n=seg_n,
                            )
                        forward.open_at(nxt, frame_index)
                        opening = False
                    else:
                        log.info(
                            "[%s] forward run continues at frame %d without reopening mcap",
                            job_id, nxt,
                        )

                    def _seek_back(
                        opened: int = forward.open_frame,
                        reading: int = nxt,
                    ) -> bool:
                        return _frontier_left_forward_run(
                            read_client_frontier(r, job_id, client_id),
                            opened,
                            total,
                            ranges,
                            seg_n,
                            reading=reading,
                        )

                    result = forward.publish_segment(
                        r,
                        nxt,
                        seg_end,
                        owner=owner,
                        epoch=int(item.get("segment_epoch") or 0),
                        seek_back=_seek_back,
                        on_playable=on_playable,
                    )
                    if forward.closed:
                        forward = None
                except Exception as exc:
                    log.exception("[%s] forward run failed at %d", job_id, nxt)
                    if forward is not None:
                        forward.close()
                        forward = None
                    if opening and isinstance(exc, ValueError) and not ranges:
                        # 索引缺失或没有可识别的视频流，重试也一样，直接判失败。
                        fail_job(r, job_id, str(exc), source=source, publish_mode="sparse")
                        break
                    try:
                        set_job_fields(
                            r,
                            job_id,
                            publish_mode="sparse",
                            encode_status="failed",
                            error=str(exc)[:500],
                        )
                    except Exception:
                        pass
                    result = "error"
            else:
                log.info("[%s] window %s encodes segment %d", job_id, client_id, nxt)
                result = _run_one_segment_job(
                    r,
                    {
                        "source": source,
                        "job_id": job_id,
                        "seg_start": nxt,
                        "seg_end": seg_end,
                        "segment_frames": seg_n,
                        "client_id": client_id,
                        "request_id": "",
                    },
                    on_playable=on_playable,
                    session_client=client_id,
                    bound_frontier=frontier,
                )
            if result == "end":
                # 可解码消息在这个索引帧之前就停了。重试
                # 同一次 seek 只会重新打开文件，再次撞上 EOF。
                log.info(
                    "[%s] window %s stops at frame %d; no more decodable messages",
                    job_id, client_id, nxt,
                )
                mark_decode_end(r, job_id, nxt)
                break
            if result == "busy":
                time.sleep(1.0)
            elif result == "error":
                nfail = fail_counts.get(nxt, 0) + 1
                fail_counts[nxt] = nfail
                delay = min(30.0, 5.0 * nfail)
                retry_after[nxt] = time.time() + delay
                log.info(
                    "[%s] window %s segment %d failed; retry it in %.0fs",
                    job_id,
                    client_id,
                    nxt,
                    delay,
                )
    finally:
        if forward is not None:
            forward.close()
        renewer.stop()
        release_segment_slot(r, job_id, held, client_id)
        current = r.get(alive)
        if isinstance(current, bytes):
            current = current.decode("utf-8", errors="replace")
        if current == owner:
            r.delete(alive)
        log.info("[%s] encode session ended for window %s", job_id, client_id)
        try:
            taken = hand_off_waiting_windows(
                r, job_id, source, exclude=client_id, segment_frames=seg_n
            )
            if taken:
                log.info("[%s] handed off to waiting windows %s", job_id, taken)
        except Exception:
            log.exception("[%s] hand-off to waiting windows failed", job_id)


def _run_one_queue_job(r, item: dict, on_playable=None) -> None:
    """处理一条从 Redis 取出的任务（跑在 worker 线程里）。"""
    if item.get("kind") == "sparse_session":
        _run_sparse_session(r, item, on_playable=on_playable)
        return
    if item.get("kind") == "segment":
        _run_one_segment_job(r, item, on_playable=on_playable)
        return
    from sview.jobs.job_status import (
        POD_IP,
        SVIEW_CLUSTER,
        acquire_job_lock,
        get_job_fields,
        is_cancel_requested,
        release_job_lock,
        renew_job_lock,
        set_job_fields,
    )
    from sview.jobs.job_status import REDIS_QUEUE_KEY as _QKEY

    redis_path = item["source"]
    job_id = item["job_id"]
    kind, _mount, _rel = classify_source(redis_path)
    local_path = redis_path_to_local(redis_path)
    is_bench = item.get("kind") == "bench" or item.get("bench_type") is not None
    log.info(
        "Dequeued [%s/%s]: %s  ->  %s  job_id=%s owner=%s cluster=%s",
        kind, item.get("kind"), redis_path, local_path, job_id, POD_IP, SVIEW_CLUSTER,
    )

    # 开始前已取消 → 跳过（不去抢锁）。
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
        # 另一个 worker 持有锁 — 避免转码两遍。
        if SVIEW_CLUSTER:
            time.sleep(0.2)
            # 只有锁消失才重新入队（赢家在认领前崩溃）。
            from sview.jobs.job_status import get_lock_owner
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
    # 总是从干净的播放缓存开始，避免取消或生命周期残留留下空洞。
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
            "frames_total": "",
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
        origin_sec = float(item.get("origin_sec") or 0)
        if origin_sec > 0:
            fields["origin_sec"] = origin_sec
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
                    "frames_total": _known_frame_total(total) or "",
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
                origin_sec=float(item.get("origin_sec") or 0),
            )

        if renewer.lost():
            raise LostJobLock("job lock lost")
        # 编码返回后再做一次取消检查。
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
                from sview.cache.oss_cache_gc import purge_job_play_cache

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
        lock_held = False  # 不要删掉别的 Pod 的锁
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
        # 放开调度，避免可播前就失败的任务卡住队列。
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

    from sview.jobs.job_status import POD_IP as _POD, parse_queue_item, set_worker_heartbeat

    lock = threading.Lock()
    active: dict = {}  # 键为 job_id，值是 {"playable": bool, "thread": Thread}
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
            # 先丢掉已死线程，避免结束的任务堵住队列。
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
            # 当前每个在跑的任务都已经可播，才开下一个
            # （或者只跑着一个，而且它可播）。避免更早的任务
            # 还在预热时再叠上第三个。
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
                if item.get("kind") == "segment":
                    from sview.jobs.job_status import segment_pending_key

                    r.delete(segment_pending_key(job_id, int(item.get("seg_start") or 0)))
                    log.info(
                        "[%s] drop queued segment %s; one encode thread per window",
                        job_id,
                        item.get("seg_start"),
                    )
                    continue
                client_id = (item.get("client_id") or "")[:128]
                active_id = (
                    f"{job_id}:client:{client_id}"
                    if item.get("kind") == "sparse_session"
                    else job_id
                )
                with lock:
                    dup = active_id in active and active[active_id]["thread"].is_alive()
                if dup:
                    log.warning(
                        "Work item %s already inflight — drop duplicate",
                        active_id,
                    )
                    continue

                def _thread_main(it=item, aid=active_id):
                    try:
                        _run_one_queue_job(r, it, on_playable=_on_playable)
                    finally:
                        with lock:
                            active.pop(aid, None)
                        wake.set()

                t = threading.Thread(
                    target=_thread_main,
                    name=f"sview-job-{job_id[:48]}",
                    daemon=True,
                )
                with lock:
                    active[active_id] = {
                        "playable": item.get("kind") == "sparse_session",
                        "thread": t,
                    }
                    inflight_now = len(active)
                log.info(
                    "Started job thread %s (inflight=%d/%d)",
                    job_id, inflight_now, MAX_INFLIGHT_JOBS,
                )
                t.start()
            else:
                # 等到可播或结束；定时醒来，回收才不会停。
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
#  入口
# ---------------------------------------------------------------------------

def _start_http_server():
    """在 HTTP_HOST:HTTP_PORT 上提供 LOCAL_OUTPUT_DIR（见 sview_http.py）。"""
    from sview.http.server import start_sview_http_server
    return start_sview_http_server(LOCAL_OUTPUT_DIR, host=HTTP_HOST, port=HTTP_PORT)


def main():
    parser = argparse.ArgumentParser(
        description="SView worker (CPU libx264 + NAL AU split, Redis queue, HTTP cache)")
    parser.add_argument("--path", help="Process a single mcap file")
    parser.add_argument("--start-sec", type=float, default=0,
                        help="Skip this many seconds from bag start (encode-from-T)")
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
        # 用 CLI 覆盖时，让 bufsize 大致跟上 maxrate
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

    from sview.jobs.job_status import POD_IP, SVIEW_CLUSTER, ensure_cluster_identity

    ensure_cluster_identity(fatal=True)
    log.info("Cluster mode=%s pod_ip=%s role=%s oss_cache=%s",
             SVIEW_CLUSTER, POD_IP, SVIEW_ROLE, int(OSS_CACHE))

    role = SVIEW_ROLE
    if args.serve_only:
        role = "master"

    if role not in ("all", "master", "worker"):
        raise SystemExit(f"Invalid SVIEW_ROLE={SVIEW_ROLE!r}; use all|master|worker")

    if role == "master":
        # 只做 API / 入队 — worker 去消费 Redis。
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

    # 角色为 worker 或 all：消费 Redis。HTTP 可选。
    env_off = os.environ.get("HTTP_ENABLED", "1").lower() in ("0", "false", "no")
    if role == "worker":
        # 默认只编码；要同时提供 HTTP 就设 HTTP_ENABLED=1 或 --serve。
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
        process_mcap(
            str(local_path),
            no_upload=args.no_upload,
            origin_sec=float(args.start_sec or 0),
        )
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
    """转码和清单用的输出宽高。

    优先级：
      1. TRANSCODE_SIZE > 0 → 正方形（SIZE x SIZE）
      2. 某一轴设了 TRANSCODE_WIDTH / TRANSCODE_HEIGHT 就用它
      3. 相机源 meta
      4. 兜底 2096
    """
    size = _tc_size()
    if size > 0:
        return size
    if which == "w" and _tc_width() > 0:
        return _tc_width()
    if which == "h" and _tc_height() > 0:
        return _tc_height()
    key = "width" if which == "w" else "height"
    for ch in HEADCAM_CHANNELS:
        if ch in metas and metas[ch].get(key):
            return int(metas[ch][key])
    for m in metas.values():
        if m.get(key):
            try:
                return int(m[key])
            except (TypeError, ValueError):
                continue
    # 还没有 meta 时的兜底
    return 2096


def _build_streams_manifest(metas: dict, fallback_w: int, fallback_h: int) -> dict:
    """生成清单里的 streams；宽高是转码后的输出尺寸。"""
    return build_streams_manifest(metas, fallback_w, fallback_h)


if __name__ == "__main__":
    main()
