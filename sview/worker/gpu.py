#!/usr/bin/env python3
"""
mcap 处理：GPU 把 HEVC 转成 H.264 MP4，上传 OSS（或 --no-upload 写到本地）。

两种模式：
--path <file>         处理单个 mcap。
(无 --path)           每 10 秒从 Redis 队列取 /head-cam/... 路径。

配置全部来自环境变量（方便挂 K8s Secret）。
"""

import os
import subprocess
import time
import threading
import tempfile
import sys
import argparse
import logging
from pathlib import Path
from collections import defaultdict

import redis
from sview.ingest.mcap_protobuf_compat import read_protobuf_messages_compat as read_protobuf_messages
import json

from sview.logging_setup import setup_logging
from sview.jobs.bundle_frame import pack_bundle_frame, build_streams_manifest

# ---------------------------------------------------------------------------
#  日志（工作目录下的项目日志）
# ---------------------------------------------------------------------------
log = setup_logging("sview_work_gpu")

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
from sview.path_map import (
    HEAD_CAM_MOUNT,
    ORIN_MOUNT,
    redis_path_to_local,
    source_to_local,
    classify_source,
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
NO_UPLOAD = os.environ.get("NO_UPLOAD", "0").lower() in ("1", "true", "yes")

# -- 本地输出（--no-upload 时使用） --
LOCAL_OUTPUT_DIR = Path(os.environ.get("LOCAL_OUTPUT_DIR", str(Path(__file__).parent / "output")))
LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -- ffmpeg 与 GPU --
_default_ffmpeg = "ffmpeg"
if sys.platform == "win32":
    _default_ffmpeg = r"C:\Users\10279\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1.2-full_build\bin\ffmpeg.exe"
FFMPEG = os.environ.get("FFMPEG_PATH", _default_ffmpeg)

GPU_CONCURRENCY = int(os.environ.get("GPU_CONCURRENCY", "2"))
nvenc_lock = threading.Semaphore(GPU_CONCURRENCY)
CHUNK_FRAMES = int(os.environ.get("CHUNK_FRAMES", "150"))

# -- 转码画质 --
# TRANSCODE_SIZE：0 表示保持源相机分辨率（本地/可播放风格）；
# 通过环境变量或 --scale 设成例如 720 来降分辨率。
TRANSCODE_SIZE = int(os.environ.get("TRANSCODE_SIZE", "0"))
TRANSCODE_CQ = int(os.environ.get("TRANSCODE_CQ", "35"))       # NVENC CQ，越大文件越小
TRANSCODE_BITRATE = os.environ.get("TRANSCODE_BITRATE", "2M")
TRANSCODE_MAXRATE = os.environ.get("TRANSCODE_MAXRATE", "3M")
TRANSCODE_BUFSIZE = os.environ.get("TRANSCODE_BUFSIZE", "4M")
TRANSCODE_PRESET = os.environ.get("TRANSCODE_PRESET", "p1")


# -- 临时工作目录 --
TEMP_DIR = Path(os.environ.get("TEMP_DIR", tempfile.gettempdir())) / "mcap_processor"
TEMP_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
#  OSS
# ---------------------------------------------------------------------------

_oss_bucket = None

def get_oss_bucket():
    global _oss_bucket
    if _oss_bucket is not None:
        return _oss_bucket
    import oss2
    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    _oss_bucket = oss2.Bucket(auth, OSS_ENDPOINT, OSS_BUCKET)
    _oss_bucket.get_bucket_info()
    log.info("OSS connected: %s / %s", OSS_ENDPOINT, OSS_BUCKET)
    return _oss_bucket


def upload_to_oss(local_path: Path, oss_key: str):
    bucket = get_oss_bucket()
    bucket.put_object_from_file(oss_key, str(local_path))
    log.info("  -> uploaded  oss://%s/%s  (%.1f MB)", OSS_BUCKET, oss_key, local_path.stat().st_size / 1e6)


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
#  路径映射（OSS URI / 旧队列路径 → 本地挂载；见 path_map.py）
# ---------------------------------------------------------------------------

def local_path_to_oss_base(local_path: Path) -> str:
    """从本地源路径推出输出 key 前缀（可选 OSS 上传用）。"""
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
    return f"{OSS_OUTPUT_PREFIX}/{OSS_OUTPUT_SUB}/{rel}/sview"


# ---------------------------------------------------------------------------
#  GPU 转码
# ---------------------------------------------------------------------------

def gpu_transcode_chunk(raw_data: bytes, output_path: Path,
                        scale_w: int = None, scale_h: int = None,
                        codec: str = "hevc") -> float:
    """用 NVENC GPU 把原始视频转成 H.264，缩放到 (scale_w, scale_h)。

    codec：源编码（'hevc'/'h265' 或 'h264'/'avc'）；决定 ffmpeg -f 输入格式。
    """
    if not scale_w:
        scale_w = TRANSCODE_SIZE if TRANSCODE_SIZE > 0 else 2096
    if not scale_h:
        scale_h = TRANSCODE_SIZE if TRANSCODE_SIZE > 0 else 2096
    t0 = time.time()
    scale_filter = f"scale_cuda={scale_w}:{scale_h}"
    in_format = "hevc" if codec.lower() in ("hevc", "h265", "h.265") else "h264"
    try:
        with nvenc_lock:
            subprocess.run(
                [
                    FFMPEG, "-y", "-hide_banner", "-loglevel", "error",
                    "-hwaccel", "cuda", "-hwaccel_output_format", "cuda",
                    "-f", in_format, "-r", "30", "-i", "pipe:0",
                    "-vf", scale_filter,
                    "-c:v", "h264_nvenc",
                    "-profile:v", "high",
                    "-cq", str(TRANSCODE_CQ),
                    "-preset", TRANSCODE_PRESET,
                    "-tune", "hq",
                    "-rc", "vbr",
                    "-b:v", TRANSCODE_BITRATE,
                    "-maxrate", TRANSCODE_MAXRATE,
                    "-bufsize", TRANSCODE_BUFSIZE,
                    "-g", "30",
                    "-bsf:v", "dump_extra=freq=keyframe",
                    "-f", "h264",
                    str(output_path),
                ],
                input=raw_data,
                capture_output=False,
                timeout=300,
            )
    except subprocess.CalledProcessError as e:
        log.error("FFmpeg failed (exit %%d):\nSTDERR:\n%%s\nSTDOUT:\n%%s",
                e.returncode,
                e.stderr.decode(errors="replace")[-3000:] if e.stderr else "(none)",
                e.stdout.decode(errors="replace")[-2000:] if e.stdout else "(none)")
        raise
    return time.time() - t0


def _transcode_chunk_worker(ch_name, chunk_idx, frames_slice, work_dir,
                            scale_w=None, scale_h=None, codec="hevc"):
    if not scale_w:
        scale_w = TRANSCODE_SIZE if TRANSCODE_SIZE > 0 else 2096
    if not scale_h:
        scale_h = TRANSCODE_SIZE if TRANSCODE_SIZE > 0 else 2096
    chunk_path = work_dir / f"{ch_name}_chunk_{chunk_idx:04d}.bin"
    raw_data = b"".join(frames_slice)
    log.debug("[%s chunk %d] %s raw: %d frames, %.1f KB input",
                ch_name, chunk_idx, len(frames_slice), len(raw_data) / 1024)
    if not raw_data:
        log.warning("[%s chunk %d] Empty %s data, skipping transcode", ch_name, chunk_idx, codec)
        return ch_name, chunk_idx, str(chunk_path), 0.0
    elapsed = gpu_transcode_chunk(raw_data, chunk_path, scale_w, scale_h, codec)
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

# ---------------------------------------------------------------------------
#  处理
# ---------------------------------------------------------------------------




def process_mcap(source_path, no_upload=False):
    """GPU 把 HEVC 转成 H.264 MP4，上传 OSS（no_upload 时存本地）。"""
    local_path = Path(source_path)
    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {source_path}")

    fname = local_path.name
    t_start = time.time()
    total_size = local_path.stat().st_size
    log.info("[%s]  %.0f MB  start", fname, total_size / 1e6)
    _size_desc = f"{TRANSCODE_SIZE}x{TRANSCODE_SIZE}" if TRANSCODE_SIZE > 0 else "source"
    log.info("[%s]  Transcode quality: size=%s cq=%s bitrate=%s maxrate=%s",
            fname, _size_desc, TRANSCODE_CQ,
            TRANSCODE_BITRATE, TRANSCODE_MAXRATE)

    oss_base = local_path_to_oss_base(local_path)
    work_dir = TEMP_DIR / f"job_{local_path.stem}_{int(time.time())}"
    work_dir.mkdir(parents=True, exist_ok=True)

    # 累积器
    video_frames = defaultdict(list)  # ch_name -> [字节, ...]
    video_timestamps = defaultdict(list)  # ch_name -> [每帧 log_time_ns]
    imu_rows = []          # 全部原始 IMU 采样
    camera_metas = {}      # ch_name -> {width, height, fps, position}
    futures = []
    channel_found = set()
    chunk_idx = 0
    total_imu = 0
    from concurrent.futures import ThreadPoolExecutor, as_completed
    trans_exec = ThreadPoolExecutor(max_workers=4)

    estimated_frames_per_ch = int(total_size * 0.60 / (4 * 30000))
    estimated_chunks = max(1, estimated_frames_per_ch // CHUNK_FRAMES)

    try:
        for msg in read_protobuf_messages(str(local_path)):
            topic = msg.topic

            # ----- 相机元数据 -----
            if "/meta" in topic and "camera" in topic.lower():
                parts = topic.split("/")
                if len(parts) >= 3:
                    ch = parts[2]
                    if ch not in camera_metas:
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

            # --- 视频 ---
            elif "/video" in topic:
                ch_name = topic.split("/")[2]
                channel_found.add(ch_name)
                video_frames[ch_name].append(msg.proto_msg.data)
                video_timestamps[ch_name].append(msg.log_time_ns)

                if len(channel_found) >= 4:
                    min_count = min(len(f) for f in video_frames.values())
                    next_boundary = (chunk_idx + 1) * CHUNK_FRAMES

                    if min_count >= next_boundary:
                        sw = _scale_dim(camera_metas, 'w')
                        sh = _scale_dim(camera_metas, 'h')
                        for ch in sorted(video_frames.keys()):
                            start = chunk_idx * CHUNK_FRAMES
                            fut = trans_exec.submit(
                                _transcode_chunk_worker,
                                ch, chunk_idx,
                                video_frames[ch][start:next_boundary],
                                work_dir, sw, sh,
                                camera_metas.get(ch, {}).get("codec", "hevc") or "hevc",
                            )
                            futures.append(fut)
                        chunk_idx += 1

                        total_est = estimated_chunks * 4
                        done = sum(1 for f in futures if f.done())
                        if total_est > 0:
                            pct = min(95, int((done / total_est) * 100))
                            print(f"\r  [{fname}] GPU {pct}%", end="", flush=True)

            # --- IMU ---
            elif topic == "/imu/sample":
                p = msg.proto_msg
                # 对齐本地/可播放 IMU 结构：用 proto 的 sample_index；
                # sensor_type 是整数，只在非 0 时保留（gyro=1）。
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
                imu_rows.append(row)
                total_imu += 1

        # 最后一段视频
        min_total = min(len(f) for f in video_frames.values()) if video_frames else 0
        processed = chunk_idx * CHUNK_FRAMES
        if processed < min_total:
            sw = _scale_dim(camera_metas, 'w')
            sh = _scale_dim(camera_metas, 'h')
            for ch in sorted(video_frames.keys()):
                fut = trans_exec.submit(
                    _transcode_chunk_worker,
                    ch, chunk_idx,
                    video_frames[ch][processed:min_total],
                    work_dir, sw, sh,
                    camera_metas.get(ch, {}).get("codec", "hevc") or "hevc",
                )
                futures.append(fut)

        total_video_frames = min_total
        total_video_ch = len(video_frames)
        log.info("[%s]  Parsed: %d video frames x %d ch,  %d IMU",
                fname, total_video_frames, len(video_frames), total_imu)

        # 把时间轴和 IMU 对齐到 monotonic_raw_ns（校验 / SView 安全）。
        if _rebase_timestamps_to_monotonic(video_timestamps, imu_rows):
            log.info("[%s]  Rebased timestamps onto monotonic_raw_ns", fname)

        # --- 等 GPU 任务，收集结果 ---
        transcode_results = defaultdict(list)
        for fut in as_completed(futures):
            ch_name, cidx, path, elapsed = fut.result()
            transcode_results[ch_name].append((cidx, path, elapsed))
        trans_exec.shutdown(wait=True)

        print(f"\r  [{fname}] Concatenating...", end="", flush=True)

        # --- 按通道拼接，再拆成逐帧 AU ---
        channel_aus = {}  # ch_name -> AU 字节数组列表
        for ch_name in sorted(transcode_results.keys()):
            chunks_sorted = sorted(transcode_results[ch_name], key=lambda x: x[0])
            chunk_paths = [c[1] for c in chunks_sorted]

            concat_bin = work_dir / f"{ch_name}_concat.bin"
            _concat_h264_chunks(chunk_paths, concat_bin)

            with open(concat_bin, 'rb') as f:
                raw_stream = f.read()
            channel_aus[ch_name] = _split_h264_access_units(raw_stream, work_dir)
            log.info("[%s]  %s: %d access units from stream (%.1f MB)",
                    fname, ch_name, len(channel_aus[ch_name]), len(raw_stream) / 1e6)

            for cp in chunk_paths:
                try:
                    os.unlink(cp)
                except OSError:
                    pass

        # --- 生成逐帧 bundle bin ---
        min_frames = min(len(aus) for aus in channel_aus.values()) if channel_aus else 0
        key_frame_indices = []
        output_frame_dir = work_dir / "frames"
        output_frame_dir.mkdir(parents=True, exist_ok=True)


        # 按 log_time_ns 排序 IMU，方便按时间窗查询
        imu_sorted = sorted(imu_rows, key=lambda r: r["log_time_ns"])
        imu_idx = 0

        for fidx in range(min_frames):
            streams_data = {}
            for ch in sorted(channel_aus.keys()):
                aus = channel_aus[ch]
                streams_data[ch] = aus[fidx] if fidx < len(aus) else b''

            # 看有没有 IDR NAL（类型 5）来判断关键帧
            is_keyframe = True
            for ch, data in streams_data.items():
                if not data:
                    continue
                if not _h264_payload_has_idr(data):
                    is_keyframe = False
                    break
            if is_keyframe:
                key_frame_indices.append(fidx)

            # ---- 多相机时间窗（约定：取所有通道的最小值） ----
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


            # ---- IMU 时间窗采样 ----
            events_payload = {"records": []}
            imu_samples = []

            while imu_idx < len(imu_sorted) and imu_sorted[imu_idx]["log_time_ns"] < tw_start:
                imu_idx += 1
            j = imu_idx
            while j < len(imu_sorted) and imu_sorted[j]["log_time_ns"] < tw_end:
                r = imu_sorted[j]
                # 字段顺序和内容对齐 local_full_mcap_flow_verify 的 bin
                # （前端可播放）：不要 segment_index；sensor_type 只给陀螺。
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

            # ---- 按文档组装 bin 里的 IMU payload ----
            imu_payload = {"samples": imu_samples}

            bundle = _build_bundle_frame(
                frame_idx=fidx,
                streams_data=streams_data,
                timestamp_ns=ts_ns,
                time_window_ns=[tw_start, tw_end],
                is_keyframe=is_keyframe,
                imu_json=imu_payload,
                events_json=events_payload,
            )

            bin_path = output_frame_dir / f"{fidx:06d}.bin"
            _write_bytes_atomic(bin_path, bundle)

        log.info("[%s]  Built %d frame bundles (%d keyframes)", fname, min_frames, len(key_frame_indices))

        # --- 写 manifest.json ---
        # 用帧间隔估算 timeline_end_ns（约定 4.1）
        if channel_aus and video_timestamps:
            first_ch = sorted(channel_aus.keys())[0]
            ts_list = video_timestamps.get(first_ch, [])
            if len(ts_list) >= 2:
                timeline_start_ns = ts_list[0]
                last_start_ns = ts_list[-1]
                previous_start_ns = ts_list[-2]
                tail_duration_ns = last_start_ns - previous_start_ns
                timeline_end_ns = last_start_ns + tail_duration_ns
            elif len(ts_list) == 1:
                timeline_start_ns = ts_list[0]
                timeline_end_ns = ts_list[0] + 33_333_333
            else:
                timeline_start_ns = 0
                timeline_end_ns = 0
        else:
            timeline_start_ns = 0
            timeline_end_ns = 0

        manifest = {
            "job_id": local_path.stem,
            "format": "head_cam",
            "status": "done",
            "bundle": {
                "frame_count": min_frames,
                "gop_size": 30,
                "key_frame_indices": key_frame_indices,
                "timeline": {
                    "start_ns": timeline_start_ns,
                    "end_ns": timeline_end_ns,
                },
            },
            "streams": _build_streams_manifest(camera_metas, _scale_dim(camera_metas, 'w'), _scale_dim(camera_metas, 'h')),
        }
        if manifest["bundle"]["timeline"]["start_ns"] and manifest["bundle"]["timeline"]["end_ns"]:
            dur = (manifest["bundle"]["timeline"]["end_ns"] - manifest["bundle"]["timeline"]["start_ns"]) / 1e9
            manifest["bundle"]["timeline"]["duration_sec"] = round(dur, 6)

        manifest_path = work_dir / "manifest.json"
        manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode('utf-8')
        _write_bytes_atomic(manifest_path, manifest_bytes)

        # --- 保存或上传输出 ---
        if no_upload:
            import shutil as _shutil
            # 拷贝前删掉中间拼接 bin
            for cb in work_dir.glob("*_concat.bin"):
                try:
                    cb.unlink()
                except OSError:
                    pass
            out_dir = LOCAL_OUTPUT_DIR / local_path.stem
            _shutil.copytree(str(work_dir), str(out_dir), dirs_exist_ok=True)
            log.info("  -> saved  %s  (%d frames)", out_dir, min_frames)
        else:
            # 把每帧 bin 和 manifest 传到 .../{job_id}/sview/
            if manifest_path.exists():
                oss_key = f"{oss_base}/manifest.json"
                upload_to_oss(manifest_path, oss_key)
            for bin_file in sorted(output_frame_dir.glob("*.bin")):
                oss_key = f"{oss_base}/frames/{bin_file.name}"
                upload_to_oss(bin_file, oss_key)


        total_elapsed = round(time.time() - t_start, 1)
        log.info("[%s]  Done in %.1fs  %d video ch + %d IMU",
                fname, total_elapsed, len(video_frames), total_imu)
    finally:
        import shutil
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass




# ---------------------------------------------------------------------------
#  时间戳重定位（wall / mcap log_time → monotonic_raw_ns）
# ---------------------------------------------------------------------------

def _parse_ns(value):
    """把 int/str 解析成纳秒时间戳；缺失或非法返回 None。"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rebase_timestamps_to_monotonic(video_timestamps: dict, imu_rows: list) -> bool:
    """用 monotonic_raw_ns 做统一时间轴（与 local_full_mcap_flow_verify 相同）。

    线上 mcap 常把 msg.log_time 打成墙上时钟纳秒（约 1e18），超过
    JS Number.MAX_SAFE_INTEGER，SView 的 IMU 曲线会坏。校验风格的
    录制已经用单调 log_time（约 1e11）。

    - mcap log_time 在 wall 域：视频时间戳减去 median(wall - mono)，
        并把每条 IMU 的 log_time_ns 设成 monotonic_raw_ns。
    - 已经是单调时间：只把 IMU 的 log_time_ns 归一到 monotonic_raw_ns。

    原地修改 video_timestamps 和 imu_rows。有改动返回 True。
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
    """用 ffprobe 把原始 H.264 Annex B 流切成逐帧 AU。

    对齐本地/可播放产物：保持 ffprobe 的 packet 边界。
    不要把 SPS/PPS 再塞回 IDR AU（会改 AU 布局，对不上本地 bin）。
    """
    if not data:
        return []
    
    # 把原始 H.264 写到临时文件给 ffprobe
    tmp = work_dir / "_split_input.h264"
    with open(tmp, 'wb') as f:
        f.write(data)
    
    try:
        _ffprobe_name = 'ffprobe.exe' if sys.platform == 'win32' else 'ffprobe'
        ffprobe = str(Path(FFMPEG).parent / _ffprobe_name)
        result = subprocess.run([
            ffprobe, '-v', 'quiet',
            '-show_packets', '-select_streams', 'v:0',
            '-print_format', 'csv=nokey=1',
            str(tmp),
        ], capture_output=True, text=True, timeout=30)
        
        # 解析 pkt_pos 和 pkt_size
        positions = []
        current = {}
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if not line:
                continue
            if line.startswith('packet,'):
                continue  # 表头
            parts = line.split(',')
            # CSV 列：codec_type,stream_index,pts,pts_time,dts,dts_time,duration,duration_time,size,pos,flags
            if len(parts) >= 10:
                try:
                    size = int(parts[8])
                    pos = int(parts[9])
                    positions.append((pos, size))
                except (ValueError, IndexError):
                    pass
        
        if not positions:
            # 兜底：试 show_frames 格式
            result2 = subprocess.run([
                ffprobe, '-v', 'quiet',
                '-show_frames', '-select_streams', 'v:0',
                '-print_format', 'csv',
                str(tmp),
            ], capture_output=True, text=True, timeout=30)
            for line in result2.stdout.strip().split('\n'):
                if line.startswith('frame,'):
                    parts = line.split(',')
                    # frame,media_type,stream_index,key_frame,pkt_pts,pkt_pts_time,pkt_dts,pkt_dts_time,best_effort_timestamp,best_effort_timestamp_time,pkt_duration,pkt_duration_time,pkt_pos,pkt_size,...
                    if len(parts) >= 14:
                        try:
                            pos = int(parts[12])
                            size = int(parts[13])
                            positions.append((pos, size))
                        except (ValueError, IndexError):
                            pass
        
        if not positions:
            log.warning('ffprobe found no frame positions, returning entire stream as single AU')
            return [data]
        
        aus = []
        for pos, size in positions:
            if pos + size <= len(data):
                aus.append(data[pos:pos + size])

        return aus
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _h264_payload_has_idr(data: bytes) -> bool:
    """检查 H.264 payload 里有没有 IDR NAL（类型 5）。"""
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


def _write_bytes_atomic(target_path: Path, payload: bytes):
    """原子写字节：先写 .tmp，再改名到目标。"""
    tmp_path = target_path.with_suffix(target_path.suffix + '.tmp')
    with open(tmp_path, 'wb') as f:
        f.write(payload)
    tmp_path.replace(target_path)


def _build_bundle_frame(frame_idx: int, streams_data: dict, timestamp_ns: int,
                        time_window_ns: list, is_keyframe: bool,
                        imu_json: dict, events_json: dict,
                        stream_codecs: dict | None = None) -> bytes:
    """组装单帧 bundle bin（v1 四路相机或 v2 命名流）。"""
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
#  Redis 队列循环
# ---------------------------------------------------------------------------

def run_redis_loop():
    r = get_redis()
    if not NO_UPLOAD:
        get_oss_bucket()
    log.info("Redis queue mode: listening on '%s'  (head_cam=%s  orin=%s)",
            REDIS_QUEUE_KEY, HEAD_CAM_MOUNT, ORIN_MOUNT)
    if NO_UPLOAD:
        log.info("Output: local cache (NO_UPLOAD=1)")
    else:
        log.info("Output: oss://%s/%s/%s/", OSS_BUCKET, OSS_OUTPUT_PREFIX, OSS_OUTPUT_SUB)

    while True:
        try:
            result = r.blpop(REDIS_QUEUE_KEY, timeout=10)
            if result is None:
                continue

            _, redis_path = result
            kind, _mount, _rel = classify_source(redis_path)
            local_path = redis_path_to_local(redis_path)
            job_id = local_path.stem
            log.info("Dequeued [%s]: %s  ->  %s", kind, redis_path, local_path)

            try:
                from sview.jobs.job_status import set_job_fields
                set_job_fields(r, job_id, status="processing", source=redis_path, error="")
            except Exception:
                log.exception("Failed to set processing status for %s", job_id)

            try:
                process_mcap(str(local_path), no_upload=NO_UPLOAD)
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
                    from sview.jobs.job_status import set_job_fields
                    set_job_fields(
                        r, job_id, status="ready", source=redis_path,
                        frame_count=frame_count, error="",
                    )
                except Exception:
                    log.exception("Failed to set ready status for %s", job_id)
            except Exception as exc:
                try:
                    from sview.jobs.job_status import set_job_fields
                    set_job_fields(r, job_id, status="failed", source=redis_path, error=str(exc)[:500])
                except Exception:
                    pass
                log.exception("Failed processing: %s", redis_path)

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

def main():
    parser = argparse.ArgumentParser(description="mcap processor (GPU H264 MP4 + IMU embedded into bin)")
    parser.add_argument("--path", help="Process a single mcap file")
    parser.add_argument("--no-upload", action="store_true", dest="no_upload",
                        help="Skip OSS upload; save output files to local output/ directory")
    parser.add_argument("--scale", type=int, default=None,
                        help="Output video size (square), e.g. 720/480/360. "
                             "Default 0 keeps source camera resolution (local style).")
    parser.add_argument("--cq", type=int, default=None,
                        help="NVENC CQ 0-51, higher = lower quality / smaller (env TRANSCODE_CQ)")
    parser.add_argument("--bitrate", default=None,
                        help="Target bitrate, e.g. 1M/2M (env TRANSCODE_BITRATE)")
    parser.add_argument("--maxrate", default=None,
                        help="Max bitrate, e.g. 2M/3M (env TRANSCODE_MAXRATE)")
    args = parser.parse_args()

    global TRANSCODE_SIZE, TRANSCODE_CQ, TRANSCODE_BITRATE, TRANSCODE_MAXRATE, TRANSCODE_BUFSIZE
    if args.scale is not None:
        TRANSCODE_SIZE = max(0, args.scale)
    if args.cq is not None:
        TRANSCODE_CQ = max(0, min(51, args.cq))
    if args.bitrate is not None:
        TRANSCODE_BITRATE = args.bitrate
    if args.maxrate is not None:
        TRANSCODE_MAXRATE = args.maxrate
        # CLI 覆盖 maxrate 时，bufsize 大致跟着走
        if args.bitrate and not os.environ.get("TRANSCODE_BUFSIZE"):
            TRANSCODE_BUFSIZE = args.maxrate

    if args.path:
        local_path = _resolve_path(args.path)
        process_mcap(str(local_path), no_upload=args.no_upload)
    else:
        run_redis_loop()


def _scale_dim(metas: dict, which: str) -> int:
    """转码和 manifest 用的输出宽高。

    - TRANSCODE_SIZE > 0：强制缩成正方形（例如 720）
    - TRANSCODE_SIZE == 0：用 meta 里的源相机分辨率（本地风格）
    """
    if TRANSCODE_SIZE > 0:
        return TRANSCODE_SIZE
    key = "width" if which == "w" else "height"
    for ch in ("RR", "FR", "RL", "FL"):
        if ch in metas and metas[ch].get(key):
            return int(metas[ch][key])
    # meta 还没有时的兜底
    return 2096


def _build_streams_manifest(metas: dict, fallback_w: int, fallback_h: int) -> dict:
    """组装 manifest 的 streams；宽高是转码后的输出尺寸。"""
    return build_streams_manifest(metas, fallback_w, fallback_h)



if __name__ == "__main__":
    main()
