"""视频探测与转码服务。

职责：
- 用 ffprobe 探测源视频编码。
- 判断是否可由浏览器直接播放。
- 将不兼容视频转码为 H.264/AAC。
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _detect_hw_encoder() -> str:
    """检测当前机器可用的硬件编码器,缓存结果(只检测一次)。

    优先级：h264_videotoolbox(Mac) > libx264(CPU)
    注意：不使用 h264_nvenc,因为远端 ffmpeg 4.x 的 nvenc preset 参数与新版不兼容。
    """
    if not shutil.which("ffmpeg"):
        return "libx264"

    result = subprocess.run(
        ["ffmpeg", "-hide_banner", "-encoders"],
        capture_output=True, text=True,
    )
    encoders_output = result.stdout or ""
    if "h264_videotoolbox" in encoders_output:
        logger.info("检测到硬件编码器：h264_videotoolbox (Mac)")
        return "h264_videotoolbox"

    logger.info("使用 libx264 (CPU)")
    return "libx264"


def _build_video_codec_args(encoder: str) -> list[str]:
    """根据编码器类型生成对应的 ffmpeg 参数。"""
    if encoder == "h264_videotoolbox":
        # Mac 硬件编码:q:v 65 约等于 libx264 crf 23 的画质,速度快 5~10 倍。
        return ["-c:v", "h264_videotoolbox", "-q:v", "65"]
    # CPU 软编码:preset fast 比 medium 快约 40%,体积仅大约 10%。
    return ["-c:v", "libx264", "-preset", "fast", "-crf", "23"]


def ffprobe_summary(video_url: str) -> dict:
    """探测视频并返回精简摘要。"""
    if not shutil.which("ffprobe"):
        raise RuntimeError("ffprobe not found in PATH")

    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_streams",
        "-show_format",
        "-of",
        "json",
        video_url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or "ffprobe failed").strip())

    payload = json.loads(proc.stdout or "{}")
    streams = payload.get("streams") or []
    format_obj = payload.get("format") or {}

    video_stream = next((s for s in streams if str(s.get("codec_type")) == "video"), {})
    audio_stream = next((s for s in streams if str(s.get("codec_type")) == "audio"), {})

    return {
        "video_codec": video_stream.get("codec_name"),
        "audio_codec": audio_stream.get("codec_name"),
        "width": video_stream.get("width"),
        "height": video_stream.get("height"),
        "pix_fmt": video_stream.get("pix_fmt"),
        "profile": video_stream.get("profile"),
        "container": format_obj.get("format_name"),
        "duration": format_obj.get("duration"),
        "raw": payload,
    }


def is_browser_playable(probe: dict) -> bool:
    """判定浏览器是否可直接播放。

    这里采用保守策略：
    - 视频编码必须是 h264
    - 像素格式推荐 yuv420p（为空时放宽）
    """
    video_codec = str(probe.get("video_codec") or "").lower()
    pix_fmt = str(probe.get("pix_fmt") or "").lower()

    if video_codec != "h264":
        return False
    if pix_fmt and pix_fmt not in {"yuv420p", "yuvj420p"}:
        return False
    return True


def build_transcoded_name(source_url: str, item_id: int) -> str:
    """基于 source_url + item_id 生成稳定文件名。"""
    digest = hashlib.sha1(f"{item_id}:{source_url}".encode("utf-8")).hexdigest()[:16]
    return f"item_{item_id}_{digest}.mp4"


def transcode_to_web_mp4(input_url: str, output_path: Path) -> None:
    """转码到浏览器友好的 mp4（H.264/AAC）。

    策略：先用优化后的参数尝试（硬件编码 / fast preset / 关键帧密度），
    如果失败则 fallback 到最保守的旧参数（libx264 medium,无关键帧强制），确保成功率。
    """
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg not found in PATH")

    encoder = _detect_hw_encoder()
    codec_args = _build_video_codec_args(encoder)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 优化版命令（快速 + 关键帧密度）
    cmd_fast = [
        "ffmpeg",
        "-y",
        "-i",
        input_url,
        *codec_args,
        "-pix_fmt",
        "yuv420p",
        "-g",
        "30",
        "-keyint_min",
        "30",
        "-movflags",
        "+faststart",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        str(output_path),
    ]
    logger.info("开始视频转码。encoder=%s input=%s output=%s", encoder, input_url[:80], output_path.name)
    proc = subprocess.run(cmd_fast, capture_output=True, text=True)
    if proc.returncode == 0:
        return

    # 优化版失败，fallback 到最保守的旧参数
    logger.warning(
        "优化转码失败，fallback 到旧参数。encoder=%s output=%s stderr=%s",
        encoder,
        output_path.name,
        (proc.stderr or "")[-200:],
    )
    cmd_fallback = [
        "ffmpeg",
        "-y",
        "-i",
        input_url,
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "23",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        str(output_path),
    ]
    proc2 = subprocess.run(cmd_fallback, capture_output=True, text=True)
    if proc2.returncode != 0:
        raise RuntimeError((proc2.stderr or "ffmpeg fallback transcode failed").strip())
