#!/usr/bin/env python3
"""
Orin teleop MCAP: ROS2 image/video streams → H.264 Annex-B.

两种输入（同一个 mcap 里不会同时出现）::

    /left_camera/.../color/image_raw/compressed   CompressedImage (jpeg)
    ...                                            → libx264 编码

    同一套 compressed topic，format=h264（Annex-B）时走 H.264 重编码，
    不要当 JPEG 解析。

    /streamer/realsense_right/h264               CompressedImage (h264 Annex-B)
    /streamer/realsense_torso/h264
    /streamer/realsense_head/h264
    /streamer/realsense_left/h264                → 复用头显 annexb 管线重编码归一化

    /camera/f_l/h264
    /camera/f_r/h264
    /camera/left_wrist/h264
    /camera/right_wrist/h264

Also harvests unknown teleop topics whose schema is sensor_msgs Image /
CompressedImage / CompressedVideo (Unitree, Franka, …). Stream ids in the
bundle keep the original topic (leading ``/`` stripped). Extra cameras are
kept; they are not forced onto FR/FL/RR/RL.
"""

from __future__ import annotations

import io
import logging
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from sview.ingest.stream_ids import bundle_stream_key

log = logging.getLogger("sview.orin_images")

# SView / frontend channels: rr, fr, rl, fl
_SVIEW_CHANNELS = ("FR", "FL", "RR", "RL")
_SVIEW_CHANNEL_SET = frozenset(_SVIEW_CHANNELS)
DEFAULT_CAMERA_MAP = {
    "left_camera": "FL",
    "right_camera": "FR",
    "head_camera": "RR",
    "torso_camera": "RL",
    # RealSense H.264 流：/streamer/realsense_*/h264（与 JPEG 不会同时出现）
    "realsense_left": "FL",
    "realsense_right": "FR",
    "realsense_head": "RR",
    "realsense_torso": "RL",
    # 遥操四路：/camera/{f_l,f_r,left_wrist,right_wrist}/h264
    "f_l": "FL",
    "f_r": "FR",
    "left_wrist": "RL",
    "right_wrist": "RR",
    # Agilex cobot：/camera_{l,r,f_l,f_r}/color/image_raw（CompressedImage JPEG）
    "camera_f_l": "FL",
    "camera_f_r": "FR",
    "camera_l": "RL",
    "camera_r": "RR",
}

_COMPRESSED_TOPIC_RE = re.compile(
    r"^/(?P<cam>[a-z0-9_]+)(?:/[a-z0-9_]+)*/color/image_raw(?:/compressed)?$",
    re.IGNORECASE,
)
# /streamer/realsense_right/h264 这种：相机名是 h264 前一段。
# 也兼容 /xxx/h264/compressed。
_H264_TOPIC_RE = re.compile(
    r"^(?:/[a-z0-9_]+)+/h264(?:/compressed)?$",
    re.IGNORECASE,
)
_HEADCAM_VIDEO_RE = re.compile(r"^/camera/(?P<ch>RR|FR|RL|FL)/video/?$", re.IGNORECASE)
_HEADCAM_META_RE = re.compile(r"^/camera/(?P<ch>RR|FR|RL|FL)/meta/?$", re.IGNORECASE)
_TELEOP_H264_RE = re.compile(
    r"^/camera/(f_l|f_r|left_wrist|right_wrist)/h264(?:/compressed)?/?$",
    re.IGNORECASE,
)


def _topic_has_h264_segment(topic: str) -> bool:
    return any(p.lower() == "h264" for p in (topic or "").split("/"))


def _looks_like_headcam(topics: List[str], names: List[str]) -> bool:
    """SSIEGO / head-cam protobuf bags must not go through the Orin ROS2 reader."""
    if any(_TELEOP_H264_RE.match((t or "").strip()) for t in topics):
        return False
    for n in names:
        low = (n or "").strip().lower()
        if low.startswith("headcam.") or "headcam_record.proto" in low:
            return True
    for t in topics:
        ts = (t or "").strip()
        if ts == "/imu/sample" or _HEADCAM_VIDEO_RE.match(ts) or _HEADCAM_META_RE.match(ts):
            return True
    return False


def _looks_like_orin_topic(topic: str) -> bool:
    t = (topic or "").strip()
    if not t:
        return False
    if _H264_TOPIC_RE.match(t) or _topic_has_h264_segment(t):
        return True
    if "image_raw/compressed" in t:
        return True
    # Agilex cobot: /camera_l/color/image_raw (CompressedImage, no /compressed suffix)
    if t.rstrip("/").lower().endswith("/color/image_raw"):
        return True
    return bool(_COMPRESSED_TOPIC_RE.match(t))


def _norm_schema_name(schema_name: Optional[str]) -> str:
    return (schema_name or "").replace("/", ".").replace("::", ".").strip().lower()


def _schema_is_compressed_video(schema_name: Optional[str]) -> bool:
    n = _norm_schema_name(schema_name)
    return "compressedimage" in n or "compressedvideo" in n


def _schema_is_raw_image(schema_name: Optional[str]) -> bool:
    n = _norm_schema_name(schema_name)
    if not n or "compressed" in n or "camera_info" in n:
        return False
    return n.endswith(".image") or n.endswith("msg.image")


def _schema_is_harvest_video(schema_name: Optional[str]) -> bool:
    return _schema_is_compressed_video(schema_name) or _schema_is_raw_image(schema_name)


def _is_headcam_protobuf_topic(topic: str) -> bool:
    ts = (topic or "").strip()
    return bool(
        ts == "/imu/sample"
        or _HEADCAM_VIDEO_RE.match(ts)
        or _HEADCAM_META_RE.match(ts)
    )


def _harvest_channel_ok(topic: str, schema_name: Optional[str] = None) -> bool:
    """Teleop video harvest: schema first, topic allowlist as fallback."""
    if _is_headcam_protobuf_topic(topic):
        return False
    if _schema_is_harvest_video(schema_name):
        return True
    return _looks_like_orin_topic(topic)


def _is_orin_h264_topic(topic: str) -> bool:
    t = (topic or "").strip()
    if not t:
        return False
    return bool(
        _H264_TOPIC_RE.match(t)
        or _topic_has_h264_segment(t)
        or _TELEOP_H264_RE.match(t)
    )


def _is_orin_jpeg_topic(topic: str) -> bool:
    t = (topic or "").strip()
    if not t or _is_orin_h264_topic(t):
        return False
    return _looks_like_orin_topic(t)


def _iter_ros2_messages_tolerant(
    path: str, topic_ok=None, channel_ok=None, start_time_ns: Optional[int] = None,
):
    """Yield ROS2 messages; skip protobuf/other encodings instead of aborting the bag.

    ``topic_ok``: optional ``topic -> bool``.
    ``channel_ok``: optional ``(topic, schema_name) -> bool``. When set, it
    replaces ``topic_ok`` and can keep unknown camera topics whose schema is
    Image / CompressedImage.
    ``start_time_ns``: MCAP ``iter_messages(start_time=)`` — skip log_time before this.
    """
    from mcap.reader import make_reader
    from mcap_ros2.decoder import DecoderFactory
    from mcap_ros2.reader import McapROS2Message

    factory = DecoderFactory()
    iter_kw = {}
    if start_time_ns is not None and int(start_time_ns) > 0:
        iter_kw["start_time"] = int(start_time_ns)
    with open(path, "rb") as fh:
        reader = make_reader(fh)
        try:
            iterator = reader.iter_messages(**iter_kw)
        except TypeError:
            iterator = reader.iter_messages()
        for schema, channel, message in iterator:
            topic = getattr(channel, "topic", "") or ""
            schema_name = getattr(schema, "name", "") if schema is not None else ""
            if channel_ok is not None:
                if not channel_ok(topic, schema_name):
                    continue
            elif topic_ok is not None and not topic_ok(topic):
                continue
            enc = getattr(channel, "message_encoding", "") or ""
            decoder = factory.decoder_for(enc, schema)
            if decoder is None or schema is None or channel is None:
                continue
            try:
                ros_msg = decoder(message.data)
            except Exception:
                continue
            yield McapROS2Message(
                ros_msg=ros_msg,
                message=message,
                channel=channel,
                schema=schema,
            )


def _parse_camera_map(env_val: Optional[str] = None) -> Dict[str, str]:
    """Parse ``ORIN_CAMERA_MAP=left_camera:FL,right_camera:FR,...``."""
    raw = env_val if env_val is not None else os.environ.get("ORIN_CAMERA_MAP", "")
    out = dict(DEFAULT_CAMERA_MAP)
    if not raw.strip():
        return out
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        src, dst = part.split(":", 1)
        src, dst = src.strip(), dst.strip().upper()
        if src and dst in _SVIEW_CHANNEL_SET:
            out[src] = dst
    return out


def _stable_assign_topics(
    topics: List[str],
    cam_map: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Map each camera topic to a stable bin stream key (original topic).

    ``cam_map`` is unused; kept so existing call sites still compile.
    """
    del cam_map
    uniq: List[str] = []
    for t in topics:
        ts = (t or "").strip()
        if ts and ts not in uniq:
            uniq.append(ts)
    used: set = set()
    out: Dict[str, str] = {}
    for topic in uniq:
        key = bundle_stream_key(topic)
        if key in used:
            n = 2
            while f"{key}_{n}" in used:
                n += 1
            key = f"{key}_{n}"
        out[topic] = key
        used.add(key)
    return out


def _stream_meta(
    topic: str,
    ch: str,
    width: int,
    height: int,
    source_codec: str,
    fps: int = 30,
) -> dict:
    return {
        "width": width,
        "height": height,
        "fps": fps,
        "position": topic or ch,
        "codec": source_codec,
        "source_codec": source_codec,
        "stored_codec": "h264",
        "source_topic": topic or ch,
    }


def _video_topics_from_summary(
    path: str | Path,
    *,
    kind: Optional[str] = None,
) -> List[str]:
    """Topics that look like cameras (schema or allowlist)."""
    from mcap.reader import make_reader

    p = Path(path)
    if not p.is_file():
        return []
    try:
        with open(p, "rb") as fh:
            summary = make_reader(fh).get_summary()
    except Exception:
        return []
    if not summary or not summary.channels:
        return []
    schemas = summary.schemas or {}
    kind_l = (kind or "").strip().lower()
    out: List[str] = []
    for channel in summary.channels.values():
        topic = (getattr(channel, "topic", "") or "").strip()
        if not topic or _is_headcam_protobuf_topic(topic):
            continue
        schema = schemas.get(getattr(channel, "schema_id", None))
        schema_name = getattr(schema, "name", "") if schema is not None else ""
        harvest = _harvest_channel_ok(topic, schema_name)
        if not harvest:
            continue
        if kind_l == "h264":
            if _is_orin_jpeg_topic(topic) and not _is_orin_h264_topic(topic):
                continue
        elif kind_l == "jpeg":
            if _is_orin_h264_topic(topic):
                continue
        if topic not in out:
            out.append(topic)
    return out


def mcap_channel_overview(path: str | Path, *, max_topics: int = 24) -> dict:
    """Cheap MCAP footer summary for errors: encodings / schemas / topics."""
    from mcap.reader import make_reader

    out: dict = {"topics": [], "encodings": {}, "schemas": []}
    p = Path(path)
    if not p.is_file():
        return out
    try:
        with open(p, "rb") as fh:
            summary = make_reader(fh).get_summary()
    except Exception:
        log.debug("mcap_channel_overview failed for %s", p, exc_info=True)
        return out
    if not summary or not summary.channels:
        return out
    stats = summary.statistics
    ch_counts = getattr(stats, "channel_message_counts", None) if stats else None
    encodings: Dict[str, int] = {}
    topics: List[str] = []
    for ch_id, channel in summary.channels.items():
        topic = getattr(channel, "topic", "") or ""
        enc = (getattr(channel, "message_encoding", "") or "") or "?"
        n = 1
        if ch_counts is not None:
            try:
                n = int(ch_counts.get(ch_id) or 0) or 1
            except (TypeError, ValueError):
                n = 1
        encodings[enc] = encodings.get(enc, 0) + n
        if topic:
            topics.append(topic)
    schemas: List[str] = []
    if summary.schemas:
        schemas = [s.name for s in summary.schemas.values() if s and getattr(s, "name", "")]
    out["encodings"] = encodings
    out["topics"] = topics[:max_topics]
    out["schemas"] = schemas[:max_topics]
    return out


def orin_stream_kind(
    path: str | Path,
    *,
    skip_headcam_guard: bool = False,
) -> Optional[str]:
    """识别 Orin 录制的流类型：'h264' | 'jpeg' | None。

    不能只看 schema=CompressedImage：H.264 袋同样是 CompressedImage，只是
    ``format=h264``（topic 可能仍是 ``.../image_raw/compressed``）。

    ``skip_headcam_guard``: teleop paths already classified by directory must
    still be sniffed even if a topic *name* looks like ``/imu/sample``.
    """
    from mcap.reader import make_reader

    p = Path(path)
    if not p.is_file():
        return None

    topics: List[str] = []
    names: List[str] = []
    try:
        with open(p, "rb") as f:
            summary = make_reader(f).get_summary()
        if summary:
            if summary.channels:
                topics = [ch.topic or "" for ch in summary.channels.values()]
            if summary.schemas:
                names = [s.name for s in summary.schemas.values()]
    except Exception:
        log.debug("orin_stream_kind: summary read failed for %s", p, exc_info=True)

    if (not skip_headcam_guard) and _looks_like_headcam(topics, names):
        return None

    if any(_H264_TOPIC_RE.match(t.strip()) or _topic_has_h264_segment(t) for t in topics):
        return "h264"

    looks_compressed = any(_looks_like_orin_topic(t) for t in topics) or any(
        _schema_is_harvest_video(n) for n in names
    )

    # CompressedImage 可能是 JPEG 或 H.264；summary 无 channel 时也要看 payload。
    # 不要在 peek 失败时默认 jpeg：头显 protobuf 包会被官方 ROS2 reader 打挂。
    if looks_compressed or not topics:
        peeked = _peek_orin_payload_kind(p)
        if peeked:
            return "h264" if peeked in ("h264", "hevc") else peeked
        if "_h264_" in p.name.lower():
            return "h264"
        if looks_compressed:
            return "jpeg"
    return None


def is_orin_images_mcap(path: str | Path) -> bool:
    """True if MCAP carries ROS2 CompressedImage (JPEG or H.264, not protobuf)."""
    return orin_stream_kind(path) in ("jpeg", "h264")


def _jpeg_size(data: bytes) -> Tuple[int, int]:
    """Return (width, height) from JPEG SOF, or (0, 0)."""
    i = 0
    n = len(data)
    while i < n - 8:
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        if marker in (0xC0, 0xC1, 0xC2):
            h = (data[i + 5] << 8) | data[i + 6]
            w = (data[i + 7] << 8) | data[i + 8]
            return int(w), int(h)
        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD9:
            i += 2
            continue
        if i + 3 >= n:
            break
        seglen = (data[i + 2] << 8) | data[i + 3]
        if seglen < 2:
            break
        i += 2 + seglen
    return 0, 0


def _topic_to_channel(topic: str, cam_map: Dict[str, str]) -> Optional[str]:
    t = (topic or "").strip()
    if _H264_TOPIC_RE.match(t) or _topic_has_h264_segment(t):
        # /streamer/realsense_right/h264 → cam = realsense_right
        # /camera/f_l/h264 → cam = f_l
        # /streamer/realsense_right/h264/compressed → 同样
        parts = [p for p in t.split("/") if p]
        if parts and parts[-1].lower() == "compressed":
            parts = parts[:-1]
        if parts and parts[-1].lower() == "h264":
            parts = parts[:-1]
        cam = parts[-1] if parts else ""
        return cam_map.get(cam) or cam_map.get(cam.lower())
    m = _COMPRESSED_TOPIC_RE.match(t)
    if not m:
        parts = [p for p in t.strip("/").split("/") if p]
        if not parts:
            return None
        if len(parts) >= 2 and parts[0].lower() == "camera":
            cam = parts[1]
            mapped = cam_map.get(cam) or cam_map.get(cam.lower())
            if mapped:
                return mapped
            up = cam.upper()
            if up in _SVIEW_CHANNEL_SET:
                return up
        cam = parts[0]
    else:
        cam = m.group("cam")
    return cam_map.get(cam) or cam_map.get(cam.lower())


def sview_channel_for_topic(
    topic: str,
    cam_map: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Map a camera topic onto FR/FL/RR/RL, or None if unknown."""
    return _topic_to_channel(topic, cam_map or _parse_camera_map())


def iter_orin_jpeg_frames(
    path: str | Path,
    cam_map: Optional[Dict[str, str]] = None,
    start_time_ns: Optional[int] = None,
):
    """Yield ``(stream_key, jpeg_bytes, timestamp_ns, camera_meta)`` without loading the bag.

    Harvests CompressedImage JPEG and sensor_msgs/Image (converted) on any topic.
    Stream keys keep the original topic (leading ``/`` stripped).
    """
    cam_map = cam_map or _parse_camera_map()
    topic_map = _stable_assign_topics(_video_topics_from_summary(path, kind="jpeg"), cam_map)
    sized: set = set()
    for msg in _iter_ros2_messages_tolerant(
        str(path), channel_ok=_harvest_channel_ok, start_time_ns=start_time_ns,
    ):
        topic = msg.channel.topic if msg.channel else ""
        schema = msg.schema.name if msg.schema else ""
        ros = msg.ros_msg
        data = b""
        w = h = 0
        if _schema_is_raw_image(schema):
            blob = _ros_image_to_jpeg(ros)
            if not blob:
                continue
            data = blob
            try:
                w = int(getattr(ros, "width", 0) or 0)
                h = int(getattr(ros, "height", 0) or 0)
            except (TypeError, ValueError):
                w = h = 0
        else:
            if not (
                _schema_is_compressed_video(schema) or _looks_like_orin_topic(topic)
            ):
                continue
            fmt = (getattr(ros, "format", "") or "").lower()
            data = bytes(getattr(ros, "data", b"") or b"")
            if not data:
                continue
            kind = _compressed_payload_kind(fmt, data)
            if kind != "jpeg":
                continue
        ch = topic_map.get(topic)
        if not ch:
            topic_map = _stable_assign_topics(list(topic_map.keys()) + [topic], cam_map)
            ch = topic_map.get(topic)
        if not ch:
            continue
        if ch not in sized:
            if not (w and h):
                w, h = _jpeg_size(data)
            sized.add(ch)
        meta = _stream_meta(topic, ch, w or 640, h or 480, "jpeg")
        yield ch, data, int(msg.log_time_ns), meta


def parse_orin_jpeg_streams(
    path: str | Path,
    cam_map: Optional[Dict[str, str]] = None,
) -> Tuple[
    Dict[str, List[bytes]],
    Dict[str, List[int]],
    Dict[str, dict],
]:
    """Read CompressedImage JPEGs → (frames, timestamps_ns, camera_metas).

    Keys are original topic slugs (leading ``/`` stripped).
    """
    cam_map = cam_map or _parse_camera_map()
    frames: Dict[str, List[bytes]] = defaultdict(list)
    timestamps: Dict[str, List[int]] = defaultdict(list)
    metas: Dict[str, dict] = {}
    skipped_h264 = 0
    topics_seen: List[str] = []

    for ch, data, ts_ns, meta in iter_orin_jpeg_frames(path, cam_map=cam_map):
        topic = str(meta.get("source_topic") or "")
        if topic and topic not in topics_seen:
            topics_seen.append(topic)
        frames[ch].append(data)
        timestamps[ch].append(ts_ns)
        if ch not in metas:
            metas[ch] = dict(meta)

    # Count H.264-on-compressed-topic for a better empty-bag error (iterator skips them).
    if not frames:
        for msg in _iter_ros2_messages_tolerant(str(path), channel_ok=_harvest_channel_ok):
            if msg.schema is None or not _schema_is_compressed_video(msg.schema.name):
                continue
            topic = msg.channel.topic if msg.channel else ""
            if topic and topic not in topics_seen:
                topics_seen.append(topic)
            data = bytes(getattr(msg.ros_msg, "data", b"") or b"")
            fmt = (getattr(msg.ros_msg, "format", "") or "")
            if _compressed_payload_kind(fmt, data) == "h264":
                skipped_h264 += 1
            if skipped_h264 or len(topics_seen) >= 8:
                break

    for ch, ts in timestamps.items():
        if ch not in metas or len(ts) < 2:
            continue
        dur = (ts[-1] - ts[0]) / 1e9
        if dur > 0:
            fps = (len(ts) - 1) / dur
            if 5.0 <= fps <= 120.0:
                metas[ch]["fps"] = int(round(fps))

    if not frames:
        hint = ""
        if skipped_h264:
            hint = (
                f"; saw {skipped_h264} H.264 CompressedImage frame(s) on "
                f"topics={topics_seen} — this bag should be parsed as Orin H.264"
            )
        elif topics_seen:
            hint = f"; CompressedImage topics={topics_seen}"
        raise ValueError(
            f"No Orin CompressedImage JPEG streams found in {path} "
            f"(camera map={cam_map}){hint}"
        )
    for ch in sorted(frames.keys()):
        log.info(
            "Orin %s: %d JPEG frames  %dx%d @ ~%d fps",
            ch,
            len(frames[ch]),
            metas[ch]["width"],
            metas[ch]["height"],
            metas[ch]["fps"],
        )
    return dict(frames), dict(timestamps), metas


def _annexb_start(data: bytes) -> Tuple[int, int]:
    """Return (offset, start_code_len) of the first Annex-B start code, or (-1, 0)."""
    head = data[:4096] if len(data) > 4096 else data
    i4 = head.find(b"\x00\x00\x00\x01")
    i3 = head.find(b"\x00\x00\x01")
    if i4 < 0 and i3 < 0:
        return -1, 0
    if i4 >= 0 and (i3 < 0 or i4 <= i3):
        return i4, 4
    return i3, 3


def _looks_like_annexb_h264(data: bytes) -> bool:
    """payload 以 Annex-B start code 开头且首个 NAL 类型合理。"""
    i, sc = _annexb_start(data)
    if i < 0 or i + sc >= len(data):
        return False
    nal = data[i + sc] & 0x1F
    return nal in (1, 5, 6, 7, 8, 9)


def _looks_like_annexb_hevc(data: bytes) -> bool:
    i, sc = _annexb_start(data)
    if i < 0 or i + sc >= len(data):
        return False
    nal = (data[i + sc] >> 1) & 0x3F
    return nal in (1, 19, 20, 32, 33, 34, 35)


def _compressed_payload_kind(fmt: str, data: bytes) -> Optional[str]:
    """Classify a CompressedImage payload as 'jpeg' | 'h264' | 'hevc' | None."""
    f = (fmt or "").lower()
    if "h265" in f or "hevc" in f:
        return "hevc"
    if "h264" in f or "avc" in f or "annexb" in f:
        return "h264"
    if "jpeg" in f or "jpg" in f or "mjpeg" in f:
        return "jpeg"
    if data[:2] == b"\xff\xd8":
        return "jpeg"
    if _looks_like_annexb_h264(data):
        return "h264"
    if _looks_like_annexb_hevc(data):
        return "hevc"
    return None


def _ros_image_to_jpeg(ros) -> Optional[bytes]:
    """sensor_msgs/Image → JPEG bytes, or None if encoding is unsupported."""
    try:
        from PIL import Image
        import numpy as np
    except Exception:
        return None
    enc = (getattr(ros, "encoding", "") or "").lower()
    try:
        width = int(getattr(ros, "width", 0) or 0)
        height = int(getattr(ros, "height", 0) or 0)
    except (TypeError, ValueError):
        return None
    data = bytes(getattr(ros, "data", b"") or b"")
    if width < 8 or height < 8 or not data:
        return None
    try:
        arr = np.frombuffer(data, dtype=np.uint8)
    except Exception:
        return None
    mode = "RGB"
    rgb = None
    if enc in ("rgb8", "rgb"):
        need = width * height * 3
        if arr.size < need:
            return None
        rgb = arr[:need].reshape((height, width, 3))
    elif enc in ("bgr8", "bgr"):
        need = width * height * 3
        if arr.size < need:
            return None
        rgb = arr[:need].reshape((height, width, 3))[:, :, ::-1].copy()
    elif enc in ("rgba8", "rgb8a"):
        need = width * height * 4
        if arr.size < need:
            return None
        rgb = arr[:need].reshape((height, width, 4))[:, :, :3]
    elif enc in ("bgra8",):
        need = width * height * 4
        if arr.size < need:
            return None
        rgb = arr[:need].reshape((height, width, 4))[:, :, 2::-1].copy()
    elif enc in ("mono8", "8uc1", "y8"):
        need = width * height
        if arr.size < need:
            return None
        gray = arr[:need].reshape((height, width))
        buf = io.BytesIO()
        Image.fromarray(gray, mode="L").save(buf, format="JPEG", quality=85)
        return buf.getvalue()
    else:
        return None
    if rgb is None:
        return None
    buf = io.BytesIO()
    Image.fromarray(rgb, mode=mode).save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def _peek_orin_payload_kind(
    path: Path, *, max_msgs: int = 200, max_image: int = 8
) -> Optional[str]:
    """Scan the start of the bag for jpeg vs h264 vs hevc vs raw Image."""
    n_msg = 0
    n_img = 0
    try:
        for msg in _iter_ros2_messages_tolerant(
            str(path), channel_ok=_harvest_channel_ok
        ):
            n_msg += 1
            topic = msg.channel.topic if msg.channel else ""
            if _H264_TOPIC_RE.match(topic.strip()) or _topic_has_h264_segment(topic):
                return "h264"
            schema = msg.schema.name if msg.schema else ""
            if _schema_is_raw_image(schema):
                return "jpeg"
            if not _schema_is_compressed_video(schema) and not _looks_like_orin_topic(topic):
                if n_msg >= max_msgs:
                    break
                continue
            n_img += 1
            ros = msg.ros_msg
            fmt = getattr(ros, "format", "") or ""
            data = bytes(getattr(ros, "data", b"") or b"")
            kind = _compressed_payload_kind(fmt, data)
            if kind:
                return kind
            if n_img >= max_image or n_msg >= max_msgs:
                break
    except Exception:
        log.debug("peek orin payload failed for %s", path, exc_info=True)
    return None


def _iter_h264_nals(data: bytes):
    """Yield Annex-B NAL units including their start codes."""
    if not data:
        return
    starts = []
    pos = 0
    n = len(data)
    while pos < n - 3:
        p4 = data.find(b"\x00\x00\x00\x01", pos)
        p3 = data.find(b"\x00\x00\x01", pos)
        if p4 < 0 and p3 < 0:
            break
        if p4 < 0:
            np, sc = p3, 3
        elif p3 < 0:
            np, sc = p4, 4
        else:
            np, sc = (p4, 4) if p4 <= p3 else (p3, 3)
        if np + sc >= n:
            break
        starts.append(np)
        pos = np + sc + 1
    if not starts:
        return
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else n
        yield data[start:end]


def _h264_nal_type(nal: bytes) -> int:
    if len(nal) >= 5 and nal.startswith(b"\x00\x00\x00\x01"):
        return nal[4] & 0x1F
    if len(nal) >= 4 and nal.startswith(b"\x00\x00\x01"):
        return nal[3] & 0x1F
    return 0


def _collect_h264_param_sets(data: bytes) -> dict:
    found = {}
    for nal in _iter_h264_nals(data):
        ntype = _h264_nal_type(nal)
        if ntype in (7, 8):
            found[ntype] = nal
    return found


def _h264_has_idr(data: bytes) -> bool:
    return any(_h264_nal_type(nal) == 5 for nal in _iter_h264_nals(data))


def _avcc_to_annexb(data: bytes) -> Optional[bytes]:
    """4-byte length-prefixed (AVCC) NALs → Annex-B. None if not AVCC."""
    i, n = 0, len(data)
    out = bytearray()
    nals = 0
    while i + 4 <= n:
        ln = int.from_bytes(data[i : i + 4], "big")
        if ln < 1 or i + 4 + ln > n:
            return None
        nal = data[i + 4 : i + 4 + ln]
        ntype = nal[0] & 0x1F
        if ntype == 0 or ntype > 13:
            return None
        out += b"\x00\x00\x00\x01"
        out += nal
        i += 4 + ln
        nals += 1
    if i != n or nals < 1:
        return None
    return bytes(out)


def _to_annexb_h264(data: bytes) -> Optional[bytes]:
    """Normalize one CompressedImage payload to Annex-B, or None if unusable."""
    if not data:
        return None
    if _looks_like_annexb_h264(data):
        i, _sc = _annexb_start(data)
        return data[i:] if i > 0 else data
    avcc = _avcc_to_annexb(data)
    if avcc:
        return avcc
    if (data[0] & 0x80) == 0 and (data[0] & 0x1F) in (1, 5, 6, 7, 8, 9):
        return b"\x00\x00\x00\x01" + data
    return None


def _coerce_annexb_h264(data: bytes) -> Optional[bytes]:
    return _to_annexb_h264(data)


def _prepare_h264_decode_start(
    frames: Dict[str, List[bytes]],
    timestamps: Dict[str, List[int]],
    metas: Dict[str, dict],
) -> None:
    """Drop leading P-frames and prefix SPS/PPS so ffmpeg can open the first chunk.

    Live encode starts a *new* ffmpeg per chunk. A chunk that begins on a
    P-slice (no PPS, no reference pictures) fails with
    ``non-existing PPS 0 referenced`` / ``no frame!``.
    """
    for ch in list(frames.keys()):
        blobs = frames[ch]
        ts = timestamps.get(ch) or []
        ps: dict = {}
        idr_idx = None
        for i, blob in enumerate(blobs):
            ps.update(_collect_h264_param_sets(blob))
            if idr_idx is None and _h264_has_idr(blob):
                idr_idx = i
        if 7 not in ps or 8 not in ps:
            raise ValueError(
                f"Orin {ch}: H.264 stream has no in-band SPS/PPS "
                f"({len(blobs)} frames). Cannot decode."
            )
        if idr_idx is None:
            raise ValueError(
                f"Orin {ch}: H.264 stream has no IDR in {len(blobs)} frames "
                "(recording started mid-GOP with no keyframe)."
            )
        if idr_idx:
            log.info("Orin %s: drop %d leading P-frames before first IDR", ch, idr_idx)
            blobs = blobs[idr_idx:]
            ts = ts[idr_idx:]
        have = _collect_h264_param_sets(blobs[0])
        if 7 not in have or 8 not in have:
            prefix = b"".join(ps[t] for t in (7, 8) if t in ps)
            blobs[0] = prefix + blobs[0]
        w, h = _h264_sps_size(blobs[0])
        if ch in metas:
            if w:
                metas[ch]["width"] = w
            if h:
                metas[ch]["height"] = h
        frames[ch] = blobs
        timestamps[ch] = ts


class _BitReader:
    __slots__ = ("d", "p")

    def __init__(self, d: bytes):
        self.d = d
        self.p = 0

    def u(self, n: int) -> int:
        v = 0
        for _ in range(n):
            byte = self.d[self.p >> 3]
            v = (v << 1) | ((byte >> (7 - (self.p & 7))) & 1)
            self.p += 1
        return v

    def ue(self) -> int:
        zeros = 0
        while self.u(1) == 0:
            zeros += 1
            if zeros > 31:
                raise ValueError("bad ue")
        return (1 << zeros) - 1 + (self.u(zeros) if zeros else 0)

    def se(self) -> int:
        k = self.ue()
        return (k + 1) // 2 if k % 2 else -(k // 2)


def _skip_scaling_list(br: _BitReader, size: int) -> None:
    last, next_ = 8, 8
    for _ in range(size):
        if next_ != 0:
            next_ = (last + br.se() + 256) % 256
        last = next_ if next_ != 0 else last


def _h264_sps_size(data: bytes) -> Tuple[int, int]:
    """Return (width, height) parsed from the first SPS NAL, or (0, 0)."""
    sps = None
    pos, n = 0, len(data)
    while pos < n - 5:
        p4 = data.find(b"\x00\x00\x00\x01", pos)
        p3 = data.find(b"\x00\x00\x01", pos)
        if p4 < 0 and p3 < 0:
            break
        if p4 < 0:
            np_, sc = p3, 3
        elif p3 < 0:
            np_, sc = p4, 4
        else:
            np_, sc = (p4, 4) if p4 <= p3 else (p3, 3)
        if np_ + sc >= n:
            break
        if data[np_ + sc] & 0x1F == 7:
            e4 = data.find(b"\x00\x00\x00\x01", np_ + sc + 1)
            e3 = data.find(b"\x00\x00\x01", np_ + sc + 1)
            ends = [e for e in (e4, e3) if e >= 0]
            end = min(ends) if ends else n
            sps = data[np_ + sc + 1 : end]
            break
        pos = np_ + sc + 1
    if not sps:
        return 0, 0
    # 去 emulation prevention (00 00 03 → 00 00)
    rbsp = bytearray()
    i = 0
    while i < len(sps):
        if i + 2 < len(sps) and sps[i] == 0 and sps[i + 1] == 0 and sps[i + 2] == 3:
            rbsp += b"\x00\x00"
            i += 3
        else:
            rbsp.append(sps[i])
            i += 1
    try:
        br = _BitReader(bytes(rbsp))
        profile_idc = br.u(8)
        br.u(8)  # constraint flags
        br.u(8)  # level_idc
        br.ue()  # sps id
        if profile_idc in (100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135):
            chroma_format_idc = br.ue()
            if chroma_format_idc == 3:
                br.u(1)
            br.ue()  # bit_depth_luma_minus8
            br.ue()  # bit_depth_chroma_minus8
            br.u(1)  # qpprime_y_zero_transform_bypass
            if br.u(1):  # seq_scaling_matrix_present
                for j in range(8 if chroma_format_idc != 3 else 12):
                    if br.u(1):
                        _skip_scaling_list(br, 16 if j < 6 else 64)
        br.ue()  # log2_max_frame_num_minus4
        poc = br.ue()
        if poc == 0:
            br.ue()
        elif poc == 1:
            br.u(1)
            br.se()
            br.se()
            for _ in range(br.ue()):
                br.se()
        br.ue()  # max_num_ref_frames
        br.u(1)  # gaps_in_frame_num
        w_mbs = br.ue() + 1
        h_maps = br.ue() + 1
        frame_only = br.u(1)
        if not frame_only:
            br.u(1)  # mb_adaptive_frame_field
        br.u(1)  # direct_8x8_inference
        cx = cy = 0
        if br.u(1):  # frame_cropping
            cl, cr, ct, cb = br.ue(), br.ue(), br.ue(), br.ue()
            cx = (cl + cr) * 2  # 4:2:0 → x2
            cy = (ct + cb) * (2 if frame_only else 4)
        w = w_mbs * 16 - cx
        h = h_maps * 16 * (2 - frame_only) - cy
        return (int(w), int(h)) if w > 0 and h > 0 else (0, 0)
    except Exception:
        return 0, 0


def _estimate_fps_into_metas(timestamps: Dict[str, List[int]], metas: Dict[str, dict]) -> None:
    for ch, ts in timestamps.items():
        if ch not in metas or len(ts) < 2:
            continue
        dur = (ts[-1] - ts[0]) / 1e9
        if dur > 0:
            fps = (len(ts) - 1) / dur
            if 5.0 <= fps <= 120.0:
                metas[ch]["fps"] = int(round(fps))


def _summary_topic_message_counts(path: str | Path) -> Dict[str, int]:
    """MCAP Summary ``channel_message_counts`` keyed by topic (footer seek)."""
    from mcap.reader import make_reader

    p = Path(path)
    if not p.is_file():
        return {}
    try:
        with open(p, "rb") as fh:
            summary = make_reader(fh).get_summary()
    except Exception:
        log.debug("orin summary read failed for %s", p.name, exc_info=True)
        return {}
    if not summary or not summary.channels:
        return {}
    stats = summary.statistics
    ch_counts = getattr(stats, "channel_message_counts", None) if stats else None
    if not ch_counts:
        return {}
    out: Dict[str, int] = {}
    for ch_id, channel in summary.channels.items():
        topic = getattr(channel, "topic", "") or ""
        n = ch_counts.get(ch_id)
        if n is None:
            continue
        try:
            out[topic] = int(n)
        except (TypeError, ValueError):
            continue
    return out


def orin_frame_count_from_channel_counts(
    topic_counts: Dict[str, int],
    kind: str,
    cam_map: Optional[Dict[str, str]] = None,
) -> Optional[int]:
    """Aligned Orin frame count from per-topic message counts.

    Min of harvested video topics. ``kind`` is ``h264`` or ``jpeg``.
    """
    kind_l = (kind or "").strip().lower()
    if kind_l not in ("h264", "jpeg"):
        return None
    cam_map = cam_map or _parse_camera_map()
    classic: List[str] = []
    unknown: List[str] = []
    counts: Dict[str, int] = {}
    for topic, n in (topic_counts or {}).items():
        if _is_headcam_protobuf_topic(topic):
            continue
        try:
            count = int(n)
        except (TypeError, ValueError):
            continue
        if count <= 0:
            continue
        counts[topic] = count
        if kind_l == "h264":
            if _is_orin_h264_topic(topic):
                classic.append(topic)
            elif not _is_orin_jpeg_topic(topic) and _topic_looks_generic_camera(topic):
                unknown.append(topic)
        else:
            if _is_orin_jpeg_topic(topic):
                classic.append(topic)
            elif not _is_orin_h264_topic(topic) and _topic_looks_generic_camera(topic):
                unknown.append(topic)
    use = classic or unknown
    if not use:
        return None
    mapping = _stable_assign_topics(use, cam_map)
    video: Dict[str, int] = {}
    for topic, ch in mapping.items():
        n = counts.get(topic, 0)
        if n <= 0:
            continue
        prev = video.get(ch)
        video[ch] = n if prev is None else min(prev, n)
    if not video:
        return None
    return min(video.values())


def _topic_looks_generic_camera(topic: str) -> bool:
    t = (topic or "").lower()
    if _is_headcam_protobuf_topic(topic):
        return False
    if any(tok in t for tok in ("/joint", "/imu", "/tf", "lidar", "point_cloud", "/odom", "/audio")):
        return False
    return any(
        tok in t
        for tok in ("camera", "image", "video", "h264", "h265", "hevc", "jpeg", "rgb", "realsense")
    )


def read_orin_video_frame_count(
    source: Union[str, Path],
    kind: str,
    cam_map: Optional[Dict[str, str]] = None,
) -> Optional[int]:
    """Read total aligned Orin video frames from MCAP Summary (footer seek)."""
    counts = _summary_topic_message_counts(source)
    if not counts:
        return None
    harvested = _video_topics_from_summary(source, kind=kind)
    if harvested:
        filtered = {t: counts[t] for t in harvested if t in counts and int(counts[t] or 0) > 0}
        if filtered:
            mapping = _stable_assign_topics(list(filtered), cam_map)
            video: Dict[str, int] = {}
            for topic, ch in mapping.items():
                n = int(filtered.get(topic) or 0)
                if n <= 0:
                    continue
                prev = video.get(ch)
                video[ch] = n if prev is None else min(prev, n)
            if video:
                return min(video.values())
    return orin_frame_count_from_channel_counts(counts, kind, cam_map)


def orin_h264_channels_from_summary(path: str | Path) -> List[str]:
    """Stream keys present as H.264 / compressed-video topics in the MCAP summary."""
    topics = _video_topics_from_summary(path, kind="h264")
    if not topics:
        return []
    mapping = _stable_assign_topics(topics)
    out: List[str] = []
    for topic in topics:
        ch = mapping.get(topic)
        if ch and ch not in out:
            out.append(ch)
    return out


def orin_jpeg_channels_from_summary(path: str | Path) -> List[str]:
    topics = _video_topics_from_summary(path, kind="jpeg")
    if not topics:
        return []
    mapping = _stable_assign_topics(topics)
    out: List[str] = []
    for topic in topics:
        ch = mapping.get(topic)
        if ch and ch not in out:
            out.append(ch)
    return out


def iter_orin_h264_annexb(
    path: str | Path,
    cam_map: Optional[Dict[str, str]] = None,
    start_time_ns: Optional[int] = None,
):
    """Yield ``(stream_key, annexb, timestamp_ns, camera_meta)`` one frame at a time.

    Stream keys keep the original topic (leading ``/`` stripped).
    Does not load the whole bag first — callers can encode the first GOP while
    the rest of the file is still being read.
    """
    cam_map = cam_map or _parse_camera_map()
    topic_map = _stable_assign_topics(_video_topics_from_summary(path, kind="h264"), cam_map)
    topics_seen: List[str] = []
    unmapped: List[str] = []
    yielded = 0
    sized: set = set()
    for msg in _iter_ros2_messages_tolerant(
        str(path), channel_ok=_harvest_channel_ok, start_time_ns=start_time_ns,
    ):
        topic = msg.channel.topic if msg.channel else ""
        schema = msg.schema.name if msg.schema else ""
        is_h264_topic = bool(
            _H264_TOPIC_RE.match(topic.strip()) or _topic_has_h264_segment(topic)
        )
        is_ci = _schema_is_compressed_video(schema)
        if not is_h264_topic and not is_ci:
            continue
        if topic and topic not in topics_seen:
            topics_seen.append(topic)
        ros = msg.ros_msg
        fmt = getattr(ros, "format", "") or ""
        data = bytes(getattr(ros, "data", b"") or b"")
        if not data:
            continue
        kind = _compressed_payload_kind(fmt, data)
        if kind == "jpeg":
            continue
        if not (is_h264_topic or kind in ("h264", "hevc")):
            continue
        annexb = _coerce_annexb_h264(data)
        if not annexb:
            log.warning("Skip non-Annex-B/AVCC H.264 payload on %s format=%r", topic, fmt)
            continue
        data = annexb
        ch = topic_map.get(topic)
        if not ch:
            topic_map = _stable_assign_topics(list(topic_map.keys()) + [topic], cam_map)
            ch = topic_map.get(topic)
        if not ch:
            if topic not in unmapped:
                unmapped.append(topic)
            continue
        w = h = 0
        if ch not in sized:
            w, h = _h264_sps_size(data)
            if w > 0 and h > 0:
                sized.add(ch)
            else:
                w, h = 1280, 720
        source_codec = "hevc" if kind == "hevc" else "h264"
        meta = _stream_meta(topic, ch, w, h, source_codec)
        yielded += 1
        yield ch, data, int(msg.log_time_ns), meta
    if yielded <= 0:
        raise ValueError(
            f"No Orin H.264 streams found in {path} "
            f"(topics={topics_seen}, unmapped={unmapped}, camera map={cam_map})"
        )


def parse_orin_h264_streams(
    path: str | Path,
    cam_map: Optional[Dict[str, str]] = None,
) -> Tuple[
    Dict[str, List[bytes]],
    Dict[str, List[int]],
    Dict[str, dict],
]:
    """Read Orin Annex-B H.264 CompressedImage streams → (frames, timestamps_ns, metas).

    两种 topic（与 JPEG 不会同时出现）::

        /streamer/realsense_*/h264
        /left_camera/.../color/image_raw/compressed   format=h264
        /camera/f_l|f_r|left_wrist|right_wrist/h264

    约定：一条消息一帧，payload 为 Annex-B。后续走
    LiveX264Encoder.annexb / cpu_transcode_chunk 重编码归一化。
    """
    frames: Dict[str, List[bytes]] = defaultdict(list)
    timestamps: Dict[str, List[int]] = defaultdict(list)
    metas: Dict[str, dict] = {}
    for ch, data, ts_ns, meta in iter_orin_h264_annexb(path, cam_map):
        frames[ch].append(data)
        timestamps[ch].append(ts_ns)
        if ch not in metas:
            metas[ch] = dict(meta)
        elif meta.get("width") and meta.get("height"):
            metas[ch]["width"] = meta["width"]
            metas[ch]["height"] = meta["height"]

    _prepare_h264_decode_start(frames, timestamps, metas)
    _estimate_fps_into_metas(timestamps, metas)
    for ch in sorted(frames.keys()):
        log.info(
            "Orin %s: %d H.264 frames  %dx%d @ ~%d fps",
            ch,
            len(frames[ch]),
            metas[ch]["width"],
            metas[ch]["height"],
            metas[ch]["fps"],
        )
    return dict(frames), dict(timestamps), metas


def jpeg_sequence_to_h264(
    jpegs: List[bytes],
    output_path: Path,
    *,
    fps: float = 30.0,
    scale_w: Optional[int] = None,
    scale_h: Optional[int] = None,
    ffmpeg: str = "ffmpeg",
    crf: int = 23,
    preset: str = "veryfast",
    bitrate: Optional[str] = "4M",
    maxrate: Optional[str] = "6M",
    bufsize: Optional[str] = "8M",
    gop: Optional[int] = None,
    direct: bool = False,
    threads: int = 2,
) -> float:
    """Encode a list of JPEG blobs to Annex-B H.264 via ffmpeg image2pipe.

    Each call starts a fresh encoder, so the first output AU is an IDR (safe for
    progressive chunk boundaries / seek).

    direct=True: near-lossless path (CRF-only, no bitrate cap / no scale knobs
    from caller). Used when Orin should “直出” without quality crushing.

    Note: libx264 ``high`` + ``yuv420p`` cannot use CRF 0 (lossless requires
    high444). Direct mode therefore uses a low CRF (default 12) instead of 0.
    """
    import time

    if not jpegs:
        raise ValueError("empty JPEG sequence")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    g = int(gop) if gop and gop > 0 else 30
    g = max(1, min(g, len(jpegs)))
    thr = max(1, int(threads or 1))

    # CRF 0 + profile high + yuv420p → x264 error "high profile doesn't support lossless".
    if direct:
        use_crf = max(1, min(51, int(crf))) if crf and int(crf) > 0 else 12
    else:
        use_crf = max(0, min(51, int(crf)))
    cmd = [
        ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
        "-fflags", "nobuffer",
        "-probesize", "32",
        "-analyzeduration", "0",
        "-f", "image2pipe",
        "-framerate", str(fps if fps > 0 else 30),
        "-vcodec", "mjpeg",
        "-i", "pipe:0",
    ]
    if scale_w and scale_h and scale_w > 0 and scale_h > 0:
        cmd += ["-vf", f"scale={scale_w}:{scale_h}:flags=fast_bilinear"]
    cmd += [
        "-c:v", "libx264",
        "-threads", str(thr),
        "-flags", "+low_delay",
        "-x264-params", (
            f"threads={thr}:sliced_threads=0:rc-lookahead=0:sync-lookahead=0:"
            "aq-mode=0:mbtree=0:weightp=0:8x8dct=0"
        ),
        "-profile:v", "baseline",
        "-preset", preset,
        "-tune", "zerolatency",
        "-crf", str(use_crf),
        "-bf", "0",
        "-pix_fmt", "yuv420p",
    ]
    # Rate caps crush quality; skip them for direct / when bitrate unset.
    if (not direct) and bitrate:
        cmd += ["-b:v", bitrate]
        if maxrate:
            cmd += ["-maxrate", maxrate]
        if bufsize:
            cmd += ["-bufsize", bufsize]
    elif (not direct) and maxrate:
        cmd += ["-maxrate", maxrate]
        if bufsize:
            cmd += ["-bufsize", bufsize]
    cmd += [
        "-g", str(g),
        "-keyint_min", str(g),
        "-sc_threshold", "0",
        "-bsf:v", "dump_extra=freq=keyframe",
        "-f", "h264",
        str(output_path),
    ]
    raw = b"".join(jpegs)
    t0 = time.time()
    try:
        subprocess.run(
            cmd,
            input=raw,
            capture_output=True,
            timeout=max(120, 2 * len(jpegs)),
            check=True,
        )
    except subprocess.CalledProcessError as e:
        err = (e.stderr or b"").decode(errors="replace")[-2000:]
        raise RuntimeError(f"ffmpeg JPEG→H264 failed: {err}") from e
    return time.time() - t0
