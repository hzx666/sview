#!/usr/bin/env python3
"""
Orin 遥操 mcap：ROS2 图像/视频流 → H.264 Annex-B。

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

也收未知遥操 topic，schema 是 sensor_msgs Image /
CompressedImage / CompressedVideo（Unitree、Franka 等）。bundle 里的
流 id 保留原 topic（去掉开头的 ``/``）。多出来的相机保留，
不强制映射到 FR/FL/RR/RL。
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

# SView / 前端通道：rr, fr, rl, fl
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
    """SSIEGO / 头显 protobuf 包不能走 Orin 的 ROS2 reader。"""
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
    # Agilex cobot：/camera_l/color/image_raw（CompressedImage，没有 /compressed 后缀）
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


def _topic_wrong_kind(
    topic: str,
    schema_name: Optional[str],
    kind: str,
    codec: Optional[str] = None,
) -> bool:
    """判断这一路不属于 ``kind``（h264 / jpeg）。

    ``codec`` 是 ``channel_payload_kinds`` 从实际消息里看出来的编码，
    有就以它为准；没有才回退到 schema / topic 名。topic 名不可靠：
    Unitree 的 H.264 叫 ``.../image_raw/compressed``，也有 CompressedImage
    里装 ``format=h264`` 的录法。
    """
    kind_l = (kind or "").strip().lower()
    if codec and kind_l in ("h264", "jpeg"):
        return codec != kind_l
    is_video_codec = "compressedvideo" in _norm_schema_name(schema_name)
    if kind_l == "h264":
        if is_video_codec:
            return False
        return _is_orin_jpeg_topic(topic) and not _is_orin_h264_topic(topic)
    if kind_l == "jpeg":
        return is_video_codec or _is_orin_h264_topic(topic)
    return False


def _is_headcam_protobuf_topic(topic: str) -> bool:
    ts = (topic or "").strip()
    return bool(
        ts == "/imu/sample"
        or _HEADCAM_VIDEO_RE.match(ts)
        or _HEADCAM_META_RE.match(ts)
    )


def _harvest_channel_ok(topic: str, schema_name: Optional[str] = None) -> bool:
    """遥操视频收集：先看 schema，topic 白名单兜底。"""
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
    path: str,
    topic_ok=None,
    channel_ok=None,
    start_time_ns: Optional[int] = None,
    topics: Optional[List[str]] = None,
):
    """产出 ROS2 消息；跳过 protobuf 和其他编码，不要整包中断。

    ``topic_ok``：可选 ``topic -> bool``。
    ``channel_ok``：可选 ``(topic, schema_name) -> bool``。设置后
    替代 ``topic_ok``，schema 是 Image / CompressedImage 的未知相机 topic 也能留。
    ``start_time_ns``：mcap ``iter_messages(start_time=)``，跳过更早的 log_time。
    """
    from mcap.reader import make_reader
    from mcap_ros2.decoder import DecoderFactory
    from mcap_ros2.reader import McapROS2Message

    factory = DecoderFactory()
    iter_kw = {}
    if start_time_ns is not None and int(start_time_ns) > 0:
        iter_kw["start_time"] = int(start_time_ns)
    if topics:
        iter_kw["topics"] = [t for t in topics if t]
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
    """解析 ``ORIN_CAMERA_MAP=left_camera:FL,right_camera:FR,...``。"""
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
    """把每个相机 topic 映射成稳定的 bin 流 key（原 topic）。

    ``cam_map`` 不用；留着是为了现有调用点还能编译。
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


def _schema_is_imu(schema_name: Optional[str]) -> bool:
    n = _norm_schema_name(schema_name)
    return bool(n) and (n.endswith(".imu") or n.endswith("msg.imu") or n == "imu")


def _schema_is_camera_info(schema_name: Optional[str]) -> bool:
    return "camerainfo" in _norm_schema_name(schema_name)


def _topic_is_imu(topic: str) -> bool:
    return "imu" in [p.lower() for p in (topic or "").split("/") if p]


def classify_teleop_forward_topic(
    topic: str,
    schema_name: Optional[str] = None,
    *,
    kind: str,
    codec: Optional[str] = None,
) -> Optional[str]:
    """遥操正向读取的角色：``video``、``meta`` 或 ``imu``。否则 None。

    头显 protobuf 视频排除。IMU 保留，包括 schema 是 ROS Imu
    或 topic 路径带 imu 的 ``/imu/sample``。
    """
    raw = (topic or "").strip()
    if not raw:
        return None
    kind_l = (kind or "").strip().lower()
    if raw != "/imu/sample" and _is_headcam_protobuf_topic(raw):
        return None
    schema = schema_name or ""
    if _schema_is_camera_info(schema):
        return "meta"
    video = _harvest_channel_ok(raw, schema) and not _is_headcam_protobuf_topic(raw)
    if video and _topic_wrong_kind(raw, schema, kind_l, codec):
        video = False
    if video:
        return "video"
    n = _norm_schema_name(schema)
    if "image" in n:
        return None
    if _schema_is_imu(schema) or _topic_is_imu(raw):
        return "imu"
    return None


def teleop_forward_topics(path: str | Path, *, kind: str) -> List[tuple]:
    """要读的 ``(topic, role)``。role 是 video、meta 或 imu。"""
    from mcap.reader import make_reader

    p = Path(path)
    if not p.is_file():
        return []
    try:
        with open(p, "rb") as fh:
            summary = make_reader(fh).get_summary()
    except Exception:
        log.debug("teleop_forward_topics failed for %s", p, exc_info=True)
        return []
    if not summary or not summary.channels:
        return []
    schemas = summary.schemas or {}
    codecs = channel_payload_kinds(p)
    found: List[tuple] = []
    seen: set = set()
    for channel in summary.channels.values():
        topic = (getattr(channel, "topic", "") or "").strip()
        if not topic or topic in seen:
            continue
        schema = schemas.get(getattr(channel, "schema_id", None))
        schema_name = getattr(schema, "name", "") if schema is not None else ""
        role = classify_teleop_forward_topic(
            topic, schema_name, kind=kind, codec=codecs.get(topic)
        )
        if not role:
            continue
        seen.add(topic)
        found.append((topic, role))
    return found


def _ros_imu_row(ros, log_time_ns: int) -> dict:
    acc = getattr(ros, "linear_acceleration", None)
    if acc is None:
        x = getattr(ros, "x", 0)
        y = getattr(ros, "y", 0)
        z = getattr(ros, "z", 0)
    else:
        x, y, z = acc.x, acc.y, acc.z
    try:
        temp = float(getattr(ros, "temperature", 0) or 0)
    except (TypeError, ValueError):
        temp = 0.0
    ts = int(log_time_ns)
    return {
        "log_time_ns": ts,
        "monotonic_ns": str(ts),
        "wall_ns": str(ts),
        "x": x,
        "y": y,
        "z": z,
        "temp_c": temp,
        "unit": "m/s^2",
        "scale": 1,
        "sample_index": 0,
    }


def _camera_info_size(ros) -> Tuple[int, int]:
    try:
        w = int(getattr(ros, "width", 0) or 0)
        h = int(getattr(ros, "height", 0) or 0)
    except (TypeError, ValueError):
        return 0, 0
    return w, h


def _matching_camera_size(topic: str, sizes: Dict[str, Tuple[int, int]]) -> Tuple[int, int]:
    best = ""
    picked = (0, 0)
    video_parts = [p for p in topic.split("/") if p]
    for info_topic, wh in sizes.items():
        if not wh[0] or not wh[1]:
            continue
        info_parts = [p for p in info_topic.split("/") if p]
        common = 0
        for a, b in zip(video_parts, info_parts):
            if a != b:
                break
            common += 1
        if common > len(best.split("/")):
            best = info_topic
            picked = wh
    return picked


def iter_teleop_forward(
    path: str | Path,
    kind: str,
    start_time_ns: Optional[int] = None,
):
    """一次正向读取，产出 ``("video", ch, data, ts, meta)`` 或 ``("imu", row)``。

    只解码视频、camera info 和 IMU topic。文件保持打开，直到生成器关闭。
    """
    kind_l = (kind or "").strip().lower()
    selected = teleop_forward_topics(path, kind=kind_l)
    video_topics = [topic for topic, role in selected if role == "video"]
    topic_map = _stable_assign_topics(video_topics)
    roles = {topic: role for topic, role in selected}
    allow = list(roles)
    sizes: Dict[str, Tuple[int, int]] = {}
    sized: set = set()
    for msg in _iter_ros2_messages_tolerant(
        str(path),
        topics=allow or None,
        start_time_ns=start_time_ns,
    ):
        topic = msg.channel.topic if msg.channel else ""
        schema = msg.schema.name if msg.schema else ""
        role = roles.get(topic) or classify_teleop_forward_topic(topic, schema, kind=kind_l)
        ros = msg.ros_msg
        if role == "meta":
            wh = _camera_info_size(ros)
            if wh[0] and wh[1]:
                sizes[topic] = wh
            continue
        if role == "imu":
            yield ("imu", _ros_imu_row(ros, int(msg.log_time_ns)))
            continue
        if role != "video":
            continue
        data = b""
        w = h = 0
        codec = "h264"
        if kind_l == "jpeg":
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
                fmt = (getattr(ros, "format", "") or "").lower()
                data = bytes(getattr(ros, "data", b"") or b"")
                if not data or _compressed_payload_kind(fmt, data) != "jpeg":
                    continue
            codec = "jpeg"
        else:
            fmt = getattr(ros, "format", "") or ""
            data = bytes(getattr(ros, "data", b"") or b"")
            if not data:
                continue
            payload_kind = _compressed_payload_kind(fmt, data)
            if payload_kind == "jpeg":
                continue
            if payload_kind not in ("h264", "hevc") and not _is_orin_h264_topic(topic):
                continue
            annexb = _coerce_annexb_h264(data)
            if not annexb:
                continue
            data = annexb
            codec = "hevc" if payload_kind == "hevc" else "h264"
            w, h = _h264_sps_size(data)
        ch = topic_map.get(topic)
        if not ch:
            continue
        if not (w and h):
            w, h = _matching_camera_size(topic, sizes)
        if ch not in sized:
            if codec == "jpeg" and not (w and h):
                w, h = _jpeg_size(data)
            sized.add(ch)
        meta = _stream_meta(topic, ch, w or 0, h or 0, codec)
        yield ("video", ch, data, int(msg.log_time_ns), meta)


def _video_topics_from_summary(
    path: str | Path,
    *,
    kind: Optional[str] = None,
) -> List[str]:
    """看起来像相机的 topic（按 schema 或白名单）。"""
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
    codecs = channel_payload_kinds(p) if kind_l in ("h264", "jpeg") else {}
    out: List[str] = []
    for channel in summary.channels.values():
        topic = (getattr(channel, "topic", "") or "").strip()
        if not topic or _is_headcam_protobuf_topic(topic):
            continue
        schema = schemas.get(getattr(channel, "schema_id", None))
        schema_name = getattr(schema, "name", "") if schema is not None else ""
        harvest = _harvest_channel_ok(topic, schema_name)
        if not harvest or _topic_wrong_kind(topic, schema_name, kind_l, codecs.get(topic)):
            continue
        if topic not in out:
            out.append(topic)
    return out


def mcap_channel_overview(path: str | Path, *, max_topics: int = 24) -> dict:
    """给报错用的便宜 mcap 文件尾摘要：编码 / schema / topic。"""
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

    ``skip_headcam_guard``：目录已经判成遥操的路径仍要嗅探，
    即使某个 topic 名字看起来像 ``/imu/sample``。
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

    codecs = set(channel_payload_kinds(p).values()) if topics else set()
    if "h264" in codecs:
        return "h264"
    if "jpeg" in codecs:
        return "jpeg"

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
    """mcap 里是 ROS2 CompressedImage（JPEG 或 H.264，不是 protobuf）则为 True。"""
    return orin_stream_kind(path) in ("jpeg", "h264")


def _jpeg_size(data: bytes) -> Tuple[int, int]:
    """从 JPEG SOF 取 (宽, 高)，否则 (0, 0)。"""
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
    """把相机 topic 映射到 FR/FL/RR/RL，未知则 None。"""
    return _topic_to_channel(topic, cam_map or _parse_camera_map())


def iter_orin_jpeg_frames(
    path: str | Path,
    cam_map: Optional[Dict[str, str]] = None,
    start_time_ns: Optional[int] = None,
):
    """不整包加载，逐帧产出 ``(stream_key, jpeg_bytes, timestamp_ns, camera_meta)``。

    收集任意 topic 上的 CompressedImage JPEG 和 sensor_msgs/Image（会转成 JPEG）。
    流 key 保留原 topic（去掉开头的 ``/``）。
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
    """读 CompressedImage JPEG → (frames, timestamps_ns, camera_metas)。

    key 是原 topic 路径（去掉开头的 ``/``）。
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

    # 统计 compressed topic 上的 H.264，空包报错更好看（迭代器会跳过它们）。
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
    """返回第一个 Annex-B start code 的 (偏移, 起始码长度)，没有则 (-1, 0)。"""
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
    """把 CompressedImage payload 分成 'jpeg' | 'h264' | 'hevc' | None。"""
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
    """sensor_msgs/Image → JPEG 字节；编码不支持则 None。"""
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
    """扫包开头，区分 jpeg、h264、hevc 和原始 Image。"""
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


_CHANNEL_KIND_CACHE: Dict[tuple, Dict[str, str]] = {}


def channel_payload_kinds(path: str | Path, *, max_msgs: int = 400) -> Dict[str, str]:
    """每路相机实际的编码：``topic -> 'h264' | 'jpeg'``。

    看每个 topic 的前几条消息（format 字段或字节头），hevc 归到 h264，
    原始 sensor_msgs/Image 归到 jpeg（转码时压成 JPEG）。看不出来的
    topic 不出现在结果里，调用方回退到 schema / topic 名判断。
    """
    from mcap.reader import make_reader

    p = Path(path)
    try:
        st = p.stat()
    except OSError:
        return {}
    key = (str(p), st.st_size, st.st_mtime_ns)
    cached = _CHANNEL_KIND_CACHE.get(key)
    if cached is not None:
        return dict(cached)
    out: Dict[str, str] = {}
    pending: List[str] = []
    try:
        with open(p, "rb") as fh:
            summary = make_reader(fh).get_summary()
        if summary and summary.channels:
            schemas = summary.schemas or {}
            for channel in summary.channels.values():
                topic = (getattr(channel, "topic", "") or "").strip()
                schema = schemas.get(getattr(channel, "schema_id", None))
                schema_name = getattr(schema, "name", "") if schema is not None else ""
                if not topic or topic in out or topic in pending:
                    continue
                if not _harvest_channel_ok(topic, schema_name):
                    continue
                if _schema_is_raw_image(schema_name):
                    out[topic] = "jpeg"
                else:
                    pending.append(topic)
        if pending:
            n = 0
            for msg in _iter_ros2_messages_tolerant(
                str(p), channel_ok=_harvest_channel_ok, topics=pending
            ):
                n += 1
                topic = (msg.channel.topic if msg.channel else "").strip()
                if topic in pending:
                    ros = msg.ros_msg
                    kind = _compressed_payload_kind(
                        getattr(ros, "format", "") or "",
                        bytes(getattr(ros, "data", b"") or b""),
                    )
                    if kind:
                        out[topic] = "h264" if kind in ("h264", "hevc") else kind
                        pending.remove(topic)
                if not pending or n >= max_msgs:
                    break
    except Exception:
        log.debug("channel_payload_kinds failed for %s", p, exc_info=True)
    _CHANNEL_KIND_CACHE[key] = dict(out)
    return out


def _iter_h264_nals(data: bytes):
    """产出 Annex-B NAL，含起始码。"""
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
    """4 字节长度前缀（AVCC）的 NAL → Annex-B。不是 AVCC 则 None。"""
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
    """把一条 CompressedImage payload 归一成 Annex-B，不能用则 None。"""
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
    """丢掉开头的 P 帧并补上 SPS/PPS，让 ffmpeg 能打开第一段。

    边转边播每个 chunk 都会新起一个 ffmpeg。chunk 若从
    P 片开始（没有 PPS、没有参考帧）会失败，报
    ``non-existing PPS 0 referenced`` / ``no frame!``。
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
    """从第一个 SPS NAL 解析 (宽, 高)，否则 (0, 0)。"""
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
        br.u(8)  # 约束标志
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
    """mcap Summary 的 ``channel_message_counts``，按 topic 做 key（文件尾寻址）。"""
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
    """用各 topic 消息数算出对齐后的 Orin 帧数。

    取收集到的视频 topic 的最小值。``kind`` 为 ``h264`` 或 ``jpeg``。
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
    """从 mcap Summary 读对齐后的 Orin 视频总帧数（文件尾寻址）。"""
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
    """mcap summary 里以 H.264 / 压缩视频 topic 出现的流 key。"""
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
    """逐帧产出 ``(stream_key, annexb, timestamp_ns, camera_meta)``。

    流 key 保留原 topic（去掉开头的 ``/``）。
    不先整包加载——调用方可以边读文件边编码第一段 GOP。
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
    """读 Orin Annex-B H.264 CompressedImage 流 → (frames, timestamps_ns, metas)。

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
    """用 ffmpeg image2pipe 把一串 JPEG 编成 Annex-B H.264。

    每次调用都新开编码器，所以第一帧输出 AU 是 IDR（分段边界和 seek 安全）。

    direct=True：近无损路径（只用 CRF，不限码率、不吃调用方的缩放参数）。
    Orin 要“直出”、不想把画质压坏时用。

    注意：libx264 的 ``high`` + ``yuv420p`` 不能用 CRF 0（无损要 high444）。
    直出模式因此用较低 CRF（默认 12），不用 0。
    """
    import time

    if not jpegs:
        raise ValueError("empty JPEG sequence")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    g = int(gop) if gop and gop > 0 else 30
    g = max(1, min(g, len(jpegs)))
    thr = max(1, int(threads or 1))

    # CRF 0 + profile high + yuv420p → x264 报错 "high profile doesn't support lossless"。
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
    # 码率上限会压画质；直出或没设码率时跳过。
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
