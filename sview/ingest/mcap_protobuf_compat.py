"""容忍非标准编码的 protobuf mcap 读取，给 SSIEGO / 头显包用。

官方 ``mcap_protobuf.reader.read_protobuf_messages`` 要求
``channel.message_encoding == schema.encoding == "protobuf"``，
遇到第一个非 protobuf 通道就整文件失败。录制端有时写成
``proto`` / 空编码，或混进 ROS2/JSON topic。

本读取器：
- 常见别名或空编码，只要 schema 数据像 FileDescriptorSet，就当 protobuf
- schema.name 对上 FDS 里的消息，即使它是文件名
- 解不了的通道跳过，不中断
"""

from __future__ import annotations

import logging
import re
from dataclasses import replace
from pathlib import Path
from typing import Iterable, Iterator, Optional, Union

from mcap.reader import make_reader
from mcap.records import Schema
from mcap_protobuf.decoder import DecoderFactory
from mcap_protobuf.reader import McapProtobufMessage

log = logging.getLogger("sview.mcap_protobuf_compat")

_PROTO_ALIASES = {
    "protobuf",
    "proto",
    "proto2",
    "proto3",
    "google.protobuf",
    "application/protobuf",
    "application/x-protobuf",
}
_NON_PROTO = {
    "cdr",
    "cdr_le",
    "ros2",
    "ros2msg",
    "json",
    "jsonschema",
    "cbor",
    "flatbuffer",
    "omgidl",
}

# SSIEGO / SView 逻辑相机。一条 `/camera/{ch}/video` 消息就是一帧。
_HEADCAM_CHANNELS = ("RR", "FR", "RL", "FL")
_HEADCAM_CHANNEL_SET = frozenset(_HEADCAM_CHANNELS)
_HEADCAM_VIDEO_RE = re.compile(r"^/camera/(RR|FR|RL|FL)/video/?$", re.IGNORECASE)
_TELEOP_H264_RE = re.compile(
    r"^/camera/(f_l|f_r|left_wrist|right_wrist)/h264(?:/compressed)?/?$",
    re.IGNORECASE,
)
_TELEOP_CAM_TO_CH = {
    "f_l": "FL",
    "f_r": "FR",
    "left_wrist": "RL",
    "right_wrist": "RR",
}


def _norm(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def _is_proto_encoding(value: Optional[str]) -> bool:
    raw = _norm(value)
    if not raw:
        return False
    if raw in _PROTO_ALIASES:
        return True
    compact = raw.replace("_", "").replace("-", "").replace(" ", "")
    return compact in {
        "protobuf",
        "proto",
        "proto2",
        "proto3",
        "googleprotobuf",
        "applicationprotobuf",
        "applicationxprotobuf",
    }


def _looks_like_fds(data: bytes) -> bool:
    if not data or data[0] != 0x0A:
        return False
    try:
        from google.protobuf.descriptor_pb2 import FileDescriptorSet

        fds = FileDescriptorSet.FromString(data)
        return bool(fds.file)
    except Exception:
        return False


def _looks_protobuf(message_encoding: Optional[str], schema: Optional[Schema]) -> bool:
    enc = _norm(message_encoding)
    if enc in _NON_PROTO:
        return False
    if schema is None:
        return False
    schema_enc = _norm(schema.encoding)
    if schema_enc in _NON_PROTO:
        return False
    if _is_proto_encoding(enc) or _is_proto_encoding(schema_enc):
        return True
    if (not enc or not schema_enc) and _looks_like_fds(schema.data):
        return True
    return _looks_like_fds(schema.data)


def _message_names_from_fds(data: bytes) -> list[str]:
    from google.protobuf.descriptor_pb2 import FileDescriptorSet

    try:
        fds = FileDescriptorSet.FromString(data)
    except Exception:
        return []
    names: list[str] = []

    def _walk(pkg: str, desc) -> None:
        full = f"{pkg}.{desc.name}" if pkg else desc.name
        names.append(full)
        for nested in desc.nested_type:
            _walk(full, nested)

    for fd in fds.file:
        pkg = fd.package or ""
        for mt in fd.message_type:
            _walk(pkg, mt)
    return names


def _name_candidates(schema: Schema) -> list[str]:
    raw = (schema.name or "").strip()
    out: list[str] = []

    def _add(item: str) -> None:
        if item and item not in out:
            out.append(item)

    _add(raw)
    _add(raw.replace("/", "."))
    if raw.endswith(".proto"):
        stem = raw[: -len(".proto")]
        _add(stem)
        _add(stem.rsplit(".", 1)[-1])
    if "." in raw:
        _add(raw.rsplit(".", 1)[-1])

    fds_names = _message_names_from_fds(schema.data or b"")
    if len(fds_names) == 1:
        _add(fds_names[0])
    else:
        shorts = {n.split(".")[-1] for n in out}
        for full in fds_names:
            if full.split(".")[-1] in shorts:
                _add(full)
    return out


def _headcam_video_channel(topic: str) -> Optional[str]:
    raw = (topic or "").strip()
    m = _HEADCAM_VIDEO_RE.match(raw)
    if m:
        return m.group(1).upper()
    m = _TELEOP_H264_RE.match(raw)
    if m:
        return _TELEOP_CAM_TO_CH.get(m.group(1).lower())
    return None


def headcam_frame_count_from_channel_counts(
    topic_counts: dict[str, int],
) -> Optional[int]:
    """用各 topic 消息数算出对齐后的头显帧数。

    有 SView 四路时取 min(RR,FR,RL,FL)；否则取已计数视频 topic 的最小值
    （头显 `/camera/{RR,FR,RL,FL}/video` 或遥操
    `/camera/{f_l,f_r,left_wrist,right_wrist}/h264`）。一个都没有则返回 None。
    """
    video: dict[str, int] = {}
    for topic, n in topic_counts.items():
        ch = _headcam_video_channel(topic)
        if ch is None:
            continue
        try:
            count = int(n)
        except (TypeError, ValueError):
            continue
        if count <= 0:
            continue
        video[ch] = count
    if not video:
        return None
    if _HEADCAM_CHANNEL_SET.issubset(video):
        return min(video[ch] for ch in _HEADCAM_CHANNELS)
    return min(video.values())


def read_headcam_video_frame_count(source: Union[str, Path]) -> Optional[int]:
    """从 mcap Summary 读对齐后的视频总帧数（文件尾寻址）。

    用 ``Statistics.channel_message_counts``，覆盖头显
    ``/camera/{RR,FR,RL,FL}/video`` 和遥操
    ``/camera/{f_l,f_r,left_wrist,right_wrist}/h264``。
    文件没有 summary、没有 statistics 或没有视频通道时返回 None。
    """
    path = Path(source)
    if not path.is_file():
        return None
    try:
        with open(path, "rb") as fh:
            summary = make_reader(fh).get_summary()
    except Exception:
        log.debug("headcam summary read failed for %s", path.name, exc_info=True)
        return None
    if summary is None or not summary.channels:
        return None
    stats = summary.statistics
    ch_counts = getattr(stats, "channel_message_counts", None) if stats else None
    if not ch_counts:
        return None
    topic_counts: dict[str, int] = {}
    for ch_id, channel in summary.channels.items():
        topic = getattr(channel, "topic", "") or ""
        n = ch_counts.get(ch_id)
        if n is None:
            continue
        topic_counts[topic] = int(n)
    return headcam_frame_count_from_channel_counts(topic_counts)


class TolerantProtobufDecoderFactory(DecoderFactory):
    """接受非标准 protobuf 编码和名字的 DecoderFactory。"""

    def decoder_for(self, message_encoding: str, schema: Optional[Schema]):
        if schema is None or not _looks_protobuf(message_encoding, schema):
            return None
        names = _name_candidates(schema)
        fds_names = _message_names_from_fds(schema.data or b"")
        fds_set = set(fds_names)
        chosen = schema.name
        for n in names:
            if n in fds_set:
                chosen = n
                break
        else:
            if len(fds_names) == 1:
                chosen = fds_names[0]
        resolved = schema
        if schema.encoding != "protobuf" or chosen != schema.name:
            resolved = replace(schema, encoding="protobuf", name=chosen or schema.name)
        try:
            return super().decoder_for("protobuf", resolved)
        except Exception as exc:
            log.debug(
                "protobuf decode skipped schema=%r encoding=%r/%r: %s",
                schema.name,
                message_encoding,
                schema.encoding,
                exc,
            )
            return None


def read_protobuf_messages_compat(
    source: Union[str, Path],
    start_time_ns: Optional[int] = None,
    end_time_ns: Optional[int] = None,
    topics: Optional[Iterable[str]] = None,
) -> Iterator[McapProtobufMessage]:
    """产出 protobuf 消息；其他编码跳过，不抛异常。"""
    path = str(source)
    factory = TolerantProtobufDecoderFactory()
    skipped_encodings: dict[str, int] = {}
    iter_kw = {}
    if start_time_ns is not None and int(start_time_ns) > 0:
        iter_kw["start_time"] = int(start_time_ns)
    if end_time_ns is not None and int(end_time_ns) > 0:
        iter_kw["end_time"] = int(end_time_ns)
    if topics is not None:
        iter_kw["topics"] = list(topics)
    with open(path, "rb") as fh:
        reader = make_reader(fh)
        try:
            iterator = reader.iter_messages(**iter_kw)
        except TypeError:
            iterator = reader.iter_messages()
        for schema, channel, message in iterator:
            encoding = channel.message_encoding if channel else ""
            decoder = factory.decoder_for(encoding, schema)
            if decoder is None:
                key = encoding or (schema.encoding if schema else "") or "?"
                skipped_encodings[key] = skipped_encodings.get(key, 0) + 1
                continue
            try:
                proto_msg = decoder(message.data)
            except Exception:
                log.debug(
                    "protobuf parse failed topic=%s schema=%s",
                    getattr(channel, "topic", ""),
                    getattr(schema, "name", ""),
                    exc_info=True,
                )
                continue
            yield McapProtobufMessage(
                proto_msg=proto_msg,
                message=message,
                channel=channel,
            )
    if skipped_encodings:
        log.info(
            "skipped non-protobuf mcap channels in %s: %s",
            Path(path).name,
            skipped_encodings,
        )
