"""按对齐帧精确记录时间戳，供稀疏 mcap 分段编码。"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from mcap.opcode import Opcode
from mcap.reader import make_reader
from mcap.records import MessageIndex
from mcap.stream_reader import ReadDataStream

from sview.ingest.orin_images import (
    _stable_assign_topics,
    _video_topics_from_summary,
    orin_stream_kind,
    sview_channel_for_topic,
)
from sview.path_map import ingest_family

INDEX_VERSION = 3


class SparseIndexUnavailable(ValueError):
    """mcap 缺少有界稀疏 seek 需要的原生索引。"""


def _atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


def _annexb_has_random_access(data: bytes) -> bool:
    """识别 H.264 IDR 以及 HEVC 的 IDR/CRA AU。"""
    raw = bytes(data or b"")
    starts: list[tuple[int, int]] = []
    i = 0
    while i + 3 < len(raw):
        if raw[i : i + 4] == b"\x00\x00\x00\x01":
            starts.append((i + 4, 4))
            i += 4
        elif raw[i : i + 3] == b"\x00\x00\x01":
            starts.append((i + 3, 3))
            i += 3
        else:
            i += 1
    for pos, _prefix in starts:
        if pos >= len(raw):
            continue
        h264_type = raw[pos] & 0x1F
        hevc_type = (raw[pos] >> 1) & 0x3F
        if h264_type == 5 or hevc_type in (19, 20, 21):
            return True
    return False


def _aligned_payload(
    channels: dict[str, list[int]],
    source: Path,
    kind: str,
) -> dict:
    nonempty = {str(k): [int(x) for x in v] for k, v in channels.items() if v}
    if not nonempty:
        raise ValueError(f"no indexed video streams in {source}")
    frame_count = min(len(v) for v in nonempty.values())
    trimmed = {k: v[:frame_count] for k, v in sorted(nonempty.items())}
    # 每路取最早时间戳，process_mcap 才能看到各流的第 N 帧；
    # 各流锚点在 ingest 循环里会再套一次。
    anchors = [min(v[i] for v in trimmed.values()) for i in range(frame_count)]
    seg_n = 30
    return {
        "version": INDEX_VERSION,
        "source": str(source),
        "kind": kind,
        "frame_count": frame_count,
        "segment_frames": seg_n,
        "segments": build_segment_table(frame_count, seg_n),
        "timestamps_ns": anchors,
        "channels": trimmed,
        "index_source": "mcap_message_index",
    }


def build_mcap_frame_index(source: str | Path) -> dict:
    """只从 mcap MessageIndex 记录建精确的序号时间戳。

    只读文件尾和紧凑的逐 chunk 索引，不遍历、不解压视频 payload。
    分段是这些已索引帧的固定切分，每一帧只属于一段。
    """
    path = Path(source)
    if path.suffix.lower() in (".hdf5", ".h5", ".hdf"):
        raise ValueError("sparse seek is not supported for HDF5")
    family = ingest_family(str(path))
    channels: dict[str, list[int]] = {}
    with path.open("rb") as fh:
        summary = make_reader(fh).get_summary()
        if summary is None or not summary.chunk_indexes:
            raise SparseIndexUnavailable(
                "MCAP has no ChunkIndex; use progressive mode or reindex the source"
            )

        if family == "head_cam":
            kind = "head_cam"
            topic_map = {}
            for channel in summary.channels.values():
                topic = (channel.topic or "").strip()
                if "/video" not in topic and "/h264" not in topic.lower():
                    continue
                stream = sview_channel_for_topic(topic)
                if stream:
                    topic_map[topic] = stream
        else:
            source_kind = orin_stream_kind(
                path, skip_headcam_guard=(family == "teleop")
            )
            if source_kind not in ("h264", "jpeg"):
                raise ValueError(f"no supported sparse MCAP video streams: {path}")
            kind = f"orin_{source_kind}"
            topic_map = _stable_assign_topics(
                _video_topics_from_summary(path, kind=source_kind)
            )

        channel_map = {
            int(channel_id): topic_map[channel.topic]
            for channel_id, channel in summary.channels.items()
            if channel.topic in topic_map
        }
        if not channel_map:
            raise ValueError(f"no indexed video streams in {path}")

        indexed_records = 0
        for chunk in sorted(
            summary.chunk_indexes, key=lambda value: value.chunk_start_offset
        ):
            for channel_id, stream in channel_map.items():
                offset = chunk.message_index_offsets.get(channel_id)
                if offset is None:
                    continue
                fh.seek(int(offset))
                data_stream = ReadDataStream(fh)
                opcode = data_stream.read1()
                length = data_stream.read8()
                if opcode != int(Opcode.MESSAGE_INDEX):
                    raise SparseIndexUnavailable(
                        f"invalid MessageIndex record at offset {offset}"
                    )
                before = data_stream.count
                record = MessageIndex.read(data_stream)
                if data_stream.count - before > length:
                    raise SparseIndexUnavailable("invalid MCAP MessageIndex length")
                if int(record.channel_id) != channel_id:
                    raise SparseIndexUnavailable("MCAP MessageIndex channel mismatch")
                values = channels.setdefault(stream, [])
                values.extend(int(timestamp) for timestamp, _offset in record.records)
                indexed_records += len(record.records)

        if indexed_records <= 0:
            raise SparseIndexUnavailable(
                "MCAP has no per-channel MessageIndex; use progressive mode "
                "or reindex the source"
            )

        for values in channels.values():
            values.sort()
        stats = summary.statistics
        counts = getattr(stats, "channel_message_counts", {}) if stats else {}
        expected_by_stream: dict[str, int] = {}
        for channel_id, stream in channel_map.items():
            expected_by_stream[stream] = expected_by_stream.get(stream, 0) + int(
                counts.get(channel_id, 0)
            )
        for stream, expected in expected_by_stream.items():
            if expected > 0 and len(channels.get(stream, [])) != expected:
                raise SparseIndexUnavailable(
                    f"incomplete MessageIndex for {stream}: "
                    f"{len(channels.get(stream, []))}/{expected}"
                )

    result = _aligned_payload(channels, path, kind)
    stream_topics: dict[str, list[str]] = {}
    for topic, stream in topic_map.items():
        stream_topics.setdefault(stream, []).append(topic)
    result["stream_topics"] = stream_topics
    return result


def build_segment_table(frame_count: int, segment_frames: int = 30) -> list[list[int]]:
    """把每一帧划进半开区间分段。不读 mcap payload。"""
    total = max(0, int(frame_count or 0))
    seg_n = max(1, int(segment_frames or 30))
    return [[start, min(total, start + seg_n)] for start in range(0, total, seg_n)]


def segment_for_frame(
    index: dict,
    frame: int,
    segment_frames: int | None = None,
) -> tuple[int, int]:
    """返回包含这一全局帧的那一段。"""
    total = int(index.get("frame_count") or 0)
    if total <= 0:
        raise ValueError("empty sparse frame index")
    seg_n = max(1, int(segment_frames or index.get("segment_frames") or 30))
    target = max(0, min(int(frame), total - 1))
    start = (target // seg_n) * seg_n
    return start, min(total, start + seg_n)


def find_sparse_decode_start(
    source: str | Path,
    index: dict,
    *,
    target_frame: int,
    segment_frames: int,
    max_lookback_frames: int = 1800,
) -> int:
    """从索引表取分段起点。

    表来自 MessageIndex 的帧数。这里不打开 mcap，也不看视频 payload。
    """
    del source, max_lookback_frames
    start, _end = segment_for_frame(index, target_frame, segment_frames)
    return start


def load_or_build_frame_index(source: str | Path, index_path: str | Path) -> dict:
    path = Path(index_path)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if (
                int(data.get("version") or 0) == INDEX_VERSION
                and Path(str(data.get("source") or "")) == Path(source)
                and int(data.get("frame_count") or 0) > 0
            ):
                return data
        except Exception:
            pass
    data = build_mcap_frame_index(source)
    _atomic_json(path, data)
    return data


def sparse_anchor_map(index: dict, frame_index: int) -> dict[str, int]:
    """某一对齐全局帧在各通道上的精确时间戳。"""
    frame = max(0, min(int(frame_index), int(index["frame_count"]) - 1))
    return {
        str(ch): int(values[frame])
        for ch, values in (index.get("channels") or {}).items()
        if len(values) > frame
    }


def sparse_origin_seconds(index: dict, frame_index: int, bag_start_ns: int) -> float:
    frame = max(0, min(int(frame_index), int(index["frame_count"]) - 1))
    ts = int((index.get("timestamps_ns") or [])[frame])
    return max(0.0, (ts - int(bag_start_ns)) / 1e9)
