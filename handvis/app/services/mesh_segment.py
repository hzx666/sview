"""HandVis mesh 段打包：与 SView 相同的 length-prefixed bin 信封。

信封（与 playertrans.bundle_segment.v1 一致）：
    header_len(uint32 LE) + header_json(UTF-8) + payload_len(uint32 LE) + payload

payload 按 streams 偏移拼接 float32 数组：left | right | hand_roots。
一段对应 30 帧（最后一段可不足），文件名 segments/{start_index:06d}.bin。
"""

from __future__ import annotations

import json
import struct
from typing import Any

PACKET_SCHEMA = "handvis.mesh_segment.v1"


def pack_mesh_segment(
    *,
    start_index: int,
    count: int,
    segment_frames: int,
    vertex_count: int,
    stride: int,
    fps: float,
    start_time: float,
    end_time: float,
    left: bytes,
    right: bytes,
    hand_roots: bytes,
) -> bytes:
    """把左右手 mesh + 腕部根节点打成一段 bin。"""
    streams: dict[str, dict[str, int | str]] = {}
    parts: list[bytes] = []
    offset = 0
    for name, blob in (("left", left), ("right", right), ("hand_roots", hand_roots)):
        streams[name] = {
            "payload_offset": offset,
            "payload_length": len(blob),
            "dtype": "f32",
        }
        parts.append(blob)
        offset += len(blob)

    header: dict[str, Any] = {
        "packet_schema": PACKET_SCHEMA,
        "start_index": int(start_index),
        "count": int(count),
        "segment_frames": int(segment_frames),
        "frame_start": int(start_index),
        "frame_count": int(count),
        "vertex_count": int(vertex_count),
        "stride": int(stride),
        "fps": float(fps),
        "start_time": float(start_time),
        "end_time": float(end_time),
        "streams": streams,
    }
    header_bytes = json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload = b"".join(parts)
    return (
        struct.pack("<I", len(header_bytes))
        + header_bytes
        + struct.pack("<I", len(payload))
        + payload
    )


def segment_bin_name(start_index: int) -> str:
    return f"{int(start_index):06d}.bin"


def segment_relpath(start_index: int) -> str:
    return f"segments/{segment_bin_name(start_index)}"
