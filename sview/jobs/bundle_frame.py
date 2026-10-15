"""长度前缀的 bundle_frame 打包（v1 四路相机，v2 为 N 路命名流）。"""

from __future__ import annotations

import json
import struct

BUNDLE_FRAME_V1 = "playertrans.bundle_frame.v1"
BUNDLE_FRAME_V2 = "playertrans.bundle_frame.v2"
FORMAT_HEAD_CAM = "head_cam"
FORMAT_TELEOP_NSTREAM = "teleop_nstream"
HEADCAM_CHANNELS = ("RR", "FR", "RL", "FL")
HEADCAM_CHANNEL_SET = frozenset(HEADCAM_CHANNELS)


def classic_quad_keys(keys) -> bool:
    """流 id 是头显 RR/FR/RL/FL（或其子集）时为 True。"""
    up = {str(k).upper() for k in (keys or []) if k}
    return bool(up) and up <= HEADCAM_CHANNEL_SET


def manifest_format_for(keys) -> str:
    return FORMAT_HEAD_CAM if classic_quad_keys(keys) else FORMAT_TELEOP_NSTREAM


def pack_bundle_frame(
    frame_idx: int,
    streams_data: dict,
    timestamp_ns: int,
    time_window_ns: list,
    is_keyframe: bool,
    imu_json: dict,
    events_json: dict,
    stream_codecs: dict | None = None,
) -> bytes:
    """header_len | header JSON | payload_len | payload（streams…|IMU|Events）。

    v1（头显）：streams 的 key 为 ``rr/fr/rl/fl``，payload 顺序 RR|FR|RL|FL。
    v2（遥操）：按原 topic 路径做 key，N 路，每路带 codec。
    """
    codecs = stream_codecs or {}
    use_v1 = classic_quad_keys(streams_data.keys())
    offset = 0
    streams_hdr: dict = {}
    combined = bytearray()

    if use_v1:
        order = list(HEADCAM_CHANNELS)
    else:
        order = [k for k in streams_data.keys()]

    for ch in order:
        raw = streams_data.get(ch, b"") or b""
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raw = bytes(raw)
        else:
            raw = bytes(raw)
        entry = {
            "payload_offset": offset,
            "payload_length": len(raw),
            "is_null": not raw,
        }
        if not use_v1:
            codec = str(codecs.get(ch) or "h264").lower()
            entry["codec"] = codec
        key = ch.lower() if use_v1 else str(ch)
        streams_hdr[key] = entry
        combined.extend(raw)
        offset += len(raw)

    imu_bytes = json.dumps(imu_json, ensure_ascii=False).encode("utf-8")
    imu_hdr = {"payload_offset": offset, "payload_length": len(imu_bytes)}
    combined.extend(imu_bytes)
    offset += len(imu_bytes)

    evt_bytes = json.dumps(events_json, ensure_ascii=False).encode("utf-8")
    evt_hdr = {"payload_offset": offset, "payload_length": len(evt_bytes)}
    combined.extend(evt_bytes)

    hdr = {
        "packet_schema": BUNDLE_FRAME_V1 if use_v1 else BUNDLE_FRAME_V2,
        "frame_index": int(frame_idx),
        "timestamp_ns": int(timestamp_ns),
        "time_window_ns": list(time_window_ns),
        "key_frame": bool(is_keyframe),
        "streams": streams_hdr,
        "imu": imu_hdr,
        "events": evt_hdr,
    }
    hdr_json = json.dumps(hdr, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    payload = bytes(combined)
    return struct.pack("<I", len(hdr_json)) + hdr_json + struct.pack("<I", len(payload)) + payload


def build_streams_manifest(metas: dict, fallback_w: int, fallback_h: int) -> dict:
    """manifest 的 ``streams``。四路相机仍用 camera_rr/…；遥操用 topic key。"""
    metas = metas or {}
    if classic_quad_keys(metas.keys()):
        result = {}
        for ch in HEADCAM_CHANNELS:
            key = f"camera_{ch.lower()}"
            m = metas.get(ch) or metas.get(ch.lower()) or {}
            result[key] = {
                "position": m.get("position", ch) if m else ch,
                "width": fallback_w,
                "height": fallback_h,
                "fps": m.get("fps", 30) if m else 30,
            }
        return result

    result = {}
    for key in sorted(metas, key=lambda k: str(k).lower()):
        m = metas.get(key) or {}
        src = m.get("source_topic") or m.get("position") or key
        result[str(key)] = {
            "position": src,
            "source_topic": src,
            "source_codec": str(m.get("source_codec") or m.get("codec") or "h264"),
            "stored_codec": str(m.get("stored_codec") or "h264"),
            "width": int(m.get("width") or fallback_w or 0) or fallback_w,
            "height": int(m.get("height") or fallback_h or 0) or fallback_h,
            "fps": m.get("fps") or 30,
        }
    return result
