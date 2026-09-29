#!/usr/bin/env python3
"""bundle_frame v1 quad vs v2 named streams."""
import json
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sview.ingest.stream_ids import bundle_stream_key, stream_file_slug
from sview.jobs.bundle_frame import (
    BUNDLE_FRAME_V1,
    BUNDLE_FRAME_V2,
    FORMAT_HEAD_CAM,
    FORMAT_TELEOP_NSTREAM,
    build_streams_manifest,
    manifest_format_for,
    pack_bundle_frame,
)
from sview.worker.cpu import _bundle_all_streams_keyframe, _rewrite_bundle_global_index


def _split(blob: bytes):
    hlen = struct.unpack_from("<I", blob, 0)[0]
    header = json.loads(blob[4 : 4 + hlen].decode("utf-8"))
    plen = struct.unpack_from("<I", blob, 4 + hlen)[0]
    payload = blob[8 + hlen : 8 + hlen + plen]
    return header, payload


def test_v1_quad_order():
    blob = pack_bundle_frame(
        0,
        {"RR": b"RR", "FR": b"FRRR", "RL": b"", "FL": b"FL"},
        1000,
        [1000, 2000],
        True,
        {"samples": []},
        {"records": []},
    )
    header, payload = _split(blob)
    assert header["packet_schema"] == BUNDLE_FRAME_V1
    assert list(header["streams"]) == ["rr", "fr", "rl", "fl"]
    assert header["streams"]["rr"]["payload_length"] == 2
    assert header["streams"]["rl"]["is_null"] is True
    assert "codec" not in header["streams"]["fr"]
    rr = payload[0:2]
    fr = payload[2:6]
    fl = payload[6:8]
    assert rr == b"RR" and fr == b"FRRR" and fl == b"FL"


def test_v2_named_topics():
    blob = pack_bundle_frame(
        3,
        {
            "frontvideostream": b"HEAD",
            "wrist/image": b"WRIST",
        },
        50,
        [50, 80],
        False,
        {"samples": [1]},
        {"records": []},
        stream_codecs={"frontvideostream": "h264", "wrist/image": "h264"},
    )
    header, payload = _split(blob)
    assert header["packet_schema"] == BUNDLE_FRAME_V2
    assert list(header["streams"]) == ["frontvideostream", "wrist/image"]
    assert header["streams"]["frontvideostream"]["codec"] == "h264"
    assert payload[:4] == b"HEAD"
    assert payload[4:9] == b"WRIST"


def test_manifest_and_slugs():
    assert bundle_stream_key("/frontvideostream") == "frontvideostream"
    assert stream_file_slug("wrist/image") == "wrist_image"
    assert manifest_format_for(["RR", "FL"]) == FORMAT_HEAD_CAM
    assert manifest_format_for(["frontvideostream"]) == FORMAT_TELEOP_NSTREAM

    v1 = build_streams_manifest(
        {"RR": {"fps": 30, "position": "RR"}, "FR": {"fps": 30, "position": "FR"},
         "RL": {"fps": 30, "position": "RL"}, "FL": {"fps": 30, "position": "FL"}},
        640,
        480,
    )
    assert set(v1) == {"camera_rr", "camera_fr", "camera_rl", "camera_fl"}
    assert "source_codec" not in v1["camera_fr"]

    v2 = build_streams_manifest(
        {
            "wrist/image": {
                "fps": 15,
                "width": 640,
                "height": 480,
                "source_topic": "/wrist/image",
                "source_codec": "jpeg",
                "stored_codec": "h264",
            },
            "frontvideostream": {
                "fps": 30,
                "width": 1280,
                "height": 720,
                "source_topic": "/frontvideostream",
                "codec": "h264",
            },
        },
        1280,
        720,
    )
    assert list(v2) == ["frontvideostream", "wrist/image"]
    assert v2["frontvideostream"]["source_topic"] == "/frontvideostream"
    assert v2["wrist/image"]["source_codec"] == "jpeg"
    assert v2["wrist/image"]["stored_codec"] == "h264"


def test_sparse_global_index_and_idr_validation():
    idr = b"\x00\x00\x00\x01\x65\x88"
    pframe = b"\x00\x00\x00\x01\x41\x9a"
    blob = pack_bundle_frame(
        0,
        {"frontvideostream": idr, "wrist/image": idr},
        50,
        [50, 80],
        True,
        {"samples": []},
        {"records": []},
        stream_codecs={"frontvideostream": "h264", "wrist/image": "h264"},
    )
    _original_header, original_payload = _split(blob)
    rewritten = _rewrite_bundle_global_index(blob, 900)
    header, payload = _split(rewritten)
    assert header["frame_index"] == 900
    assert payload == original_payload
    assert _bundle_all_streams_keyframe(rewritten)

    non_idr = pack_bundle_frame(
        0,
        {"frontvideostream": idr, "wrist/image": pframe},
        50,
        [50, 80],
        True,
        {"samples": []},
        {"records": []},
        stream_codecs={"frontvideostream": "h264", "wrist/image": "h264"},
    )
    assert not _bundle_all_streams_keyframe(non_idr)


if __name__ == "__main__":
    test_v1_quad_order()
    test_v2_named_topics()
    test_manifest_and_slugs()
    test_sparse_global_index_and_idr_validation()
    print("ok")
