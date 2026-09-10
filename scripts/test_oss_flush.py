#!/usr/bin/env python3
"""OSS flush: advertise frames_available only after the segment PUT succeeds."""
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import sview_worker as sw


def _man(frames_available: int) -> dict:
    return {
        "job_id": "j1",
        "status": "partial",
        "bundle": {"frames_available": frames_available, "frame_count": 300},
    }


def _write_frames(frame_dir: Path, n: int) -> None:
    for i in range(n):
        (frame_dir / f"{i:06d}.bin").write_bytes(b"x")


def test_skip_manifest_when_frames_missing():
    uploaded: list[str] = []
    with tempfile.TemporaryDirectory() as td:
        with (
            patch.object(sw, "OSS_CACHE", True),
            patch.object(sw, "get_oss_bucket", return_value=object()),
            patch.object(sw, "upload_bytes_to_oss", side_effect=lambda key, *_a, **_k: uploaded.append(key)),
        ):
            sw._oss_cache_maybe_flush("j1", 30, 300, Path(td), _man(30))
    assert uploaded == []


def test_skip_manifest_when_segment_put_fails():
    uploaded: list[str] = []

    def boom(key, *_a, **_k):
        uploaded.append(key)
        raise RuntimeError("oss down")

    with tempfile.TemporaryDirectory() as td:
        frame_dir = Path(td)
        _write_frames(frame_dir, 30)
        with (
            patch.object(sw, "OSS_CACHE", True),
            patch.object(sw, "get_oss_bucket", return_value=object()),
            patch.object(sw, "upload_bytes_to_oss", side_effect=boom),
        ):
            sw._oss_cache_maybe_flush("j1", 30, 300, frame_dir, _man(30))
    assert uploaded == [sw.oss_cache_key("j1", "segments", "000000.bin")]


def test_manifest_after_successful_segment():
    uploaded: list[str] = []
    with tempfile.TemporaryDirectory() as td:
        frame_dir = Path(td)
        _write_frames(frame_dir, 30)
        with (
            patch.object(sw, "OSS_CACHE", True),
            patch.object(sw, "get_oss_bucket", return_value=object()),
            patch.object(sw, "upload_bytes_to_oss", side_effect=lambda key, *_a, **_k: uploaded.append(key)),
        ):
            sw._oss_cache_maybe_flush("j1", 30, 300, frame_dir, _man(30))
    assert uploaded == [
        sw.oss_cache_key("j1", "segments", "000000.bin"),
        sw.oss_cache_key("j1", "manifest.json"),
    ]


def test_short_final_segment_and_done_publish():
    uploaded: list[str] = []
    payloads: list[bytes] = []

    def rec(key, data, **_k):
        uploaded.append(key)
        payloads.append(data)

    with tempfile.TemporaryDirectory() as td:
        frame_dir = Path(td)
        _write_frames(frame_dir, 23)
        man = {
            "job_id": "j1",
            "status": "done",
            "bundle": {"frames_available": 23, "frame_count": 23},
        }
        with (
            patch.object(sw, "OSS_CACHE", True),
            patch.object(sw, "get_oss_bucket", return_value=object()),
            patch.object(sw, "upload_bytes_to_oss", side_effect=rec),
        ):
            sw._oss_cache_publish_done("j1", 23, frame_dir, man)
    assert uploaded == [
        sw.oss_cache_key("j1", "segments", "000000.bin"),
        sw.oss_cache_key("j1", "manifest.json"),
    ]
    body = json.loads(payloads[-1].decode("utf-8"))
    assert body["status"] == "done"
    assert body["bundle"]["frames_available"] == 23
    assert body["bundle"]["frame_count"] == 23


if __name__ == "__main__":
    test_skip_manifest_when_frames_missing()
    test_skip_manifest_when_segment_put_fails()
    test_manifest_after_successful_segment()
    test_short_final_segment_and_done_publish()
    print("ok")
