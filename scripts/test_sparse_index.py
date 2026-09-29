#!/usr/bin/env python3
"""Sparse MCAP frame-index alignment helpers."""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mcap.writer import IndexType, Writer

from sview.ingest.frame_index import (
    SparseIndexUnavailable,
    _annexb_has_random_access,
    _aligned_payload,
    build_mcap_frame_index,
    find_sparse_decode_start,
    sparse_anchor_map,
    sparse_origin_seconds,
)


def test_ordinal_alignment_and_anchors():
    index = _aligned_payload(
        {
            "front": [100, 200, 300],
            "wrist": [110, 210],
        },
        Path("sample.mcap"),
        "orin_h264",
    )
    assert index["frame_count"] == 2
    assert index["timestamps_ns"] == [100, 200]
    assert index["channels"] == {
        "front": [100, 200],
        "wrist": [110, 210],
    }
    assert index["index_source"] == "mcap_message_index"
    assert sparse_anchor_map(index, 1) == {"front": 200, "wrist": 210}
    assert sparse_anchor_map(index, 99) == {"front": 200, "wrist": 210}
    assert sparse_origin_seconds(index, 1, 50) == 150 / 1e9


def test_shared_keyframe_intersection_and_nal_detection():
    assert _annexb_has_random_access(b"\x00\x00\x00\x01\x65\x88")
    assert _annexb_has_random_access(b"\x00\x00\x00\x01\x26\x01")
    assert not _annexb_has_random_access(b"\x00\x00\x00\x01\x41\x9a")


def _write_headcam(path: Path, *, indexes: bool):
    with path.open("wb") as fh:
        writer = Writer(
            fh,
            chunk_size=64,
            index_types=IndexType.ALL if indexes else IndexType.NONE,
        )
        writer.start()
        schema = writer.register_schema("Video", "protobuf", b"\x0a\x00")
        channel_ids = [
            writer.register_channel(
                f"/camera/{ch}/video", "protobuf", schema
            )
            for ch in ("RR", "FR", "RL", "FL")
        ]
        for frame in range(5):
            for offset, channel_id in enumerate(channel_ids):
                timestamp = 1_000_000_000 + frame * 33_333_333 + offset
                writer.add_message(
                    channel_id,
                    log_time=timestamp,
                    publish_time=timestamp,
                    data=b"payload",
                )
        writer.finish()


def test_native_message_index_is_exact():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "head_cam"
        root.mkdir()
        path = root / "sample.mcap"
        _write_headcam(path, indexes=True)
        index = build_mcap_frame_index(path)
    assert index["frame_count"] == 5
    assert index["segments"] == [[0, 5]]
    assert index["index_source"] == "mcap_message_index"
    assert index["channels"]["RR"][3] == 1_099_999_999
    assert sparse_anchor_map(index, 3)["FL"] == 1_100_000_002


def test_missing_native_index_fails_fast():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "head_cam"
        root.mkdir()
        path = root / "sample.mcap"
        _write_headcam(path, indexes=False)
        try:
            build_mcap_frame_index(path)
        except SparseIndexUnavailable as exc:
            assert "ChunkIndex" in str(exc) or "MessageIndex" in str(exc)
        else:
            raise AssertionError("unindexed MCAP must not trigger a full scan")


def test_segment_table_covers_every_frame_without_payload_reads():
    from sview.ingest.frame_index import build_segment_table, segment_for_frame

    table = build_segment_table(95, 30)
    assert table == [[0, 30], [30, 60], [60, 90], [90, 95]]
    covered = [frame for start, end in table for frame in range(start, end)]
    assert covered == list(range(95))
    index = {"frame_count": 95, "segment_frames": 30, "segments": table}
    assert segment_for_frame(index, 0) == (0, 30)
    assert segment_for_frame(index, 94) == (90, 95)
    assert find_sparse_decode_start(
        "head_cam/missing.mcap",
        index,
        target_frame=64,
        segment_frames=30,
    ) == 60


def test_hdf5_explicitly_unsupported():
    try:
        build_mcap_frame_index("sample.hdf5")
    except ValueError as exc:
        assert "HDF5" in str(exc)
    else:
        raise AssertionError("HDF5 sparse indexing must be rejected")


if __name__ == "__main__":
    test_ordinal_alignment_and_anchors()
    test_shared_keyframe_intersection_and_nal_detection()
    test_native_message_index_is_exact()
    test_missing_native_index_fails_fast()
    test_segment_table_covers_every_frame_without_payload_reads()
    test_hdf5_explicitly_unsupported()
    print("ok")
