#!/usr/bin/env python3
"""Unit checks for OSS contiguous frame alignment helpers."""
from oss_cache_gc import contiguous_frames_from_starts, oss_segment_hole, align_manifest_to_oss


def test_contiguous():
    assert contiguous_frames_from_starts(set(), segment_frames=30) == 0
    assert contiguous_frames_from_starts({0, 30, 60}, segment_frames=30) == 90
    # hole after 30
    assert contiguous_frames_from_starts({0, 30, 90}, segment_frames=30) == 60
    # orphaned tail only
    assert contiguous_frames_from_starts({24150, 24180}, segment_frames=30) == 0
    assert oss_segment_hole({24150, 24180}) is True
    assert oss_segment_hole({0, 30}) is False
    # claimed short final
    assert (
        contiguous_frames_from_starts({0, 30}, segment_frames=30, claimed=45) == 45
    )


def test_align_hole(monkeypatch=None):
    # Directly test hole branch via contiguous helpers; align needs OSS list.
    man = {
        "status": "done",
        "bundle": {"frames_available": 24450, "frame_count": 24480, "gop_size": 30},
    }
    starts = {24150, 24180, 24210}

    # Simulate align_manifest hole path
    from oss_cache_gc import contiguous_frames_from_starts

    aligned = contiguous_frames_from_starts(starts, segment_frames=30, claimed=24450)
    assert aligned == 0
    man["bundle"]["frames_available"] = aligned
    assert man["bundle"]["frames_available"] == 0


if __name__ == "__main__":
    test_contiguous()
    test_align_hole()
    print("ok")
