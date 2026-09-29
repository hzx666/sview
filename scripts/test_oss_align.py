#!/usr/bin/env python3
"""Unit checks for OSS contiguous frame alignment helpers."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sview.cache.oss_cache_gc import (
    align_manifest_to_oss,
    available_ranges_from_starts,
    contiguous_prefix_from_ranges,
    contiguous_frames_from_starts,
    frame_range_ready,
    filter_playable_segment_starts,
    merge_available_ranges,
    oss_segment_hole,
)


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

    aligned = contiguous_frames_from_starts(starts, segment_frames=30, claimed=24450)
    assert aligned == 0
    man["bundle"]["frames_available"] = aligned
    assert man["bundle"]["frames_available"] == 0


def test_align_list_failure_does_not_pass_through():
    man = {
        "status": "partial",
        "bundle": {"frames_available": 330, "frame_count": 1579, "gop_size": 30},
    }

    def _none(_job_id):
        return None

    import sview.cache.oss_cache_gc as gc

    orig = gc.list_oss_segment_starts
    gc.list_oss_segment_starts = _none
    try:
        out = align_manifest_to_oss("j1", man, segment_frames=30)
    finally:
        gc.list_oss_segment_starts = orig
    assert out["cache_align_failed"] is True
    assert out["cache_align_reason"] == "oss_list_failed"
    assert out["bundle"]["frames_available"] == 330


def test_filter_signed_starts_drops_tail_and_frames_index():
    kept, skipped = filter_playable_segment_starts(
        [0, 15, 300, 330, 360],
        frames_available=330,
        segment_frames=30,
    )
    assert kept == [0, 300]
    reasons = {s["start"]: s["reason"] for s in skipped}
    assert reasons[15] == "unaligned"
    assert reasons[330] == "not_ready"
    assert reasons[360] == "not_ready"


def test_sparse_ranges_and_signed_filter():
    ranges = available_ranges_from_starts(
        {0, 30, 900, 930},
        segment_frames=30,
        frame_count=1000,
    )
    assert ranges == [[0, 60], [900, 960]]
    assert merge_available_ranges([[30, 60], [0, 30], [930, 960], [900, 930]]) == ranges
    assert contiguous_prefix_from_ranges(ranges) == 60
    assert frame_range_ready(ranges, 915, 916)
    assert not frame_range_ready(ranges, 100, 101)
    kept, skipped = filter_playable_segment_starts(
        [0, 60, 900, 930],
        frames_available=60,
        available_ranges=ranges,
        segment_frames=30,
    )
    assert kept == [0, 900, 930]
    assert skipped == [{"start": 60, "reason": "not_ready"}]


def test_sparse_align_preserves_holes():
    import sview.cache.oss_cache_gc as gc

    original = gc.list_oss_segment_starts
    gc.list_oss_segment_starts = lambda _job_id: {0, 900}
    try:
        out = align_manifest_to_oss(
            "sparse",
            {
                "publish_mode": "sparse",
                "status": "partial",
                "bundle": {
                    "frame_count": 1000,
                    "frames_available": 0,
                    "gop_size": 30,
                    "available_ranges": [],
                },
            },
            segment_frames=30,
        )
    finally:
        gc.list_oss_segment_starts = original
    assert out["bundle"]["available_ranges"] == [[0, 30], [900, 930]]
    assert out["bundle"]["frames_available"] == 30
    assert out["status"] == "partial"


if __name__ == "__main__":
    test_contiguous()
    test_align_hole()
    test_align_list_failure_does_not_pass_through()
    test_filter_signed_starts_drops_tail_and_frames_index()
    test_sparse_ranges_and_signed_filter()
    test_sparse_align_preserves_holes()
    print("ok")
