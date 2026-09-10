#!/usr/bin/env python3
"""Unit checks for OSS contiguous frame alignment helpers."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from oss_cache_gc import (
    align_manifest_to_oss,
    contiguous_frames_from_starts,
    filter_playable_segment_starts,
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

    import oss_cache_gc as gc

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


if __name__ == "__main__":
    test_contiguous()
    test_align_hole()
    test_align_list_failure_does_not_pass_through()
    test_filter_signed_starts_drops_tail_and_frames_index()
    print("ok")
