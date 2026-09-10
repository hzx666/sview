#!/usr/bin/env python3
"""Stable progressive frame_count helpers."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mcap_protobuf_compat import headcam_frame_count_from_channel_counts
from sview_http import _optional_frame_count
from sview_worker import _progressive_frame_count


def test_headcam_frame_count_from_channel_counts():
    assert headcam_frame_count_from_channel_counts({
        "/camera/RR/video": 919,
        "/camera/FR/video": 919,
        "/camera/RL/video": 900,
        "/camera/FL/video": 919,
        "/imu/sample": 40000,
    }) == 900
    assert headcam_frame_count_from_channel_counts({
        "/camera/f_l/h264": 200,
        "/camera/f_r/h264": 180,
        "/camera/left_wrist/h264": 200,
        "/camera/right_wrist/h264": 200,
    }) == 180
    assert headcam_frame_count_from_channel_counts({
        "/camera/RR/video": 10,
        "/camera/FR/video": 12,
    }) == 10
    assert headcam_frame_count_from_channel_counts({}) is None


def test_progressive_frame_count_keeps_summary_until_done():
    assert _progressive_frame_count(1000, 30, final=False) == 1000
    assert _progressive_frame_count(1000, 1000, final=False) == 1000
    assert _progressive_frame_count(1000, 1005, final=False) == 1005
    assert _progressive_frame_count(1000, 950, final=True) == 950
    assert _progressive_frame_count(None, 30, final=False) is None
    assert _progressive_frame_count(0, 30, final=False) is None
    assert _progressive_frame_count(None, 30, final=True) == 30
    assert _progressive_frame_count(None, 0, final=True) is None


def test_optional_frame_count_treats_zero_as_unknown():
    assert _optional_frame_count(0, "", None) is None
    assert _optional_frame_count(0, 919) == 919
    assert _optional_frame_count("", "0", "42") == 42
    assert _optional_frame_count(None) is None


if __name__ == "__main__":
    test_headcam_frame_count_from_channel_counts()
    test_progressive_frame_count_keeps_summary_until_done()
    test_optional_frame_count_treats_zero_as_unknown()
    print("ok")
