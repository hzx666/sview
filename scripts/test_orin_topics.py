#!/usr/bin/env python3
"""Agilex / Orin camera topic matching."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orin_images import (
    _looks_like_orin_topic,
    _parse_camera_map,
    _topic_to_channel,
    orin_frame_count_from_channel_counts,
)


def test_agilex_image_raw_topics():
    cam_map = _parse_camera_map()
    cases = {
        "/camera_f_l/color/image_raw": "FL",
        "/camera_f_r/color/image_raw": "FR",
        "/camera_l/color/image_raw": "RL",
        "/camera_r/color/image_raw": "RR",
        "/left_camera/foo/color/image_raw/compressed": "FL",
        "/camera/f_l/h264": "FL",
        "/streamer/realsense_head/h264": "RR",
    }
    for topic, ch in cases.items():
        assert _looks_like_orin_topic(topic), topic
        assert _topic_to_channel(topic, cam_map) == ch, (topic, ch)
    assert not _looks_like_orin_topic("/camera_l/color/camera_info")
    assert not _looks_like_orin_topic("/master/joint_left")


def test_orin_frame_count_from_channel_counts():
    h264_counts = {
        "/streamer/realsense_left/h264": 1000,
        "/streamer/realsense_right/h264": 998,
        "/streamer/realsense_head/h264": 1000,
        "/streamer/realsense_torso/h264": 1001,
        "/joint_states": 5000,
    }
    assert orin_frame_count_from_channel_counts(h264_counts, "h264") == 998
    assert orin_frame_count_from_channel_counts(h264_counts, "jpeg") is None

    jpeg_counts = {
        "/left_camera/foo/color/image_raw/compressed": 400,
        "/right_camera/foo/color/image_raw/compressed": 390,
        "/head_camera/foo/color/image_raw/compressed": 400,
        "/torso_camera/foo/color/image_raw/compressed": 400,
    }
    assert orin_frame_count_from_channel_counts(jpeg_counts, "jpeg") == 390
    assert orin_frame_count_from_channel_counts(jpeg_counts, "h264") is None

    agilex = {
        "/camera_f_l/color/image_raw": 210,
        "/camera_f_r/color/image_raw": 200,
        "/camera_l/color/image_raw": 210,
        "/camera_r/color/image_raw": 210,
    }
    assert orin_frame_count_from_channel_counts(agilex, "jpeg") == 200

    teleop = {
        "/camera/f_l/h264": 200,
        "/camera/f_r/h264": 200,
        "/camera/left_wrist/h264": 180,
        "/camera/right_wrist/h264": 200,
    }
    assert orin_frame_count_from_channel_counts(teleop, "h264") == 180

    # Head-cam protobuf topics must not be counted as Orin.
    headcam = {
        "/camera/RR/video": 919,
        "/camera/FR/video": 919,
        "/camera/RL/video": 900,
        "/camera/FL/video": 919,
    }
    assert orin_frame_count_from_channel_counts(headcam, "h264") is None
    assert orin_frame_count_from_channel_counts(headcam, "jpeg") is None

    two = {
        "/streamer/realsense_left/h264": 50,
        "/streamer/realsense_right/h264": 40,
    }
    assert orin_frame_count_from_channel_counts(two, "h264") == 40
    assert orin_frame_count_from_channel_counts({}, "h264") is None


if __name__ == "__main__":
    test_agilex_image_raw_topics()
    test_orin_frame_count_from_channel_counts()
    print("ok")
