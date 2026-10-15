#!/usr/bin/env python3
"""Agilex / Orin 相机 topic 匹配。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sview.ingest.orin_images import (
    _harvest_channel_ok,
    _looks_like_orin_topic,
    _parse_camera_map,
    _ros_image_to_jpeg,
    _schema_is_raw_image,
    _stable_assign_topics,
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

    # 头显 protobuf topic 不能算成 Orin。
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


def test_unknown_teleop_topics_keep_names():
    mapping = _stable_assign_topics(["/frontvideostream", "/wrist/image"])
    assert mapping["/frontvideostream"] == "frontvideostream"
    assert mapping["/wrist/image"] == "wrist/image"
    five = _stable_assign_topics([f"/cam_{i}/image" for i in range(5)])
    assert len(five) == 5
    assert set(five.values()) == {f"cam_{i}/image" for i in range(5)}

    assert _schema_is_raw_image("sensor_msgs/msg/Image")
    assert _schema_is_raw_image("sensor_msgs/Image")
    assert not _schema_is_raw_image("sensor_msgs/msg/CompressedImage")
    assert _harvest_channel_ok("/whatever", "sensor_msgs/msg/Image")
    assert _harvest_channel_ok("/ut/cam", "foxglove.CompressedVideo")
    assert not _harvest_channel_ok("/imu/sample", "sensor_msgs/msg/Image")
    assert not _harvest_channel_ok("/camera/RR/video", "headcam.Video")
    assert not _harvest_channel_ok("/master/joint_left", "sensor_msgs/msg/JointState")

    from sview.ingest.orin_images import classify_teleop_forward_topic

    assert classify_teleop_forward_topic(
        "/camera/f_l/h264", "sensor_msgs/msg/CompressedImage", kind="h264",
    ) == "video"
    assert classify_teleop_forward_topic(
        "/frontvideostream", "sensor_msgs/msg/Image", kind="jpeg",
    ) == "video"
    assert classify_teleop_forward_topic(
        "/imu/data", "sensor_msgs/msg/Imu", kind="h264",
    ) == "imu"
    assert classify_teleop_forward_topic(
        "/camera/f_l/camera_info", "sensor_msgs/msg/CameraInfo", kind="jpeg",
    ) == "meta"
    assert classify_teleop_forward_topic(
        "/master/joint_left", "sensor_msgs/msg/JointState", kind="h264",
    ) is None
    assert classify_teleop_forward_topic(
        "/camera/RR/video", "headcam.Video", kind="h264",
    ) is None

    unitree = {"/frontvideostream": 100, "/joint_states": 5000}
    assert orin_frame_count_from_channel_counts(unitree, "jpeg") == 100

    class Msg:
        pass

    msg = Msg()
    msg.encoding = "rgb8"
    msg.width = 8
    msg.height = 8
    msg.data = bytes([255, 0, 0] * 64)
    jpeg = _ros_image_to_jpeg(msg)
    assert jpeg and jpeg[:2] == b"\xff\xd8"


def test_unitree_compressed_video_named_like_jpeg():
    from sview.ingest.orin_images import classify_teleop_forward_topic

    topic = "/camera/head/color/image_raw/compressed"
    video = "foxglove_msgs/msg/CompressedVideo"
    image = "sensor_msgs/msg/CompressedImage"
    assert classify_teleop_forward_topic(topic, video, kind="h264") == "video"
    assert classify_teleop_forward_topic(topic, video, kind="jpeg") is None
    assert classify_teleop_forward_topic(topic, image, kind="h264") is None
    assert classify_teleop_forward_topic(topic, image, kind="jpeg") == "video"


_COMPRESSED_IMAGE_MSGDEF = """std_msgs/Header header
string format
uint8[] data
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"""


def _write_compressed_image_mcap(path: Path, streams: dict) -> None:
    from mcap_ros2.writer import Writer

    with open(path, "wb") as fh:
        writer = Writer(fh)
        schema = writer.register_msgdef("sensor_msgs/msg/CompressedImage", _COMPRESSED_IMAGE_MSGDEF)
        for i in range(3):
            for topic, (fmt, data) in streams.items():
                t = 1_000_000_000 + i * 33_000_000
                writer.write_message(
                    topic,
                    schema,
                    {
                        "header": {"stamp": {"sec": 1, "nanosec": i}, "frame_id": ""},
                        "format": fmt,
                        "data": data,
                    },
                    log_time=t,
                    publish_time=t,
                )
        writer.finish()


def test_codec_comes_from_payload_not_topic_name():
    import tempfile

    from sview.ingest.frame_index import build_mcap_frame_index
    from sview.ingest.orin_images import (
        _video_topics_from_summary,
        channel_payload_kinds,
        orin_stream_kind,
        teleop_forward_topics,
    )

    idr = b"\x00\x00\x00\x01\x67\x42\x00\x1e" + b"\x00\x00\x00\x01\x65" + b"\x11" * 16
    jpeg = b"\xff\xd8\xff\xe0" + b"\x00" * 16 + b"\xff\xd9"
    h264_topic = "/camera/left/image_raw/compressed"
    jpeg_topic = "/camera/right/image_raw/compressed"
    with tempfile.TemporaryDirectory() as tmp:
        mixed = Path(tmp) / "mixed.mcap"
        _write_compressed_image_mcap(
            mixed, {h264_topic: ("h264", idr), jpeg_topic: ("jpeg", jpeg)}
        )
        assert channel_payload_kinds(mixed) == {h264_topic: "h264", jpeg_topic: "jpeg"}
        assert orin_stream_kind(mixed, skip_headcam_guard=True) == "h264"
        assert _video_topics_from_summary(mixed, kind="h264") == [h264_topic]
        assert _video_topics_from_summary(mixed, kind="jpeg") == [jpeg_topic]
        assert [t for t, role in teleop_forward_topics(mixed, kind="jpeg") if role == "video"] == [jpeg_topic]

        h264_only = Path(tmp) / "h264_in_compressed_image.mcap"
        _write_compressed_image_mcap(h264_only, {h264_topic: ("", idr)})
        assert orin_stream_kind(h264_only, skip_headcam_guard=True) == "h264"
        index = build_mcap_frame_index(h264_only)
        assert index["kind"] == "orin_h264"
        assert index["frame_count"] == 3


if __name__ == "__main__":
    test_codec_comes_from_payload_not_topic_name()
    test_unitree_compressed_video_named_like_jpeg()
    test_agilex_image_raw_topics()
    test_orin_frame_count_from_channel_counts()
    test_unknown_teleop_topics_keep_names()
    print("ok")
