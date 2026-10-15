#!/usr/bin/env python3
"""按路径判断采集族：head_cam 与 teleoperation/{Robot}。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sview.path_map import ingest_family, teleop_robot


UNITREE_OSS = (
    "oss://ss-oss-data-platform/original_data/teleoperation/Unitree/260917/"
    "G1D_001_device/Unitree_g1d_001_test-Organize-Desktop_20260917_102508_77b4_0.mcap"
)


def test_ingest_family():
    assert ingest_family(
        "oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/"
        "260706/001U_device/SSIEGO_Cam_001U_20260706_101930_342.mcap"
    ) == "head_cam"
    assert ingest_family("/head-data/260706/001U_device/foo.mcap") == "head_cam"
    assert ingest_family("/head-cam/260706/001U_device/foo.mcap") == "head_cam"

    assert ingest_family(UNITREE_OSS) == "teleop"
    assert teleop_robot(UNITREE_OSS) == "Unitree"
    assert ingest_family("/teleoperation/Unitree/260917/foo.mcap") == "teleop"
    assert teleop_robot("/teleoperation/Unitree/260917/foo.mcap") == "Unitree"

    assert ingest_family(
        "oss://ss-oss-data-platform/original_data/teleoperation/Orin/260701/"
        "004_device/m1_004_camera_20260701_081700_0.mcap"
    ) == "teleop"
    assert teleop_robot(
        "oss://ss-oss-data-platform/original_data/teleoperation/Agilex/x.mcap"
    ) == "Agilex"
    assert ingest_family("/teleoperation/Astribot/ep.h5") == "teleop"
    assert ingest_family("/orin-data/260701/004_device/bar.mcap") == "teleop"

    assert ingest_family("/tmp/copied.mcap") == "unknown"
    assert teleop_robot("/tmp/copied.mcap") is None


def test_unitree_never_head_cam():
    """曾经触发 'Head-cam MCAP has no aligned video frames' 的那个包。"""
    assert ingest_family(UNITREE_OSS) != "head_cam"
    assert ingest_family(
        "/teleoperation/Unitree/260917/G1D_001_device/"
        "Unitree_g1d_001_test-Organize-Desktop_20260917_102508_77b4_0.mcap"
    ) == "teleop"


if __name__ == "__main__":
    test_ingest_family()
    test_unitree_never_head_cam()
    print("ok")
