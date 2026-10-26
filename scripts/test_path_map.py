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


ASTRIBOT_HDF5 = (
    "oss://ss-oss-data-platform/original_data/teleoperation/Astribot/260903/"
    "S1_002_device/Astribot_S1_002_20260903_InsertWire_DAgger_242.hdf5"
)
ASTRIBOT_MCAP = (
    "oss://ss-oss-data-platform/hdf5_to_mcap/teleoperation/Astribot/260903/"
    "S1_002_device/Astribot_S1_002_20260903_InsertWire_DAgger_242.mcap"
)


def test_astribot_hdf5_maps_to_converted_mcap():
    import tempfile

    import sview.path_map as pm

    assert pm.converted_mcap_source(ASTRIBOT_HDF5) == ASTRIBOT_MCAP
    assert pm.converted_mcap_source(UNITREE_OSS) is None
    assert pm.converted_mcap_source(ASTRIBOT_MCAP) is None

    kind, _mount, rel = pm.classify_source(ASTRIBOT_MCAP)
    assert kind == "converted"
    assert rel == "Astribot/260903/S1_002_device/Astribot_S1_002_20260903_InsertWire_DAgger_242.mcap"
    assert pm.ingest_family(ASTRIBOT_MCAP) == "teleop"
    assert pm.teleop_robot(ASTRIBOT_MCAP) == "Astribot"
    # 去掉 bucket 的写法也不能被当成原始遥操数据。
    assert pm.classify_source(ASTRIBOT_MCAP.split("/", 3)[3])[0] == "converted"

    saved = pm.HDF5_MCAP_MOUNT
    with tempfile.TemporaryDirectory() as tmp:
        pm.HDF5_MCAP_MOUNT = Path(tmp)
        try:
            assert pm.prefer_converted_source(ASTRIBOT_HDF5) == ASTRIBOT_HDF5
            target = Path(tmp) / rel
            target.parent.mkdir(parents=True)
            target.write_bytes(b"\x89MCAP0\r\n")
            assert pm.source_to_local(ASTRIBOT_MCAP) == target
            assert pm.prefer_converted_source(ASTRIBOT_HDF5) == ASTRIBOT_MCAP
            assert pm.prefer_converted_source(UNITREE_OSS) == UNITREE_OSS
        finally:
            pm.HDF5_MCAP_MOUNT = saved


if __name__ == "__main__":
    test_ingest_family()
    test_unitree_never_head_cam()
    test_astribot_hdf5_maps_to_converted_mcap()
    print("ok")
