#!/usr/bin/env python3
"""
把提交的源路径（OSS URI 或旧 redis 路径）映射到本地挂载路径。

K8s 应挂载：
  original_data/ego_human_data/head_cam/  ->  HEAD_CAM_MOUNT  （默认 /head-data）
  original_data/teleoperation/            ->  ORIN_MOUNT      （默认 /teleoperation）
    （Orin / Agilex / Astribot / Franka / Unitree 同级，在挂载点下保留机体目录）

示例：
  oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/260706/001U_device/xxx.mcap
    -> /head-data/260706/001U_device/xxx.mcap

  oss://ss-oss-data-platform/original_data/teleoperation/Orin/260701/004_device/xxx.mcap
    -> /teleoperation/Orin/260701/004_device/xxx.mcap

  oss://ss-oss-data-platform/original_data/teleoperation/Agilex/260825/COBOT_001_device/xxx.mcap
    -> /teleoperation/Agilex/260825/COBOT_001_device/xxx.mcap
"""

import os
from pathlib import Path
from typing import List, Optional, Tuple

HEAD_CAM_MOUNT = Path(
    os.environ.get("HEAD_CAM_MOUNT", os.environ.get("LOCAL_MOUNT_PATH", "/head-data"))
)
ORIN_MOUNT = Path(
    os.environ.get("ORIN_MOUNT")
    or os.environ.get("TELEOPERATION_MOUNT")
    or "/teleoperation"
)
_LEGACY_ORIN_MOUNT = Path("/orin-data")

# 可选 "oss://{bucket}/" 之后的 key 前缀
_HEAD_CAM_KEY_PREFIXES = (
    "original_data/ego_human_data/head_cam/",
    "ego_human_data/head_cam/",
)
# teleoperation/ 下与 Orin 同级的机体目录，都映射到 ORIN_MOUNT/{Robot}/...
_TELEOP_ROBOTS = ("Orin", "Agilex", "Astribot", "Franka", "Unitree")


def _norm_mount(p: Path) -> str:
    return str(p).replace("\\", "/").rstrip("/")


def _strip_oss_scheme(raw: str) -> str:
    """oss://bucket/key → bucket/key；仍当路径字符串。"""
    s = raw.strip().replace("\\", "/")
    if s.startswith("oss://"):
        s = s[len("oss://") :]
    return s.lstrip("/")


def _relative_after_prefix(key: str, prefixes: Tuple[str, ...]) -> Optional[str]:
    """
    按下面两种形式匹配 key：
      {prefix}{rel}
      {bucket}/{prefix}{rel}
    返回 rel，否则 None。
    """
    for pref in prefixes:
        if key.startswith(pref):
            return key[len(pref) :].lstrip("/")
        # bucket/前缀...
        slash = key.find("/")
        if slash > 0:
            rest = key[slash + 1 :]
            if rest.startswith(pref):
                return rest[len(pref) :].lstrip("/")
    return None


def _teleop_robot_and_rel(key: str) -> Optional[Tuple[str, str]]:
    """匹配 teleoperation/{Robot}/{rel} → (Robot, rel)。"""
    for name in _TELEOP_ROBOTS:
        prefixes = (
            f"original_data/teleoperation/{name}/",
            f"teleoperation/{name}/",
        )
        rel = _relative_after_prefix(key, prefixes)
        if rel is not None:
            return name, rel
    return None


def ingest_family(raw_path: str) -> str:
    """这条源走哪条 ingest 管线。

    返回 ``head_cam`` | ``teleop`` | ``unknown``。
    路径优先：``ego_human_data/head_cam`` 一定是头环 protobuf；
    ``teleoperation/{Orin,Agilex,...}`` 一定是遥操（不会回退成 protobuf）。
    ``unknown`` 是拷出挂载树的文件（CLI）；调用方可自行探测。
    """
    kind, _mount, _rel = classify_source(raw_path)
    if kind in ("head_cam", "legacy"):
        return "head_cam"
    if kind == "orin":
        return "teleop"
    key = _strip_oss_scheme(raw_path.strip().replace("\\", "/"))
    if _relative_after_prefix(key, _HEAD_CAM_KEY_PREFIXES) is not None:
        return "head_cam"
    if _teleop_robot_and_rel(key) is not None:
        return "teleop"
    low = f"/{key.lower().strip('/')}/"
    if "/head_cam/" in low or "/head-cam/" in low or "/head-data/" in low:
        return "head_cam"
    if "/teleoperation/" in low:
        return "teleop"
    return "unknown"


def teleop_robot(raw_path: str) -> Optional[str]:
    """Orin / Agilex / Astribot / Franka / Unitree，否则 None。"""
    kind, _mount, rel = classify_source(raw_path)
    if kind == "orin" and rel:
        name = rel.split("/")[0]
        if name in _TELEOP_ROBOTS:
            return name
    hit = _teleop_robot_and_rel(_strip_oss_scheme(raw_path.strip().replace("\\", "/")))
    return hit[0] if hit else None


def classify_source(raw_path: str) -> Tuple[str, Path, str]:
    """
    返回 (kind, mount_root, relative)。
    kind：'head_cam' | 'orin' | 'legacy' | 'raw'
    遥操的 relative 保留机体目录：``Orin/260701/...``、``Agilex/260825/...``。
    """
    s = raw_path.strip().replace("\\", "/")

    teleop_mounts: List[Tuple[str, Path]] = [("orin", ORIN_MOUNT)]
    if _norm_mount(ORIN_MOUNT) != _norm_mount(_LEGACY_ORIN_MOUNT):
        teleop_mounts.append(("orin", _LEGACY_ORIN_MOUNT))

    for kind, mount in (("head_cam", HEAD_CAM_MOUNT), *teleop_mounts):
        mount_s = _norm_mount(mount)
        if s == mount_s or s.startswith(mount_s + "/"):
            return kind, mount, s[len(mount_s) :].lstrip("/")

    key = _strip_oss_scheme(s)

    rel = _relative_after_prefix(key, _HEAD_CAM_KEY_PREFIXES)
    if rel is not None:
        return "head_cam", HEAD_CAM_MOUNT, rel

    hit = _teleop_robot_and_rel(key)
    if hit is not None:
        robot, rel = hit
        inner = f"{robot}/{rel}" if rel else robot
        return "orin", ORIN_MOUNT, inner

    # 旧队列路径：/head-cam/...
    legacy = s.lstrip("/")
    if legacy.startswith("head-cam/"):
        return "legacy", HEAD_CAM_MOUNT, legacy[len("head-cam/") :]

    return "raw", HEAD_CAM_MOUNT, _strip_oss_scheme(s)


def source_to_local(raw_path: str) -> Path:
    """把 OSS URI / 旧队列路径 / 本地路径映射成文件系统 Path。"""
    kind, mount, rel = classify_source(raw_path)
    if not rel:
        p = Path(raw_path)
        if p.is_absolute():
            return p
        return mount
    primary = mount / rel
    if kind != "orin":
        return primary
    robot, sep, rest = rel.partition("/")
    if robot not in _TELEOP_ROBOTS or not rest:
        return primary
    # 父目录 PVC：/teleoperation/Agilex/...
    # 旧 PVC 只挂 Orin/ 到挂载根：/teleoperation/260701/... 或 /orin-data/260701/...
    legacy = mount / rest
    if primary.exists():
        return primary
    if legacy.exists():
        return legacy
    return primary


def redis_path_to_local(redis_path: str) -> Path:
    """给现有 worker 调用点用的别名。"""
    return source_to_local(redis_path)


if __name__ == "__main__":
    samples = [
        "oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/260706/001U_device/SSIEGO_Cam_001U_20260706_101930_342.mcap",
        "oss://ss-oss-data-platform/original_data/teleoperation/Orin/260701/004_device/m1_004_camera_20260701_081700_0.mcap",
        "oss://ss-oss-data-platform/original_data/teleoperation/Agilex/260825/COBOT_001_device/Agilex_cobot_001_20260825_104750_22_0.mcap",
        "oss://ss-oss-data-platform/original_data/teleoperation/Astribot/x.mcap",
        "oss://ss-oss-data-platform/original_data/teleoperation/Franka/x.mcap",
        "oss://ss-oss-data-platform/original_data/teleoperation/Unitree/x.mcap",
        "/head-cam/260706/001U_device/foo.mcap",
        "/head-data/260706/001U_device/foo.mcap",
        "/teleoperation/Agilex/260825/foo.mcap",
        "/orin-data/260701/004_device/bar.mcap",
    ]
    for s in samples:
        print(s)
        print(" ->", source_to_local(s))
        print("   ", classify_source(s))
        print("   family=", ingest_family(s), "robot=", teleop_robot(s))
