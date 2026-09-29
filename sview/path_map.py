#!/usr/bin/env python3
"""
Map submitted source paths (OSS URI or legacy redis path) to local mount paths.

K8s is expected to mount:
  original_data/ego_human_data/head_cam/  ->  HEAD_CAM_MOUNT  (default /head-data)
  original_data/teleoperation/            ->  ORIN_MOUNT      (default /teleoperation)
    (Orin / Agilex / Astribot / Franka / Unitree 同级，在挂载点下保留机体目录)

Examples:
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

# Key prefixes after optional "oss://{bucket}/"
_HEAD_CAM_KEY_PREFIXES = (
    "original_data/ego_human_data/head_cam/",
    "ego_human_data/head_cam/",
)
# teleoperation/ 下与 Orin 同级的机体目录，都映射到 ORIN_MOUNT/{Robot}/...
_TELEOP_ROBOTS = ("Orin", "Agilex", "Astribot", "Franka", "Unitree")


def _norm_mount(p: Path) -> str:
    return str(p).replace("\\", "/").rstrip("/")


def _strip_oss_scheme(raw: str) -> str:
    """oss://bucket/key -> bucket/key; keep as path-like string."""
    s = raw.strip().replace("\\", "/")
    if s.startswith("oss://"):
        s = s[len("oss://") :]
    return s.lstrip("/")


def _relative_after_prefix(key: str, prefixes: Tuple[str, ...]) -> Optional[str]:
    """
    Match key as:
      {prefix}{rel}
      {bucket}/{prefix}{rel}
    Return rel, or None.
    """
    for pref in prefixes:
        if key.startswith(pref):
            return key[len(pref) :].lstrip("/")
        # bucket/prefix...
        slash = key.find("/")
        if slash > 0:
            rest = key[slash + 1 :]
            if rest.startswith(pref):
                return rest[len(pref) :].lstrip("/")
    return None


def _teleop_robot_and_rel(key: str) -> Optional[Tuple[str, str]]:
    """Match teleoperation/{Robot}/{rel} → (Robot, rel)."""
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
    """Which ingest pipeline this source belongs to.

    Returns ``head_cam`` | ``teleop`` | ``unknown``.
    Path wins: ``ego_human_data/head_cam`` is always head-ring protobuf;
    ``teleoperation/{Orin,Agilex,...}`` is always teleop (never protobuf fallback).
    ``unknown`` is a file copied off the mount tree (CLI); callers may auto-detect.
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
    """Orin / Agilex / Astribot / Franka / Unitree, or None."""
    kind, _mount, rel = classify_source(raw_path)
    if kind == "orin" and rel:
        name = rel.split("/")[0]
        if name in _TELEOP_ROBOTS:
            return name
    hit = _teleop_robot_and_rel(_strip_oss_scheme(raw_path.strip().replace("\\", "/")))
    return hit[0] if hit else None


def classify_source(raw_path: str) -> Tuple[str, Path, str]:
    """
    Returns (kind, mount_root, relative).
    kind: 'head_cam' | 'orin' | 'legacy' | 'raw'
    Teleop relative keeps the robot folder: ``Orin/260701/...``, ``Agilex/260825/...``.
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

    # Legacy queue path: /head-cam/...
    legacy = s.lstrip("/")
    if legacy.startswith("head-cam/"):
        return "legacy", HEAD_CAM_MOUNT, legacy[len("head-cam/") :]

    return "raw", HEAD_CAM_MOUNT, _strip_oss_scheme(s)


def source_to_local(raw_path: str) -> Path:
    """Map OSS URI / legacy queue path / local path to a filesystem Path."""
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
    """Alias for existing worker call sites."""
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
