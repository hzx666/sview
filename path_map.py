#!/usr/bin/env python3
"""
Map submitted source paths (OSS URI or legacy redis path) to local mount paths.

K8s is expected to mount:
  original_data/ego_human_data/head_cam/  ->  HEAD_CAM_MOUNT  (default /head-data)
  original_data/teleoperation/Orin/       ->  ORIN_MOUNT      (default /orin-data)

Examples:
  oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/260706/001U_device/xxx.mcap
    -> /head-data/260706/001U_device/xxx.mcap

  oss://ss-oss-data-platform/original_data/teleoperation/Orin/260701/004_device/xxx.mcap
    -> /orin-data/260701/004_device/xxx.mcap
"""

import os
from pathlib import Path
from typing import Optional, Tuple

HEAD_CAM_MOUNT = Path(
    os.environ.get("HEAD_CAM_MOUNT", os.environ.get("LOCAL_MOUNT_PATH", "/head-data"))
)
ORIN_MOUNT = Path(os.environ.get("ORIN_MOUNT", "/orin-data"))

# Key prefixes after optional "oss://{bucket}/"
_HEAD_CAM_KEY_PREFIXES = (
    "original_data/ego_human_data/head_cam/",
    "ego_human_data/head_cam/",
)
_ORIN_KEY_PREFIXES = (
    "original_data/teleoperation/Orin/",
    "teleoperation/Orin/",
)


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


def classify_source(raw_path: str) -> Tuple[str, Path, str]:
    """
    Returns (kind, mount_root, relative).
    kind: 'head_cam' | 'orin' | 'legacy' | 'raw'
    """
    s = raw_path.strip().replace("\\", "/")

    for kind, mount in (("head_cam", HEAD_CAM_MOUNT), ("orin", ORIN_MOUNT)):
        mount_s = str(mount).replace("\\", "/").rstrip("/")
        if s == mount_s or s.startswith(mount_s + "/"):
            return kind, mount, s[len(mount_s) :].lstrip("/")

    key = _strip_oss_scheme(s)

    rel = _relative_after_prefix(key, _HEAD_CAM_KEY_PREFIXES)
    if rel is not None:
        return "head_cam", HEAD_CAM_MOUNT, rel

    rel = _relative_after_prefix(key, _ORIN_KEY_PREFIXES)
    if rel is not None:
        return "orin", ORIN_MOUNT, rel

    # Legacy queue path: /head-cam/...
    legacy = s.lstrip("/")
    if legacy.startswith("head-cam/"):
        return "legacy", HEAD_CAM_MOUNT, legacy[len("head-cam/") :]

    return "raw", HEAD_CAM_MOUNT, legacy


def source_to_local(raw_path: str) -> Path:
    """Map OSS URI / legacy queue path / local path to a filesystem Path."""
    _kind, mount, rel = classify_source(raw_path)
    if not rel:
        p = Path(raw_path)
        if p.is_absolute():
            return p
        return mount
    return mount / rel


def redis_path_to_local(redis_path: str) -> Path:
    """Alias for existing worker call sites."""
    return source_to_local(redis_path)


if __name__ == "__main__":
    samples = [
        "oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/260706/001U_device/SSIEGO_Cam_001U_20260706_101930_342.mcap",
        "oss://ss-oss-data-platform/original_data/teleoperation/Orin/260701/004_device/m1_004_camera_20260701_081700_0.mcap",
        "/head-cam/260706/001U_device/foo.mcap",
        "/head-data/260706/001U_device/foo.mcap",
        "/orin-data/260701/004_device/bar.mcap",
    ]
    for s in samples:
        print(s)
        print(" ->", source_to_local(s))
        print("   ", classify_source(s))
