"""视频投影元数据服务。

职责：
- 从 pose3d_hand 的 slam_data 中读取相机轨迹、内参与尺度；
- 输出供前端实时投影使用的元数据（位置 + 四元数 + 内参 + 基准分辨率）；
- 不再做后端预投影计算（已迁移到前端）。

存储协议（v5）：
- 内参/尺度/帧数/stride/base_width/base_height 留在 index.json 内联；
- positions / quaternions / valid_mask 改为单独的 video_projection.bin
  二进制文件，避免 index.json 因长视频膨胀（5 分钟视频可降 77% 体积）。

video_projection.bin 布局：
  magic(4 bytes) = "VP01"
  version(uint32 LE)
  frame_count(uint32 LE)
  positions:    frame_count × 3 × float32 LE   (无效帧填 NaN)
  quaternions:  frame_count × 4 × float32 LE   (无效帧填 NaN)
  valid_mask:   frame_count × uint8            (1=valid, 0=invalid)
"""

import logging
import math
import struct
from io import BytesIO
from pathlib import Path

import numpy as np
from fastapi import HTTPException

from scipy.interpolate import interp1d

logger = logging.getLogger(__name__)

VIDEO_PROJECTION_MAGIC = b"VP01"
VIDEO_PROJECTION_VERSION = 1
VIDEO_PROJECTION_BIN_NAME = "video_projection.bin"


def _to_np(value) -> np.ndarray:
    """tensor/list/numpy -> numpy。"""
    if hasattr(value, "detach") and hasattr(value, "cpu") and hasattr(value, "numpy"):
        return value.detach().cpu().numpy()
    return np.asarray(value)


def _parse_slam_traj_arrays(slam_data: dict) -> tuple[np.ndarray | None, np.ndarray | None]:
    """从 slam_data.traj 中解析位置与四元数。

    标准格式：traj 为 ndarray[N, 7]，列布局为 [x, y, z, qx, qy, qz, qw]。
    """
    traj = slam_data.get("traj")
    if traj is None:
        return None, None
    arr = _to_np(traj).astype(np.float32, copy=False)
    if arr.ndim != 2 or arr.shape[1] < 7:
        return None, None
    return arr[:, :3], arr[:, 3:7]


def _parse_number_pair(value) -> tuple[float, float] | None:
    """把常见数组/张量字段解析成两个有限正数。"""
    if value is None:
        return None
    try:
        arr = _to_np(value).reshape(-1).astype(np.float64)
    except (TypeError, ValueError):
        return None
    if arr.shape[0] < 2:
        return None

    first = float(arr[0])
    second = float(arr[1])
    if first > 0 and second > 0 and math.isfinite(first) and math.isfinite(second):
        return first, second
    return None


def _parse_image_size(slam_data: dict) -> tuple[float, float] | None:
    """读取图像基准尺寸，避免把偏移主点误当成图像中心。

    尺寸字段按 [width, height] 解析；图像 shape 字段按 [height, width] 解析。
    """
    for key in ("img_size", "image_size"):
        pair = _parse_number_pair(slam_data.get(key))
        if pair is not None:
            return pair

    pair = _parse_number_pair(slam_data.get("img_shape"))
    if pair is not None:
        height, width = pair
        return width, height
    return None


def _infer_pose_frame_count(data: dict) -> int | None:
    """从 mano transl 推断 pose 全帧数，用于将稀疏 SLAM traj 插值对齐。"""
    for key in ("left_hand", "right_hand"):
        hand = data.get(key)
        if not isinstance(hand, dict):
            continue
        mano_params = hand.get("mano_params")
        if not isinstance(mano_params, dict):
            continue
        transl = mano_params.get("transl")
        if transl is None:
            continue
        arr = _to_np(transl)
        if arr.ndim >= 2 and arr.shape[0] > 0:
            return int(arr.shape[0])
    return None


def _interpolate_traj_to_frame_count(
    positions: np.ndarray,
    quaternions: np.ndarray,
    target_frames: int,
) -> tuple[np.ndarray, np.ndarray]:
    """将相机轨迹线性插值到与 pose 帧数一致，避免投影索引系统性错位。"""
    n_src = int(min(positions.shape[0], quaternions.shape[0]))
    target = max(1, int(target_frames))
    if n_src <= 0:
        raise HTTPException(status_code=500, detail="posehand camera trajectory is empty")
    if n_src >= target:
        return positions[:target, :3], quaternions[:target, :4]

    src_indices = np.linspace(0, target - 1, n_src)
    dst_indices = np.arange(target, dtype=np.float64)
    traj = np.concatenate([positions[:n_src, :3], quaternions[:n_src, :4]], axis=1).astype(np.float64)
    interp_func = interp1d(src_indices, traj, axis=0, kind="linear", fill_value="extrapolate")
    full = interp_func(dst_indices).astype(np.float32)
    pos_out = full[:, :3]
    quat_out = full[:, 3:7]
    quat_norm = np.linalg.norm(quat_out, axis=-1, keepdims=True)
    quat_out = quat_out / np.maximum(quat_norm, 1e-8)
    return pos_out, quat_out


def _load_projection_source(
    pose3d_hand_bytes: bytes | None = None,
    *,
    fallback_image_size: tuple[float, float] | None = None,
    pose_data: dict | None = None,
) -> tuple[np.ndarray, np.ndarray, float, float, float, float, float, float]:
    """读取投影元数据所需的相机轨迹、内参和尺度。"""
    data = pose_data
    if data is None:
        try:
            import torch
        except ModuleNotFoundError as exc:
            raise HTTPException(status_code=500, detail="Missing dependency 'torch' for projection metadata") from exc

        if not pose3d_hand_bytes:
            raise HTTPException(status_code=500, detail="posehand bytes missing for projection metadata")
        try:
            data = torch.load(BytesIO(pose3d_hand_bytes), map_location="cpu", weights_only=False)
        except Exception as exc:
            logger.warning("读取 pose3d_hand 投影元数据失败。err=%s", exc)
            raise HTTPException(status_code=500, detail="Failed to read posehand projection data") from exc

    slam_data = data.get("slam_data")
    if not isinstance(slam_data, dict):
        raise HTTPException(status_code=500, detail="posehand missing slam_data for projection metadata")

    pos, quat = _parse_slam_traj_arrays(slam_data)
    if pos is None or quat is None:
        raise HTTPException(status_code=500, detail="posehand missing camera trajectory (traj must be ndarray[N,7])")

    pos = pos.astype(np.float32, copy=False)
    quat = quat.astype(np.float32, copy=False)

    img_focal_raw = slam_data.get("img_focal")
    img_center_raw = slam_data.get("img_center")
    scale_raw = slam_data.get("scale")

    if img_focal_raw is None:
        raise HTTPException(status_code=500, detail="posehand missing img_focal in slam_data")

    img_focal = float(_to_np(img_focal_raw).reshape(-1)[0])
    image_size = _parse_image_size(slam_data) or fallback_image_size
    img_center_pair = _parse_number_pair(img_center_raw)
    scale = float(scale_raw) if scale_raw is not None else 1.0

    if img_center_pair is None and image_size is None:
        raise HTTPException(status_code=500, detail="posehand missing img_center and image size in slam_data")

    if img_center_pair is not None:
        cx, cy = img_center_pair
    else:
        width, height = image_size
        cx = width * 0.5
        cy = height * 0.5

    if image_size is not None:
        base_width, base_height = image_size
    else:
        base_width = cx * 2.0
        base_height = cy * 2.0

    if not (img_focal > 0 and cx > 0 and cy > 0 and base_width > 0 and base_height > 0 and math.isfinite(scale)):
        raise HTTPException(status_code=500, detail="posehand camera intrinsics are invalid")

    length = min(int(pos.shape[0]), int(quat.shape[0]))
    if length <= 0:
        raise HTTPException(status_code=500, detail="posehand camera trajectory is empty")

    target_frames = _infer_pose_frame_count(data)
    if target_frames is not None and target_frames > length:
        logger.info(
            "SLAM traj 帧数(%s) 小于 pose 帧数(%s)，插值对齐后再写入 video_projection。",
            length,
            target_frames,
        )
        pos, quat = _interpolate_traj_to_frame_count(pos[:length], quat[:length], target_frames)
    else:
        pos = pos[:length, :3]
        quat = quat[:length, :4]

    return pos, quat, img_focal, cx, cy, base_width, base_height, scale


def _serialize_projection_binary(positions: np.ndarray, quaternions: np.ndarray, valid_mask: np.ndarray) -> bytes:
    """把采样后的 positions/quaternions/valid_mask 序列化为二进制。

    无效帧的 positions/quaternions 用 NaN 填充，前端按 valid_mask 判断跳过。
    """
    n = int(positions.shape[0])
    pos_out = positions.astype(np.float32, copy=True)
    quat_out = quaternions.astype(np.float32, copy=True)
    mask_out = valid_mask.astype(np.uint8, copy=False)

    invalid_idx = mask_out <= 0
    if invalid_idx.any():
        pos_out[invalid_idx] = np.nan
        quat_out[invalid_idx] = np.nan

    header = (
        VIDEO_PROJECTION_MAGIC
        + struct.pack("<I", VIDEO_PROJECTION_VERSION)
        + struct.pack("<I", n)
    )
    return (
        header
        + pos_out.reshape(-1).tobytes(order="C")
        + quat_out.reshape(-1).tobytes(order="C")
        + mask_out.tobytes(order="C")
    )


def extract_video_projection_payload(
    *,
    pose3d_hand_bytes: bytes | None = None,
    stride: int,
    chunk_root_dir: Path,
    fallback_image_size: tuple[float, float] | None = None,
    pose_data: dict | None = None,
) -> dict | None:
    """提取前端实时投影所需的元数据，并把大数组写入 video_projection.bin。

    返回的 dict 只含小体积的"内参 + 引用"，可放心内联到 index.json。
    实际逐帧位姿数据存放在 chunk_root_dir / VIDEO_PROJECTION_BIN_NAME。
    """
    try:
        positions, quaternions, focal, cx, cy, base_width, base_height, scale = _load_projection_source(
            pose3d_hand_bytes,
            fallback_image_size=fallback_image_size,
            pose_data=pose_data,
        )
    except HTTPException as exc:
        logger.warning("提取视频投影元数据失败。detail=%s", exc.detail)
        return None

    step = max(1, int(stride))
    positions = positions[::step]
    quaternions = quaternions[::step]
    n = min(int(positions.shape[0]), int(quaternions.shape[0]))
    if n <= 0:
        return None

    positions = positions[:n]
    quaternions = quaternions[:n]

    pos_finite = np.all(np.isfinite(positions), axis=1)
    quat_finite = np.all(np.isfinite(quaternions), axis=1)
    valid_mask = np.logical_and(pos_finite, quat_finite).astype(np.uint8, copy=False)

    binary_payload = _serialize_projection_binary(positions, quaternions, valid_mask)
    bin_path = chunk_root_dir / VIDEO_PROJECTION_BIN_NAME
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    bin_path.write_bytes(binary_payload)

    return {
        "frame_count": int(n),
        "stride": int(step),
        "img_focal": float(focal),
        "img_center": [float(cx), float(cy)],
        "scale": float(scale),
        "base_width": float(base_width),
        "base_height": float(base_height),
        # 引用关系：前端自行 fetch 这个 bin 文件解出 positions/quaternions/valid_mask。
        "binary_url": VIDEO_PROJECTION_BIN_NAME,
        "binary_bytes": int(len(binary_payload)),
    }
