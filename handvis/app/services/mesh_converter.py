from __future__ import annotations

import inspect
import logging
from collections import namedtuple
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
from fastapi import HTTPException

if TYPE_CHECKING:
    import torch

# 兼容旧版 chumpy 在现代 Python/NumPy 下的别名行为。
for _name, _value in (
    ("bool", bool),
    ("int", int),
    ("float", float),
    ("complex", complex),
    ("object", object),
    ("unicode", str),
    ("str", str),
):
    # 直接检查模块字典，避免 hasattr 触发 NumPy 对已移除别名的 FutureWarning。
    if _name not in np.__dict__:
        setattr(np, _name, _value)

if not hasattr(inspect, "getargspec"):
    ArgSpec = namedtuple("ArgSpec", ["args", "varargs", "keywords", "defaults"])

    def _getargspec_compat(func):
        spec = inspect.getfullargspec(func)
        return ArgSpec(spec.args, spec.varargs, spec.varkw, spec.defaults)

    inspect.getargspec = _getargspec_compat

logger = logging.getLogger(__name__)

EXTRA_FACES = np.array(
    [
        [92, 38, 234],
        [234, 38, 239],
        [38, 122, 239],
        [239, 122, 279],
        [122, 118, 279],
        [279, 118, 215],
        [118, 117, 215],
        [215, 117, 214],
        [117, 119, 214],
        [214, 119, 121],
        [119, 120, 121],
        [121, 120, 78],
        [120, 108, 78],
        [78, 108, 79],
    ],
    dtype=np.int32,
)


def _run_mano_batch(
    mano_model,
    global_orient_aa: np.ndarray,
    hand_pose_aa: np.ndarray,
    betas: np.ndarray,
    transl: np.ndarray,
    device: torch.device,
    batch_size: int = 128,
) -> tuple[np.ndarray, np.ndarray]:
    """按批次执行 MANO 前向推理，返回网格顶点和腕部 0 号根关节。"""
    import torch

    def _to_f32_tensor(x):
        if isinstance(x, torch.Tensor):
            # 当输入已经是 Tensor 时避免重复拷贝告警。
            return x[s:e].to(device=device, dtype=torch.float32)
        return torch.as_tensor(x[s:e], dtype=torch.float32, device=device)

    t = global_orient_aa.shape[0]
    out_verts = np.zeros((t, 778, 3), dtype=np.float32)
    out_roots = np.zeros((t, 3), dtype=np.float32)
    mano_model = mano_model.to(device)
    mano_model.eval()
    with torch.no_grad():
        for s in range(0, t, batch_size):
            e = min(s + batch_size, t)
            out = mano_model(
                global_orient=_to_f32_tensor(global_orient_aa),
                hand_pose=_to_f32_tensor(hand_pose_aa),
                betas=_to_f32_tensor(betas),
                transl=_to_f32_tensor(transl),
                pose2rot=True,
            )
            out_verts[s:e] = out.vertices.cpu().numpy()
            # joints[:, 0] 是腕部根关节，不能用位于手掌附近的 mesh vertex[0] 替代。
            out_roots[s:e] = out.joints[:, 0, :].cpu().numpy()
    return out_verts, out_roots


def _to_np(value) -> np.ndarray:
    """tensor/list/numpy -> numpy，保留原始数值用于可视化问题暴露。"""
    if hasattr(value, "detach") and hasattr(value, "cpu") and hasattr(value, "numpy"):
        return value.detach().cpu().numpy()
    return np.asarray(value)


def _hand_valid_mask(hand_data: dict, length: int) -> np.ndarray:
    """根据 posehand 有效性字段生成逐帧有效掩码。

    支持的字段（按优先级）：
    - pred_valid: bool/float array（>0 表示有效），可配合 detection_failed 使用
    - annotated: float array（>0 表示有效），仅在 pred_valid 缺失时兜底
    """
    pred_valid = hand_data.get("pred_valid")
    if pred_valid is not None and np.asarray(_to_np(pred_valid)).size > 0:
        arr = _to_np(pred_valid).reshape(-1)
        if arr.shape[0] < length:
            arr = np.resize(arr, (length,))
        valid = arr[:length] > 0

        # detection_failed 为 True 的帧也视为无效
        detection_failed = hand_data.get("detection_failed")
        if detection_failed is not None:
            failed = _to_np(detection_failed).reshape(-1)
            if failed.shape[0] < length:
                failed = np.resize(failed, (length,))
            valid = valid & (failed[:length] <= 0)

        return valid.astype(np.uint8, copy=False)

    annotated = hand_data.get("annotated")
    if annotated is not None:
        arr = _to_np(annotated).reshape(-1)
        if arr.shape[0] < length:
            arr = np.resize(arr, (length,))
        valid = arr[:length] > 0

        detection_failed = hand_data.get("detection_failed")
        if detection_failed is not None:
            failed = _to_np(detection_failed).reshape(-1)
            if failed.shape[0] < length:
                failed = np.resize(failed, (length,))
            valid = valid & (failed[:length] <= 0)

        return valid.astype(np.uint8, copy=False)

    # 无有效性字段时默认全部有效
    return np.ones((length,), dtype=np.uint8)


def _import_torch_smplx():
    """懒加载 torch/smplx，避免 API 进程启动就拉满科学计算依赖。"""
    try:
        import torch
    except ModuleNotFoundError as exc:
        logger.warning("缺少依赖 torch，无法执行 mesh 转换。")
        raise HTTPException(
            status_code=500,
            detail=(
                "Missing dependency 'torch'. Install it in current backend env, "
                "for example: pip install torch"
            ),
        ) from exc

    try:
        import smplx
    except ModuleNotFoundError as exc:
        logger.warning("缺少依赖 smplx，无法执行 mesh 转换。")
        raise HTTPException(
            status_code=500,
            detail=(
                "Missing dependency 'smplx'. Install it in current backend env, "
                "for example: pip install smplx"
            ),
        ) from exc
    return torch, smplx


def load_pose3d_hand_data(pose3d_hand_bytes: bytes) -> dict:
    """反序列化 pose3d_hand，只做一次，供切片 MANO 与投影元数据复用。"""
    torch, _ = _import_torch_smplx()
    return torch.load(BytesIO(pose3d_hand_bytes), map_location="cpu", weights_only=False)


def infer_pose_frame_count(data: dict) -> int:
    """从 mano transl 推断全帧数。"""
    for key in ("right_hand", "left_hand"):
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
    raise HTTPException(status_code=500, detail="posehand missing mano transl frames")


def _slice_mano_param(value, start: int, end: int, frame_count: int):
    """按时间切片；首维不是帧数的参数（如共享 betas）原样返回。"""
    if value is None:
        return value
    length = int(value.shape[0]) if hasattr(value, "shape") and getattr(value, "ndim", 1) >= 1 else 0
    if length == frame_count:
        return value[start:end]
    return value


class ManoRuntime:
    """MANO 会话：模型只加载一次，按 [start, end) 切片前向。"""

    def __init__(self, *, mano_right_pkl: str | Path, mano_left_pkl: str | Path):
        torch, smplx = _import_torch_smplx()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.mano_r = smplx.MANO(
            str(mano_right_pkl), use_pca=False, is_rhand=True, num_betas=10, flat_hand_mean=True
        )
        self.mano_l = smplx.MANO(
            str(mano_left_pkl), use_pca=False, is_rhand=False, num_betas=10, flat_hand_mean=True
        )
        self.mano_l.shapedirs[:, 0, :] *= -1
        faces_r = np.concatenate([self.mano_r.faces.copy(), EXTRA_FACES], axis=0).astype(np.int32)
        self.faces_right = faces_r
        self.faces_left = faces_r[:, [0, 2, 1]].astype(np.int32)

    def convert_range(self, data: dict, start: int, end: int) -> dict:
        """转换 [start, end) 帧，返回与整段转换相同的字段，但时间维是切片长度。"""
        left = data["left_hand"]
        right = data["right_hand"]
        fps = float(data.get("fps", 30.0))
        rp = right["mano_params"]
        lp = left["mano_params"]
        frame_count = infer_pose_frame_count(data)
        start = max(0, int(start))
        end = min(int(end), frame_count)
        if end <= start:
            raise HTTPException(status_code=500, detail="Invalid MANO slice range")

        verts_r, root_r = _run_mano_batch(
            self.mano_r,
            _slice_mano_param(rp["global_orient"], start, end, frame_count),
            _slice_mano_param(rp["hand_pose"], start, end, frame_count),
            _slice_mano_param(rp["betas"], start, end, frame_count),
            _slice_mano_param(rp["transl"], start, end, frame_count),
            self.device,
        )
        verts_l, root_l = _run_mano_batch(
            self.mano_l,
            _slice_mano_param(lp["global_orient"], start, end, frame_count),
            _slice_mano_param(lp["hand_pose"], start, end, frame_count),
            _slice_mano_param(lp["betas"], start, end, frame_count),
            _slice_mano_param(lp["transl"], start, end, frame_count),
            self.device,
        )
        valid_r = _hand_valid_mask(right, frame_count)[start:end]
        valid_l = _hand_valid_mask(left, frame_count)[start:end]
        return {
            "fps": np.float32(fps),
            "verts_right": verts_r,
            "verts_left": verts_l,
            "root_right": root_r,
            "root_left": root_l,
            "faces_right": self.faces_right,
            "faces_left": self.faces_left,
            "valid_right": valid_r,
            "valid_left": valid_l,
        }


def convert_pose3d_hand_bytes_to_mesh(
    pose3d_hand_bytes: bytes,
    *,
    mano_right_pkl: str | Path,
    mano_left_pkl: str | Path,
) -> dict:
    """将 pose3d_hand 二进制转换为左右手 mesh 顶点与面片（整段，供脚本复用）。"""
    data = load_pose3d_hand_data(pose3d_hand_bytes)
    runtime = ManoRuntime(mano_right_pkl=mano_right_pkl, mano_left_pkl=mano_left_pkl)
    return runtime.convert_range(data, 0, infer_pose_frame_count(data))
