"""mesh -> chunk 构建服务。

将 pose3d_hand 原始字节转换为双手 mesh，并按预设策略分块写入本地 posehand 目录。

支持流式构建：
- on_first_chunk_ready: 首 chunk 一写完就回调（让前端尽早开始播放）；
- on_chunk_ready: 每写完一个 chunk 都回调（前端可显示构建进度）；
- 每写完一个 chunk 都重写 index.json，标记 still_building 与 ready_chunks。
"""

import json
import math
import logging
from pathlib import Path
from typing import Callable

import numpy as np
from fastapi import HTTPException

from app.core.config import (
    BALANCED_CHUNK_FRAMES,
    BALANCED_STRIDE,
    BASE_DIR,
)
from app.services.mesh_converter import ManoRuntime, infer_pose_frame_count, load_pose3d_hand_data
from app.services.mesh_segment import pack_mesh_segment, segment_bin_name, segment_relpath
from app.services.projection_pipeline import extract_video_projection_payload

logger = logging.getLogger(__name__)


def _f32_bytes(arr: np.ndarray) -> bytes:
    """将数组编码为 float32 二进制。"""
    return arr.reshape(-1).astype(np.float32, copy=False).tobytes(order="C")


def _u32_bytes(arr: np.ndarray) -> bytes:
    """将数组编码为 uint32 二进制。"""
    return arr.reshape(-1).astype(np.uint32, copy=False).tobytes(order="C")


def _write_bytes(path: Path, data: bytes) -> None:
    """写入二进制/文本到本地文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _atomic_write_text(path: Path, text: str) -> None:
    """原子写文本：先写 .tmp 再 rename，避免读到半成品 JSON。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _write_chunk(
    *,
    segment_dir: Path,
    segment_frames: int,
    left_chunk: np.ndarray,
    right_chunk: np.ndarray,
    left_root_chunk: np.ndarray,
    right_root_chunk: np.ndarray,
    stride: int,
    fps: float,
    frame_start: int,
) -> dict:
    """写入一段 mesh bin（SView 同款信封），返回 index 条目。"""
    frame_count = int(left_chunk.shape[0])
    vertex_count = int(left_chunk.shape[1])
    step_sec = stride / float(fps)
    start_time = frame_start * step_sec
    end_time = start_time + frame_count * step_sec
    roots_chunk = np.stack((left_root_chunk, right_root_chunk), axis=1)
    blob = pack_mesh_segment(
        start_index=int(frame_start),
        count=frame_count,
        segment_frames=int(segment_frames),
        vertex_count=vertex_count,
        stride=int(stride),
        fps=float(fps),
        start_time=float(start_time),
        end_time=float(end_time),
        left=_f32_bytes(left_chunk),
        right=_f32_bytes(right_chunk),
        hand_roots=_f32_bytes(roots_chunk),
    )
    _write_bytes(segment_dir / segment_bin_name(frame_start), blob)
    return {
        "id": f"{int(frame_start):06d}",
        "start_time": float(start_time),
        "end_time": float(end_time),
        "frame_start": int(frame_start),
        "frame_count": frame_count,
        "bin": segment_relpath(frame_start),
        "ready": True,
    }


def _placeholder_chunk_entry(frame_start: int, frame_count: int, stride: int, fps: float) -> dict:
    """生成尚未就绪的 segment 占位索引项。"""
    step_sec = stride / float(fps)
    start_time = frame_start * step_sec
    end_time = start_time + frame_count * step_sec
    return {
        "id": f"{int(frame_start):06d}",
        "start_time": float(start_time),
        "end_time": float(end_time),
        "frame_start": int(frame_start),
        "frame_count": int(frame_count),
        "bin": segment_relpath(frame_start),
        "ready": False,
    }


def _normalize_1d_length(arr: np.ndarray, length: int, fill: int = 1) -> np.ndarray:
    """将一维数组对齐到指定长度，缺省按有效值填充。"""
    values = np.asarray(arr).reshape(-1)
    if values.shape[0] >= length:
        return values[:length]
    if values.shape[0] == 0:
        return np.full((length,), fill, dtype=np.uint8)
    padded = np.full((length,), fill, dtype=values.dtype)
    padded[: values.shape[0]] = values
    return padded


def _build_index_payload(
    *,
    fps_from_data: float,
    frame_count: int,
    step_sec: float,
    vertex_count: int,
    chunks: list[dict | None],
    chunk_entries_total: int,
    invalid_mask: list,
    invalid_left_mask: list,
    invalid_right_mask: list,
    video_projection: dict | None,
    face_count: int,
    still_building: bool,
    chunk_duration: float,
) -> dict:
    """生成 index.json 的 payload，含构建进度字段。"""
    ready_chunks = [c for c in chunks if c and c.get("ready")]
    return {
        "version": 7,
        "default_quality": "balanced",
        "faces_bin": "faces.u32",
        "face_count": int(face_count),
        "video_projection": video_projection,
        "still_building": bool(still_building),
        "ready_chunks": len(ready_chunks),
        "total_chunks": int(chunk_entries_total),
        "presets": {
            "balanced": {
                "fps": float(fps_from_data),
                "total_duration": float(frame_count * step_sec),
                "chunk_duration": float(chunk_duration),
                "stride": int(BALANCED_STRIDE),
                "frame_count": int(frame_count),
                "vertex_count": int(vertex_count),
                "still_building": bool(still_building),
                "ready_chunks": len(ready_chunks),
                "total_chunks": int(chunk_entries_total),
                "chunks": [c for c in chunks],
                "invalid_mask": invalid_mask,
                "invalid_left_mask": invalid_left_mask,
                "invalid_right_mask": invalid_right_mask,
            }
        },
    }


def _source_range(sampled_start: int, sampled_end: int, stride: int, source_count: int) -> tuple[int, int]:
    """采样帧 [sampled_start, sampled_end) 对应的源帧半开区间。"""
    src0 = int(sampled_start) * int(stride)
    src1 = min(int(source_count), (int(sampled_end) - 1) * int(stride) + 1)
    return src0, max(src0, src1)


def build_mesh_and_chunks(
    *,
    pose3d_hand_bytes: bytes,
    source_name: str,
    local_root_dir: Path,
    unique_id: str,
    fps: int = 30,
    fallback_image_size: tuple[float, float] | None = None,
    on_first_chunk_ready: Callable[[dict], None] | None = None,
    on_chunk_ready: Callable[[int, int, dict], None] | None = None,
    job_id: str | None = None,
) -> dict:
    """按 30 帧一段边算边写：MANO 只加载一次，每段算完立刻落盘。

    流式行为：
    - 写完第一个 chunk 后立即更新 index.json（still_building=True）并触发 on_first_chunk_ready；
    - 后续每个 chunk 完成都更新 index.json 并触发 on_chunk_ready；
    - 全部完成后写入 still_building=False 的最终版本。
    """
    logger.info("开始构建 chunk。source_name=%s local_root_dir=%s", source_name, local_root_dir)
    mano_right = BASE_DIR / "_DATA" / "data" / "mano" / "MANO_RIGHT.pkl"
    mano_left = BASE_DIR / "_DATA" / "data_left" / "mano_left" / "MANO_LEFT.pkl"
    if not mano_right.exists() or not mano_left.exists():
        logger.warning("MANO 模型缺失。right=%s left=%s", mano_right, mano_left)
        raise HTTPException(status_code=500, detail="MANO model files missing under _DATA")

    pose_data = load_pose3d_hand_data(pose3d_hand_bytes)
    source_count = infer_pose_frame_count(pose_data)
    fps_from_data = float(pose_data.get("fps", fps))
    if not math.isfinite(fps_from_data) or fps_from_data <= 0:
        fps_from_data = float(fps)

    stride = max(1, int(BALANCED_STRIDE))
    sampled_count = (source_count - 1) // stride + 1 if source_count > 0 else 0
    if sampled_count <= 0:
        raise HTTPException(status_code=500, detail="posehand has no frames to convert")

    frames_per_chunk = max(1, int(BALANCED_CHUNK_FRAMES))
    num_chunks = int(math.ceil(sampled_count / frames_per_chunk))
    step_sec = stride / fps_from_data
    chunk_duration = float(frames_per_chunk * step_sec)

    runtime = ManoRuntime(mano_right_pkl=mano_right, mano_left_pkl=mano_left)
    faces = runtime.faces_right.astype(np.uint32, copy=False)
    vertex_count = 778

    chunk_root_dir = local_root_dir / "chunks"
    _write_bytes(chunk_root_dir / "faces.u32", _u32_bytes(faces))
    video_projection = extract_video_projection_payload(
        pose3d_hand_bytes=pose3d_hand_bytes,
        stride=stride,
        chunk_root_dir=chunk_root_dir,
        fallback_image_size=fallback_image_size,
        pose_data=pose_data,
    )

    segment_dir = chunk_root_dir / "segments"
    index_path = chunk_root_dir / "index.json"
    placeholder_entries: list[dict] = []
    for i in range(num_chunks):
        f0 = i * frames_per_chunk
        f1 = min(sampled_count, (i + 1) * frames_per_chunk)
        placeholder_entries.append(
            _placeholder_chunk_entry(
                frame_start=f0,
                frame_count=f1 - f0,
                stride=stride,
                fps=fps_from_data,
            )
        )
    chunks: list[dict | None] = list(placeholder_entries)
    invalid_left_mask = [0] * sampled_count
    invalid_right_mask = [0] * sampled_count
    invalid_mask = [0] * sampled_count

    def _flush_index(still_building: bool) -> dict:
        payload = _build_index_payload(
            fps_from_data=fps_from_data,
            frame_count=sampled_count,
            step_sec=step_sec,
            vertex_count=vertex_count,
            chunks=chunks,
            chunk_entries_total=num_chunks,
            invalid_mask=invalid_mask,
            invalid_left_mask=invalid_left_mask,
            invalid_right_mask=invalid_right_mask,
            video_projection=video_projection,
            face_count=int(faces.shape[0]),
            still_building=still_building,
            chunk_duration=chunk_duration,
        )
        _atomic_write_text(index_path, json.dumps(payload, indent=2))
        return payload

    _flush_index(still_building=True)

    for i in range(num_chunks):
        f0 = i * frames_per_chunk
        f1 = min(sampled_count, (i + 1) * frames_per_chunk)
        src0, src1 = _source_range(f0, f1, stride, source_count)
        mesh = runtime.convert_range(pose_data, src0, src1)
        left_chunk = mesh["verts_left"][::stride].astype(np.float32, copy=False)
        right_chunk = mesh["verts_right"][::stride].astype(np.float32, copy=False)
        left_root_chunk = mesh["root_left"][::stride].astype(np.float32, copy=False)
        right_root_chunk = mesh["root_right"][::stride].astype(np.float32, copy=False)
        if left_chunk.shape != right_chunk.shape:
            raise HTTPException(status_code=500, detail="Left/right sampled mesh shape mismatch")
        if left_chunk.shape[0] != (f1 - f0) or right_root_chunk.shape[0] != (f1 - f0):
            logger.warning(
                "切片帧数与 chunk 预期不一致。chunk=%s got=%s expected=%s",
                i,
                left_chunk.shape[0],
                f1 - f0,
            )
            raise HTTPException(status_code=500, detail="Chunk frame count mismatch after MANO slice")

        valid_left = np.asarray(mesh["valid_left"], dtype=np.uint8).reshape(-1)[::stride]
        valid_right = np.asarray(mesh["valid_right"], dtype=np.uint8).reshape(-1)[::stride]
        valid_left = _normalize_1d_length(valid_left, f1 - f0)
        valid_right = _normalize_1d_length(valid_right, f1 - f0)
        invalid_left_mask[f0:f1] = (valid_left <= 0).astype(np.uint8).tolist()
        invalid_right_mask[f0:f1] = (valid_right <= 0).astype(np.uint8).tolist()
        invalid_mask[f0:f1] = np.logical_and(valid_left <= 0, valid_right <= 0).astype(np.uint8).tolist()

        entry = _write_chunk(
            segment_dir=segment_dir,
            segment_frames=frames_per_chunk,
            left_chunk=left_chunk,
            right_chunk=right_chunk,
            left_root_chunk=left_root_chunk,
            right_root_chunk=right_root_chunk,
            stride=stride,
            fps=fps_from_data,
            frame_start=f0,
        )
        chunks[i] = entry
        still_building = i < num_chunks - 1
        index_now = _flush_index(still_building=still_building)
        if job_id:
            try:
                from app.services.play_cache import publish_progress

                publish_progress(
                    job_id=job_id,
                    chunk_root_dir=chunk_root_dir,
                    segment_rel=entry.get("bin"),
                    index_payload=index_now,
                    still_building=still_building,
                    frames_available=f1,
                    frame_count=sampled_count,
                    duration_sec=float(sampled_count * step_sec),
                    gop_size=frames_per_chunk,
                    sidecars=(i == 0),
                )
            except Exception as exc:
                logger.warning("play-cache 推流失败。job_id=%s chunk=%s err=%s", job_id, i, exc)
        if i == 0:
            logger.info(
                "首 chunk 就绪。source_name=%s chunk_count=%s first_chunk_id=%s frames=%s",
                source_name,
                num_chunks,
                entry["id"],
                f1 - f0,
            )
            if on_first_chunk_ready is not None:
                try:
                    on_first_chunk_ready(entry)
                except Exception as exc:
                    logger.warning("on_first_chunk_ready 回调异常。err=%s", exc)
        if on_chunk_ready is not None:
            try:
                on_chunk_ready(i, num_chunks, entry)
            except Exception as exc:
                logger.warning("on_chunk_ready 回调异常。idx=%s err=%s", i, exc)

    if any(item is None or not item.get("ready") for item in chunks):
        logger.warning("chunk 构建不完整。num_chunks=%s", num_chunks)
        raise HTTPException(status_code=500, detail="Chunk build incomplete")

    _flush_index(still_building=False)
    final_index_payload = _build_index_payload(
        fps_from_data=fps_from_data,
        frame_count=sampled_count,
        step_sec=step_sec,
        vertex_count=vertex_count,
        chunks=chunks,
        chunk_entries_total=num_chunks,
        invalid_mask=invalid_mask,
        invalid_left_mask=invalid_left_mask,
        invalid_right_mask=invalid_right_mask,
        video_projection=video_projection,
        face_count=int(faces.shape[0]),
        still_building=False,
        chunk_duration=chunk_duration,
    )

    manifest_payload = {
        "version": 7,
        "source_name": source_name,
        "chunk_root": f"/posehand/{unique_id}/chunks",
        "index_path": f"/posehand/{unique_id}/chunks/index.json",
        "preset": "balanced",
        "fps": float(fps_from_data),
    }
    manifest_path = local_root_dir / "manifest.json"
    _write_bytes(manifest_path, json.dumps(manifest_payload, indent=2).encode("utf-8"))

    result = {
        "chunk_root": f"/posehand/{unique_id}/chunks",
        "index_path": f"/posehand/{unique_id}/chunks/index.json",
        "index_payload": final_index_payload,
        "manifest_path": f"/posehand/{unique_id}/manifest.json",
        "manifest_payload": manifest_payload,
        "preset": "balanced",
        "cleanup_intermediate": True,
        "cached": False,
    }
    logger.info(
        "chunk 构建完成。source_name=%s frame_count=%s chunk_count=%s",
        source_name,
        sampled_count,
        len(chunks),
    )
    return result
