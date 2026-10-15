#!/usr/bin/env python3
"""HDF5 遥操 episode（Astribot DAgger 等）→ JPEG 帧，走 Orin 编码路径。

找出图像 dataset（RGB 数组，或逐帧 JPEG/PNG 字节）。流 key
保留相机名 / dataset 路径，不强制映射到 FR/FL/RR/RL。

Astribot 文件把相机放在 `images_dict/<cam>_rgbd/color/count_0`。这个叶子
往往就是像素数据（vlen JPEG/PNG、打包 RGB，或 RGB 栈），
而不是旁边的计数器。
"""

from __future__ import annotations

import gzip
import io
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sview.ingest.stream_ids import bundle_stream_key

log = logging.getLogger("sview.hdf5_images")

_HDF5_MAGIC = b"\x89HDF\r\n\x1a\n"
_JPEG_MAGIC = b"\xff\xd8"
_PNG_MAGIC = b"\x89PNG"
_GZIP_MAGIC = b"\x1f\x8b"

# 更长的 token 优先。
_CAM_NAME_TO_CH: Tuple[Tuple[str, str], ...] = (
    ("left_wrist_rgbd", "FL"),
    ("right_wrist_rgbd", "FR"),
    ("left_wrist", "FL"),
    ("right_wrist", "FR"),
    ("head_rgbd", "RR"),
    ("head_rgb", "RR"),
    ("cam_high", "RR"),
    ("cam_left", "FL"),
    ("cam_right", "FR"),
    ("cam_low", "RL"),
    ("left_hand", "FL"),
    ("right_hand", "FR"),
    ("head_camera", "RR"),
    ("torso_camera", "RL"),
    ("camera_head", "RR"),
    ("camera_left", "FL"),
    ("camera_right", "FR"),
    ("head", "RR"),
    ("chest", "RL"),
    ("torso", "RL"),
    ("left", "FL"),
    ("right", "FR"),
)

# 头 1280x720、腕 640x360 是公开数据里见过的 Astribot S1 尺寸。
_COMMON_HW: Tuple[Tuple[int, int], ...] = (
    (720, 1280),
    (360, 640),
    (480, 640),
    (240, 320),
    (1080, 1920),
    (720, 960),
    (480, 848),
    (376, 672),
    (720, 1280),
    (1280, 720),
    (640, 360),
    (640, 480),
    (1920, 1080),
    (960, 720),
    (320, 240),
)

_INDEX_LEAVES = {
    "index",
    "indices",
    "timestamp",
    "timestamps",
    "time",
    "times",
    "fps",
    "width",
    "height",
    "size",
    "length",
    "len",
    "step",
}


def is_hdf5_file(path: str | Path) -> bool:
    p = Path(path)
    if not p.is_file():
        return False
    if p.suffix.lower() in (".hdf5", ".h5", ".hdf"):
        return True
    try:
        with open(p, "rb") as fh:
            return fh.read(8) == _HDF5_MAGIC
    except Exception:
        return False


def _leaf(name: str) -> str:
    return name.replace("\\", "/").rstrip("/").split("/")[-1].lower()


def _parent(name: str) -> str:
    parts = name.replace("\\", "/").rstrip("/").split("/")
    return "/".join(parts[:-1])


def _camera_token(name: str) -> Optional[str]:
    """已知相机名 token（left_wrist_rgbd 等），与 FR/FL 槽位无关。"""
    low = name.replace("\\", "/").lower()
    leaf = _leaf(name)
    visual = any(
        tok in low
        for tok in ("image", "rgb", "camera", "cam", "video", "color", "wrist")
    )
    for token, _ch in _CAM_NAME_TO_CH:
        if token in ("left", "right", "head", "chest", "torso") and not visual:
            continue
        if leaf == token or low.endswith("/" + token) or f"/{token}/" in f"/{low}/":
            return token
    return None


def _channel_for_name(name: str) -> Optional[str]:
    token = _camera_token(name)
    if not token:
        return None
    for tok, ch in _CAM_NAME_TO_CH:
        if tok == token:
            return ch
    return None


def _stream_key_for_dataset(name: str, dset) -> Optional[str]:
    if _skip_non_image_path(name) or _is_index_array(dset):
        return None
    token = _camera_token(name)
    if token:
        return bundle_stream_key(token)
    if _looks_like_image_dataset(name, dset):
        return bundle_stream_key(_parent(name) or name)
    return None


def _looks_like_image_dataset(name: str, dset) -> bool:
    if _skip_non_image_path(name):
        return False
    if _is_index_array(dset):
        return False
    shape = tuple(getattr(dset, "shape", ()) or ())
    if len(shape) == 4 and _looks_hwc(shape[1:]):
        return True
    if len(shape) == 3 and _looks_hwc(shape):
        return True
    if _is_vlen_or_object(dset):
        low = name.lower()
        return any(
            tok in low for tok in ("image", "rgb", "color", "camera", "jpeg", "jpg", "video")
        )
    return False


def _skip_non_image_path(name: str) -> bool:
    low = name.lower()
    leaf = _leaf(name)
    if leaf in _INDEX_LEAVES:
        return True
    if any(tok in low for tok in ("depth", "seg", "mask", "point_cloud", "pcl", "lidar")):
        if "rgb" not in low and "color" not in low:
            return True
    return False


def _natural_key(name: str):
    return [int(p) if p.isdigit() else p.lower() for p in re.split(r"(\d+)", name)]


def _as_bytes(sample: Any) -> Optional[bytes]:
    if sample is None:
        return None
    if isinstance(sample, (bytes, bytearray, memoryview)):
        raw = bytes(sample)
        return raw or None
    if isinstance(sample, str):
        try:
            return sample.encode("latin-1")
        except Exception:
            return sample.encode("utf-8", errors="ignore") or None
    try:
        import numpy as np

        arr = np.asarray(sample)
        if arr.dtype == object:
            if arr.shape == ():
                return _as_bytes(arr.item())
            if arr.size == 1:
                return _as_bytes(arr.flat[0])
            return None
        if arr.ndim == 0:
            return None
        if arr.dtype == np.uint8 and arr.ndim == 1:
            return arr.tobytes()
        if arr.dtype.kind in ("S", "V", "U") and arr.ndim <= 1:
            return bytes(arr.tobytes().rstrip(b"\x00"))
    except Exception:
        return None
    return None


def _scan_image_blob(raw: bytes) -> Optional[bytes]:
    if not raw:
        return None
    for magic in (_JPEG_MAGIC, _PNG_MAGIC):
        idx = raw.find(magic)
        if idx >= 0:
            payload = raw[idx:]
            try:
                from PIL import Image

                im = Image.open(io.BytesIO(payload))
                im.load()
                if magic == _JPEG_MAGIC and idx == 0:
                    return payload
                im = im.convert("RGB")
                buf = io.BytesIO()
                im.save(buf, format="JPEG", quality=85)
                return buf.getvalue()
            except Exception:
                continue
    if raw.startswith(_GZIP_MAGIC):
        try:
            return _scan_image_blob(gzip.decompress(raw))
        except Exception:
            return None
    return None


def _hw_from_nbytes(n: int) -> Optional[Tuple[int, int]]:
    if n < 8 * 8 * 3 or n % 3 != 0:
        return None
    pix = n // 3
    for h, w in _COMMON_HW:
        if h * w == pix:
            return h, w
    # 拆成像样的视频分辨率（面积对上，宽高比 1:4 到 4:1）。
    side = int(pix ** 0.5)
    for h in range(max(8, side - 64), side + 65):
        if h > 0 and pix % h == 0:
            w = pix // h
            if 8 <= w <= 4096 and 0.2 <= (w / h) <= 5.0:
                return h, w
    return None


def _rgb_to_jpeg(arr) -> Optional[bytes]:
    from PIL import Image
    import numpy as np

    rgb = np.asarray(arr)
    if rgb.ndim == 3 and rgb.shape[0] in (3, 4) and rgb.shape[-1] not in (3, 4):
        rgb = np.transpose(rgb, (1, 2, 0))
    if rgb.ndim == 3 and rgb.shape[-1] == 4:
        rgb = rgb[:, :, :3]
    if rgb.ndim != 3 or rgb.shape[-1] != 3:
        return None
    if rgb.dtype != np.uint8:
        mx = float(np.nanmax(rgb)) if rgb.size else 0.0
        if mx <= 1.5:
            rgb = np.clip(rgb * 255.0, 0, 255).astype("uint8")
        else:
            rgb = np.clip(rgb, 0, 255).astype("uint8")
    buf = io.BytesIO()
    Image.fromarray(rgb, mode="RGB").save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def _looks_hwc(shape: Tuple[int, ...]) -> bool:
    if len(shape) != 3:
        return False
    a, b, c = (int(x) for x in shape)
    if c in (3, 4) and a >= 8 and b >= 8:
        return True
    if a in (3, 4) and b >= 8 and c >= 8:
        return True
    return False


def _is_vlen_or_object(dset) -> bool:
    try:
        import h5py
        import numpy as np

        dt = dset.dtype
        if dt == object or dt.kind in ("O", "S", "V"):
            return True
        return bool(h5py.check_vlen_dtype(dt) or h5py.check_string_dtype(dt))
    except Exception:
        return str(getattr(dset, "dtype", "")).lower() in ("object", "o")


def _is_index_array(dset) -> bool:
    """一维整数计数器为 True，像素或 JPEG payload 不是。"""
    import numpy as np

    shape = tuple(getattr(dset, "shape", ()) or ())
    dt = getattr(dset, "dtype", None)
    if dt is None or len(shape) != 1:
        return False
    if _is_vlen_or_object(dset):
        return False
    if dt == np.uint8 or (getattr(dt, "itemsize", 1) == 1 and dt.kind in ("u", "i")):
        return False
    return bool(np.issubdtype(dt, np.integer) or np.issubdtype(dt, np.floating))


def _describe_sample(dset) -> str:
    try:
        import numpy as np

        sample = dset[0]
        arr = np.asarray(sample)
        raw = _as_bytes(sample) or b""
        head = raw[:16].hex() if raw else ""
        return (
            f"sample0 type={type(sample).__name__} ndarray={arr.shape}/{arr.dtype} "
            f"nbytes={len(raw)} magic={head}"
        )
    except Exception as exc:
        return f"sample0 unreadable ({exc})"


def _deref(sample: Any, h5) -> Any:
    try:
        import h5py

        if isinstance(sample, h5py.Reference):
            return h5[sample]
        arr = sample
        import numpy as np

        if isinstance(arr, np.ndarray) and arr.shape == () and arr.dtype == object:
            item = arr.item()
            if isinstance(item, h5py.Reference):
                return h5[item]
    except Exception:
        return sample
    return sample


def _sample_to_jpeg(sample: Any, h5=None) -> Optional[bytes]:
    sample = _deref(sample, h5) if h5 is not None else sample
    try:
        import h5py

        if h5 is not None and isinstance(sample, h5py.Dataset):
            if sample.shape == ():
                return _sample_to_jpeg(sample[()], h5)
            return _sample_to_jpeg(sample[0] if len(sample.shape) >= 1 else sample[()], h5)
    except Exception:
        pass

    raw = _as_bytes(sample)
    if raw:
        blob = _scan_image_blob(raw)
        if blob:
            return blob
        hw = _hw_from_nbytes(len(raw))
        if hw:
            import numpy as np

            try:
                arr = np.frombuffer(raw, dtype=np.uint8).reshape(hw[0], hw[1], 3)
                return _rgb_to_jpeg(arr)
            except Exception:
                pass

    try:
        import numpy as np

        arr = np.asarray(sample)
        if arr.dtype == object:
            if arr.shape == ():
                return _sample_to_jpeg(arr.item(), h5)
            if arr.size >= 1:
                return _sample_to_jpeg(arr.flat[0], h5)
            return None
        if arr.ndim == 3 and _looks_hwc(tuple(arr.shape)):
            return _rgb_to_jpeg(arr)
        if arr.ndim == 2 and arr.shape[0] >= 8 and arr.shape[1] >= 8:
            if arr.dtype != np.uint8:
                arr = np.clip(arr, 0, 255).astype("uint8")
            from PIL import Image

            buf = io.BytesIO()
            Image.fromarray(arr, mode="L").convert("RGB").save(buf, format="JPEG", quality=85)
            return buf.getvalue()
        if arr.dtype == np.uint8 and arr.ndim == 1:
            hw = _hw_from_nbytes(arr.size)
            if hw:
                return _rgb_to_jpeg(arr.reshape(hw[0], hw[1], 3))
    except Exception:
        return None
    return None


def _extract_jpegs(dset, h5=None) -> Tuple[List[bytes], int, int]:
    import numpy as np

    shape = tuple(dset.shape)
    jpegs: List[bytes] = []
    w = h = 0

    def _note_size(blob: bytes) -> None:
        nonlocal w, h
        if w or not blob:
            return
        try:
            from PIL import Image

            im = Image.open(io.BytesIO(blob))
            w, h = im.size
        except Exception:
            pass

    if len(shape) == 4 and _looks_hwc(shape[1:]):
        for i in range(int(shape[0])):
            blob = _rgb_to_jpeg(dset[i])
            if blob:
                jpegs.append(blob)
                _note_size(blob)
        return jpegs, w, h

    if len(shape) == 3 and _looks_hwc(shape):
        blob = _rgb_to_jpeg(dset[()])
        if not blob:
            return [], 0, 0
        if shape[0] in (3, 4):
            hh, ww = int(shape[1]), int(shape[2])
        else:
            hh, ww = int(shape[0]), int(shape[1])
        return [blob], ww, hh

    n = int(shape[0]) if shape else 0
    # 别把单张打包图 (H*W*3,) 当成 N 个小“帧”。
    if len(shape) == 1 and not _is_vlen_or_object(dset) and n >= 8 * 8 * 3:
        blob = _sample_to_jpeg(np.asarray(dset[()]), h5)
        if blob:
            _note_size(blob)
            return [blob], w, h

    for i in range(n):
        blob = _sample_to_jpeg(dset[i], h5)
        if not blob:
            continue
        jpegs.append(blob)
        _note_size(blob)
    return jpegs, w, h


def _candidate_score(name: str, dset, n_frames: int) -> int:
    score = n_frames
    leaf = _leaf(name)
    shape = tuple(getattr(dset, "shape", ()) or ())
    if len(shape) == 4 and _looks_hwc(shape[1:]):
        score += 50
    if any(tok in leaf for tok in ("image", "color", "rgb", "jpeg", "jpg", "data", "compressed")):
        score += 5
    if leaf.startswith("count") and n_frames <= 1:
        score -= 5
    return score


def parse_hdf5_image_streams(
    path: str | Path,
) -> Tuple[Dict[str, List[bytes]], Dict[str, List[int]], Dict[str, dict]]:
    """从 HDF5 episode 读相机图像 → (jpeg_frames, timestamps_ns, metas)。"""
    import h5py

    p = Path(path)
    frames: Dict[str, List[bytes]] = defaultdict(list)
    timestamps: Dict[str, List[int]] = defaultdict(list)
    metas: Dict[str, dict] = {}
    found: List[str] = []

    with h5py.File(p, "r") as h5:
        by_key_candidates: Dict[str, List[Tuple[str, object]]] = defaultdict(list)
        by_group: Dict[Tuple[str, str], List[Tuple[str, object]]] = defaultdict(list)

        def _walk(group, prefix: str) -> None:
            for key in group.keys():
                item = group[key]
                name = f"{prefix}/{key}" if prefix else str(key)
                if isinstance(item, h5py.Group):
                    _walk(item, name)
                    continue
                shape = tuple(getattr(item, "shape", ()) or ())
                dt = str(getattr(item, "dtype", ""))
                found.append(f"{name}{shape}:{dt}")
                if _skip_non_image_path(name):
                    continue
                if _is_index_array(item):
                    log.debug("HDF5 skip index dataset %s shape=%s dtype=%s", name, shape, dt)
                    continue
                stream_key = _stream_key_for_dataset(name, item)
                if not stream_key:
                    continue
                by_key_candidates[stream_key].append((name, item))
                by_group[(stream_key, _parent(name))].append((name, item))

        _walk(h5, "")

        if not by_key_candidates:
            raise ValueError(
                f"No mapped camera image datasets in {p.name}; "
                f"datasets={found[:80]}"
            )

        fps = 30
        for ch, items in by_key_candidates.items():
            best: Optional[Tuple[int, str, List[bytes], int, int]] = None

            # 优先选已经是帧栈的那个 dataset。
            for name, dset in items:
                jpegs, w, h = _extract_jpegs(dset, h5)
                if not jpegs:
                    log.warning(
                        "HDF5 dataset %s produced no JPEG frames shape=%s dtype=%s %s",
                        name,
                        getattr(dset, "shape", None),
                        getattr(dset, "dtype", None),
                        _describe_sample(dset),
                    )
                    continue
                score = _candidate_score(name, dset, len(jpegs))
                if best is None or score > best[0]:
                    best = (score, name, jpegs, w, h)

            # `color/count_0`、`color/count_1` 等，每个只存一帧。
            if best is None or (best and best[2] and len(best[2]) <= 1):
                for (gch, parent), group_items in by_group.items():
                    if gch != ch or len(group_items) < 2:
                        continue
                    ordered = sorted(group_items, key=lambda row: _natural_key(row[0]))
                    stacked: List[bytes] = []
                    w = h = 0
                    for name, dset in ordered:
                        jpegs, ww, hh = _extract_jpegs(dset, h5)
                        stacked.extend(jpegs)
                        w = w or ww
                        h = h or hh
                    if len(stacked) > (len(best[2]) if best else 0):
                        best = (len(stacked), parent + "/*", stacked, w, h)

            if not best:
                continue
            _, name, jpegs, w, h = best
            frames[ch] = jpegs
            step = int(1e9 / max(1, fps))
            timestamps[ch] = [i * step for i in range(len(jpegs))]
            metas[ch] = {
                "width": w or 640,
                "height": h or 480,
                "fps": fps,
                "position": name,
                "codec": "jpeg",
                "source_codec": "jpeg",
                "stored_codec": "h264",
                "source_topic": name,
            }
            log.info(
                "HDF5 %s <- %s: %d frames %dx%d",
                ch,
                name,
                len(jpegs),
                metas[ch]["width"],
                metas[ch]["height"],
            )

    if not frames:
        raise ValueError(
            f"HDF5 had camera datasets but no decodable frames in {p}; "
            f"datasets={found[:80]}"
        )
    return dict(frames), dict(timestamps), metas
