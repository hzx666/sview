#!/usr/bin/env python3
"""HTTP 测试接口和离线 bench 共用的类型定义（1–8）。"""

from __future__ import annotations

import os
from typing import Any, Optional

# type → 场景。`source_kind` 用环境变量 BENCH_H264_SOURCE / BENCH_H265_SOURCE 选默认文件。
BENCH_TYPES: dict[int, dict[str, Any]] = {
    1: {
        "name": "H264 直出(passthrough)",
        "source_kind": "h264",
        "size": 0,
        "crf": 35,
        "force_reencode": 0,
        "notes": "SIZE=0 FORCE=0；源 H264 不走 ffmpeg",
    },
    2: {
        "name": "H264 降分辨率 720x720",
        "source_kind": "h264",
        "size": 720,
        "crf": 35,
        "force_reencode": 0,
        "notes": "SIZE=720 强制缩放重编码；CRF=35",
    },
    3: {
        "name": "H264 降画质 CRF35",
        "source_kind": "h264",
        "size": 0,
        "crf": 35,
        "force_reencode": 1,
        "notes": "SIZE=0 FORCE=1 CRF=35；保持分辨率重编码",
    },
    4: {
        "name": "H264 720x720 + CRF35",
        "source_kind": "h264",
        "size": 720,
        "crf": 35,
        "force_reencode": 0,
        "notes": "SIZE=720 CRF=35",
    },
    5: {
        "name": "H265→H264 保持分辨率",
        "source_kind": "h265",
        "size": 0,
        "crf": 35,
        "force_reencode": 0,
        "notes": "HEVC 必须转码；SIZE=0 CRF=35",
    },
    6: {
        "name": "H265→H264 720x720",
        "source_kind": "h265",
        "size": 720,
        "crf": 35,
        "force_reencode": 0,
        "notes": "SIZE=720 CRF=35",
    },
    7: {
        "name": "H265→H264 降画质 CRF35",
        "source_kind": "h265",
        "size": 0,
        "crf": 35,
        "force_reencode": 0,
        "notes": "HEVC 无 passthrough；与 type=5 参数相同",
    },
    8: {
        "name": "H265→H264 720x720 + CRF35",
        "source_kind": "h265",
        "size": 720,
        "crf": 35,
        "force_reencode": 0,
        "notes": "与 type=6 参数相同",
    },
}


def get_bench_type(type_id: int) -> dict[str, Any]:
    cfg = BENCH_TYPES.get(int(type_id))
    if not cfg:
        raise KeyError(f"unknown bench type {type_id}; valid: 1-8")
    return dict(cfg)


def list_bench_types() -> list[dict[str, Any]]:
    out = []
    for tid, cfg in BENCH_TYPES.items():
        out.append(
            {
                "type": tid,
                "name": cfg["name"],
                "source_kind": cfg["source_kind"],
                "transcode_size": cfg["size"],
                "transcode_crf": cfg["crf"],
                "force_reencode": cfg["force_reencode"],
                "notes": cfg["notes"],
            }
        )
    return out


def bench_job_id(source: str, type_id: int) -> str:
    """按 (source, type) 固定缓存 id，场景之间不撞。"""
    from sview.jobs.job_status import job_id_from_source

    base = job_id_from_source(source)
    return f"{base}__t{int(type_id)}"


def default_source_for_kind(kind: str) -> Optional[str]:
    """从环境变量解析 h264/h265 bench 文件的默认 OSS 或本地路径。"""
    kind = (kind or "").lower()
    if kind == "h264":
        return (os.environ.get("BENCH_H264_SOURCE") or "").strip() or None
    if kind in ("h265", "hevc"):
        return (os.environ.get("BENCH_H265_SOURCE") or "").strip() or None
    return None


def resolve_bench_source(type_id: int, source: Optional[str] = None) -> str:
    """优先用显式 source，否则用该类型 kind 的环境变量默认值。"""
    source = (source or "").strip()
    if source:
        return source
    cfg = get_bench_type(type_id)
    default = default_source_for_kind(cfg["source_kind"])
    if not default:
        raise ValueError(
            f"missing source for type={type_id}; pass body.source or set "
            f"BENCH_{'H264' if cfg['source_kind'] == 'h264' else 'H265'}_SOURCE"
        )
    return default
