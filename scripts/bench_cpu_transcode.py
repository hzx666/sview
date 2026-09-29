#!/usr/bin/env python3
"""CPU transcode benchmark: first-frame + full-job times under fixed settings."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sview.logging_setup import setup_logging

log = setup_logging("bench_cpu_transcode")

H264 = ROOT / "SSIEGO_Cam_000X_20260717_150735_124.mcap"
H265 = ROOT / "SSIEGO_Cam_002X_20260709_203231_777.mcap"
BENCH_ROOT = ROOT / "output" / "bench_cpu_transcode"
REPORT_JSON = BENCH_ROOT / "report.json"

FIXED_ENV = {
    "NO_UPLOAD": "1",
    "PROGRESSIVE_PUBLISH": "1",
    "PARTIAL_MANIFEST_EVERY": "30",
    "PLAYABLE_MIN_FRAMES": "30",
    "CPU_CONCURRENCY": "2",
    "CHUNK_FRAMES": "150",
    "HTTP_PORT": "0",
    "HTTP_ENABLED": "0",
    "LOG_LEVEL": "WARNING",
    "TRANSCODE_BITRATE": "2M",
    "TRANSCODE_MAXRATE": "3M",
    "TRANSCODE_BUFSIZE": "4M",
    "TRANSCODE_PRESET": "veryfast",
    "TRANSCODE_WIDTH": "0",
    "TRANSCODE_HEIGHT": "0",
    "PYTHONUNBUFFERED": "1",
}


@dataclass
class CaseResult:
    case_id: int
    name: str
    source_label: str
    source_path: str
    source_codec_expected: str
    transcode_size: int
    transcode_crf: int
    force_reencode: int
    ok: bool
    error: str
    first_frame_sec: float | None
    full_sec: float | None
    frame_count: int | None
    frames_available: int | None
    output_status: str | None
    wall_start: str
    notes: str


CASES = [
    {
        "case_id": tid,
        "name": cfg["name"],
        "source": cfg["source_kind"],
        "codec": "h264" if cfg["source_kind"] == "h264" else "hevc",
        "size": cfg["size"],
        "crf": cfg["crf"],
        "force": cfg["force_reencode"],
        "notes": cfg["notes"],
    }
    for tid, cfg in __import__("sview.jobs.bench_types", fromlist=["BENCH_TYPES"]).BENCH_TYPES.items()
]


def _source_path(label: str) -> Path:
    return H264 if label == "h264" else H265


def run_one_inprocess(cfg: dict) -> CaseResult:
    source = _source_path(cfg["source"])
    out_dir = BENCH_ROOT / f"case_{cfg['case_id']:02d}"
    if out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_dir / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    env_update = {
        **FIXED_ENV,
        "LOCAL_OUTPUT_DIR": str(out_dir),
        "TEMP_DIR": str(tmp),
        "TRANSCODE_SIZE": str(cfg["size"]),
        "TRANSCODE_CRF": str(cfg["crf"]),
        "TRANSCODE_FORCE_REENCODE": str(cfg["force"]),
    }
    for k, v in env_update.items():
        os.environ[k] = str(v)

    # Fresh import each subprocess invocation.
    import sview.worker.cpu as mod

    mod.TRANSCODE_SIZE = int(cfg["size"])
    mod.TRANSCODE_WIDTH = 0
    mod.TRANSCODE_HEIGHT = 0
    mod.TRANSCODE_CRF = int(cfg["crf"])
    mod.TRANSCODE_FORCE_REENCODE = bool(int(cfg["force"]))
    mod.NO_UPLOAD = True
    mod.PROGRESSIVE_PUBLISH = True
    mod.LOCAL_OUTPUT_DIR = out_dir
    mod.TEMP_DIR = tmp
    mod.CPU_CONCURRENCY = 2
    import threading

    mod.cpu_encode_lock = threading.Semaphore(2)

    t_first: list[float | None] = [None]
    t0 = time.perf_counter()
    wall_start = time.strftime("%Y-%m-%d %H:%M:%S")

    def progress_cb(done: int, total: int) -> None:
        if done >= 1 and t_first[0] is None:
            t_first[0] = time.perf_counter() - t0

    err = ""
    ok = True
    try:
        print(f"=== Case {cfg['case_id']}: {cfg['name']} ===", flush=True)
        print(
            f"source={source.name} size={cfg['size']} crf={cfg['crf']} force={cfg['force']}",
            flush=True,
        )
        mod.process_mcap(str(source), no_upload=True, progress_cb=progress_cb)
    except Exception as e:
        ok = False
        err = f"{type(e).__name__}: {e}"
        print(f"FAILED: {err}", flush=True)

    t_full = time.perf_counter() - t0

    frame_count = None
    frames_available = None
    output_status = None
    mf = out_dir / source.stem / "manifest.json"
    if mf.is_file():
        data = json.loads(mf.read_text(encoding="utf-8"))
        bundle = data.get("bundle") or {}
        frame_count = bundle.get("frame_count")
        frames_available = bundle.get("frames_available")
        output_status = data.get("status")

    if t_first[0] is None and ok:
        first_bin = out_dir / source.stem / "frames" / "000000.bin"
        if first_bin.is_file():
            t_first[0] = t_full

    print(
        f"first_frame={None if t_first[0] is None else round(t_first[0], 3)}s "
        f"full={t_full:.3f}s frames={frame_count}",
        flush=True,
    )

    result = CaseResult(
        case_id=cfg["case_id"],
        name=cfg["name"],
        source_label=cfg["source"],
        source_path=str(source),
        source_codec_expected=cfg["codec"],
        transcode_size=int(cfg["size"]),
        transcode_crf=int(cfg["crf"]),
        force_reencode=int(cfg["force"]),
        ok=ok,
        error=err,
        first_frame_sec=None if t_first[0] is None else round(t_first[0], 3),
        full_sec=round(t_full, 3),
        frame_count=frame_count,
        frames_available=frames_available,
        output_status=output_status,
        wall_start=wall_start,
        notes=cfg["notes"],
    )
    out_json = out_dir / "result.json"
    out_json.write_text(json.dumps(asdict(result), indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def run_one_subprocess(cfg: dict) -> CaseResult:
    env = os.environ.copy()
    env.update(FIXED_ENV)
    cmd = [sys.executable, str(Path(__file__).resolve()), "--case", str(cfg["case_id"])]
    print(f"\n>>> launching case {cfg['case_id']}: {cfg['name']}", flush=True)
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
    result_path = BENCH_ROOT / f"case_{cfg['case_id']:02d}" / "result.json"
    if not result_path.is_file():
        return CaseResult(
            case_id=cfg["case_id"],
            name=cfg["name"],
            source_label=cfg["source"],
            source_path=str(_source_path(cfg["source"])),
            source_codec_expected=cfg["codec"],
            transcode_size=cfg["size"],
            transcode_crf=cfg["crf"],
            force_reencode=cfg["force"],
            ok=False,
            error=f"missing result.json (exit={proc.returncode})",
            first_frame_sec=None,
            full_sec=None,
            frame_count=None,
            frames_available=None,
            output_status=None,
            wall_start="",
            notes=cfg["notes"],
        )
    data = json.loads(result_path.read_text(encoding="utf-8"))
    return CaseResult(**data)


def write_report(results: list[CaseResult]) -> None:
    try:
        import cpuinfo  # type: ignore

        cpu_name = cpuinfo.get_cpu_info().get("brand_raw", platform.processor())
    except Exception:
        cpu_name = platform.processor() or "AMD Ryzen 5 7500F 6-Core Processor"

    meta = {
        "cpu": cpu_name,
        "logical_processors": os.cpu_count(),
        "cpu_concurrency": 2,
        "chunk_frames": 150,
        "ffmpeg_preset": "veryfast",
        "bitrate": "2M / maxrate 3M / bufsize 4M",
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "definition": {
            "first_frame_sec": "process_mcap 开始 → 首次写出第 1 帧 bin（progress frames_done>=1）",
            "full_sec": "process_mcap 开始 → 全部完成返回（manifest done）",
        },
        "files": {
            "h264": {"path": str(H264), "bytes": H264.stat().st_size},
            "h265": {"path": str(H265), "bytes": H265.stat().st_size},
        },
    }
    payload = {"meta": meta, "results": [asdict(r) for r in results]}
    REPORT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    md = BENCH_ROOT / "report.md"
    lines = [
        "# CPU 转码基准报告",
        "",
        f"- CPU: `{meta['cpu']}`（逻辑核 {meta['logical_processors']}）",
        f"- 固定参数: `CPU_CONCURRENCY={meta['cpu_concurrency']}`, `CHUNK_FRAMES={meta['chunk_frames']}`, "
        f"preset=`{meta['ffmpeg_preset']}`, bitrate=`{meta['bitrate']}`",
        f"- 首帧: {meta['definition']['first_frame_sec']}",
        f"- 全帧: {meta['definition']['full_sec']}",
        f"- H264 文件: `{H264.name}` ({H264.stat().st_size / 1e6:.1f} MB)",
        f"- H265 文件: `{H265.name}` ({H265.stat().st_size / 1e6:.1f} MB)",
        "",
        "| # | 场景 | 首帧(s) | 全帧(s) | 帧数 | OK | 备注 |",
        "|---|------|--------:|--------:|-----:|----|------|",
    ]
    for r in results:
        lines.append(
            f"| {r.case_id} | {r.name} | "
            f"{'-' if r.first_frame_sec is None else r.first_frame_sec} | "
            f"{'-' if r.full_sec is None else r.full_sec} | "
            f"{'-' if r.frame_count is None else r.frame_count} | "
            f"{'Y' if r.ok else 'N'} | {r.notes} |"
        )
    md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {REPORT_JSON}", flush=True)
    print(f"Wrote {md}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", type=int, default=0, help="Run a single case id in-process")
    args = parser.parse_args()

    if not H264.is_file() or not H265.is_file():
        print("missing mcap test files", file=sys.stderr)
        return 1

    BENCH_ROOT.mkdir(parents=True, exist_ok=True)

    if args.case:
        cfg = next((c for c in CASES if c["case_id"] == args.case), None)
        if not cfg:
            print(f"unknown case {args.case}", file=sys.stderr)
            return 1
        r = run_one_inprocess(cfg)
        return 0 if r.ok else 2

    results = [run_one_subprocess(c) for c in CASES]
    write_report(results)
    return 0 if all(r.ok for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
