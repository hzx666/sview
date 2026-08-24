#!/usr/bin/env python3
"""Run online typed bench for N rounds; keep every round; trimmed-mean summary.

Trimmed mean: drop 1 lowest + 1 highest per metric, average the rest.
Requires at least 3 successful samples for a type metric.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from bench_online import run_type, write_report
from bench_types import BENCH_TYPES, list_bench_types
from sview_logging import setup_logging

log = setup_logging("bench_online_rounds")


def trimmed_mean(values: list[float]) -> float | None:
    vals = [float(v) for v in values if v is not None]
    if len(vals) < 3:
        return None if not vals else round(statistics.mean(vals), 3)
    vals = sorted(vals)
    core = vals[1:-1]  # drop lowest + highest
    return round(statistics.mean(core), 3)


def summarize_rounds(rounds: list[dict]) -> dict:
    """Aggregate per-type trimmed means across rounds."""
    by_type: dict[int, dict] = {}
    for tid, cfg in BENCH_TYPES.items():
        firsts: list[float] = []
        fulls: list[float] = []
        frames: list[int] = []
        ok_count = 0
        per_round = []
        for rnd in rounds:
            hit = next((r for r in rnd["results"] if r["type"] == tid), None)
            if not hit:
                continue
            entry = {
                "round": rnd["round"],
                "ok": hit.get("ok"),
                "first_frame_sec": hit.get("first_frame_sec"),
                "full_sec": hit.get("full_sec"),
                "frame_count": hit.get("frame_count"),
                "error": hit.get("error") or "",
            }
            per_round.append(entry)
            if hit.get("ok") and hit.get("first_frame_sec") is not None and hit.get("full_sec") is not None:
                ok_count += 1
                firsts.append(float(hit["first_frame_sec"]))
                fulls.append(float(hit["full_sec"]))
                if hit.get("frame_count") is not None:
                    frames.append(int(hit["frame_count"]))
        by_type[tid] = {
            "type": tid,
            "name": cfg["name"],
            "ok_rounds": ok_count,
            "total_rounds": len(rounds),
            "frame_count": frames[0] if frames else None,
            "first_frame_sec_all": firsts,
            "full_sec_all": fulls,
            "first_frame_sec_min": round(min(firsts), 3) if firsts else None,
            "first_frame_sec_max": round(max(firsts), 3) if firsts else None,
            "full_sec_min": round(min(fulls), 3) if fulls else None,
            "full_sec_max": round(max(fulls), 3) if fulls else None,
            "first_frame_sec_avg_trim": trimmed_mean(firsts),
            "full_sec_avg_trim": trimmed_mean(fulls),
            "rounds": per_round,
        }
    return by_type


def write_multi_report(out_dir: Path, base: str, rounds: list[dict], summary: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "base_url": base,
        "rounds_requested": len(rounds),
        "trim_rule": "drop 1 lowest + 1 highest, mean of remaining",
        "types": list_bench_types(),
        "rounds": rounds,
        "summary": summary,
    }
    (out_dir / "report_10rounds.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    lines = [
        "# 线上 CPU 转码 · 10 轮测试报告",
        "",
        f"- Base URL: `{base}`",
        f"- 轮数: {len(rounds)}",
        "- 均值规则: **去掉最高与最低各 1 个后求平均**（每指标独立）",
        "- 每轮明细见 `rounds/round_XX.json` 与下方汇总表",
        "",
        "## 去极值后平均值",
        "",
        "| # | 场景 | 首帧均值(s) | 全帧均值(s) | 首帧 min/max | 全帧 min/max | 成功轮次 | 帧数 |",
        "|---|------|----------:|----------:|-------------:|-------------:|--------:|-----:|",
    ]
    for tid in sorted(summary.keys()):
        s = summary[tid]
        lines.append(
            f"| {tid} | {s['name']} | "
            f"{'-' if s['first_frame_sec_avg_trim'] is None else s['first_frame_sec_avg_trim']} | "
            f"{'-' if s['full_sec_avg_trim'] is None else s['full_sec_avg_trim']} | "
            f"{s['first_frame_sec_min']}/{s['first_frame_sec_max']} | "
            f"{s['full_sec_min']}/{s['full_sec_max']} | "
            f"{s['ok_rounds']}/{s['total_rounds']} | "
            f"{'-' if s['frame_count'] is None else s['frame_count']} |"
        )

    lines += ["", "## 每轮全帧耗时 (s)", ""]
    header = "| 轮次 | " + " | ".join(f"T{t}" for t in sorted(summary.keys())) + " |"
    sep = "|-----:|" + "|".join("-------:" for _ in summary) + "|"
    lines += [header, sep]
    for rnd in rounds:
        cells = [str(rnd["round"])]
        for tid in sorted(summary.keys()):
            hit = next((r for r in rnd["results"] if r["type"] == tid), None)
            if not hit or not hit.get("ok") or hit.get("full_sec") is None:
                cells.append("-")
            else:
                cells.append(str(hit["full_sec"]))
        lines.append("| " + " | ".join(cells) + " |")

    lines += ["", "## 每轮首帧耗时 (s)", ""]
    lines += [header, sep]
    for rnd in rounds:
        cells = [str(rnd["round"])]
        for tid in sorted(summary.keys()):
            hit = next((r for r in rnd["results"] if r["type"] == tid), None)
            if not hit or not hit.get("ok") or hit.get("first_frame_sec") is None:
                cells.append("-")
            else:
                cells.append(str(hit["first_frame_sec"]))
        lines.append("| " + " | ".join(cells) + " |")

    (out_dir / "report_10rounds.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_dir / 'report_10rounds.md'}", flush=True)
    print(f"Wrote {out_dir / 'report_10rounds.json'}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--types", default="1,2,3,4,5,6,7,8")
    parser.add_argument("--h264", default=os.environ.get("BENCH_H264_SOURCE") or "")
    parser.add_argument("--h265", default=os.environ.get("BENCH_H265_SOURCE") or "")
    parser.add_argument("--poll", type=float, default=1.5)
    parser.add_argument("--timeout", type=float, default=1800.0)
    parser.add_argument(
        "--out",
        default=str(ROOT / "output" / "bench_online_10rounds"),
    )
    parser.add_argument("--start-round", type=int, default=1, help="resume from this round (1-based)")
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    out_dir = Path(args.out)
    rounds_dir = out_dir / "rounds"
    rounds_dir.mkdir(parents=True, exist_ok=True)

    type_ids = [int(x) for x in args.types.split(",") if x.strip()]
    h264 = args.h264 or None
    h265 = args.h265 or None

    rounds: list[dict] = []
    # Load prior rounds if resuming
    for i in range(1, args.start_round):
        p = rounds_dir / f"round_{i:02d}.json"
        if p.is_file():
            rounds.append(json.loads(p.read_text(encoding="utf-8")))
            print(f"loaded prior round {i}", flush=True)

    for rnd in range(args.start_round, args.rounds + 1):
        print(f"\n========== ROUND {rnd}/{args.rounds} ==========", flush=True)
        t0 = time.perf_counter()
        results = []
        for tid in type_ids:
            print(f"\n>>> round {rnd} type {tid}: {BENCH_TYPES[tid]['name']}", flush=True)
            r = run_type(
                base,
                tid,
                h264=h264,
                h265=h265,
                force=True,
                poll_sec=args.poll,
                timeout_sec=args.timeout,
            )
            results.append(r)
            print(
                f"    ok={r['ok']} first={r['first_frame_sec']} full={r['full_sec']} "
                f"frames={r['frame_count']} err={r.get('error') or '-'}",
                flush=True,
            )

        round_payload = {
            "round": rnd,
            "wall_sec": round(time.perf_counter() - t0, 3),
            "results": results,
        }
        (rounds_dir / f"round_{rnd:02d}.json").write_text(
            json.dumps(round_payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        # also keep a per-round markdown via existing helper
        write_report(rounds_dir / f"round_{rnd:02d}", base, results)
        rounds.append(round_payload)
        print(f"--- round {rnd} done in {round_payload['wall_sec']}s ---", flush=True)

        # rolling summary after each round
        summary = summarize_rounds(rounds)
        write_multi_report(out_dir, base, rounds, summary)

    summary = summarize_rounds(rounds)
    write_multi_report(out_dir, base, rounds, summary)
    ok = all(
        summary[t]["ok_rounds"] == summary[t]["total_rounds"] for t in type_ids if t in summary
    )
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
