#!/usr/bin/env python3
"""Online bench client: hit /api/test/jobs for types 1-8 and write a report.

Usage:
  python scripts/bench_online.py --base-url http://139.224.211.114:8080
  python scripts/bench_online.py --base-url http://139.224.211.114:8080 --types 1,2,3 --h264 SOURCE --h265 SOURCE

Env (optional, same as server defaults):
  BENCH_H264_SOURCE / BENCH_H265_SOURCE
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bench_types import BENCH_TYPES, list_bench_types
from sview_logging import setup_logging

log = setup_logging("bench_online")


def _http_json(method: str, url: str, body: dict | None = None, timeout: float = 60.0) -> tuple[int, dict]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    last_err: Exception | None = None
    for attempt in range(1, 6):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(raw) if raw.strip() else {}
                return resp.status, payload
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw) if raw.strip() else {"error": str(e)}
            except Exception:
                payload = {"error": raw or str(e)}
            return e.code, payload
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.RemoteDisconnected, http.client.IncompleteRead) as e:
            last_err = e
            time.sleep(min(8.0, 0.5 * attempt * attempt))
            continue
        except Exception as e:
            # http.client may raise RemoteDisconnected as subclass of ConnectionResetError on some versions
            if "Remote end closed" in str(e) or "Connection reset" in str(e):
                last_err = e
                time.sleep(min(8.0, 0.5 * attempt * attempt))
                continue
            raise
    raise RuntimeError(f"HTTP {method} {url} failed after retries: {last_err}")


def run_type(
    base: str,
    type_id: int,
    *,
    h264: str | None,
    h265: str | None,
    force: bool,
    poll_sec: float,
    timeout_sec: float,
) -> dict:
    cfg = BENCH_TYPES[type_id]
    source = h264 if cfg["source_kind"] == "h264" else h265
    body: dict = {"type": type_id, "force": force}
    if source:
        body["source"] = source

    code, created = _http_json("POST", f"{base}/api/test/jobs", body)
    result = {
        "type": type_id,
        "name": cfg["name"],
        "ok": False,
        "http_create": code,
        "create": created,
        "final": None,
        "error": "",
        "first_frame_sec": None,
        "full_sec": None,
        "frame_count": None,
        "wall_poll_sec": None,
    }
    if code not in (200, 202):
        result["error"] = created.get("error") or f"create failed HTTP {code}"
        return result

    job_id = created.get("job_id")
    if not job_id:
        result["error"] = "missing job_id"
        return result

    if code == 200 and created.get("status") == "ready" and not force:
        result["ok"] = True
        result["final"] = created
        result["first_frame_sec"] = created.get("first_frame_sec")
        result["full_sec"] = created.get("full_sec")
        result["frame_count"] = created.get("frame_count")
        result["wall_poll_sec"] = 0.0
        return result

    t0 = time.perf_counter()
    deadline = t0 + timeout_sec
    last = created
    while time.perf_counter() < deadline:
        time.sleep(poll_sec)
        code, status = _http_json("GET", f"{base}/api/test/jobs/{job_id}")
        last = status
        if code != 200:
            continue
        st = (status.get("status") or "").lower()
        if st == "failed":
            result["error"] = status.get("error") or "job failed"
            result["final"] = status
            result["wall_poll_sec"] = round(time.perf_counter() - t0, 3)
            return result
        if st == "ready":
            # After a forced re-run, require worker timing fields so we don't
            # accept a stale ready response without measurements.
            full = status.get("full_sec")
            if force and full is None:
                continue
            result["ok"] = True
            result["final"] = status
            result["first_frame_sec"] = status.get("first_frame_sec")
            result["full_sec"] = full
            result["frame_count"] = status.get("frame_count")
            result["wall_poll_sec"] = round(time.perf_counter() - t0, 3)
            return result

    result["error"] = f"timeout after {timeout_sec}s"
    result["final"] = last
    result["wall_poll_sec"] = round(time.perf_counter() - t0, 3)
    return result


def write_report(out_dir: Path, base: str, results: list[dict]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "base_url": base,
        "types": list_bench_types(),
        "results": results,
    }
    (out_dir / "report.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    lines = [
        "# 线上 CPU 转码测试报告",
        "",
        f"- Base URL: `{base}`",
        "",
        "| # | 场景 | 首帧(s) | 全帧(s) | 帧数 | OK | 错误 |",
        "|---|------|--------:|--------:|-----:|----|------|",
    ]
    for r in results:
        lines.append(
            f"| {r['type']} | {r['name']} | "
            f"{'-' if r['first_frame_sec'] is None else r['first_frame_sec']} | "
            f"{'-' if r['full_sec'] is None else r['full_sec']} | "
            f"{'-' if r['frame_count'] is None else r['frame_count']} | "
            f"{'Y' if r['ok'] else 'N'} | {r.get('error') or ''} |"
        )
    (out_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_dir / 'report.md'}")
    print(f"Wrote {out_dir / 'report.json'}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Online typed bench client")
    parser.add_argument("--base-url", required=True, help="e.g. http://139.224.211.114:8080")
    parser.add_argument("--types", default="1,2,3,4,5,6,7,8", help="comma-separated type ids")
    parser.add_argument("--h264", default=os.environ.get("BENCH_H264_SOURCE") or "")
    parser.add_argument("--h265", default=os.environ.get("BENCH_H265_SOURCE") or "")
    parser.add_argument("--force", action="store_true", default=True)
    parser.add_argument("--no-force", action="store_true", help="allow cache hit")
    parser.add_argument("--poll", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=1800.0)
    parser.add_argument(
        "--out",
        default=str(ROOT / "output" / "bench_online"),
    )
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    code, health = _http_json("GET", f"{base}/healthz")
    if code != 200:
        print(f"healthz failed: HTTP {code} {health}", file=sys.stderr)
        return 1
    print(f"healthz ok: {health}")

    code, types_payload = _http_json("GET", f"{base}/api/test/types")
    if code != 200:
        print(
            f"/api/test/types missing (HTTP {code}). Deploy updated HTTP+worker first.",
            file=sys.stderr,
        )
        return 1
    print(f"types ok: {len(types_payload.get('types') or [])} scenarios")

    type_ids = [int(x) for x in args.types.split(",") if x.strip()]
    force = not args.no_force
    results = []
    for tid in type_ids:
        print(f"\n>>> type {tid}: {BENCH_TYPES[tid]['name']}", flush=True)
        r = run_type(
            base,
            tid,
            h264=args.h264 or None,
            h265=args.h265 or None,
            force=force,
            poll_sec=args.poll,
            timeout_sec=args.timeout,
        )
        results.append(r)
        print(
            f"    ok={r['ok']} first={r['first_frame_sec']} full={r['full_sec']} "
            f"frames={r['frame_count']} err={r.get('error') or '-'}",
            flush=True,
        )

    write_report(Path(args.out), base, results)
    return 0 if all(r["ok"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
