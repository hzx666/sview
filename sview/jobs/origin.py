"""Encode-from-T: start_sec is seconds into the recording, not wall-clock."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Tuple, Union


def parse_start_sec(value: Any) -> float:
    """Non-negative seconds from bag start. Invalid / missing → 0."""
    if value is None or value is False or value == "":
        return 0.0
    try:
        sec = float(value)
    except (TypeError, ValueError):
        return 0.0
    if sec != sec or sec <= 0:  # NaN or non-positive
        return 0.0
    return min(sec, 24 * 3600)


def origin_job_id(base_id: str, start_sec: float) -> str:
    """Keep from-zero cache; origin windows get ``{stem}_o{seconds}``."""
    base = (base_id or "").strip() or "job"
    sec = parse_start_sec(start_sec)
    if sec <= 0:
        return base
    return f"{base}_o{int(round(sec))}"


def mcap_log_time_range(path: Union[str, Path]) -> Tuple[Optional[int], Optional[int]]:
    """(message_start_time, message_end_time) from MCAP summary, if present."""
    from mcap.reader import make_reader

    p = Path(path)
    if not p.is_file():
        return None, None
    try:
        with open(p, "rb") as fh:
            summary = make_reader(fh).get_summary()
    except Exception:
        return None, None
    stats = getattr(summary, "statistics", None) if summary else None
    if stats is None:
        return None, None
    t0 = getattr(stats, "message_start_time", None)
    t1 = getattr(stats, "message_end_time", None)
    try:
        t0i = int(t0) if t0 is not None else None
        t1i = int(t1) if t1 is not None else None
    except (TypeError, ValueError):
        return None, None
    if t0i is not None and t0i < 0:
        t0i = None
    if t1i is not None and t1i < 0:
        t1i = None
    return t0i, t1i


def cut_log_time_ns(path: Union[str, Path], start_sec: float) -> Optional[int]:
    """Absolute MCAP log_time where encode should begin, or None."""
    sec = parse_start_sec(start_sec)
    if sec <= 0:
        return None
    t0, _t1 = mcap_log_time_range(path)
    if t0 is None:
        return None
    return int(t0 + sec * 1e9)


def estimate_remaining_frames(
    summary_n: Optional[int],
    start_sec: float,
    t0: Optional[int] = None,
    t1: Optional[int] = None,
    *,
    fps: float = 30.0,
) -> Optional[int]:
    """Scale summary frame count after dropping ``start_sec`` from the start."""
    try:
        total = int(summary_n) if summary_n is not None else None
    except (TypeError, ValueError):
        total = None
    sec = parse_start_sec(start_sec)
    if sec <= 0:
        return total if total and total > 0 else None
    if t0 is not None and t1 is not None and t1 > t0:
        dur = (int(t1) - int(t0)) / 1e9
        if dur > 0:
            remain = max(0.0, dur - sec)
            if total and total > 0:
                return max(1, int(round(total * remain / dur)))
            rate = fps if fps > 0 else 30.0
            return max(1, int(round(remain * rate)))
    skip = int(round(sec * (fps if fps > 0 else 30.0)))
    if total and total > skip:
        return total - skip
    return None
