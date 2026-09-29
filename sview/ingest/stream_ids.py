"""Stable stream ids for teleop bins: keep the original topic, sanitize for files."""

from __future__ import annotations

import re

_FILE_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def bundle_stream_key(topic: str) -> str:
    """Bin / manifest stream key: original topic without a leading slash."""
    t = (topic or "").strip()
    return t.lstrip("/") or "cam"


def stream_file_slug(key: str, *, max_len: int = 80) -> str:
    """Filesystem-safe stem for encode temp files (topics may contain '/')."""
    raw = (key or "").strip() or "cam"
    slug = _FILE_SAFE.sub("_", raw).strip("._-")
    if not slug:
        slug = "cam"
    if len(slug) > max_len:
        slug = slug[:max_len].rstrip("._-") or "cam"
    return slug
