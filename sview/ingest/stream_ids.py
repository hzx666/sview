"""遥操 bin 的稳定流 id：保留原 topic，并做成文件名安全。"""

from __future__ import annotations

import re

_FILE_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def bundle_stream_key(topic: str) -> str:
    """bin / manifest 的流 key：原 topic，去掉开头的斜杠。"""
    t = (topic or "").strip()
    return t.lstrip("/") or "cam"


def stream_file_slug(key: str, *, max_len: int = 80) -> str:
    """编码临时文件用的文件名安全 stem（topic 里可能有 '/'）。"""
    raw = (key or "").strip() or "cam"
    slug = _FILE_SAFE.sub("_", raw).strip("._-")
    if not slug:
        slug = "cam"
    if len(slug) > max_len:
        slug = slug[:max_len].rstrip("._-") or "cam"
    return slug
