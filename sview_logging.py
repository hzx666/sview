#!/usr/bin/env python3
"""Shared logging for the whole SView project → console + file under cwd.

All Python entry points should call ``setup_logging(...)`` once. Child loggers
(``sview_http``, ``sview.orin_images``, scripts, …) propagate into the same file.

Env:
  LOG_LEVEL      default INFO
  LOG_FILE       default sview.log (relative → Path.cwd())
  LOG_TEE_STDOUT default 1 — also append print() stdout into the log file
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

_CONFIGURED = False
_LOG_PATH: Path | None = None

DEFAULT_LOG_FILE = "sview.log"


class _StdoutFileTee:
    """Mirror stdout writes into the project log file (for print())."""

    def __init__(self, stream, path: Path):
        self._stream = stream
        self._path = path

    def write(self, s: str) -> int:
        self._stream.write(s)
        if s:
            try:
                with open(self._path, "a", encoding="utf-8") as f:
                    f.write(s)
            except Exception:
                pass
        return len(s)

    def flush(self) -> None:
        self._stream.flush()

    def fileno(self):
        return self._stream.fileno()

    def isatty(self) -> bool:
        return self._stream.isatty()

    @property
    def encoding(self):
        return getattr(self._stream, "encoding", "utf-8")


def log_path() -> Path:
    """Resolved path of the project log file (cwd-relative by default)."""
    global _LOG_PATH
    if _LOG_PATH is not None:
        return _LOG_PATH
    name = os.environ.get("LOG_FILE", DEFAULT_LOG_FILE)
    p = Path(name)
    if not p.is_absolute():
        p = Path.cwd() / p
    _LOG_PATH = p
    return p


def setup_logging(name: str | None = None) -> logging.Logger:
    """Configure root handlers once; return a named (or root) logger."""
    global _CONFIGURED

    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(level)

    if not _CONFIGURED:
        # Drop leftover handlers (e.g. prior basicConfig) so everything goes
        # through our console + cwd file sinks.
        for h in list(root.handlers):
            root.removeHandler(h)

        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        sh.setLevel(level)
        root.addHandler(sh)

        path = log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setFormatter(fmt)
        fh.setLevel(level)
        root.addHandler(fh)

        tee = os.environ.get("LOG_TEE_STDOUT", "1").lower() in ("1", "true", "yes", "on")
        if tee and not isinstance(sys.stdout, _StdoutFileTee):
            sys.stdout = _StdoutFileTee(sys.stdout, path)  # type: ignore[assignment]

        _CONFIGURED = True
        root.info("Project logging → %s", path)

    return logging.getLogger(name) if name else root
