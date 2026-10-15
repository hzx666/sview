#!/usr/bin/env python3
"""整个 SView 项目共用的日志 → 控制台 + 当前目录下的文件。

所有 Python 入口都应调用一次 ``setup_logging(...)``。子 logger
（``sview_http``、``sview.orin_images``、脚本等）会进同一个文件。

环境变量：
  LOG_LEVEL      默认 INFO
  LOG_FILE       默认 sview.log（相对路径 → Path.cwd()）
  LOG_TEE_STDOUT 默认 1 — print() 的 stdout 也追加进日志文件
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
    """把 stdout 写入镜像到项目日志（给 print() 用）。"""

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
    """项目日志文件的解析路径（默认相对当前目录）。"""
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
    """配置一次 root handler，返回具名（或 root）logger。"""
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
        # 丢掉残留 handler（例如之前的 basicConfig），全部走
        # 我们的控制台和当前目录文件。
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
