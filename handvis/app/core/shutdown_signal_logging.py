"""关闭信号日志：用于定位服务为何退出。

目标：
- 在收到常见退出信号时，记录信号名、PID、PPID；
- 不改变原有退出语义（记录后继续调用原 handler）。
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from types import FrameType

logger = logging.getLogger(__name__)

_INSTALL_LOCK = threading.Lock()
_INSTALLED = False
_SIGNALS = ("SIGTERM", "SIGINT", "SIGHUP", "SIGQUIT")


def _handler_name(handler) -> str:
    if handler is signal.SIG_DFL:
        return "SIG_DFL"
    if handler is signal.SIG_IGN:
        return "SIG_IGN"
    if callable(handler):
        return getattr(handler, "__name__", handler.__class__.__name__)
    return str(handler)


def _signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except Exception:
        return f"SIG_{signum}"


def _read_cmdline(pid: int) -> str:
    """读取 Linux /proc cmdline，失败时返回空串。"""
    path = f"/proc/{pid}/cmdline"
    try:
        raw = open(path, "rb").read()
    except Exception:
        return ""
    if not raw:
        return ""
    parts = [x.decode("utf-8", errors="replace") for x in raw.split(b"\x00") if x]
    return " ".join(parts)


def install_shutdown_signal_logging() -> None:
    """安装关闭信号日志包装器（幂等）。"""
    global _INSTALLED
    with _INSTALL_LOCK:
        if _INSTALLED:
            return

        installed_names: list[str] = []
        for sig_name in _SIGNALS:
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            try:
                previous = signal.getsignal(sig)
            except Exception as exc:
                logger.warning("读取信号处理器失败。signal=%s err=%s", sig_name, exc)
                continue

            def _wrapped(signum: int, frame: FrameType | None, *, _previous=previous):
                name = _signal_name(signum)
                logger.warning(
                    "收到退出信号。signal=%s signum=%s pid=%s ppid=%s ppid_cmd=%s prev=%s",
                    name,
                    signum,
                    os.getpid(),
                    os.getppid(),
                    _read_cmdline(os.getppid()),
                    _handler_name(_previous),
                )
                if frame is not None:
                    logger.warning(
                        "信号触发位置。file=%s line=%s func=%s",
                        frame.f_code.co_filename,
                        frame.f_lineno,
                        frame.f_code.co_name,
                    )

                # 继续走原始处理逻辑，避免改变 uvicorn 的优雅退出行为。
                if callable(_previous):
                    _previous(signum, frame)
                    return
                if _previous is signal.SIG_IGN:
                    return
                raise SystemExit(128 + int(signum))

            try:
                signal.signal(sig, _wrapped)
                installed_names.append(sig_name)
            except Exception as exc:
                logger.warning("安装信号日志失败。signal=%s err=%s", sig_name, exc)

        _INSTALLED = True
        logger.info(
            "关闭信号日志已启用。signals=%s pid=%s ppid=%s",
            ",".join(installed_names),
            os.getpid(),
            os.getppid(),
        )
