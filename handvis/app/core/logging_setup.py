"""统一日志初始化模块。

提供控制台 + 轮转文件双通道日志输出。
"""

import logging
import logging.config
from pathlib import Path

from app.core.config import LOG_BACKUP_COUNT, LOG_DIR, LOG_FILE_PATH, LOG_LEVEL, LOG_MAX_BYTES


def _ensure_log_dir(path: Path) -> None:
    """确保日志目录存在。"""
    path.mkdir(parents=True, exist_ok=True)


def setup_logging() -> None:
    """初始化全局 logging 配置。"""
    _ensure_log_dir(LOG_DIR)

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                },
                "access": {
                    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": LOG_LEVEL,
                    "formatter": "default",
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": LOG_LEVEL,
                    "formatter": "default",
                    "filename": str(LOG_FILE_PATH),
                    "maxBytes": LOG_MAX_BYTES,
                    "backupCount": LOG_BACKUP_COUNT,
                    "encoding": "utf-8",
                },
            },
            "root": {
                "level": LOG_LEVEL,
                "handlers": ["console", "file"],
            },
            "loggers": {
                "uvicorn": {
                    "level": LOG_LEVEL,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "uvicorn.error": {
                    "level": LOG_LEVEL,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "uvicorn.access": {
                    "level": LOG_LEVEL,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "fastapi": {
                    "level": LOG_LEVEL,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
            },
        }
    )

    logging.getLogger(__name__).info(
        "日志系统初始化完成。level=%s file=%s", LOG_LEVEL, str(LOG_FILE_PATH)
    )
