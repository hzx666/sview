from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

ASIA_SHANGHAI = ZoneInfo("Asia/Shanghai")


def shanghai_now_naive() -> datetime:
    """返回上海时区当前时间（naive），用于业务表默认时间戳。"""
    return datetime.now(ASIA_SHANGHAI).replace(tzinfo=None)
