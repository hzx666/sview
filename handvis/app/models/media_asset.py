"""媒体资产模型。

职责：
- 管理原始视频与 Web 可播视频地址。
- 记录转码状态与探测结果，支撑“可播直连 / 不可播转码”策略。
"""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.timezone import shanghai_now_naive
from app.models.ep_asset import Base


class MediaAsset(Base):
    __tablename__ = "media_assets"
    __table_args__ = (
        CheckConstraint(
            "transcode_status IN ('pending', 'running', 'success', 'failed')",
            name="ck_media_assets_transcode_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    web_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    transcode_status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", index=True)
    transcode_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    probe_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=shanghai_now_naive,
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=shanghai_now_naive,
        server_default=func.now(),
        onupdate=shanghai_now_naive,
        nullable=False,
    )
