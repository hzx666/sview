"""播放资产模型。

用途：
- 记录某一条 ep_asset_item 对应的 chunk 构建结果与统计元信息。
- 通过 ep_asset_item_id 保证“一个配对组一条播放记录”。
"""

from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.timezone import shanghai_now_naive
from app.models.ep_asset import Base


class PlaybackAsset(Base):
    __tablename__ = "playback_assets"

    # 主键：播放资产唯一标识。
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # 兼容字段：保留上层 EP 维度关联，便于历史查询与排错。
    ep_asset_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ep_assets.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    # 新主关联：精确绑定到某个配对项（同一 item 只保留一条构建记录）。
    ep_asset_item_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("ep_asset_items.id", ondelete="CASCADE"),
        unique=True,
        index=True,
        nullable=True,
    )
    unique_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    source_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    chunk_root: Mapped[str] = mapped_column(Text, nullable=False)
    index_path: Mapped[str] = mapped_column(Text, nullable=False)
    preset: Mapped[str] = mapped_column(String(32), nullable=False, default="balanced")
    fps: Mapped[int] = mapped_column(Integer, nullable=False, default=30)

    manifest_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    default_quality: Mapped[str] = mapped_column(String(32), nullable=False, default="balanced")
    face_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    stride: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    frame_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    vertex_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_duration: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    chunk_duration: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

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
