"""EP 子资产模型。

用途：
- 在同一 ep_dir 下存储多组同名配对（item_name -> mp4 + posehand）。
- 与 ep_assets 形成一对多关系，供前端按组展示与构建。
"""

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.timezone import shanghai_now_naive
from app.models.ep_asset import Base


class EpAssetItem(Base):
    __tablename__ = "ep_asset_items"
    __table_args__ = (
        UniqueConstraint("ep_asset_id", "item_name", name="uq_ep_asset_items_ep_asset_id_item_name"),
    )

    # 主键：配对条目唯一标识。
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # 外键：所属 EP 主资产。
    ep_asset_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ep_assets.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    # 配对键：由文件名去扩展名得到（例如 clip_001）。
    item_name: Mapped[str] = mapped_column(String(255), nullable=False)
    # 同名配对的媒体路径。
    video_path: Mapped[str] = mapped_column(Text, nullable=False)
    posehand_path: Mapped[str] = mapped_column(Text, nullable=False)
    # 关联媒体资产：记录 source/web URL 与转码状态。
    media_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    scan_status: Mapped[str] = mapped_column(String, nullable=False, default="ok")
    scan_error: Mapped[str | None] = mapped_column(Text, nullable=True)
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
