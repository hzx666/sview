from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.core.timezone import shanghai_now_naive


class Base(DeclarativeBase):
    pass


class EpAsset(Base):
    __tablename__ = "ep_assets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False, default="")
    ep_dir: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    video_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    posehand_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_host: Mapped[str] = mapped_column(String, nullable=False)
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
