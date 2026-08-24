"""素材评分数据库模型。"""

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Text, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class ScoreBase(DeclarativeBase):
    """评分数据库独立元数据，避免与 HandVis 主业务库模型混用。"""


class MaterialScore(ScoreBase):
    """远端生产 RDS 评分记录。"""

    __tablename__ = "tmp_ss_data_platform_sview_comment"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    oss_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_name: Mapped[str] = mapped_column(Text, nullable=False)
    is_ego: Mapped[bool] = mapped_column(Boolean, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
