"""评分 PostgreSQL 数据库连接。

评分表由部署前的 SQL 脚本创建，应用生命周期不负责建表或迁移。
"""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import POSTGRES_CONNECT_TIMEOUT, SCORE_DB_URL

if not SCORE_DB_URL:
    raise RuntimeError("POSTGRES_* 评分数据库配置缺失，无法连接评分数据库")

score_engine = create_async_engine(
    SCORE_DB_URL,
    echo=False,
    pool_pre_ping=True,
    connect_args={"timeout": POSTGRES_CONNECT_TIMEOUT},
)
ScoreSessionLocal = async_sessionmaker(score_engine, class_=AsyncSession, expire_on_commit=False)
