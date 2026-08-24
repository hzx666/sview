"""API 路由聚合入口。

该模块统一挂载各个子路由，供 `app.main` 一次性引入。
"""

from fastapi import APIRouter

from app.core.config import HANDVIS_DISABLE_PG
from app.api.routes.ep_assets import router as ep_assets_router
from app.api.routes.pages import router as pages_router
from app.api.routes.playback.routes import router as playback_router
from app.api.routes.playback.sview_media import router as sview_media_router

api_router = APIRouter()
api_router.include_router(pages_router)
if not HANDVIS_DISABLE_PG:
    # 资产列表/编辑依赖 PG；无 PG 模式不挂载。
    api_router.include_router(ep_assets_router)
api_router.include_router(playback_router)
api_router.include_router(sview_media_router)
if not HANDVIS_DISABLE_PG:
    from app.api.routes.score_api import router as score_router

    api_router.include_router(score_router)
