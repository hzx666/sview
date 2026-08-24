"""FastAPI 应用入口。

负责初始化日志、中间件、路由和静态资源挂载。
"""

import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.router import api_router
from app.core.config import (
    BASE_DIR,
    CORS_ALLOW_ORIGINS,
    DEV_DISABLE_HTTP_CACHE,
    PLAYBACK_POSEHAND_DIR,
    TRANSCODE_VIDEO_DIR,
    TRANSCODE_VIDEO_STATIC_PREFIX,
)
from app.core.logging_setup import setup_logging
from app.core.shutdown_signal_logging import install_shutdown_signal_logging
from app.db.database import lifespan

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="HandVis API", lifespan=lifespan)
app.add_event_handler("startup", install_shutdown_signal_logging)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def disable_cache_for_web_assets(request: Request, call_next):
    """开发期禁用页面与静态资源缓存，避免每次都要强制刷新。"""
    response = await call_next(request)
    if not DEV_DISABLE_HTTP_CACHE:
        return response

    path = request.url.path or ""
    if path in {"/", "/playback"} or path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

app.include_router(api_router)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "web" / "assets")), name="static")
PLAYBACK_POSEHAND_DIR.mkdir(parents=True, exist_ok=True)
TRANSCODE_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
# 新路径：posehand 产物目录（位于 HandVis 同级“统计”目录下）。
app.mount("/posehand", StaticFiles(directory=str(PLAYBACK_POSEHAND_DIR)), name="posehand")

# 转码视频静态目录（仅用于不兼容源视频的本地可播产物）。
app.mount(TRANSCODE_VIDEO_STATIC_PREFIX, StaticFiles(directory=str(TRANSCODE_VIDEO_DIR)), name="statistics-video")
logger.info("FastAPI 应用启动完成，静态资源路由已挂载。")
