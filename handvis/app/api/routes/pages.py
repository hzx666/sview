"""前端页面入口路由。"""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from app.core.config import STATIC_ASSET_VERSION, WEB_INDEX_PATH

router = APIRouter()


def _render_html_with_asset_version(page_path) -> HTMLResponse:
    """读取 HTML 并注入静态资源版本号，支持按发版切换缓存键。"""
    html = page_path.read_text(encoding="utf-8")
    rendered = html.replace("__ASSET_VERSION__", STATIC_ASSET_VERSION)
    return HTMLResponse(content=rendered)


@router.get("/healthz")
async def healthz() -> dict:
    """K8s / Compose 探活用，不读库。"""
    return {"ok": True}


@router.get("/")
async def home() -> HTMLResponse:
    """返回首页文档。"""
    return _render_html_with_asset_version(WEB_INDEX_PATH)


@router.get("/playback")
async def playback_page() -> HTMLResponse:
    """返回播放页文档。"""
    page = WEB_INDEX_PATH.parent / "playback.html"
    return _render_html_with_asset_version(page)
