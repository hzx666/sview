"""EP 资产相关 Pydantic 协议。

说明：
- 统一首页查询、刷新、列表与播放构建请求结构。
- BuildChunksRequest 强制携带 item_name，避免多组目录误选素材。
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class EpDirRequest(BaseModel):
    ep_dir: str = Field(..., description="OSS ep directory, must start with oss:// and end with /")

    @property
    def normalized_ep_dir(self) -> str:
        return self.ep_dir.strip()


class EpPreviewRequest(EpDirRequest):
    limit: int = Field(50, ge=1, le=200, description="Max preview items")
    page: int = Field(1, ge=1, description="Preview page number")
    page_size: int | None = Field(None, ge=1, le=100, description="Preview page size")
    cursor: str | None = Field(None, description="OSS marker cursor for current page")


class DirectPairPreviewRequest(BaseModel):
    video_path: str = Field(..., min_length=1, description="OSS video object path or prefix")
    posehand_path: str = Field("", description="OSS posehand object path or prefix")

    @property
    def normalized_video_path(self) -> str:
        return self.video_path.strip()

    @property
    def normalized_posehand_path(self) -> str:
        return self.posehand_path.strip()


class BuildChunksRequest(EpDirRequest):
    # 在同一 ep_dir 下唯一定位配对组。
    item_name: str = Field(..., min_length=1, max_length=255, description="Paired item name under ep_dir")
    force_rebuild: bool = Field(False, description="Whether to skip local chunk cache and rebuild")
    video_path: str | None = Field(None, description="Optional direct video object path, not prefix")
    posehand_path: str | None = Field(None, description="Optional direct posehand object path, not prefix")

    @property
    def normalized_item_name(self) -> str:
        return self.item_name.strip()

    @property
    def normalized_video_path(self) -> str:
        return (self.video_path or "").strip()

    @property
    def normalized_posehand_path(self) -> str:
        return (self.posehand_path or "").strip()

class EpAssetItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    item_name: str
    video_path: str
    posehand_path: str | None = None


class EpAssetRefreshResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    ep_dir: str
    # 当前 ep_dir 下全部可用配对组（已过滤无法配对文件）。
    items: list[EpAssetItemResponse]
    source: str
    updated_at: datetime


class EpAssetDebugResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    ep_dir: str
    video_path: str | None
    posehand_path: str | None
    # 仅用于列表快速判断是否为多组目录。
    items_count: int = 0
    source_host: str
    scan_status: str
    scan_error: str | None
    created_at: datetime
    updated_at: datetime


class EpAssetListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[EpAssetDebugResponse]


class EpAssetNameUpdateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="Display name of ep asset")

    @property
    def normalized_name(self) -> str:
        return self.name.strip()


class EpChildDirItem(BaseModel):
    """父目录下的 EP 子目录条目。"""

    name: str
    ep_dir: str
    cached: bool = False


class EpChildDirListResponse(BaseModel):
    """父目录解析结果。"""

    parent_dir: str
    total: int
    items: list[EpChildDirItem]


class EpAssetPreviewResponse(BaseModel):
    """只读目录预览结果，不写入数据库。"""

    ep_dir: str
    prefix: str = ""
    limit: int = 50
    page: int = 1
    page_size: int = 50
    offset: int = 0
    cursor: str = ""
    next_cursor: str = ""
    has_more: bool = False
    source: str = "remote-preview"
    items: list[EpAssetItemResponse]
    child_dirs: list[EpChildDirItem]
    total: int
    scanned_lines: int = 0
    truncated: bool = False
    pair_truncated: bool = False
    message: str | None = None
