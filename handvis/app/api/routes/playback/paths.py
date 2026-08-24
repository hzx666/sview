"""播放资源本地路径与静态 URL 映射。"""

from pathlib import Path

from app.core.config import PLAYBACK_POSEHAND_DIR, POSEHAND_STATIC_PREFIX, TRANSCODE_VIDEO_STATIC_PREFIX


def local_posthands_root(unique_id: str) -> Path:
    """返回 unique_id 对应的本地 posehand 根目录。"""
    return PLAYBACK_POSEHAND_DIR / unique_id


def local_chunk_root(unique_id: str) -> Path:
    """返回 unique_id 对应的本地 chunk 根目录。"""
    return local_posthands_root(unique_id) / "chunks"


def local_index_url(unique_id: str) -> str:
    return f"{POSEHAND_STATIC_PREFIX}/{unique_id}/chunks/index.json"


def local_manifest_url(unique_id: str) -> str:
    return f"{POSEHAND_STATIC_PREFIX}/{unique_id}/manifest.json"


def local_chunk_root_url(unique_id: str) -> str:
    return f"{POSEHAND_STATIC_PREFIX}/{unique_id}/chunks"


def local_root_url(unique_id: str) -> str:
    return f"{POSEHAND_STATIC_PREFIX}/{unique_id}"


def transcoded_video_url(file_name: str) -> str:
    return f"{TRANSCODE_VIDEO_STATIC_PREFIX.rstrip('/')}/{file_name}"
