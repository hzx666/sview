"""OSS URL 签名封装。"""

from app.services.oss_object_store import sign_oss_path


def sign_video_url(video_path: str) -> str:
    """生成视频对象的签名访问 URL。"""
    return sign_oss_path(video_path)


def sign_oss_url(oss_path: str) -> str:
    """生成任意 OSS 对象的签名访问 URL。"""
    return sign_oss_path(oss_path)
