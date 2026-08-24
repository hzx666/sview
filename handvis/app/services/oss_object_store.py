"""OSS 对象存储访问封装。"""

import logging
import re
from pathlib import Path

import oss2
from fastapi import HTTPException

from app.core.config import (
    OSS_ACCESS_KEY_ID,
    OSS_ACCESS_KEY_SECRET,
    OSS_ENDPOINT,
    OSS_REGION,
    OSS_SECURITY_TOKEN,
    SIGNED_URL_EXPIRE_SECONDS,
)
from app.services.oss_path_utils import normalize_oss_path

logger = logging.getLogger(__name__)


def is_oss_path(path: str | None) -> bool:
    """判断字符串是否是合法 oss:// 路径。"""
    try:
        normalize_oss_path(path or "")
        return True
    except ValueError:
        return False


def parse_oss_path(oss_path: str) -> tuple[str, str]:
    """解析 oss://bucket/object 为 (bucket, object_key)。"""
    try:
        text = normalize_oss_path(oss_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Path must be a valid OSS path: {exc}") from exc
    match = re.match(r"^oss://([^/]+)/(.+)$", text)
    if not match:
        raise HTTPException(status_code=400, detail="Path must be a valid oss://bucket/object path")
    return match.group(1), match.group(2)


def _build_auth() -> oss2.Auth | oss2.StsAuth:
    """构建 OSS 鉴权对象。"""
    if not OSS_ACCESS_KEY_ID or not OSS_ACCESS_KEY_SECRET:
        raise HTTPException(
            status_code=500,
            detail=(
                "Missing OSS credentials. Please set OSS_ACCESS_KEY_ID and "
                "OSS_ACCESS_KEY_SECRET in environment."
            ),
        )
    if OSS_SECURITY_TOKEN:
        return oss2.StsAuth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_SECURITY_TOKEN)
    return oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)


def _endpoint() -> str:
    """获取 OSS endpoint。"""
    return OSS_ENDPOINT or f"https://oss-{OSS_REGION}.aliyuncs.com"


def _bucket_for_name(bucket_name: str) -> oss2.Bucket:
    """按 bucket 名称创建客户端。"""
    return oss2.Bucket(_build_auth(), _endpoint(), bucket_name)


def download_oss_to_file(oss_path: str, local_path: Path) -> Path:
    """下载 OSS 文件到本地路径。"""
    bucket_name, object_key = parse_oss_path(oss_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        bucket = _bucket_for_name(bucket_name)
        bucket.get_object_to_file(object_key, str(local_path))
        return local_path
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("下载 OSS 文件失败。oss_path=%s err=%s", oss_path, exc)
        raise HTTPException(status_code=502, detail=f"Failed to download OSS object: {exc}") from exc


def list_oss_directory_page(
    oss_dir: str,
    *,
    prefix: str = "",
    marker: str = "",
    max_keys: int = 100,
) -> dict:
    """按 OSS marker 分页列出当前目录下一层文件与子目录。"""
    bucket_name, base_key = parse_oss_path(oss_dir)
    base_prefix = base_key.rstrip("/") + "/"
    cleaned_prefix = (prefix or "").strip()
    object_prefix = f"{base_prefix}{cleaned_prefix}"
    normalized_max_keys = max(1, min(1000, int(max_keys or 100)))
    try:
        bucket = _bucket_for_name(bucket_name)
        result = bucket.list_objects(
            prefix=object_prefix,
            delimiter="/",
            marker=(marker or "").strip(),
            max_keys=normalized_max_keys,
        )
        objects = [
            f"oss://{bucket_name}/{item.key}"
            for item in getattr(result, "object_list", [])
            if getattr(item, "key", "") and not str(item.key).endswith("/")
        ]
        child_dirs = [
            f"oss://{bucket_name}/{str(item)}"
            for item in getattr(result, "prefix_list", [])
            if str(item).strip()
        ]
        return {
            "objects": objects,
            "child_dirs": child_dirs,
            "next_marker": str(getattr(result, "next_marker", "") or ""),
            "is_truncated": bool(getattr(result, "is_truncated", False)),
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("分页列出 OSS 目录失败。oss_dir=%s marker=%s err=%s", oss_dir, marker, exc)
        raise HTTPException(status_code=502, detail=f"Failed to list OSS directory: {exc}") from exc


def oss_object_exists(oss_path: str) -> bool:
    """判断 OSS 对象是否存在。"""
    bucket_name, object_key = parse_oss_path(oss_path)
    try:
        bucket = _bucket_for_name(bucket_name)
        return bool(bucket.object_exists(object_key))
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("检查 OSS 对象存在性失败。oss_path=%s err=%s", oss_path, exc)
        raise HTTPException(status_code=502, detail=f"Failed to check OSS object: {exc}") from exc


def upload_directory_to_oss(local_dir: Path, oss_prefix: str) -> int:
    """递归上传本地目录到 OSS 前缀。"""
    if not local_dir.exists() or not local_dir.is_dir():
        raise HTTPException(status_code=400, detail=f"Local directory not found: {local_dir}")

    bucket_name, base_key = parse_oss_path(oss_prefix)
    base_key = base_key.rstrip("/")
    bucket = _bucket_for_name(bucket_name)

    uploaded = 0
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_dir).as_posix()
        key = f"{base_key}/{rel}"
        try:
            bucket.put_object_from_file(key, str(path))
            uploaded += 1
        except Exception as exc:
            logger.warning("上传目录文件到 OSS 失败。key=%s err=%s", key, exc)
            raise HTTPException(status_code=502, detail=f"Failed to upload to OSS: {key}: {exc}") from exc
    return uploaded


def read_oss_object_bytes(oss_path: str) -> bytes:
    """读取 OSS 对象并返回二进制内容。"""
    bucket_name, object_key = parse_oss_path(oss_path)
    try:
        bucket = _bucket_for_name(bucket_name)
        return bucket.get_object(object_key).read()
    except oss2.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="OSS object not found")
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("读取 OSS 对象失败。oss_path=%s err=%s", oss_path, exc)
        raise HTTPException(status_code=502, detail=f"Failed to read OSS object: {exc}") from exc


def sign_oss_path(oss_path: str, expires_seconds: int | None = None) -> str:
    """生成 OSS 对象 GET 签名 URL。"""
    expires = SIGNED_URL_EXPIRE_SECONDS if expires_seconds is None else int(expires_seconds)
    if expires <= 0:
        raise HTTPException(status_code=500, detail="SIGNED_URL_EXPIRE_SECONDS must be greater than 0")
    bucket_name, object_key = parse_oss_path(oss_path)
    try:
        bucket = _bucket_for_name(bucket_name)
        return bucket.sign_url("GET", object_key, expires)
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("生成 OSS 签名失败。oss_path=%s err=%s", oss_path, exc)
        raise HTTPException(status_code=502, detail=f"Failed to generate signed OSS URL: {exc}") from exc


def list_oss_directory_page(
    oss_dir: str,
    *,
    prefix: str = "",
    marker: str = "",
    max_keys: int = 100,
) -> dict[str, object]:
    """按 OSS 原生 marker 分页列出当前目录的一页对象与一层子目录。

    这里刻意不计算总数，也不跨页补齐候选，行为对齐文件浏览器：后端只拉取
    OSS 返回的这一页，前端根据 next_marker 控制“下一页”。
    """
    bucket_name, base_key = parse_oss_path(oss_dir)
    base_key = base_key.rstrip("/") + "/"
    cleaned_prefix = (prefix or "").strip()
    object_prefix = f"{base_key}{cleaned_prefix}"
    normalized_max_keys = max(1, min(1000, int(max_keys or 100)))

    try:
        bucket = _bucket_for_name(bucket_name)
        result = bucket.list_objects(
            prefix=object_prefix,
            delimiter="/",
            marker=(marker or "").strip(),
            max_keys=normalized_max_keys,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("分页列出 OSS 目录失败。oss_dir=%s prefix=%s marker=%s err=%s", oss_dir, prefix, marker, exc)
        raise HTTPException(status_code=502, detail=f"Failed to list OSS directory: {exc}") from exc

    objects: list[str] = []
    for item in getattr(result, "object_list", []) or []:
        key = getattr(item, "key", "")
        if not key or key.endswith("/"):
            continue
        objects.append(f"oss://{bucket_name}/{key}")

    child_dirs: list[str] = []
    for item in getattr(result, "prefix_list", []) or []:
        key = getattr(item, "key", item)
        if not key:
            continue
        child_dirs.append(f"oss://{bucket_name}/{str(key).rstrip('/')}/")

    return {
        "objects": objects,
        "child_dirs": child_dirs,
        "next_marker": getattr(result, "next_marker", "") or "",
        "is_truncated": bool(getattr(result, "is_truncated", False)),
    }


def oss_object_exists(oss_path: str) -> bool:
    """判断 OSS 对象是否存在。"""
    bucket_name, object_key = parse_oss_path(oss_path)
    try:
        bucket = _bucket_for_name(bucket_name)
        return bool(bucket.object_exists(object_key))
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("判断 OSS 对象是否存在失败。oss_path=%s err=%s", oss_path, exc)
        raise HTTPException(status_code=502, detail=f"Failed to stat OSS object: {exc}") from exc
