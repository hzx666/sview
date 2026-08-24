"""OSS 目录扫描与预览服务。

所有扫描均直连 OSS SDK，避免依赖额外的 SSH 主机及其 ossutil 环境。
"""

import logging
import re
from dataclasses import dataclass

from fastapi import HTTPException

from app.core.config import OSS_PREVIEW_LIMIT
from app.services.oss_object_store import list_oss_directory_page
from app.services.oss_path_utils import normalize_oss_path

logger = logging.getLogger(__name__)
PREVIEW_VIDEO_GUARD_OBJECTS = 100


@dataclass(slots=True)
class PairedEpAsset:
    """同一 ep_dir 下的一组候选资产（视频必有，posehand 可空）。"""

    item_name: str
    video_path: str
    posehand_path: str


@dataclass(slots=True)
class OssPreviewResult:
    """OSS 当前层预览结果，不参与入库。"""

    pairs: list[PairedEpAsset]
    child_dirs: list[str]
    scanned_lines: int
    truncated: bool
    has_more: bool = False
    next_cursor: str = ""
    pair_truncated: bool = False
    message: str | None = None


def validate_ep_dir(ep_dir: str) -> str:
    """校验并规范化 ep_dir 输入。"""
    try:
        return normalize_oss_path(ep_dir, require_trailing_slash=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid ep_dir: {exc}") from exc


def is_likely_leaf_ep_dir(ep_dir: str) -> bool:
    """判断输入是否为单个 ep 叶子目录（例如 .../ep1/）。"""
    normalized = (ep_dir or "").strip().rstrip("/")
    if not normalized.startswith("oss://"):
        return False
    tail = normalized[normalized.rfind("/") + 1 :]
    return bool(re.fullmatch(r"ep[\w-]*", tail, flags=re.IGNORECASE))


def ensure_leaf_ep_dir(ep_dir: str) -> str:
    """确保输入是可扫描目录。

    历史上只允许 ep* 目录（例如 .../ep1/），但线上已有“平铺多组素材”的普通目录。
    因此这里仅做基础格式校验，把“是否可配对”交给扫描结果判定。
    """
    normalized = validate_ep_dir(ep_dir)
    if not is_likely_leaf_ep_dir(normalized):
        logger.info("输入目录不是 ep* 命名，按普通可扫描目录处理。ep_dir=%s", normalized)
    return normalized


def _is_direct_child_file(path: str, parent_dir: str) -> bool:
    """判断对象是否为 parent_dir 下一层文件（不递归）。"""
    if not path.startswith(parent_dir):
        return False
    if path.endswith("/"):
        return False
    tail = path[len(parent_dir) :]
    return bool(tail) and "/" not in tail


def _item_name_from_path(path: str, suffix: str) -> str:
    name = path.rsplit("/", 1)[-1]
    return name[: -len(suffix)].strip()


def _matches_prefix(name: str, prefix: str) -> bool:
    cleaned_prefix = (prefix or "").strip()
    return not cleaned_prefix or name.startswith(cleaned_prefix)


def _object_key_from_oss_path(path: str) -> str:
    """把 oss://bucket/key 转成 OSS marker 需要的 object key。"""
    if path.startswith("oss://"):
        return path.replace("oss://", "", 1).split("/", 1)[1]
    return path


def _is_matching_direct_video(path: str, parent_dir: str, prefix: str) -> bool:
    """判断对象是否是当前目录下符合前缀的视频文件。"""
    if not (path.endswith(".mp4") and _is_direct_child_file(path, parent_dir)):
        return False
    return _matches_prefix(_item_name_from_path(path, ".mp4"), prefix)


def _parse_oss_pairs(paths: list[str], parent_dir: str, *, prefix: str = "") -> list[PairedEpAsset]:
    """从 OSS 对象路径提取当前层视频候选，posehand 缺失时保留为空。"""
    mp4_list = sorted(
        {
            p
            for p in paths
            if p.endswith(".mp4")
            and _is_direct_child_file(p, parent_dir)
            and _matches_prefix(_item_name_from_path(p, ".mp4"), prefix)
        }
    )
    posehand_list = sorted(
        {
            p
            for p in paths
            if p.endswith(".pose3d_hand")
            and _is_direct_child_file(p, parent_dir)
            and _matches_prefix(_item_name_from_path(p, ".pose3d_hand"), prefix)
        }
    )

    # 映射规则：以“文件名去后缀”作为配对键；孤立文件会在交集阶段被自然过滤。
    mp4_map: dict[str, str] = {}
    posehand_map: dict[str, str] = {}
    for path in mp4_list:
        stem = _item_name_from_path(path, ".mp4")
        # 同名重复时保留首个，避免路径抖动导致返回顺序不稳定。
        if stem and stem not in mp4_map:
            mp4_map[stem] = path
    for path in posehand_list:
        stem = _item_name_from_path(path, ".pose3d_hand")
        if stem and stem not in posehand_map:
            posehand_map[stem] = path

    pairs: list[PairedEpAsset] = []
    for item_name in sorted(mp4_map.keys()):
        pairs.append(
            PairedEpAsset(
                item_name=item_name,
                video_path=mp4_map[item_name],
                posehand_path=posehand_map.get(item_name, ""),
            )
        )

    if not pairs:
        raise HTTPException(
            status_code=422,
            detail=(
                "No .mp4 candidates found under ep_dir, "
                f"got mp4={len(mp4_list)}, posehand={len(posehand_list)}"
            ),
        )
    return pairs


def _parse_ep_child_dirs(paths: list[str], parent_dir: str, *, prefix: str = "") -> list[str]:
    """从 OSS 子目录路径提取父目录下一层子目录（不限制名称前缀）。"""
    paths = sorted({path for path in paths if path.endswith("/")})
    children: list[str] = []
    for path in paths:
        if not path.startswith(parent_dir) or path == parent_dir:
            continue
        tail = path[len(parent_dir) :]
        # 仅保留一层目录，避免把 ep1/foo/ 这类深层路径当作子目录。
        if not tail or tail.count("/") != 1:
            continue
        child_name = tail.strip("/")
        if _matches_prefix(child_name, prefix):
            children.append(path)
    return sorted(set(children))


def _extract_direct_child_dir(path: str, parent_dir: str) -> str | None:
    """从单个 oss 路径提取 parent_dir 下一层子目录。"""
    if not path.endswith("/"):
        return None
    if not path.startswith(parent_dir) or path == parent_dir:
        return None
    tail = path[len(parent_dir) :]
    if not tail or tail.count("/") != 1:
        return None
    return path


def _list_oss_directory(oss_dir: str, *, prefix: str = "") -> tuple[list[str], list[str]]:
    """遍历 OSS marker，返回目录下一层全部对象和子目录。

    刷新与构建需要得到完整配对结果，不能只读取预览接口的一页。
    """
    objects: list[str] = []
    child_dirs: list[str] = []
    marker = ""
    while True:
        page = list_oss_directory_page(oss_dir, prefix=prefix, marker=marker, max_keys=1000)
        objects.extend(str(path) for path in page.get("objects") or [])
        child_dirs.extend(str(path) for path in page.get("child_dirs") or [])
        if not page.get("is_truncated"):
            return sorted(set(objects)), sorted(set(child_dirs))
        next_marker = str(page.get("next_marker") or "")
        if not next_marker or next_marker == marker:
            raise HTTPException(status_code=502, detail="OSS directory listing returned an invalid next marker")
        marker = next_marker


def scan_ep_dir_over_oss(ep_dir: str, *, prefix: str = "") -> tuple[list[PairedEpAsset], str]:
    """直连 OSS 扫描 ep_dir，返回同名配对列表与来源标识。"""
    logger.info("开始直连 OSS 扫描目录。ep_dir=%s", ep_dir)
    objects, _ = _list_oss_directory(ep_dir, prefix=prefix)
    pairs = _parse_oss_pairs(objects, parent_dir=ep_dir, prefix=prefix)
    logger.info("直连 OSS 扫描成功。ep_dir=%s pair_count=%s", ep_dir, len(pairs))
    return pairs, "oss-sdk"


def scan_ep_children_over_oss(parent_dir: str) -> tuple[list[str], str]:
    """直连 OSS 扫描父目录，返回一层子目录列表与来源标识。"""
    logger.info("开始直连 OSS 扫描父目录子目录。parent_dir=%s", parent_dir)
    _, child_dirs = _list_oss_directory(parent_dir)
    ep_dirs = _parse_ep_child_dirs(child_dirs, parent_dir=parent_dir)
    logger.info("直连 OSS 扫描父目录成功。parent_dir=%s count=%s", parent_dir, len(ep_dirs))
    return ep_dirs, "oss-sdk"


def preview_ep_dir_over_oss(
    ep_dir: str,
    *,
    prefix: str = "",
    limit: int = OSS_PREVIEW_LIMIT,
    cursor: str = "",
) -> tuple[OssPreviewResult, str]:
    """按 OSS marker 预览当前目录候选，不计算总数。"""
    normalized_limit = max(1, min(100, int(limit or OSS_PREVIEW_LIMIT)))
    list_max_keys = min(1000, max(normalized_limit + 1, normalized_limit * 3 + 10))
    logger.info(
        "开始预览 OSS 目录。ep_dir=%s prefix=%s cursor=%s limit=%s",
        ep_dir,
        prefix,
        cursor,
        normalized_limit,
    )

    page = list_oss_directory_page(
        ep_dir,
        prefix=prefix,
        marker=cursor,
        max_keys=list_max_keys,
    )
    objects = list(page.get("objects") or [])
    child_dirs = sorted(set(str(x) for x in (page.get("child_dirs") or []) if str(x).strip()))
    next_marker = str(page.get("next_marker") or "")
    is_truncated = bool(page.get("is_truncated"))
    guard_objects = [
        str(path)
        for path in objects
        if _is_direct_child_file(str(path), ep_dir)
    ][:PREVIEW_VIDEO_GUARD_OBJECTS]
    if len(guard_objects) >= PREVIEW_VIDEO_GUARD_OBJECTS and not any(
        _is_matching_direct_video(path, ep_dir, prefix) for path in guard_objects
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                f"当前页前 {PREVIEW_VIDEO_GUARD_OBJECTS} 条文件未发现视频，"
                "文件排序错乱，请输入更精确的文件名前缀后再查询。"
            ),
        )

    mp4_order: list[str] = []
    mp4_map: dict[str, str] = {}
    posehand_map: dict[str, str] = {}
    last_consumed_path = ""

    for path in objects:
        if path.endswith(".pose3d_hand") and _is_direct_child_file(path, ep_dir):
            stem = _item_name_from_path(path, ".pose3d_hand")
            if _matches_prefix(stem, prefix):
                posehand_map.setdefault(stem, path)
            continue
        if not (path.endswith(".mp4") and _is_direct_child_file(path, ep_dir)):
            continue
        stem = _item_name_from_path(path, ".mp4")
        if not _matches_prefix(stem, prefix) or stem in mp4_map:
            continue
        if len(mp4_order) >= normalized_limit:
            break
        mp4_order.append(stem)
        mp4_map[stem] = path
        last_consumed_path = path

    selected_children: list[str] = []
    if not mp4_order:
        selected_children = child_dirs[:normalized_limit]
        if selected_children:
            last_consumed_path = selected_children[-1]

    has_unconsumed_objects = bool(last_consumed_path and objects and objects[-1] != last_consumed_path)
    has_unconsumed_children = bool(selected_children and len(child_dirs) > len(selected_children))
    next_cursor = ""
    if has_unconsumed_objects or has_unconsumed_children:
        next_cursor = _object_key_from_oss_path(last_consumed_path)
    elif is_truncated:
        next_cursor = next_marker

    pairs: list[PairedEpAsset] = []
    for name in mp4_order:
        pairs.append(
            PairedEpAsset(
                item_name=name,
                video_path=mp4_map[name],
                posehand_path=posehand_map.get(name, ""),
            )
        )

    result = OssPreviewResult(
        pairs=pairs,
        child_dirs=selected_children,
        scanned_lines=len(objects) + len(child_dirs),
        truncated=is_truncated or bool(next_cursor),
        has_more=bool(next_cursor),
        next_cursor=next_cursor,
        pair_truncated=bool(next_cursor and pairs),
        message=None,
    )
    logger.info(
        "OSS 目录预览完成。ep_dir=%s pairs=%s children=%s cursor=%s next_cursor=%s has_more=%s",
        ep_dir,
        len(result.pairs),
        len(result.child_dirs),
        cursor,
        next_cursor,
        result.has_more,
    )
    return result, "oss-list"
