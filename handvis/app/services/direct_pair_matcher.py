"""手动输入视频与 posehand 路径的配对解析。"""

import logging

from fastapi import HTTPException

from app.services.oss_object_store import list_oss_directory_page
from app.services.oss_path_utils import normalize_oss_path
from app.services.remote_scanner import PairedEpAsset

logger = logging.getLogger(__name__)

VIDEO_SUFFIXES = (".mp4", ".mov", ".m4v", ".avi", ".webm", ".mkv")
POSEHAND_SUFFIXES = (".pose3d_hand", ".posehand3d", ".posehand")
DIRECT_PAIR_LIMIT = 50


def strip_known_suffix(filename: str, suffixes: tuple[str, ...]) -> str:
    """去掉已知素材后缀，得到配对用的 item_name。"""
    lowered = filename.lower()
    for suffix in suffixes:
        if lowered.endswith(suffix):
            return filename[: -len(suffix)].strip()
    if "." in filename:
        return filename.rsplit(".", 1)[0].strip()
    return filename.strip()


def split_object_prefix(path: str) -> tuple[str, str]:
    """把 OSS 对象/前缀拆成父目录和文件名前缀。"""
    normalized = normalize_oss_path(path, require_trailing_slash=False)
    if normalized.endswith("/"):
        return normalized, ""
    parent, prefix = normalized.rsplit("/", 1)
    return f"{parent}/", prefix


def _matches_suffix(path: str, suffixes: tuple[str, ...]) -> bool:
    lowered = path.lower()
    return any(lowered.endswith(suffix) for suffix in suffixes)


def _direct_child_file(path: str, parent_dir: str) -> bool:
    if not path.startswith(parent_dir) or path.endswith("/"):
        return False
    return "/" not in path[len(parent_dir) :]


def _list_candidates_from_parent(parent_dir: str, prefix: str, suffixes: tuple[str, ...], *, limit: int) -> dict[str, list[str]]:
    page = list_oss_directory_page(parent_dir, prefix=prefix, max_keys=1000)
    candidates: dict[str, list[str]] = {}
    consumed = 0
    for raw_path in page.get("objects") or []:
        path = str(raw_path)
        if not _direct_child_file(path, parent_dir) or not _matches_suffix(path, suffixes):
            continue
        consumed += 1
        filename = path.rsplit("/", 1)[-1]
        stem = strip_known_suffix(filename, suffixes)
        if stem:
            candidates.setdefault(stem, []).append(path)
        if consumed >= limit:
            break
    return candidates


def list_prefix_candidates(path_prefix: str, suffixes: tuple[str, ...], *, limit: int = DIRECT_PAIR_LIMIT) -> dict[str, list[str]]:
    """列出某个 OSS 前缀下前 limit 个相关文件，按 stem 建索引。"""
    try:
        parent_dir, prefix = split_object_prefix(path_prefix)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid OSS path: {exc}") from exc

    candidates = _list_candidates_from_parent(parent_dir, prefix, suffixes, limit=limit)
    if candidates or str(path_prefix or "").strip().endswith("/"):
        return candidates

    # 用户常输入 .../seg 表示目录；前缀模式未命中时补一次目录模式。
    try:
        directory_prefix = normalize_oss_path(f"{path_prefix.rstrip('/')}/", require_trailing_slash=True)
    except ValueError:
        return candidates
    return _list_candidates_from_parent(directory_prefix, "", suffixes, limit=limit)


def match_manual_pair_prefixes(video_prefix: str, posehand_prefix: str, *, limit: int = DIRECT_PAIR_LIMIT) -> tuple[str, list[PairedEpAsset], str]:
    """按两个 OSS 前缀各取前 50 条并匹配同名素材。"""
    video_map = list_prefix_candidates(video_prefix, VIDEO_SUFFIXES, limit=limit)
    posehand_map = list_prefix_candidates(posehand_prefix, POSEHAND_SUFFIXES, limit=limit)
    matched_names = sorted(set(video_map).intersection(posehand_map))[:limit]
    ep_dir = split_object_prefix(video_prefix)[0]

    for name in matched_names:
        video_paths = video_map.get(name) or []
        posehand_paths = posehand_map.get(name) or []
        if len(video_paths) > 1 or len(posehand_paths) > 1:
            pair = PairedEpAsset(item_name=name, video_path=video_paths[0], posehand_path=posehand_paths[0])
            message = (
                "存在多对多关系，已给出第一条匹配。"
                f"item_name={name} video_count={len(video_paths)} posehand_count={len(posehand_paths)}"
            )
            return ep_dir, [pair], message

    pairs = [
        PairedEpAsset(item_name=name, video_path=video_map[name][0], posehand_path=posehand_map[name][0])
        for name in matched_names
    ]
    message = (
        f"已分别读取前 {limit} 条候选，匹配到 {len(pairs)} 组"
        if pairs
        else f"分别读取前 {limit} 条候选后未找到同名视频和 posehand3D"
    )
    return ep_dir, pairs, message


def manual_pair_from_paths(video_path: str, posehand_path: str = "") -> tuple[str, PairedEpAsset]:
    """根据两个 OSS 文件路径构造可播放配对。

    ep_dir 采用视频文件所在目录，item_name 采用视频文件名去后缀；posehand 文件名
    可以不同名，仍按用户显式输入配对。
    """
    try:
        normalized_video = normalize_oss_path(video_path, require_trailing_slash=False)
        normalized_posehand = (
            normalize_oss_path(posehand_path, require_trailing_slash=False)
            if (posehand_path or "").strip()
            else ""
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid OSS path: {exc}") from exc

    video_filename = normalized_video.rsplit("/", 1)[-1]
    video_stem = strip_known_suffix(video_filename, VIDEO_SUFFIXES)
    if not video_stem:
        raise HTTPException(status_code=422, detail="video_path 缺少可识别文件名")

    if normalized_posehand:
        pose_filename = normalized_posehand.rsplit("/", 1)[-1]
        pose_stem = strip_known_suffix(pose_filename, POSEHAND_SUFFIXES)
        if pose_stem and pose_stem != video_stem:
            logger.info(
                "手动配对文件名不一致，使用视频文件名作为 item_name。video_stem=%s pose_stem=%s",
                video_stem,
                pose_stem,
            )

    return (
        split_object_prefix(normalized_video)[0],
        PairedEpAsset(item_name=video_stem, video_path=normalized_video, posehand_path=normalized_posehand),
    )
