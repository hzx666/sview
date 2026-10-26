"""在 HTTP 输出边界生成中文提示，保留 Worker 原始错误用于排查。"""

from typing import Any


_EXACT = {
    "source not found on local mount": "数据文件不存在。",
    "Invalid remote MCAP URL": "数据文件不存在。",
    "MCAP has no ChunkIndex; use progressive mode or reindex the source": "数据文件已损坏。",
    "no supported sparse MCAP video streams": "数据文件已损坏。",
    "no indexed video streams": "数据文件已损坏。",
    "no more decodable messages": "数据文件已损坏。",
    "PAGE_TIMEOUT": "数据源打开耗时较长，尚未完成加载，请稍后重试。",
    "missing source (oss://...mcap)": "缺少数据源地址（oss://...mcap）。",
    "publish_mode must be progressive or sparse": "publish_mode 必须为 progressive 或 sparse。",
    "start_sec/origin_sec cannot be combined with sparse mode": "稀疏模式不能同时指定 start_sec/origin_sec。",
    "sparse seek is not supported for HDF5; use progressive": "HDF5 不支持稀疏跳转，请使用渐进模式。",
    "handvis job needs posehand_path or ep_dir+item_name": "手部可视化任务需要 posehand_path 或 ep_dir+item_name。",
    "job not found": "任务不存在。",
    "encode request not found": "编码请求不存在。",
    "cancelled by client": "任务已由客户端取消。",
    "cancelled before start": "任务在开始前已取消。",
    "frame_index must be an integer": "frame_index 必须为整数。",
    "frame_index must be non-negative": "frame_index 不能为负数。",
    "frame_index past end": "帧索引超出数据范围。",
    "job is not in sparse publish mode": "任务未使用稀疏发布模式。",
    "sparse job has no source": "稀疏任务缺少数据源。",
    "type must be an integer 1-8": "type 必须为 1 至 8 的整数。",
    "OSS_CACHE disabled": "OSS 缓存未启用。",
    "signed URL used internal endpoint; set OSS_PUBLIC_ENDPOINT": "签名地址使用了内部端点，请配置 OSS_PUBLIC_ENDPOINT。",
    "not found": "请求的资源不存在。",
    "file not found": "数据文件不存在。",
    "asset not found": "资源不存在。",
    "asset unreadable": "资源无法读取。",
    "handvis page not found in image": "镜像中不存在手部可视化页面。",
    "handvis page unreadable": "手部可视化页面无法读取。",
    "kind must be segments": "kind 必须为 segments。",
    "kind=frames is not stored on OSS play cache; use kind=segments": "OSS 播放缓存不存储单帧，请使用 kind=segments。",
    "count/step must be positive": "count/step 必须为正数。",
    "no starts": "缺少起始帧。",
    "negative start": "起始帧不能为负数。",
    "owner pod unavailable": "任务所属的服务实例不可用。",
    "asset not ready yet": "资源尚未生成，请稍后重试。",
    "play cache not aligned yet": "播放缓存尚未完成对齐，请稍后重试。",
    "play cache missing or not contiguous from frame 0": "播放缓存缺失或未从第 0 帧连续生成。",
    "sparse segment advertised but object missing": "稀疏分段已发布，但对应文件不存在。",
    "sparse segment not encoded yet": "稀疏分段尚未编码，请稍后重试。",
    "segment past end": "分段超出数据范围。",
    "segment not ready yet": "分段尚未生成，请稍后重试。",
    "frame not ready yet": "帧尚未生成，请稍后重试。",
    "lost job lock (another owner or TTL expired)": "任务锁已丢失（已被其他实例持有或已过期）。",
    "body too large": "请求体过大。",
}

_PREFIX = {
    "Head-cam MCAP has no aligned video frames": "数据加载失败。",
    "Invalid remote MCAP URL": "数据文件不存在。",
    "probe failed:": "数据文件已损坏。",
    "invalid json:": "请求的 JSON 格式无效。",
    "sparse enqueue failed:": "稀疏任务入队失败，请稍后重试。",
    "redis enqueue failed:": "任务入队失败，请稍后重试。",
    "redis unavailable:": "Redis 服务不可用，请稍后重试。",
    "encode enqueue failed:": "编码任务入队失败，请稍后重试。",
    "bench_types not available:": "基准测试类型不可用。",
    "metrics unavailable:": "监控指标暂不可用。",
    "sign failed:": "媒体地址签名失败。",
    "missing frame ": "帧文件不存在。",
    "missing ": "缺少所需资源。",
    "segment start must be multiple of ": "分段起始帧必须为分段帧数的整数倍。",
    "invalid ": "请求参数无效。",
    "max ": "单次请求数量超过限制。",
}


def _translate(message: str, context: dict) -> str:
    if message in _EXACT:
        return _EXACT[message]
    for prefix, translation in _PREFIX.items():
        if message.startswith(prefix):
            return translation
    if context.get("encode_status") == "failed":
        return "数据文件已损坏。"
    return "数据加载失败。"


def localize_errors(value: Any) -> Any:
    """递归处理统计列表及任务；不修改原始状态，不凭空生成错误。"""
    if isinstance(value, list):
        return [localize_errors(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: localize_errors(item) for key, item in value.items()}
    if "error" in value:
        original = value["error"]
        result["error-en"] = original
        result["error"] = _translate(original, value) if original else original
    return result
