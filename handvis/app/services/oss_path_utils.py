"""OSS 路径归一化工具。

职责：
- 兼容输入 `oss://bucket/key` 与 `bucket/key` 两种写法；
- 根据支持桶列表自动识别并标准化为 `oss://bucket/key`；
- 统一目录/对象路径的格式校验，避免各模块重复实现。
"""

from app.core.config import SUPPORTED_OSS_BUCKETS


def _supported_bucket_set() -> set[str]:
    return {x.strip() for x in SUPPORTED_OSS_BUCKETS if x and x.strip()}


def normalize_oss_path(path: str, *, require_trailing_slash: bool = False) -> str:
    """规范化 OSS 路径为 `oss://bucket/key` 或 `oss://bucket/dir/`。

    输入允许：
    - `oss://ss-oss-intern/user/a/b`
    - `ss-oss1/user/a/b`
    """
    raw = (path or "").strip()
    if not raw:
        raise ValueError("OSS path cannot be empty")

    text = raw
    if not text.startswith("oss://"):
        bucket_set = _supported_bucket_set()
        for bucket in bucket_set:
            prefix = f"{bucket}/"
            if text.startswith(prefix):
                text = f"oss://{text}"
                break

    if not text.startswith("oss://"):
        raise ValueError("OSS path must start with oss:// or supported bucket name")

    without_scheme = text[len("oss://") :]
    slash_pos = without_scheme.find("/")
    if slash_pos <= 0:
        raise ValueError("OSS path must include bucket and object key")
    bucket = without_scheme[:slash_pos].strip()
    object_key = without_scheme[slash_pos + 1 :].strip()
    if not bucket or not object_key:
        raise ValueError("OSS path must include bucket and object key")

    bucket_set = _supported_bucket_set()
    if bucket_set and bucket not in bucket_set:
        raise ValueError(f"Unsupported OSS bucket: {bucket}")

    normalized = f"oss://{bucket}/{object_key}"
    if require_trailing_slash and not normalized.endswith("/"):
        normalized = f"{normalized}/"
    return normalized
