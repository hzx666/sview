import os
import socket
import time
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from sqlalchemy.engine import URL

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
APP_DB_HOST = os.getenv("APP_DB_HOST", "127.0.0.1").strip()
APP_DB_PORT = int(os.getenv("APP_DB_PORT", "5433"))
APP_DB_USER = os.getenv("APP_DB_USER", "root").strip()
APP_DB_PASSWORD = os.getenv("APP_DB_PASSWORD", "123456")
APP_DB_NAME = os.getenv("APP_DB_NAME", "handvis").strip() or "handvis"
APP_DB_CONNECT_TIMEOUT = int(os.getenv("APP_DB_CONNECT_TIMEOUT", "5"))
APP_DB_URL = URL.create(
    "postgresql+asyncpg",
    username=APP_DB_USER,
    password=APP_DB_PASSWORD,
    host=APP_DB_HOST,
    port=APP_DB_PORT,
    database=APP_DB_NAME,
)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "pgm-uf60971hzojtgi27.pg.rds.aliyuncs.com").strip()
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "ss_data_platform_admin").strip()
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Ss20251029!")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "data_platform_prod").strip()
POSTGRES_CONNECT_TIMEOUT = int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "5"))
DAILY_RESET_TIMEZONE = os.getenv("DAILY_RESET_TIMEZONE", "Asia/Shanghai").strip() or "Asia/Shanghai"
SCORE_DB_URL = URL.create(
    "postgresql+asyncpg",
    username=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DATABASE,
)
# 重启脚本先单进程校准表结构，再拉起多 worker；worker 内不重复执行 create_all。
HANDVIS_SKIP_CREATE_ALL = os.getenv("HANDVIS_SKIP_CREATE_ALL", "0") == "1"
# 无 PostgreSQL 模式（SView 风格队列 worker）：资产扫 OSS，状态走 Redis，缓存看 OSS。
HANDVIS_DISABLE_PG = os.getenv("HANDVIS_DISABLE_PG", "0").lower() in ("1", "true", "yes", "on")


def _compose_redis_url() -> str:
    """REDIS_URL 优先；否则用 REDIS_HOST/PORT/DB + 账号拼（K8s Secret 同款）。"""
    explicit = (os.getenv("REDIS_URL") or "").strip()
    if explicit:
        return explicit
    host = (os.getenv("REDIS_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    port = (os.getenv("REDIS_PORT") or "6379").strip() or "6379"
    db = (os.getenv("REDIS_DB") or "0").strip() or "0"
    user = os.getenv("REDIS_USERNAME") or ""
    password = os.getenv("REDIS_PASSWORD") or ""
    if user and password:
        auth = f"{quote(user, safe='')}:{quote(password, safe='')}@"
    elif password:
        auth = f":{quote(password, safe='')}@"
    else:
        auth = ""
    return f"redis://{auth}{host}:{port}/{db}"


REDIS_URL = _compose_redis_url()
BUILD_JOB_TTL_SECONDS = max(300, int(os.getenv("BUILD_JOB_TTL_SECONDS", "7200")))

OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")
OSS_SECURITY_TOKEN = os.getenv("OSS_SECURITY_TOKEN")
OSS_REGION = os.getenv("OSS_REGION", "cn-shanghai")
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT")
SIGNED_URL_EXPIRE_SECONDS = int(os.getenv("SIGNED_URL_EXPIRE_SECONDS", "1800"))
# 与 SView 同一套播放缓存：边算边推 OSS player-bin/{job_id}/segments/*.bin
OSS_CACHE = os.getenv("OSS_CACHE", "0").lower() in ("1", "true", "yes", "on")
OSS_CACHE_PREFIX = (os.getenv("OSS_CACHE_PREFIX", "player-bin") or "player-bin").strip("/")
OSS_CACHE_BUCKET = (os.getenv("OSS_CACHE_BUCKET") or os.getenv("OSS_BUCKET") or "ss-oss-data-platform").strip()
OSS_CACHE_PUBLIC = os.getenv("OSS_CACHE_PUBLIC", "0").lower() in ("1", "true", "yes", "on")
OSS_PUBLIC_BASE = (os.getenv("OSS_PUBLIC_BASE") or "").rstrip("/")
_oss_public_endpoint = os.getenv("OSS_PUBLIC_ENDPOINT", "oss-cn-shanghai.aliyuncs.com")
if _oss_public_endpoint.startswith("https://"):
    _oss_public_endpoint = _oss_public_endpoint[len("https://") :]
elif _oss_public_endpoint.startswith("http://"):
    _oss_public_endpoint = _oss_public_endpoint[len("http://") :]
OSS_PUBLIC_ENDPOINT = _oss_public_endpoint
PLAYABLE_MIN_FRAMES = max(1, int(os.getenv("PLAYABLE_MIN_FRAMES", "30") or "30"))
SVIEW_JOB_KEY_PREFIX = os.getenv("SVIEW_JOB_KEY_PREFIX", "sview:job:")
# 前端拉段走 SView Master；空则用相对路径 /api/jobs/{id}/（需 HandVis 或网关转发）。
SVIEW_MASTER_BASE = (os.getenv("SVIEW_MASTER_BASE") or "").rstrip("/")

CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://192.168.1.23:5173,http://localhost:5173,http://127.0.0.1:5173",
    ).split(",")
    if origin.strip()
]

WEB_INDEX_PATH = BASE_DIR / "app" / "web" / "index.html"
# 静态资源版本号：
# - 若显式设置 STATIC_ASSET_VERSION，则按该值走（便于发布固定版本）。
# - 未设置时默认使用进程启动时间戳，保证每次重启后前端资源 URL 变化，避免手动强刷缓存。
STATIC_ASSET_VERSION = (os.getenv("STATIC_ASSET_VERSION", "").strip() or str(int(time.time())))

# 后端日志配置
LOG_DIR = Path(os.getenv("LOG_DIR", str(BASE_DIR / "logs")))
LOG_FILE_PATH = LOG_DIR / os.getenv("LOG_FILE_NAME", "handvis-backend.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

BALANCED_CHUNK_DURATION = float(os.getenv("BALANCED_CHUNK_DURATION", "5.0"))
# 按帧切段：优先于时长。30 帧一段，便于边算边播；时长仅作 index 里的兼容展示。
BALANCED_CHUNK_FRAMES = max(1, int(os.getenv("BALANCED_CHUNK_FRAMES", "30")))
BALANCED_STRIDE = int(os.getenv("BALANCED_STRIDE", "1"))
# 资源目录：默认放在 HandVis 同级 HandAssets 目录下，统一承载本地可回放产物。
STATISTICS_DIR = Path(os.getenv("STATISTICS_DIR", str(BASE_DIR.parent / "HandAssets")))
OSS_PREVIEW_LIMIT = max(1, min(200, int(os.getenv("OSS_PREVIEW_LIMIT", "50"))))
# posehand 本地产物目录。
PLAYBACK_POSEHAND_DIR = Path(os.getenv("PLAYBACK_POSEHAND_DIR", str(STATISTICS_DIR / "posehand")))
# 转码后浏览器可播视频目录（H.264/AAC）。
TRANSCODE_VIDEO_DIR = Path(os.getenv("TRANSCODE_VIDEO_DIR", str(STATISTICS_DIR / "video")))
# 静态 URL 前缀：供后端返回给前端直接播放。
POSEHAND_STATIC_PREFIX = os.getenv("POSEHAND_STATIC_PREFIX", "/posehand").strip() or "/posehand"
TRANSCODE_VIDEO_STATIC_PREFIX = (
    os.getenv("TRANSCODE_VIDEO_STATIC_PREFIX", "/statistics/video").strip() or "/statistics/video"
)
# 转码并发上限：避免 ffmpeg 抢占过多 CPU 影响整体请求响应。
# 通过 Redis 令牌桶跨进程控制；多 worker 部署时全局总并发 = 此值。
TRANSCODE_MAX_CONCURRENCY = max(1, int(os.getenv("TRANSCODE_MAX_CONCURRENCY", "2")))
# chunk 构建（MANO 推理）并发上限：保留给 API 侧令牌桶；实际执行并发由 worker 副本数决定。
BUILD_MAX_CONCURRENCY = max(1, int(os.getenv("BUILD_MAX_CONCURRENCY", "5")))
# 外部集群 master 投递任务的队列；worker 只消费，不从 HTTP 拉任务。
BUILD_QUEUE_KEY = os.getenv("BUILD_QUEUE_KEY", "handvis:jobs:queue").strip() or "handvis:jobs:queue"
BUILD_PROCESSING_KEY_PREFIX = (
    os.getenv("BUILD_PROCESSING_KEY_PREFIX", "handvis:jobs:processing").strip()
    or "handvis:jobs:processing"
)
# 终态结果队列：master 用 BLPOP 收 succeeded/failed，不必调 HandVis HTTP。
BUILD_RESULT_QUEUE_KEY = (
    os.getenv("BUILD_RESULT_QUEUE_KEY", "handvis:jobs:result").strip() or "handvis:jobs:result"
)
# 进度/终态广播（含首 chunk 部分就绪）。master 可选 SUBSCRIBE。
BUILD_EVENT_CHANNEL = os.getenv("BUILD_EVENT_CHANNEL", "handvis:jobs:events").strip() or "handvis:jobs:events"
BUILD_QUEUE_BLOCK_SECONDS = max(1, int(os.getenv("BUILD_QUEUE_BLOCK_SECONDS", "5")))
# ACS 上建议设为 Pod 名，便于崩溃后按 worker 回收未完成任务。
BUILD_WORKER_ID = os.getenv("BUILD_WORKER_ID", "").strip() or socket.gethostname()
# 单 worker 内 PyTorch/OpenMP 线程数。ACS 上应 <= 该 Pod 的 vCPU，避免 CFS 限流空转。
TORCH_NUM_THREADS = max(1, int(os.getenv("TORCH_NUM_THREADS", "2")))
# 令牌等待超时（秒）：超时则任务标记为失败。
TOKEN_ACQUIRE_TIMEOUT_SECONDS = max(60, int(os.getenv("TOKEN_ACQUIRE_TIMEOUT_SECONDS", "1800")))
# 联合播放页等待转码完成的超时时间（秒）。
TRANSCODE_WAIT_TIMEOUT_SECONDS = max(30, int(os.getenv("TRANSCODE_WAIT_TIMEOUT_SECONDS", "600")))
PLAYBACK_CHUNK_WRITE_WORKERS = int(os.getenv("PLAYBACK_CHUNK_WRITE_WORKERS", "8"))

# 支持的 OSS 桶列表：用于按输入地址自动识别并归一化路径。
SUPPORTED_OSS_BUCKETS = [
    item.strip()
    for item in os.getenv(
        "SUPPORTED_OSS_BUCKETS",
        "ss-oss-intern,ss-oss1,ss-oss-external-qingyun",
    ).split(",")
    if item.strip()
]

# 开发态静态资源禁缓存：建议仅在本地开发时开启。
DEV_DISABLE_HTTP_CACHE = os.getenv("DEV_DISABLE_HTTP_CACHE", "0").strip() not in {"0", "false", "False"}
