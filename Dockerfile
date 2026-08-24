# syntax=docker/dockerfile:1

# CPU worker + local SView HTTP cache (sview_worker.py by default)
# Build from repo root:  docker build -t mcap-processor-cpu:<tag> .
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    # Redis
    REDIS_HOST=localhost \
    REDIS_PORT=6379 \
    REDIS_DB=0 \
    REDIS_QUEUE_KEY=player:queue \
    # Local mounts (override in K8s)
    HEAD_CAM_MOUNT=/head-data \
    ORIN_MOUNT=/orin-data \
    LOCAL_OUTPUT_DIR=/cache \
    TEMP_DIR=/tmp \
    # HTTP
    HTTP_HOST=0.0.0.0 \
    HTTP_PORT=8080 \
    HTTP_ENABLED=1 \
    # Local-cache mode: do not upload processed bins to OSS
    NO_UPLOAD=1 \
    # FFmpeg on PATH
    FFMPEG_PATH=ffmpeg \
    LOG_LEVEL=INFO \
    # Persist project log on the cache volume (not ephemeral /app)
    LOG_FILE=/cache/sview.log \
    # Transcode defaults (override in K8s); SIZE/WIDTH/HEIGHT=0 keeps source
    TRANSCODE_SIZE=0 \
    TRANSCODE_WIDTH=0 \
    TRANSCODE_HEIGHT=0 \
    TRANSCODE_CRF=35 \
    TRANSCODE_BITRATE=2M \
    TRANSCODE_MAXRATE=3M \
    TRANSCODE_BUFSIZE=4M \
    TRANSCODE_PRESET=veryfast \
    TRANSCODE_FORCE_REENCODE=0 \
    PROGRESSIVE_PUBLISH=1 \
    PARTIAL_MANIFEST_EVERY=30 \
    PLAYABLE_MIN_FRAMES=30 \
    ORIN_CHUNK_FRAMES=30 \
    # Optional defaults for POST /api/test/jobs when body.source is omitted
    BENCH_H264_SOURCE= \
    BENCH_H265_SOURCE=

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Runtime modules only (bench clients live in scripts/, not in the image)
COPY path_map.py \
     job_status.py \
     bench_types.py \
     orin_images.py \
     sview_logging.py \
     sview_http.py \
     sview_worker.py \
     sview_work_gpu.py \
     oss_cache_gc.py \
     ./

# Data / cache mount points (K8s will bind real volumes)
RUN mkdir -p /head-data /orin-data /cache /tmp/sview_worker

EXPOSE 8080

# Default: CPU worker + Redis queue + HTTP :8080
# GPU one-shot: python sview_work_gpu.py (needs CUDA base image)
CMD ["python", "sview_worker.py"]
