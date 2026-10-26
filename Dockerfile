# syntax=docker/dockerfile:1

# CPU worker + local SView HTTP cache (sview_worker.py by default)
# Build from repo root:  docker build -t mcap-processor-cpu:<tag> .
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
    PIP_TRUSTED_HOST=mirrors.aliyun.com \
    # Redis
    REDIS_HOST=localhost \
    REDIS_PORT=6379 \
    REDIS_DB=0 \
    REDIS_QUEUE_KEY=player:queue \
    # Local mounts (override in K8s)
    # /head-data       <= OSS original_data/ego_human_data/head_cam/
    # /teleoperation   <= OSS original_data/teleoperation/{Orin,Agilex,Astribot,Franka,Unitree}/
    HEAD_CAM_MOUNT=/head-data \
    ORIN_MOUNT=/teleoperation \
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

# FFmpeg: H.264 transcode. libhdf5: Astribot .hdf5 (h5py). jpeg/png: Pillow.
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        ca-certificates \
        libhdf5-103-1 \
        libjpeg62-turbo \
        libpng16-16 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Runtime: sview package + CLI shims (bench clients live in scripts/, not in the image)
COPY sview/ ./sview/
COPY sview_worker.py sview_http.py sview_work_gpu.py ./

# Data / cache mount points (K8s binds OSS PVCs here)
RUN mkdir -p /head-data /teleoperation /hdf5-mcap /orin-data /cache /tmp/sview_worker

EXPOSE 8080

# Default: CPU worker + Redis queue + HTTP :8080
# GPU one-shot: python sview_work_gpu.py (needs CUDA base image)
CMD ["python", "sview_worker.py"]
