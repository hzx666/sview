# HandVis 镜像（自包含 build context）。
#
# 构建：
#   cd <player-bin>/handvis
#   docker build -t ss-container-lib-registry.cn-shanghai.cr.aliyuncs.com/data-platform/handvis:v0.1.0 .
#

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
    PIP_TRUSTED_HOST=mirrors.aliyun.com \
    LOG_DIR=/app/logs \
    STATISTICS_DIR=/data/HandAssets \
    PLAYBACK_POSEHAND_DIR=/data/HandAssets/posehand \
    TRANSCODE_VIDEO_DIR=/data/HandAssets/video

WORKDIR /app

# FFmpeg 用于视频转码；libgomp 为 PyTorch CPU 推理提供 OpenMP。
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install --no-install-recommends -y ffmpeg libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

# MANO 权重必须打进镜像；缺失时构建直接失败，避免上线才报 500。
RUN test -f /app/_DATA/data/mano/MANO_RIGHT.pkl \
    && test -f /app/_DATA/data_left/mano_left/MANO_LEFT.pkl

RUN mkdir -p /app/logs /data/HandAssets/posehand /data/HandAssets/video

EXPOSE 8000

# 默认 HTTP API。Worker：python -m app.workers.build_worker
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
