# gpu-cpu-four-test（SView 头显 mcap 转码 + 边转边播）

## 目录结构

```text
.
├── sview_worker.py      # 主 Worker（CPU libx264 + NAL 拆帧，默认入口）
├── sview_http.py          # HTTP：入队 / 状态 / 缓存文件服务
├── job_status.py          # Redis 任务状态与队列
├── path_map.py            # oss:// → 本地挂载路径
├── bench_types.py         # 测试 type 1–8 定义（HTTP 运行时依赖）
├── orin_images.py         # Orin ROS2 CompressedImage (JPEG) 解析
├── sview_work_gpu.py      # GPU 备选 Worker（NVENC）
├── Dockerfile             # CPU Worker + HTTP 镜像
├── legacy/
│   └── server_cpu.py      # 旧 CPU Worker（无 NAL 拆帧），勿用于现网
├── scripts/               # 本地/线上压测客户端（不进镜像）
│   ├── bench_online.py
│   ├── bench_online_rounds.py
│   └── bench_cpu_transcode.py
├── deploy/                # K8s 清单
│   ├── deployment.yaml                    # 推荐：完整单副本 Deployment+Service
│   ├── deployment-cpu-redis.yaml          # 旧单副本清单
│   ├── deployment-cpu-redis-cluster.yaml  # 可选多副本粘滞
│   ├── job-pvc.yaml
│   ├── job-gpu.yaml
│   └── secret-template.yaml
├── docs/                  # 接口与流程文档
├── frontend/              # WebCodecs 播放器
├── output/                # 本地跑测产物（可删，已 gitignore）
└── requirements.txt
```

## 快速开始

```bash
# 依赖
pip install -r requirements.txt

# 本地：处理单个 mcap + HTTP
python sview_worker.py --path your.mcap --no-upload

# 本地：仅 HTTP（已有缓存）
python sview_worker.py --serve-only

# 线上同款：Redis 队列模式（默认 CMD）
python sview_worker.py
```

前端：`cd frontend && npm i && npm run dev`（见 `frontend/README.md`）。

## 文档索引

| 文档 | 说明 |
|------|------|
| [docs/sview-fullstack-flow.md](docs/sview-fullstack-flow.md) | 前后端流程 + 接口调用 |
| [docs/sview-frontend-api.md](docs/sview-frontend-api.md) | 前端对接（Master API / SSE / segments） |
| [docs/acs-deploy.md](docs/acs-deploy.md) | **ACS 部署步骤（Master/Worker + OSS）** |
| [docs/sview-oss-cache-roles.md](docs/sview-oss-cache-roles.md) | OSS `player-bin` 缓存 + Master/Worker + HPA/KEDA |
| [deploy/deployment-master.yaml](deploy/deployment-master.yaml) | Master Deployment + Service |
| [deploy/deployment-worker.yaml](deploy/deployment-worker.yaml) | Worker Deployment |
| [deploy/keda-worker.yaml](deploy/keda-worker.yaml) | 按 Redis 队列深度扩 Worker（推荐） |
| [docs/sview-test-api.md](docs/sview-test-api.md) | `/api/test/*` 压测 type |
| [docs/archive/](docs/archive/) | 过期草案，仅供参考 |

## 部署

**请按 [docs/acs-deploy.md](docs/acs-deploy.md) 操作**（Master + Worker + OSS `player-bin` + KEDA）。

```bash
docker build -t <REGISTRY>/playertrans-backend:<tag> .
docker push  <REGISTRY>/playertrans-backend:<tag>
# 改 deploy/deployment-master.yaml 与 deployment-worker.yaml 的 image tag，再：
kubectl apply -f deploy/secret-template.yaml      # Redis + OSS Secret
kubectl apply -f deploy/deployment-master.yaml
kubectl apply -f deploy/deployment-worker.yaml
kubectl apply -f deploy/keda-worker.yaml          # 可选；或 hpa-worker-cpu.yaml
```

注意：`REDIS_QUEUE_KEY=player:queue`（Master 与 Worker 必须一致）。

Worker 并发（可选 env）：`MAX_INFLIGHT_JOBS=2`、`OVERLAP_AFTER_PLAYABLE=1` — 第一个任务可播后再开下一个；头显大文件按块边转边播（首块=`PLAYABLE_MIN_FRAMES`）。

**多副本必读：** Worker 必须 `SVIEW_CLUSTER=1` + Downward API `POD_IP`。Master 可用 `SVIEW_CLUSTER=0`。取消：`POST /api/jobs/{job_id}/cancel`；队列：`GET /api/jobs/stats`；指标：`GET /metrics`。

日志：全项目共用当前工作目录下的 `sview.log`（可用 `LOG_FILE` 改名）。Worker / HTTP / GPU / Orin / 压测脚本 / Vite 前端都会写入该文件。

## 压测脚本

```bash
python scripts/bench_online.py --base-url http://139.224.211.114:8080 --h264 "oss://..." --h265 "oss://..."
python scripts/bench_online_rounds.py --base-url http://139.224.211.114:8080 --rounds 10 --h264 "..." --h265 "..."
```
