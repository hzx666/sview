# ACS 部署指南（Master / Worker + OSS `player-bin`）

面向阿里云 ACS / ACK：API 与缓存治理跑 **Master**，转码跑 **Worker**，任务经 Redis 队列分发，播放缓存写 OSS `player-bin/`。

```text
Frontend ──API/SSE──► Master Service :8080
    │                      │
    │                 RPUSH Redis player:queue
    │                      │
    │                 BLPOP Worker × N  ──PUT──► OSS player-bin/{job_id}/
    │                      │
    └── media ──► Master（本地优先；缺本地时签名/回源 OSS）
```

相关清单：

| 文件 | 作用 |
|------|------|
| [deploy/deployment-master.yaml](../deploy/deployment-master.yaml) | Master Deployment + `sview-master-svc` |
| [deploy/deployment-worker.yaml](../deploy/deployment-worker.yaml) | Worker Deployment |
| [deploy/keda-worker.yaml](../deploy/keda-worker.yaml) | 按 Redis 队列深度扩 Worker（推荐） |
| [deploy/hpa-worker-cpu.yaml](../deploy/hpa-worker-cpu.yaml) | 无 KEDA 时的 CPU HPA |
| [deploy/secret-template.yaml](../deploy/secret-template.yaml) | Redis / OSS Secret 模板 |
| [deploy/oss-lifecycle-player-bin.md](../deploy/oss-lifecycle-player-bin.md) | OSS Lifecycle / CORS 说明 |
| [docs/sview-oss-cache-roles.md](sview-oss-cache-roles.md) | 角色与缓存语义 |

> 旧单进程清单 `deploy/deployment.yaml` / `deployment-cpu-redis.yaml` 仅作兼容，**新环境请用 Master+Worker 拆分**。

---

## 0. 前置条件

1. ACS/ACK 集群，命名空间建议：`mcap-extract`（与现 YAML 一致）。
2. 已有（或新建）PVC，挂载原始 mcap：
   - `mcap-input-head-oss-pvc` → Pod 内 `/head-data`
   - `mcap-input-teleoperation-oss-pvc` → Pod 内 `/orin-data`
3. 阿里云 Redis（与清单中 `REDIS_HOST` 一致，或改 YAML）。
4. OSS Bucket（默认 `ss-oss-data-platform`），内网 endpoint 给 Pod 用。
5. ACR 镜像仓库 + 集群内 `acr-secret`（imagePullSecrets）。
6. kubectl 已连上目标集群：`kubectl config current-context`。

---

## 1. 构建并推送镜像

本地改动（OSS 缓存、签名回源、Redis 3 兼容等）**必须打进新镜像**，不要继续用未含这些改动的旧 tag。

```bash
# 仓库根目录
export REGISTRY=ss-container-lib-registry.cn-shanghai.cr.aliyuncs.com/data-platform
export TAG=v0.3.0   # 自行递增，并同步改两个 Deployment 的 image

docker build -t ${REGISTRY}/playertrans-backend:${TAG} .
docker push ${REGISTRY}/playertrans-backend:${TAG}
```

然后编辑：

- `deploy/deployment-master.yaml` → `image: ...:${TAG}`
- `deploy/deployment-worker.yaml` → `image: ...:${TAG}`

---

## 2. 创建 Secret

复制模板后填真实值（**不要把 AK/SK 写进 Git**）：

```bash
cp deploy/secret-template.yaml /tmp/sview-secrets.yaml
# 编辑 /tmp/sview-secrets.yaml
kubectl apply -f /tmp/sview-secrets.yaml
```

至少需要：

```yaml
# redis-credentials
apiVersion: v1
kind: Secret
metadata:
  name: redis-credentials
  namespace: mcap-extract
type: Opaque
stringData:
  username: ""          # 无用户名可空
  password: "<REDIS_PASSWORD>"
---
# oss-credentials  （OSS_CACHE=1 时 Master GC / 回源、Worker 上传都需要）
apiVersion: v1
kind: Secret
metadata:
  name: oss-credentials
  namespace: mcap-extract
type: Opaque
stringData:
  akId: "<OSS_ACCESS_KEY_ID>"
  akSecret: "<OSS_ACCESS_KEY_SECRET>"
```

若尚未有拉镜像 Secret：

```bash
kubectl -n mcap-extract create secret docker-registry acr-secret \
  --docker-server=ss-container-lib-registry.cn-shanghai.cr.aliyuncs.com \
  --docker-username=<ACR_USER> \
  --docker-password=<ACR_PASSWORD>
```

---

## 3. 应用 Master / Worker

```bash
kubectl -n mcap-extract apply -f deploy/deployment-master.yaml
kubectl -n mcap-extract apply -f deploy/deployment-worker.yaml
```

核对：

```bash
kubectl -n mcap-extract get deploy,po,svc -l 'app in (sview-master,sview-worker)'
kubectl -n mcap-extract logs -l app=sview-master --tail=50
kubectl -n mcap-extract logs -l app=sview-worker --tail=50
```

期望日志关键字：

- Master：`role=master`、`OSS_CACHE=1`、`HTTP ...:8080`
- Worker：`role=worker`、`listening on 'player:queue'`、`OSS play cache: .../player-bin/{job_id}`

### 关键环境变量（已在 YAML 中）

| 变量 | Master | Worker | 说明 |
|------|--------|--------|------|
| `SVIEW_ROLE` | `master` | `worker` | 角色 |
| `NO_UPLOAD` | `1` | `1` | 不上传整包 processed_data |
| `OSS_CACHE` | `1` | `1` | 播放缓存 `player-bin/` |
| `OSS_CACHE_PREFIX` | `player-bin` | 同左 | OSS 前缀 |
| `OSS_ENDPOINT` | 内网 | 内网 | 上传/回源 |
| `OSS_PUBLIC_ENDPOINT` | 公网域名 | 同左 | 拼 `play_base` |
| `REDIS_QUEUE_KEY` | `player:queue` | **必须相同** | 队列 |
| `SVIEW_CLUSTER` | `0` | `1` + `POD_IP` | Worker 多副本锁身份 |
| `MAX_INFLIGHT_JOBS` | — | 默认 `3` | 单 Worker 并行任务数 |
| `CPU_CONCURRENCY` | — | 默认 `12` | 单 Worker ffmpeg 总槽位 |
| `ENCODE_JOB_MAX_CONCURRENT` | — | 默认 `4` | 单 job 硬上限（12/4 → 3 job 打满） |
| `ENCODE_PLAYABLE_MAX_CONCURRENT` | — | 默认 `1` | 可播后每任务最多占用的 ffmpeg 槽（让位给新任务） |
| `ENCODE_FOREGROUND_RESERVE` | — | 默认 `0`（自动约 25%） | 留给未可播任务的槽数 |
| `TRANSCODE_SIZE` | — | 默认 `1080` | 正方形输出边长；`0`=源分辨率 |
| `TRANSCODE_FORCE_REENCODE` | — | 默认 `1` | 强制重编码（关掉 H.264 直通，压缩才生效） |
| `TRANSCODE_CRF` | — | 默认 `28` | 高清压缩；越大越小越糊 |
| `TRANSCODE_BITRATE` | — | 默认 `0` | `0`=CRF+VBV；设成 `2M` 等则走 ABR |
| `TRANSCODE_MAXRATE` / `BUFSIZE` | — | `1.5M` / `3M` | CRF 模式码率天花板 |
| `ORIN_CRF` | — | 默认 `23` | Orin JPEG→H.264 质量 |
| `PLAYABLE_MIN_FRAMES` | — | `30` | 可播阈值 |
| `CACHE_MAX_BYTES` | ~500GiB | — | Master GC 上限 |

建议在 Master 上额外加上（若 YAML 尚未含，可在 ACS 控制台补）：

```yaml
- name: OSS_MEDIA_LOCAL_FIRST
  value: "1"
- name: OSS_MEDIA_REDIRECT
  value: "1"
```

含义：媒体优先本地 `/cache`；本地没有时再签名跳转/回源 OSS。**manifest 始终由 Master 返回 JSON**（避免浏览器跨域 302 解析失败）。

---

## 4. 对外暴露 Master

集群内 Service 名：`sview-master-svc:8080`。

对外任选其一：

1. **SLB / LoadBalancer Service**（改 `sview-master-svc` 的 `type: LoadBalancer`，或另建 LB Service）
2. **Ingress** 指到 `sview-master-svc:8080`
3. ACS 控制台「服务暴露」

前端 **Base URL** 填对外的 `http(s)://<master-host>:8080`。

健康检查：

```bash
curl -sS http://<master-host>:8080/healthz
curl -sS http://<master-host>:8080/api/jobs/stats
curl -sS http://<master-host>:8080/metrics
```

---

## 5. Worker 扩缩（推荐 KEDA）

```bash
# 集群需已安装 KEDA
kubectl apply -f deploy/keda-worker.yaml
```

- 触发：`LLEN player:queue`（`listLength: "2"` 表示约每 2 个排队任务扩 1 个副本）
- `minReplicaCount: 1` / `maxReplicaCount: 50` 可按预算改
- 认证引用 Secret `redis-credentials`

无 KEDA 时：

```bash
kubectl apply -f deploy/hpa-worker-cpu.yaml
```

**不要**让 Master 去 `kubectl scale` 或直连 Worker Pod IP。

---

## 6. OSS 控制台配置

### 6.1 Lifecycle（兜底）

见 [deploy/oss-lifecycle-player-bin.md](../deploy/oss-lifecycle-player-bin.md)：前缀 `player-bin/`，例如 **2 天**过期删除。应用层 GC（空闲 TTL + 500G LRU）仍是主策略。

### 6.2 公网签名 vs 内网上传

| 用途 | 环境变量 | 示例 |
|------|----------|------|
| Pod 上传 / Master 回源 | `OSS_ENDPOINT` | `oss-cn-shanghai-internal.aliyuncs.com` |
| 浏览器签名 URL / `play_base` | `OSS_PUBLIC_ENDPOINT` | `oss-cn-shanghai.aliyuncs.com` |

浏览器打不开带 `*-internal*` 的签名链（「无法加载响应数据」）——那是 VPC 内网域名。Master 签名已改为走公网 endpoint。

### 6.3 CORS

Bucket 私有（Block Public Access）时，浏览器**不要**直打裸 `play_base`。前端应打 Master 媒体 API；Master 本地优先，缺文件时再签名/回源。

若仍希望浏览器直拉签名 URL，CORS 需放行前端 Origin，例如：

- AllowedOrigins：前端域名（及本地调试 `http://127.0.0.1:5173`）
- AllowedMethods：`GET`, `HEAD`
- AllowedHeaders：`*`
- ExposeHeaders：`ETag`, `Content-Range`, `Accept-Ranges`, `Content-Length`

本地合并 CORS 可用：`scripts/fix_oss_play_cache_access.py`（只改 CORS，勿强行 public-read）。

### 6.4 权限

RAM 用户至少：`oss:PutObject` / `GetObject` / `DeleteObject` / `ListObjects`（前缀 `player-bin/`），Master GC 需要删除能力。

---

## 7. 前端

1. 构建静态资源：`cd frontend && npm i && npm run build`
2. 部署到任意静态托管 / Nginx；或临时 `npm run dev`
3. 页面 **Base URL** = 对外 Master 地址
4. Source 示例：

```text
oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/<相对 /head-data 的路径>.mcap
```

路径必须能映射到 PVC 上的真实文件（见 `path_map.py`）。

---

## 8. 冒烟验收

```bash
# 入队（把 SOURCE 换成集群内真实存在的 mcap）
curl -sS -X POST http://<master>/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"source":"oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/....mcap","force":true}'

# 进度
curl -sS http://<master>/api/jobs/<job_id>
# 期望：playable=true 后有 frames_done；OSS 上出现 player-bin/<job_id>/segments/

# 媒体（应 200 JSON，不要长期卡在跨域 302）
curl -sS -D- http://<master>/api/jobs/<job_id>/manifest.json | head

# 队列 / 工人
curl -sS http://<master>/api/jobs/stats
curl -sS http://<master>/api/workers
```

OSS 控制台确认：`player-bin/<job_id>/manifest.json`、`segments/000000.bin` 已生成。

---

## 9. 日志与排障

| 现象 | 排查 |
|------|------|
| 入队 503 / Redis HSET 错 | Redis 版本过旧；代码需含 `job_status.set_job_fields` 多字段兼容 |
| `source not found on local mount` | PVC/路径映射不对；核对 `HEAD_CAM_MOUNT` 与 OSS URI |
| Worker 不消费 | `REDIS_QUEUE_KEY` 不一致；看 Worker 日志是否连上 Redis |
| 前端 manifest「无法解析」 | 确认镜像含「manifest 不 302」修复；Master 直接返回 JSON |
| 匿名 `play_base` 403 | Bucket 禁公开读，属预期；走 Master 媒体 API |
| 队列堆积 | 看 KEDA/HPA；提高 `maxReplicaCount` 或 `MAX_INFLIGHT_JOBS` |
| CrashLoop | `kubectl describe po` / 日志；常为 Redis/OSS Secret 或 PVC 未挂上 |

日志：容器 **stdout** → 阿里云 SLS / 事件中心（不要 Master SSH 刮各 Pod 的 `sview.log`）。

---

## 10. 推荐落地顺序（ACS）

1. 打新镜像并推送 ACR，改 YAML `image` tag  
2. 建/更新 `redis-credentials`、`oss-credentials`、`acr-secret`  
3. `apply` Master → 验证 `/healthz`  
4. `apply` Worker → 提交 1 条小 mcap 冒烟  
5. 配 OSS Lifecycle + CORS  
6. 暴露 Master（LB/Ingress）并对接前端  
7. `apply` KEDA（或 CPU HPA）  
8. 再压多任务，观察 `GET /metrics`、`/api/workers`、队列深度  

本仓库本地联调经验：挂载用 `HEAD_CAM_MOUNT` 指向数据目录；`OSS_CACHE=1` + 私有桶时前端只打 Master，不要依赖裸 `play_base` 匿名播放。
