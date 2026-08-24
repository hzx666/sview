# SView 本地缓存播放方案（多副本 Deployment · 不上 OSS）

## 1. 目标与前提

| 项 | 约定 |
|---|---|
| 运行环境 | 阿里云 ACS / ACK，**多副本 Deployment** |
| 对象存储 | **不上 OSS、不上共享盘（NAS）** |
| 协调组件 | **Redis**（队列、状态、锁、可选通知） |
| 处理耗时 | 约数秒（如 CPU NAL 方案 ~5s），允许缓存未命中时重算 |
| 产物 | `{job_id}/manifest.json` + `{job_id}/frames/*.bin` |

**核心原则：**

1. 每个 Pod 用**本地磁盘**缓存自己处理过的 job（可丢）。
2. Redis 只存**元数据与调度信息**，不存 bin。
3. 播放时本地没有文件 = **缓存未命中 → 再处理一遍**，不跨 Pod 传文件。
4. `owner_pod` 仅作加速提示，**不作为正确性依赖**（滚动升级 / 扩缩容后会失效）。

---

## 2. 总体架构

```text
前端
  │  ① 提交任务 / 查询状态 / 拉播放地址
  ▼
API 网关（或业务 API）
  │  ② 读写 Redis
  │  ③ HTTP 反代或直连 Pod Service 拉文件
  ▼
Redis ── 队列 · 锁 · job 状态 · 可选 Pub/Sub
  │
  ▼
Deployment（N 副本）
  Pod-1                Pod-2                Pod-N
  ├─ 处理 worker         ├─ ...                ├─ ...
  ├─ HTTP :8080          ├─ :8080               ├─ :8080
  └─ emptyDir:/cache     └─ /cache              └─ /cache
       └─ {job_id}/
            ├─ manifest.json
            └─ frames/000000.bin ...
```

- **处理**与**提供文件**跑在同一进程：`server_cpu.py` / `sview_worker.py` 内置 `sview_http.py`。
- 默认端口 **`HTTP_PORT=8080`**（`HTTP_HOST=0.0.0.0`）；`HTTP_PORT=0` 或 `--no-serve` 关闭。
- Redis 队列模式默认开 HTTP；单文件 `--path` 需加 `--serve`；也可 `python sview_http.py` / `--serve-only`。
- 路由：
  - `GET /healthz`
  - `GET /{job_id}/manifest.json`
  - `GET /{job_id}/frames/000000.bin`
- 集群内用 **ClusterIP Service** 暴露 `8080`；对外由 API 鉴权后转发。

---

## 3. Redis 数据模型

### 3.1 队列（沿用现有思路）

| Key | 类型 | 说明 |
|---|---|---|
| `mcap:queue` | List | 待处理路径或 `job_id`；`BLPOP` 领取 |

### 3.2 Job 状态

Key：`sview:job:{job_id}`（Hash 或 JSON String）

| 字段 | 说明 |
|---|---|
| `status` | `queued` / `processing` / `ready` / `failed` |
| `source` | 原始 mcap 路径或对象标识 |
| `owner` | 可选，当前处理/持有缓存的 Pod 名（如 `HOSTNAME`） |
| `error` | 失败原因 |
| `updated_at` | Unix 秒 |
| `frame_count` | ready 后可写，便于前端展示 |
| `duration_sec` | 可选 |

### 3.3 分布式锁

| Key | 说明 |
|---|---|
| `sview:lock:{job_id}` | `SET NX EX`，防止多 Pod 同时处理同一 job |

建议锁过期时间 ≥ 单次处理上限（如 120s），处理结束主动 `DEL`。

### 3.4 可选：完成通知

- Channel：`sview:events`
- 消息示例：`{"job_id":"...","status":"ready"}`

前端也可用短轮询 `GET /jobs/{id}`，Pub/Sub 非必须。

---

## 4. 本地缓存目录

### 4.1 路径约定

```text
/cache/{job_id}/
  manifest.json
  frames/
    000000.bin
    000001.bin
    ...
```

- 挂载：`emptyDir`（或节点本地盘；Deployment 多副本下仍以 emptyDir 最常见）。
- **整 job 为淘汰单位**，不要按单帧删除。

### 4.2 原子写入

1. 写入 `/cache/{job_id}.tmp/...`
2. 全部成功后 `rename` → `/cache/{job_id}`
3. 再更新 Redis `status=ready`

避免前端读到半成品。

### 4.3 容量与淘汰（每 Pod 独立）

| 配置 | 建议 |
|---|---|
| `CACHE_MAX_BYTES` | 如 50GiB / 200GiB（按节点盘定） |
| 策略 | LRU：按 `last_access`；简易版 FIFO：按目录 mtime |
| 触发 | 每次 job 写入完成后；或定时扫描 |
| 保护 | 最近 N 分钟访问过、或 `status=processing` 的目录不删 |
| 目标 | 超过上限时删到例如 80% |

每 Pod 维护一份本地元数据即可（sidecar 文件或目录 mtime）；不必把每帧索引写入 Redis。

---

## 5. 处理流程（Worker）

```text
1. BLPOP mcap:queue
2. 解析 job_id / source
3. 若本地 /cache/{job_id}/manifest.json 已存在且完整
     → 更新 last_access，SET Redis ready，结束（命中缓存）
4. SET sview:lock:{job_id} NX
     → 失败则稍等再查 Redis/本地，避免重复算
5. status=processing，owner=HOSTNAME
6. 跑 server_cpu / sview_worker（输出到本地 cache，--no-upload）
7. 原子发布目录
8. status=ready，写 frame_count 等；DEL lock
9. 可选 PUBLISH sview:events
10. 执行缓存淘汰检查
```

说明：

- 与现有脚本对齐时，使用 `--no-upload`，`LOCAL_OUTPUT_DIR` / `TEMP_DIR` 指到 `/cache` 体系。
- 源 mcap 仍从现有挂载路径读取（与当前 `LOCAL_MOUNT_PATH` 一致）。

---

## 6. 播放流程（API + 前端）

### 6.1 API

| 接口 | 作用 |
|---|---|
| `POST /jobs` | 入队；返回 `job_id` |
| `GET /jobs/{job_id}` | 返回 `status`、进度字段、`frame_count` 等 |
| `GET /jobs/{job_id}/manifest` | 鉴权后返回或反代 manifest |
| `GET /jobs/{job_id}/frames/{name}` | 鉴权后反代单帧 bin |

**文件不要经业务逻辑读入再拷贝一遍大 buffer**；优先 Nginx/网关 `X-Accel-Redirect` 或反代到 Pod 文件端口。

### 6.2 拉文件时的多副本策略（关键）

请求到达**任意** Pod（或经 Service 负载均衡）时：

```text
IF 本地存在 /cache/{job_id}/manifest.json:
    更新 last_access
    直接返回文件
ELSE:
    # 缓存未命中（含：别的 Pod 处理的、本 Pod 重启丢盘、被淘汰）
    触发同步/异步重处理（见下）
    等待 ready（短超时，如 30~60s）
    再返回文件；超时则 503 + Retry-After
```

**重处理触发建议（二选一）：**

- **同步（实现简单）**：当前 Pod 拿锁后当场处理，处理完返回（用户多等 ~5s）。
- **异步**：重新 `LPUSH` 队列，API 返回 `202` / `status=processing`，前端继续等 `ready`。

推荐先做**异步入队 + 前端等 ready**，与「先处理完再播」产品节奏一致；若希望「点播放即出」，可用同步。

### 6.3 前端

1. 提交 job → 轮询或订阅直到 `ready`（首次处理约数秒）。
2. 拉取 `manifest.json`（GOP、`key_frame_indices`、分辨率）。
3. **按需**请求帧：起播对齐关键帧，预取后续 1~2 秒（约 30~60 帧），**不要一次性拉全量**。
4. 浏览器 HTTP 缓存 / Cache API 缓存已拉帧（可选）。
5. Seek：跳到最近 `key_frame_indices`，再顺序预取。

### 6.4 为何不做「转发到 owner Pod」为强依赖

多副本 Deployment 下 Pod 会重建、IP 会变、Service 不保证粘滞。  
Redis 里的 `owner` 可做优化（API 尝试定向），但**必须有本地 miss → 重算**兜底。

---

## 7. ACS / K8s 配置要点

### 7.1 Deployment（示意）

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sview-worker
spec:
  replicas: 3
  template:
    spec:
      containers:
        - name: worker
          # 内含：队列消费 + 转码 + 文件 HTTP
          volumeMounts:
            - name: cache
              mountPath: /cache
          env:
            - name: REDIS_HOST
              valueFrom: { ... }
            - name: LOCAL_OUTPUT_DIR
              value: /cache
            - name: HTTP_PORT
              value: "8080"
            - name: HTTP_HOST
              value: "0.0.0.0"
            - name: CACHE_MAX_BYTES
              value: "53687091200"   # 50GiB 示例
          ports:
            - name: http
              containerPort: 8080
      volumes:
        - name: cache
          emptyDir:
            sizeLimit: 50Gi
```

Service 暴露 `port: 8080` → `targetPort: http`。

### 7.2 Service

- `ClusterIP`：供 API/Ingress 反代访问文件端口。
- 不依赖 `sessionAffinity` 保证正确性（可开但不作为前提）。

### 7.3 滚动升级

- 升级时旧 Pod 缓存丢失 → 新 Pod miss → 重算，预期行为。
- 处理中任务：靠锁过期 + 队列重试，避免丢任务（失败回队列或 `failed` 可重试）。

---

## 8. 压力与容量粗算

| 项 | 建议 |
|---|---|
| 播放 | 按需拉帧 + 浏览器缓存，避免全量下载 |
| 并发 | 流量落在各 Pod 本地盘与网卡；无 CDN 时注意副本数与带宽 |
| 重算 | 同一热门 job 被多 Pod 反复 miss 时，可对 `job_id` 加短时「处理中」合并，避免惊群 |
| Redis | 仅元数据，QPS 与内存压力通常很小 |

单 job 全量若约 100~300MB：一人偶尔全量可接受；多人同时全量拉取仍会打满带宽——前端预取窗口必须限制。

---

## 9. 明确不做的事

1. 不把 bin 写入 Redis。  
2. 不在 Pod 间同步整份 cache 目录（复杂度高于重算）。  
3. 不依赖 Session 粘滞作为唯一正确性手段。  
4. 本阶段不上 OSS / NAS（若日后公网大规模分发或强共享，再评估）。

---

## 10. 落地步骤（建议顺序）

1. **单 Pod 跑通**：`--no-upload` 写入 `/cache`，HTTP 能返回 manifest/帧，前端能播。  
2. **接 Redis 状态机**：queued → processing → ready / failed，锁与队列。  
3. **多副本**：验证 miss 重算；加每 Pod 缓存上限淘汰。  
4. **前端按需拉帧 + 预取窗口**。  
5. （可选）完成事件 Pub/Sub、owner 定向优化。

---

## 11. 接口草例

### 创建任务

队列 / API 直接传**完整 OSS 路径**（K8s 已把对应前缀挂到本地）：

```json
{ "source": "oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/260706/001U_device/SSIEGO_Cam_001U_20260706_101930_342.mcap" }
```

或 Orin：

```json
{ "source": "oss://ss-oss-data-platform/original_data/teleoperation/Orin/260701/004_device/m1_004_camera_20260701_081700_0.mcap" }
```

映射（`path_map.py`）：

| source 含 | 本地 |
|---|---|
| `.../original_data/ego_human_data/head_cam/` | `/head-data/` + 相对路径 |
| `.../original_data/teleoperation/Orin/` | `/orin-data/` + 相对路径 |

```json
{ "job_id": "SSIEGO_Cam_xxx", "status": "queued" }
```

### 查状态

`GET /api/sview/jobs/{job_id}`

```json
{
  "job_id": "SSIEGO_Cam_xxx",
  "status": "ready",
  "frame_count": 919,
  "duration_sec": 30.8
}
```

### 拉资源（经鉴权）

- `GET /api/sview/jobs/{job_id}/manifest.json`
- `GET /api/sview/jobs/{job_id}/frames/000030.bin`

未 ready：`409` 或 `425`，body 带 `status`；处理中可 `Retry-After: 2`。

---

## 12. 一句话总结

**多副本 Deployment + Redis 负责任务与状态；每个 Pod 本地 emptyDir 当可丢缓存；播放命中就读盘，未命中就再处理几秒。**  
这是当前「不上 OSS / 不上共享盘」下，与 ACS 多副本模型最匹配、实现成本最低的方案。
