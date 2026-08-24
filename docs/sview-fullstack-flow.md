# SView 前后端流程与接口调用说明

适用版本：当前仓库（Master `sview_http` + Worker `sview_worker` + `frontend`）  
Base URL：**Master**（例：`http(s)://<master-host>:8080`；本地可用 Vite `http://127.0.0.1:5173/sview`）  
前端对接字段以 [sview-frontend-api.md](./sview-frontend-api.md) 为准。

---

## 1. 整体架构

```text
┌─────────────┐     HTTP      ┌──────────────────┐     Redis      ┌─────────────────┐
│  前端 Web   │ ────────────► │  sview_http      │ ─────────────► │  Redis Queue    │
│  (Vite)     │ ◄──────────── │  :8080           │ ◄───────────── │  player:queue   │
└─────────────┘   JSON/bin    │  + 本地缓存目录   │   状态 Hash    └────────┬────────┘
                              └────────┬─────────┘                          │
                                       │ 读/写                            │ BLPOP
                                       ▼                                   ▼
                              ┌──────────────────┐               ┌─────────────────┐
                              │ LOCAL_OUTPUT_DIR │               │ sview_worker  │
                              │ /{job_id}/       │ ◄── 写帧 ──── │ Worker          │
                              │  manifest.json   │               │ (转码+拆帧)     │
                              │  frames/*.bin    │               └────────┬────────┘
                              └──────────────────┘                        │
                                                                          ▼
                                                                 挂载盘读 mcap
                                                                 (oss 映射本地)
```

| 角色 | 职责 |
|------|------|
| 前端 | 提交 source、轮询状态、拉 manifest/帧、WebCodecs 解码播放 |
| HTTP | 入队、查状态、提供缓存文件；**不负责转码** |
| Redis | 队列 `REDIS_QUEUE_KEY=player:queue` + job 状态 Hash |
| Worker | 出队、按参数转码、渐进写出 frames + manifest、回写状态/计时 |

---

## 2. 正式播放流程（前端主路径）

### 2.1 时序

```text
前端                    HTTP(:8080)                 Redis                 Worker
 │                         │                         │                      │
 │ POST /api/jobs          │                         │                      │
 │ {source, force?}        │                         │                      │
 │────────────────────────►│                         │                      │
 │                         │ 检查本地缓存            │                      │
 │                         │ 映射 source→本地路径     │                      │
 │                         │ HSET job + RPUSH 队列    │                      │
 │◄──────── 202 ───────────│────────────────────────►│                      │
 │  {job_id,status=queued} │                         │                      │
 │                         │                         │◄──── BLPOP ──────────│
 │                         │                         │                      │ 读 mcap
 │ GET /api/jobs/{id} 轮询  │                         │                      │ 转码/直出
 │────────────────────────►│◄── HGET 状态 ───────────│                      │ 写 frames
 │◄── processing/playable ─│                         │◄── 进度 HSET ────────│
 │                         │                         │                      │
 │ GET /api/jobs/{id}/manifest.json │                │                      │
 │────────────────────────►│ 读缓存目录              │                      │
 │◄── partial/done ────────│                         │                      │
 │                         │                         │                      │
 │ GET /api/jobs/{id}/frames/000000.bin …            │                      │
 │────────────────────────►│                         │                      │
 │◄── bin / 425 ───────────│                         │                      │
 │  WebCodecs 解码播放      │                         │                      │
 │  (partial 时继续刷 manifest)                      │                      │
 │                         │                         │◄── ready + 计时 ─────│
```

### 2.2 步骤与接口

#### ① 健康检查（可选）

```http
GET /healthz
```

```json
{ "status": "ok" }
```

#### ② 提交任务

```http
POST /api/jobs
Content-Type: application/json

{
  "source": "oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/.../xxx.mcap",
  "force": false
}
```

| 返回 | 含义 | 前端动作 |
|------|------|----------|
| `200` + `cached/ready` | 本地已有完整结果 | 直接拉 manifest |
| `202` | 已入队 | 用 `job_id` 轮询 |
| `404` | 源文件未挂载到本地 | 报错 |
| `503` | Redis 不可用 | 报错 |

`job_id` = 文件名去掉 `.mcap`（例：`SSIEGO_Cam_001U_20260706_101930_342`）。

前端代码：`api.submitJob(source)` → `frontend/src/api.ts` / `main.ts`。

#### ③ 等可播（SSE 优先，否则轮询）

```http
GET /api/jobs/{job_id}/events
```

`text/event-stream`：`event: progress` / `done`，字段含 `playable`、`percent`、`play_base` 等。不可用时退避轮询：

```http
GET /api/jobs/{job_id}
```

| 字段 | 含义 |
|------|------|
| `status` | `queued` / `processing` / `ready` / `failed` / `cancelled` |
| `playable` | 是否可起播（通常 `frames_done >= 30`） |
| `frames_done` | 已写出帧数 |
| `frame_count` | 全片总帧数（可能稍后才有） |
| `percent` | 0–100（总帧已知时） |
| `play_base` | OSS 播放前缀（私有桶勿匿名直打） |
| `duration_sec` | 全片时长 |
| `error` | 失败原因 |

| 条件 | 前端动作 |
|------|----------|
| `failed` | 展示 `error`，结束 |
| `playable=true` 或 `ready` / `cached` | 进入拉 manifest |
| 其它 | 继续等进度 |

前端：`waitUntilPlayable(jobId)`（SSE → 轮询回退）。长播可 `POST /api/jobs/{id}/touch`。

#### ③b 取消任务（单实例与集群均可用）

```http
POST /api/jobs/{job_id}/cancel
```

Redis 置 `cancel=1` / `status=cancelled`；持锁 Worker 在 chunk 边界停转并清理本机 `/cache/{job_id}`。`force` 重提也会先 cancel 再入队。

#### ③c 负载统计（可选）

```http
GET /api/jobs/stats
```

返回 `queued`（队列长度）、`processing`（持锁处理中）、`inflight`、`jobs[]`。用于看当前有多少任务在转，不参与播放主路径。

#### ④ 拉播放清单

```http
GET /api/jobs/{job_id}/manifest.json
```

| 字段 | 用途 |
|------|------|
| `status` | `partial` 边转边写；`done` 全部完成 |
| `bundle.frame_count` | 进度条 / seek **最大值**（全片） |
| `bundle.frames_available` | 当前可请求帧：`0 .. frames_available-1` |
| `bundle.key_frame_indices` | Seek 对齐关键帧 |
| `bundle.timeline.duration_sec` | 展示时长 |
| `streams.camera_*` | 解码器宽高 / fps |

#### ⑤ 按段拉帧并播放

```http
GET /api/jobs/{job_id}/segments/{start:06d}.bin
GET /api/jobs/{job_id}/segments/{start:06d}.bin?proxy=1
```

例：`/api/jobs/SSIEGO_Cam_xxx/segments/000000.bin`（约 30 帧一段；单帧 `frames/` 仍兼容）。  
媒体打 **Master**；`302`/CORS/内网 OSS 失败时用 `?proxy=1` 强制 Master 回源流转发。

| HTTP | 处理 |
|------|------|
| `200` | 拆段为单帧 bundle，四路 WebCodecs 解码 |
| `425` | 段未凑齐 →「加载中」，按 `Retry-After` 重试 |
| `404` | 处理中可当加载中；完成后仍 404 再报错 |

前端：`player.ts` / `api.ts` 按段预取 + 解码；追上 `frames_available` 时等待，不提前结束。

#### ⑥ 边转边播（manifest=`partial`）

1. 约每秒刷新 `manifest.json`
2. 更新 `frames_available` / 关键帧列表
3. 进度条 max **始终**用 `frame_count`
4. `status=done` 且 `frames_available === frame_count` 后停止刷新

---

## 3. 状态对照表

| 阶段 | `GET /api/jobs` status | manifest status | 前端表现 |
|------|------------------------|-----------------|----------|
| 刚提交 | `queued` | 无文件 | 等待 |
| 转码写帧中 | `processing` | `partial` | `playable` 后可边转边播 |
| 全部完成 | `ready` | `done` | 完整播放 |
| 失败 | `failed` | — | 展示 error |

---

## 4. 后端内部（Worker）在干什么

1. `BLPOP player:queue` 取出任务（调度器可同时跑最多 `MAX_INFLIGHT_JOBS` 个）  
   - 正式任务：队列里是 **纯 source 字符串**  
   - 测试任务：队列里是 **JSON**（含 type / 转码参数 / job_id）
   - 默认 `OVERLAP_AFTER_PLAYABLE=1`：当前任务达到可播（`frames_done >= PLAYABLE_MIN_FRAMES`）后，才再取下一个  
   - ffmpeg 受 `CPU_CONCURRENCY` 限制，并由 `FairEncodeGate` 做公平分配：  
     - 每个 job 硬上限 `ENCODE_JOB_MAX_CONCURRENT`（默认 4；配合 12 槽 → 最多 3 个 job 打满）  
     - 可播后的任务最多占 `ENCODE_PLAYABLE_MAX_CONCURRENT`（默认 1）个槽，并给未可播任务留 `ENCODE_FOREGROUND_RESERVE`（0=自动约 25%）  
     - 另有 fair share：`ceil(CPU_CONCURRENCY / n_jobs)`，与硬上限取更小值
2. 把 Redis job 标为 `processing`，记录 `started_at`
3. 读本地 mcap，**按块**转码/直通并立刻出帧（头显与 Orin 均为边转边播）  
   - 首块大小 = `PLAYABLE_MIN_FRAMES`（默认 30），之后头显用 `CHUNK_FRAMES`、Orin 用 `ORIN_CHUNK_FRAMES`  
   - 大文件无需等整包读完/转完即可 `playable`
4. 渐进写入 `LOCAL_OUTPUT_DIR/{job_id}/frames/*.bin` + `manifest.json`（`partial`→`done`）
5. 通过 `progress_cb` 更新 `frames_done` / `playable`
6. 结束写 `status=ready`（测试任务额外写 `first_frame_sec` / `full_sec`）

正式任务转码参数来自 **进程环境变量**（全局）；  
测试任务参数来自队列 JSON（按 job **线程局部**覆盖 size/crf/force，可与并行任务共存）。

---

## 5. 测试 / 压测流程（与播放并行存在）

不影响正式 `/api/jobs`。用于 type 1–8 场景测速。

```text
POST /api/test/jobs  { "type": 3, "source": "oss://...", "force": true }
        │
        ├─ force=true：删除旧缓存目录 {stem}__t3 ，再入队 JSON
        ▼
GET /api/test/jobs/{job_id}  轮询
        │  status: queued → processing → ready
        │  完成后读 first_frame_sec / full_sec
        ▼
（可选）GET /api/jobs/{job_id}/manifest.json + frames 验证可播
```

| 接口 | 作用 |
|------|------|
| `GET /api/test/types` | 列出 type 1–8 |
| `POST /api/test/jobs` | 提交；`force` 默认 `true` |
| `GET /api/test/jobs/{id}` | 状态 + 首帧/全帧耗时 |

`job_id` 形如：`SSIEGO_Cam_000X_..._124__t3`。

客户端脚本：`scripts/bench_online.py` / `scripts/bench_online_rounds.py`。

---

## 6. 前端模块对应关系

| 模块 | 调用 |
|------|------|
| `frontend/src/main.ts` | `submitJob` → `waitUntilPlayable` → 起播 |
| `frontend/src/api.ts` | 封装 `/api/jobs`、`manifest`、`frames` |
| `frontend/src/player.ts` | 拉帧、刷新 manifest、WebCodecs、seek |

默认 Base URL：`http://139.224.211.114:8080`（可用 URL 参数 `?base=` 覆盖）。

---

## 7. 部署：单实例 vs 多副本集群

| 模式 | 清单 | 要点 |
|------|------|------|
| **单实例** | `deploy/deployment-cpu-redis.yaml` 或把主清单 `replicas=1` + `SVIEW_CLUSTER=0` | 本地 `/cache`（emptyDir） |
| **多副本（推荐清单）** | [`deploy/deployment.yaml`](../deploy/deployment.yaml) + [`service.yaml`](../deploy/service.yaml) | `replicas: 2`、`SVIEW_CLUSTER=1`、`POD_IP=status.podIP`、`POD_HTTP_PORT`；Service 可开 `sessionAffinity`；**每 Pod emptyDir**，不用 NAS/OSS 作 `/cache` |

**只扩 replicas、不开 `SVIEW_CLUSTER=1` / 不注入 `POD_IP` 会坏：** 锁 owner 全是 `local`（互相可抢/误删），且不会跨 Pod 代理 → 帧 404、双转码。进程在 `SVIEW_CLUSTER=1` 且无真实 `POD_IP` 时会直接退出。

集群下非 owner Pod 对 `manifest` / `frames` **一律内部代理**（即使本机有陈旧缓存）。入队对已 `queued`/`processing`（锁未过期）去重；Worker 后台续期 job 锁。

## 8. 关键注意

1. **HTTP 与 Worker 的 `REDIS_QUEUE_KEY` 必须一致**，线上为 `player:queue`。  
2. 进度条 max = `frame_count`；可播范围 = `frames_available` / `frames_done`。  
3. `425` / 短暂缺帧 = 加载中，不是致命错误。  
4. 浏览器需支持 WebCodecs（Chrome / Edge）。  
5. OSS 路径会映射到 PVC 挂载点；映射不到会 `404 source not found on local mount`。

---

## 9. 相关文档

| 文档 | 内容 |
|------|------|
| `docs/sview-frontend-api.md` | 正式接口字段详解 |
| `docs/sview-test-api.md` | 测试 type 接口详解 |
| `docs/sview-playback-flow.md` | 播放侧更细的操作说明 |
| `output/bench_online_10rounds/飞书报告.md` | 线上 10 轮压测结果（可贴飞书） |
