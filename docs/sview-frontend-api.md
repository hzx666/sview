# SView HTTP 接口文档（前端对接）

适用架构：**Master（HTTP/API）+ Worker（转码）+ OSS `player-bin/` 播放缓存**。  
前端 **Base URL 只指向 Master**（Service / LB / Ingress），不要指向 Worker。

| 环境 | Base URL 示例 |
|------|----------------|
| ACS 对外 | `http(s)://<master-host>:8080` |
| 本仓库 Vite 联调 | `http://127.0.0.1:5173/sview`（经 Vite 反代到 Master，见 [frontend/README.md](../frontend/README.md)） |

CORS：Master 已放行跨域。私有 Bucket 时媒体仍建议走 Master（见 §0）。

角色与 OSS 细节：[sview-oss-cache-roles.md](./sview-oss-cache-roles.md) · 部署：[acs-deploy.md](./acs-deploy.md)

**接口一览**

| 方法 | 路径 | 用途 |
|------|------|------|
| `GET` | `/healthz` | 健康检查 |
| `POST` | `/api/jobs` | 提交正式转码任务（仅 `.mcap`） |
| `GET` | `/api/jobs/stats` | 队列深度与处理中任务 |
| `POST` | `/api/jobs/{job_id}/cancel` | 取消任务 |
| `POST` | `/api/jobs/{job_id}/touch` | 刷新访问时间（防 GC） |
| `POST` | `/api/jobs/{job_id}/encode` | sparse 模式按需请求目标 segment |
| `GET` | `/api/jobs/{job_id}` | 查正式任务状态（会 touch） |
| `GET` | `/api/jobs/{job_id}/events` | SSE 进度（推荐替代密轮询） |
| `GET` | `/api/jobs/{job_id}/signed-media` | 批量签发公网 OSS 播放 URL（浏览器直连） |
| `GET` | `/api/jobs/{job_id}/manifest.json` | 播放清单（Master 返回 JSON，不 302） |
| `GET` | `/api/jobs/{job_id}/segments/{name}` | 拉约 30 帧打包段（推荐） |
| `GET` | `/api/jobs/{job_id}/frames/{name}` | 拉单帧 bin（兼容） |
| `GET` | `/api/test/types` | 测试场景列表（压测用） |
| `POST` | `/api/test/jobs` | 按 type 提交测试任务 |
| `GET` | `/api/test/jobs/{job_id}` | 查测试任务（含首帧/全帧耗时） |

> 兼容：旧路径 `/{job_id}/manifest.json`、`/{job_id}/segments/{name}`、`/{job_id}/frames/{name}` 仍可用，推荐统一走 `/api/jobs/...`。播放器优先用 segments。

正式播放对接看本文 §0–§5；**操作流程（起播阈值、前沿垫、signed-media）见 [`sview-playback-flow.md`](./sview-playback-flow.md)**。压测见 [`sview-test-api.md`](./sview-test-api.md)。

---

## 0. 对接约定（Master / OSS）

```text
Frontend ──API / SSE / touch──► Master :8080
    │                              │
    │                         Redis 队列
    │                              │
    │                         Worker 转码 → PUT OSS player-bin/{job_id}/
    │
    └── 媒体（manifest / segments）──► 优先打 Master
            Master 本地 /cache 命中 → 直接 200
            未命中 → 302 公网签名 OSS，或 ?proxy=1 由 Master 回源后流式返回
```

| 规则 | 说明 |
|------|------|
| Base URL = Master | Worker 默认不对外提供播放 API |
| `source` 必须是 `.mcap` | `.mp4` 等会入队后被 Worker 以 InvalidMagic 失败 |
| 媒体默认走 Master | 私有桶（Block Public Access）下**不要**匿名 GET 裸 `play_base` |
| **推荐：签名直连 OSS** | `GET .../signed-media` 拿短时签名 URL，浏览器直拉公网 OSS（短 RTT）；Bucket 需 CORS |
| `play_base` | 状态/SSE 可能返回公网前缀；私有桶勿匿名用；本仓库前端用 signed-media |
| `?proxy=1` | 签名/CORS 失败时的兜底：Master 流式代理 |
| 播放中 touch | SSE / `GET job` 会 touch（signed-media 不再 touch）；长播需周期性 `POST .../touch` |
| cancel / force / GC | 会 purge OSS `player-bin/{job_id}/` 并清进度；manifest 按从 0 连续 segment 对齐，避免虚高 `frames_available` |
| manifest | **始终由 Master 返回 JSON 正文**（避免跨域 302 导致解析失败） |

本仓库前端实现：`frontend/src/api.ts`（SSE → 轮询回退；segment 失败时自动 `?proxy=1`）。

---

## 1. `GET /healthz`

**作用：** 健康检查。

**返回：** `200`

```json
{ "status": "ok" }
```

---

## 2. `POST /api/jobs`

**作用：** 提交转码任务；已有完整可播缓存时直接返回可播。

**请求头：** `Content-Type: application/json`

**请求体：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `source` | string | 是 | 完整 `oss://.../*.mcap`（不要传 `.mp4`） |
| `force` | boolean | 否 | `true` 强制重跑，默认 `false` |
| `start_sec` | number | 否 | 从源文件开头跳过的秒数；大于 0 时任务 ID 会追加 `_o{秒}` |
| `publish_mode` | string | 否 | `progressive`（默认）或 `sparse`；sparse 保留全局时间轴并按需转 segment，暂不支持 HDF5 |

```json
{
  "source": "oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/260706/001U_device/SSIEGO_Cam_001U_20260706_101930_342.mcap",
  "start_sec": 3000
}
```

默认 `job_id` = 文件名去掉 `.mcap`（无扩展名、无路径）；设置
`start_sec > 0` 时为 `{job_id}_o{规范化秒数}`，应始终使用提交响应里的 `job_id`。
`publish_mode=sparse` 与 `start_sec` 不能同时使用。
Sparse 模式的 `job_id/cache_id` 会追加源 URI、文件版本和编码 profile 的
短哈希。同一源版本与编码配置的所有用户共用同一 manifest 和 segment；
同名但不同路径的文件不会误共享。
Sparse 首次定位只读取 MCAP Footer、ChunkIndex 和 MessageIndex，用帧数生成
帧段表。每一帧只属于表里的一段。建表不读取视频 payload。源文件必须包含
ChunkIndex/MessageIndex；
缺少原生索引时任务返回 `encode_status=unsupported`，应改用 progressive
或先使用 MCAP 工具重建索引。

**返回：**

| HTTP | 说明 |
|------|------|
| `200` | 已可播（本地或共享缓存命中） |
| `202` | 已入队 |
| `400` | 参数错误 |
| `404` | 本地挂载找不到源文件 |
| `503` | Redis 不可用 |

**响应体（常用字段）：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `job_id` | string | 任务 ID |
| `status` | string | 见 §3 |
| `source` | string | 源路径 |
| `cached` | boolean | 是否命中完整缓存 |
| `playable` | boolean | 服务端是否已写出足够帧可拉（约 ≥30）；起播请再等领先量，见播放流程 |
| `frames_done` | number | 已写出帧数 |
| `frame_count` | number \| null | 全片总帧数（入队时可能还没有） |
| `percent` | number \| null | 0–100，总帧已知时有值 |
| `duration_sec` | number | 时长（秒），有则返回 |
| `play_base` | string \| null | OSS 播放前缀（见 §0） |
| `local_path` | string | 本地映射路径（可选） |
| `error` | string \| null | 错误信息 |

---

## 2b. `GET /api/jobs/stats`

**作用：** 查看排队 / 处理中任务数（运维与负载提示）。

**返回：** `200`

| 字段 | 类型 | 说明 |
|------|------|------|
| `queued` | number | Redis 队列长度 |
| `processing` | number | 持锁处理中的任务数 |
| `inflight` | number | `queued + processing` |
| `workers_alive` | number | 可选，近期有心跳的 Worker |
| `cache_bytes` | number | 可选，播放缓存计量 |
| `jobs` | array | 处理中任务摘要 |

`jobs[]`：`job_id`、`status`、`owner_pod`、`source`、`playable`、`frames_done`。

Redis 不可用时 `503`。

---

## 3. `GET /api/jobs/{job_id}`

**作用：** 查任务状态与进度；成功时会 **touch**（刷新 `last_access_at` / pin，减缓 GC）。

**返回：** `200`；不存在时多为 `404`（`status=not_found`）

| 字段 | 类型 | 说明 |
|------|------|------|
| `job_id` | string | 任务 ID |
| `status` | string | `queued` / `processing` / `ready` / `failed` / `cancelled`；不存在时 `not_found` |
| `source` | string | 源路径 |
| `error` | string \| null | 失败原因 |
| `frames_done` | number \| null | 已写出、可拉取的帧数 |
| `frame_count` | number \| null | 全片总帧数 |
| `percent` | number \| null | 0–100 |
| `playable` | boolean | 服务端可拉帧（处理中约 `frames_done ≥ 30`）。**不要当作立刻起播**：建议等到约 150 帧，见播放流程文档 |
| `cached` | boolean | 是否完整缓存 |
| `duration_sec` | number | 全片时长（秒） |
| `play_base` | string \| null | 如 `https://{bucket}.{OSS_PUBLIC_ENDPOINT}/player-bin/{job_id}` |
| `cache_bytes` | number \| null | 可选 |
| `updated_at` | string | 更新时间 |

```json
{
  "job_id": "SSIEGO_Cam_001U_20260706_101930_342",
  "status": "processing",
  "frames_done": 60,
  "frame_count": 919,
  "percent": 6.5,
  "playable": true,
  "cached": false,
  "duration_sec": 30.76,
  "play_base": "https://ss-oss-data-platform.oss-cn-shanghai.aliyuncs.com/player-bin/SSIEGO_Cam_001U_20260706_101930_342"
}
```

**前端建议：**

| 条件 | 动作 |
|------|------|
| `failed` / `cancelled` | 展示 `error`，结束 |
| `ready` / `cached` | 拉 manifest，完整播 |
| `processing` 且 `frames_done ≥ 150` | 拉 manifest，边转边播（`playable` 在约 30 帧就会为 true，太早起播会顶到编码前沿） |
| 其它 | 继续等进度（优先用 §3c SSE） |

---

## 3b. `POST /api/jobs/{job_id}/cancel`

**作用：** 取消排队或处理中的任务。

**返回：** `200` + `status=cancelled`；不存在 `404`；Redis 不可用 `503`。

持锁 Worker 在 chunk 边界停转；`force` 重提也会先 cancel 再入队。

---

## 3c. `GET /api/jobs/{job_id}/events`（SSE，推荐）

**作用：** 服务端推送进度，减少密轮询。Master 读 Redis 推事件，连接期间会 touch。

**响应头：** `Content-Type: text/event-stream`

**事件：**

| `event` | 说明 |
|---------|------|
| `progress` | 进度变化时推送 |
| `done` | `status` 为 `ready` / `failed` / `cancelled` 时推送并结束流 |
| `error` | 流内错误（如 Redis） |

`data` 为 JSON，字段与 §3 对齐的子集，例如：

```json
{
  "job_id": "SSIEGO_Cam_...",
  "status": "processing",
  "frames_done": 90,
  "frame_count": 919,
  "percent": 9.8,
  "playable": true,
  "play_base": "https://...",
  "error": null
}
```

**前端策略：**

1. 连 `GET .../events`；以 `frames_done ≥ 150` 或 `ready` 为起播条件（不要只看 `playable`，约 30 帧就会亮）  
2. 单次 SSE 有服务端最长时长（约 300s）；未完成可重连或回退轮询
3. SSE 失败时可退避轮询 `GET /api/jobs/{id}`。本仓库演示页默认开启轮询回退（`allowPollFallback=true`）

---

## 3d. `POST /api/jobs/{job_id}/touch`

**作用：** 显式刷新访问时间与 pin，避免长播 / 暂停时被 Master GC 或空闲 TTL 清掉。

**返回：** 与任务状态类似的 JSON（`200`）。

查状态与 SSE 已会 touch；单独播放页若很少打 status，建议按分钟级调用。

---

## 3e. `POST /api/jobs/{job_id}/encode`

**作用：** 仅用于 `publish_mode=sparse` 的任务。请求目标全局帧所在 segment
及后续预取段；重复请求通过 Redis 段锁去重。

```json
{
  "frame_index": 900,
  "prefetch_segments": 8,
  "client_epoch": 12,
  "client_id": "browser-session-uuid"
}
```

- 已发布目标段：`200`，并继续把播放位置之后尚未缓存的段排进编码队列。
- 目标段尚未发布：`202`，从该段开始编码。当前段可播后播放继续，后续段按同一方向接着编码。
- 同一个播放窗口只启动一条编码线程。seek 只改这条线程的 `encode_frontier`，
  不额外开线程。另一个浏览器窗口使用不同的 `client_id`，才会再启动一条线程。
- 新的 seek 会把这条线程转到目标段，并从这个段继续往后编码。
- 非 sparse Job：`409`；超过全片范围：`416`。
- `encode_status` 可能为 `reading_mcap_index`、`locating`、`decoding`、
  `encoding`、`idle`、`failed` 或 `unsupported`。
- `decode_start` 是帧段表里该段的起点，不通过解开视频 payload 查找。
  `output_frames` 只统计目标 segment 已编码帧数。同一 Job 同时只编码一个
  segment，连续播放段优先于后续预取段。没有 seek 时前沿从第 0 段一直往后；
  seek 之后前沿改到当前帧所在段，已有段立即播放，没有的段从这里开始边转边播。

段任务只在 segment PUT 成功后更新 manifest，因此前端必须以
`bundle.available_ranges` 判断目标段是否可读。
多个请求命中同一 segment 时只会入队一次；每个 `request_id` 独立等待，
最终读取同一份 OSS 缓存。

- `GET /api/jobs/{job_id}/encode/{request_id}`：查询本次请求状态。
- `POST /api/jobs/{job_id}/encode/{request_id}/cancel`：仅取消当前用户的
  订阅，不删除共享 segment，也不中断其他用户需要的转码。

---

## 3f. `GET /api/jobs/{job_id}/signed-media`

**作用：** 批量为 `player-bin/{job_id}/segments/*.bin` 签发**公网** GET URL，供浏览器直连 OSS（去掉 Master 中转大包）。

OSS 播放缓存只有打包段，没有逐帧 `frames/*.bin`。`kind=frames` 返回 **400**。

**Query：**

| 参数 | 说明 |
|------|------|
| `starts` | 逗号分隔段起始索引，如 `0,30,60`（优先） |
| `start` / `count` / `step` | 区间形式；`step` 默认 `SEGMENT_FRAMES`；单次最多 `OSS_SIGN_BATCH_MAX` 条（默认 600，约 10 分钟视频） |
| `expires` | 可选，签名有效秒数；最小 60，最大不超过服务端 `OSS_SIGN_EXPIRES`（默认 7200） |
| `kind` | 仅 `segments`（默认）。`frames` 非法 |

Master **不读清单、不列 OSS**，只从 Redis 读 `frame_count`，按段文件名直接签名。还没生成的段也会签出地址，浏览器 `fetch` 拿到 `404` 就当还没生成，稍后重试；是否该去取某段仍以 `manifest.json` 的 `available_ranges` 为准。超出 `frame_count` 或不在段边界（30 的整数倍）上的起点**不会签发**，出现在响应 `skipped` 里。任务在 Redis 里不存在返回 **404** `job not found`；Redis 不可用返回 **503**（带 `Retry-After`）。签名本身不访问 OSS，不会再因为读清单失败返回 404 或 425。

**返回：** `200`

```json
{
  "job_id": "SSIEGO_Cam_...",
  "kind": "segments",
  "expires_in": 7200,
  "items": [
    {
      "start": 0,
      "name": "000000.bin",
      "url": "https://{bucket}.oss-cn-shanghai.aliyuncs.com/player-bin/...?Expires=...&Signature=..."
    }
  ]
}
```

**前端策略（推荐）：** 打开、Seek 到已签范围外、播到已签范围末尾前约 2 分钟、或签名快过期时，用 `start=当前段&count=600&expires=1800` 一次签一批并缓存；其余时间不再请求签名。`fetch(url)` 直连 OSS：`404` 当作还没生成，`403` 先清掉这一批重签一次，连续失败再回退 Master `?proxy=1`。  
调试：`?media=proxy` / `?media=signed`。

**Bucket CORS：** 需允许前端 Origin 的 `GET`/`HEAD`（含 `Authorization` 非必须；签名在 query）。本地 Vite 源为 `http://127.0.0.1:5173`。

---

## 4. `GET /api/jobs/{job_id}/manifest.json`

**作用：** 播放清单。Master **直接返回 JSON**（`OSS_CACHE` 时优先 OSS 上的 manifest，再按从 0 起连续 segment 对齐，避免本地虚高；OSS list 失败返回 **425**）。

> 兼容旧路径：`GET /{job_id}/manifest.json`

| 字段 | 类型 | 说明 |
|------|------|------|
| `job_id` | string | 任务 ID |
| `format` | string | 如 `head_cam` |
| `status` | string | `partial` 仍在写帧；`done` 全部完成 |
| `publish_mode` | string | `progressive` 或 `sparse`；省略时按 progressive |
| `play_base` | string | 可选，OSS 播放前缀 |
| `bundle.frame_count` | number | 全片总帧数（进度条最大值） |
| `bundle.frames_available` | number | 当前可请求帧数，合法索引 `0 .. frames_available-1` |
| `bundle.available_ranges` | `[number,number][]` | sparse 模式已原子发布的半开帧区间；例如 `[[0,150],[900,960]]` |
| `bundle.gop_size` | number | 约 30 |
| `bundle.key_frame_indices` | number[] | 关键帧索引 |
| `bundle.timeline.*` | | `start_ns` / `end_ns` / `duration_sec` |
| `streams.camera_rr/fr/rl/fl` | object | `{ position, width, height, fps }` |

progressive 模式继续使用 `frames_available` 连续前缀。sparse 模式可在
`frames_available=0` 时存在远端可播段，必须查询 `available_ranges`。
`status=done` 仅表示全片所有 segment 都已发布。

---

## 5. `GET /api/jobs/{job_id}/segments/{name}`

**作用：** 一次拉取约 30 帧（`SEGMENT_FRAMES`，与 `gop_size` 对齐）打包段。

**路径参数：**

| 字段 | 说明 |
|------|------|
| `job_id` | 任务 ID |
| `name` | `000000.bin` / `000030.bin` / …（起始索引为段长倍数） |

**查询参数：**

| 参数 | 说明 |
|------|------|
| `proxy=1` | 禁止 302，由 Master 从本地或内网 OSS 读出后流式返回（浏览器跨网 / CORS 问题时用） |

> 兼容旧路径：`GET /{job_id}/segments/{name}`

**返回：**

| HTTP | 说明 |
|------|------|
| `200` | `application/octet-stream`；可含 `X-Sview-Segment-Start` / `X-Sview-Segment-Count` |
| `302` | （未带 `proxy=1` 且开启媒体重定向时）Location 为**公网**签名 URL；浏览器需能访问且 Bucket CORS 放行 |
| `425` | 段未凑满（边转边播前沿），看 `Retry-After` |
| `404` | 越界、未生成，或 OSS/本地皆无 |
| `400` | 起始索引未按段对齐等 |

**策略：** 整段齐了才返回；任务完成后最后一段可不足 30 帧。

**二进制结构：**

```text
header_len(uint32 LE) + header_json(UTF-8) + payload_len(uint32 LE) + payload
```

`header_json`：

| 字段 | 说明 |
|------|------|
| `packet_schema` | `playertrans.bundle_segment.v1` |
| `start_index` | 段内第一帧索引 |
| `count` | 本段帧数 |
| `segment_frames` | 标称段长（默认 30） |

`payload`：`count` 次重复 `[frame_len(uint32 LE) + 单帧 bundle 字节]`（单帧格式见下节）。

**前端拉取建议：**

1. 优先 `GET /api/jobs/{id}/segments/...`（Base = Master）  
2. 若跟 302 失败（`*-internal*`、CORS、连接被关）→ 同一 URL 加 `?proxy=1` 重试  
3. 可选：公网可读 + CORS 完备时用 `play_base` 直拉；403/CORS 失败必须回退 Master  

---

## 5b. `GET /api/jobs/{job_id}/frames/{name}`

**作用：** 拉单帧二进制（四路 H.264 + IMU）。兼容保留；新播放器用
segments。该路径只读取 Master/owner Pod 的本地帧，不从 OSS 回源，
`?proxy=1` 不改变其行为。

**路径参数：**

| 字段 | 说明 |
|------|------|
| `job_id` | 任务 ID |
| `name` | 必须为 `000000.bin` 这种 6 位序号 |

> 兼容旧路径：`GET /{job_id}/frames/{name}`

**返回：**

| HTTP | 说明 |
|------|------|
| `200` | `application/octet-stream` |
| `425` | 帧未写好，看 `Retry-After` 后重试 |
| `404` | 不存在 |
| `400` | 参数非法 |

**二进制结构：**

```text
header_len(uint32 LE) + header_json(UTF-8) + payload_len(uint32 LE) + payload
```

**header_json：**

| 字段 | 说明 |
|------|------|
| `packet_schema` | `playertrans.bundle_frame.v1` |
| `frame_index` | 帧序号 |
| `timestamp_ns` | 时间戳 |
| `time_window_ns` | `[start, end]` |
| `key_frame` | 四路是否都为关键帧 |
| `streams.rr/fr/rl/fl` | `{ payload_offset, payload_length, is_null }` |
| `imu` | `{ payload_offset, payload_length }` |
| `events` | `{ payload_offset, payload_length }` |

**payload：** rr→fr→rl→fl 的 H.264 Annex-B，再接 IMU JSON、Events JSON（offset 相对 payload 起点）。

---

## 6. 推荐前端主路径（摘要）

```text
POST /api/jobs  { source: "oss://.../*.mcap" }
        │
        ├─ 200 cached/ready ───────────────────────┐
        └─ 202 ──► GET .../events (SSE)            │
                      └─ 可选：失败则退避轮询 GET job │
                               │                   │
                      frames_done≥150 / ready ─────┤
                                                   ▼
                              GET .../manifest.json  （Master）
                                                   │
                              GET .../signed-media   （批量短签）
                                   └─ fetch → 公网 OSS segments/*.bin
                                      （失败 → Master ?proxy=1）
                                                   │
                              预缓冲数段 → WebCodecs 四路播放
                              + 定时刷新 manifest（partial）
                              + 播放头落后前沿约 90 帧；顶到后等领先约 150 帧
                              + 周期性 touch（长播）
```

---

## 7. 测试接口（压测 / type 切换）

与正式 `/api/jobs` **相互独立**，不影响前端播放对接。

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/test/types` | type 1–8 场景定义 |
| `POST` | `/api/test/jobs` | `{"type":1-8,"source":"oss://...","force":true}` |
| `GET` | `/api/test/jobs/{job_id}` | 额外返回 `first_frame_sec` / `full_sec` |

`job_id` 形如 `{文件名}__t{type}`。完整字段见 [`sview-test-api.md`](./sview-test-api.md)。

**相关环境变量（部署侧，供联调对照）：**

| 变量 | 说明 |
|------|------|
| `REDIS_QUEUE_KEY` | 队列名，线上为 `player:queue` |
| `OSS_CACHE` / `OSS_CACHE_PREFIX` | 播放缓存，前缀默认 `player-bin` |
| `OSS_PUBLIC_ENDPOINT` | 签名 / `play_base` 用公网域名 |
| `OSS_MEDIA_REDIRECT` | `1` 时 segment 可 302；前端应用 `proxy=1` 兜底 |
| `BENCH_H264_SOURCE` / `BENCH_H265_SOURCE` | 测试接口默认源 |
