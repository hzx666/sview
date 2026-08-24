# SView 播放流程（前端对接）

给播放器实现用。接口字段以 [sview-frontend-api.md](./sview-frontend-api.md) 为准。  
Base URL = **Master**（例：`http(s)://<master-host>:8080`，本地 Vite：`http://127.0.0.1:5173/sview`）。

---

## 必读（和「API playable」的差别）

| 概念 | 值 | 含义 |
|------|----|------|
| API `playable` | `frames_done ≥ 30` | 服务端「可以开始拉帧」，**不是**建议立刻起播 |
| 建议起播 | `frames_done ≥ 150`（约 5s @ 30fps） | 编码前沿要有领先量，否则播 1～2 秒就会卡住 |
| 播放垫 | 落后 `frames_available` 约 **90 帧（3s）** | 不要把播放头顶到已发布最后一帧 |
| 卡住后恢复 | 领先恢复到约 **150 帧** 再继续播 | 避免「播一点、停一点」抖动 |
| 段长 | **30 帧**（1 GOP / 约 1s） | `segments/000000.bin`、`000030.bin`、… |

Worker 边转边写：`manifest.status=partial` 时 `frames_available` 会涨。转码若略慢于实时，播放头会追上前沿——这是正常的「加载中」，不是接口错误。

---

## 一、完整流程

```text
POST /api/jobs  { source: "oss://.../*.mcap" }
  → 200 已缓存 / ready     → 拉 manifest 起播
  → 202 已入队             → SSE /api/jobs/{id}/events
       → failed / cancelled → 展示 error，结束
       → frames_done ≥ 150（或 ready）→ 拉 manifest
  → GET .../manifest.json   （必须走 Master，拿 JSON 正文）
  → GET .../signed-media    → 浏览器直拉公网 OSS segments
       → 失败则同一路径 ?proxy=1
  → 配置四路 WebCodecs → 预缓冲数段 → 播放
  → partial 时每秒刷 manifest + 长播 touch
  → 追上前沿 → 加载中（保持垫）
  → status=done 且 frames_available === frame_count 后停刷，播完结束
```

---

## 二、步骤说明

### 1. 提交任务

`POST /api/jobs`

```json
{ "source": "oss://.../*.mcap" }
```

| 返回 | 下一步 |
|------|--------|
| `200` 且 `cached=true` / `status=ready` | 跳过等待，直接拉 manifest |
| `202` | 用 `job_id` 走 SSE |
| `4xx` / `5xx` | 展示错误，结束 |

不要提交 `.mp4`：Worker 只读 MCAP。  
`job_id` = 文件名去掉 `.mcap`（无路径）。

### 2. 等可播（SSE）

`GET /api/jobs/{job_id}/events`（`text/event-stream`）

| 事件 | 动作 |
|------|------|
| `progress` | 看 `frames_done`；**≥ 150** 再进播放器（不要只看 `playable`） |
| `done` 且 `ready` | 拉 manifest |
| `done` 且 `failed` / `cancelled` | 展示 `error`，结束 |

关注字段：`status`、`playable`、`frames_done`、`frame_count`、`percent`、`play_base`、`error`。

- SSE 单次最长约 300s，未完成应重连。
- 对接方可在 SSE 失败后退避轮询 `GET /api/jobs/{job_id}`。本仓库演示页默认 SSE-only（失败直接报错，便于查代理）。

### 3. 拉播放清单

`GET /api/jobs/{job_id}/manifest.json`（必须走 Master，拿 JSON 正文；不要跟 302 在浏览器里解析）

| 字段 | 用途 |
|------|------|
| `status` | `partial` 还在写帧；`done` 已全部写完 |
| `bundle.frame_count` | 进度条 / seek **最大值** |
| `bundle.frames_available` | 合法索引 `0 .. frames_available-1` |
| `bundle.gop_size` | 30 |
| `bundle.key_frame_indices` | Seek 对齐关键帧 |
| `bundle.timeline.duration_sec` | 展示总时长 |
| `streams.camera_rr/fr/rl/fl` | `{ position, width, height, fps }` |

`play_base` 可能出现在状态/manifest 里：私有桶**不要**匿名 GET。媒体走 signed-media 或 Master。

### 4. 起播

1. 拉 `segments/000000.bin`，从各路 AU 取 SPS/PPS，配置四路 `VideoDecoder`
2. 先预缓冲约 **5 段**（约 5s），再 `play`
3. 播放中预取播放头前方约 **8 段**
4. 只请求起始 `start < frames_available` 且按 30 对齐的段
5. **partial 时播放上限 = `frames_available - 90`**（约 3s 垫）

媒体 URL：

1. `GET /api/jobs/{id}/signed-media?starts=0,30,60,...` → `fetch(url)` 直连公网 OSS  
2. CORS / 403 / 内网签名失败 → `{Base}/api/jobs/{id}/segments/{start:06d}.bin?proxy=1`

调试：`?media=proxy` 强制走 Master；`?media=signed` 强制签名 OSS。

### 5. 拉帧段

| HTTP | 处理 |
|------|------|
| `200` | 解析 segment → 单帧 bundle → 四路解码 |
| `425` | 段未凑齐 →「加载中」，按 `Retry-After` 重试（通常 1s） |
| `404` | 处理中可当加载中；`done` 后仍 404 再报错 |

单帧兼容：`.../frames/{index:06d}.bin`（同样可 `proxy=1`）。新播放器用 segments。

二进制格式见 [sview-frontend-api.md](./sview-frontend-api.md) §5 / §5b。

### 6. 边转边播（`manifest.status=partial`）

1. 约每秒刷新 `manifest.json`（`Cache-Control: no-store`）
2. 用新的 `frames_available`、`key_frame_indices` 更新可播范围
3. 播放头距前沿 &lt; 90 帧 →「加载中…」，**不要显示播放结束**
4. 暂停后等到领先 ≥ 150 帧再恢复，避免抖动
5. 进度条 max 始终用 `frame_count`；当前进度用播放头索引
6. 长播每分钟 `POST .../touch`（查状态 / SSE / signed-media 也会 touch）

`status=done` 且 `frames_available === frame_count` 后停刷 manifest；播到最后一帧再结束。

### 7. Seek

1. 目标 `>= frames_available`（或落入垫内）：限制在可播范围或提示加载中
2. 在 `key_frame_indices` 找 `≤ 目标` 的最近关键帧
3. 从该关键帧解码到目标
4. 单路是否关键帧以该路 AU 是否含 IDR 为准（不要只信 bundle 的 `key_frame`）

---

## 三、流程图

```text
POST /api/jobs
        │
        ├─ 200 cached/ready ─────────────────────────┐
        │                                            │
        └─ 202 ──► SSE /events                       │
                      │                              │
                      ├─ failed / cancelled → 结束     │
                      ├─ frames_done < 150 → 继续等    │
                      └─ ≥150 或 ready ───────────────┤
                                                     ▼
                                       GET .../manifest.json
                                                     │
                                       GET .../signed-media
                                         └─ 失败 → segments?proxy=1
                                                     │
                                       预缓冲 → 四路解码播放
                                                     │
                            partial? ──是──► 刷 manifest + touch
                                  │                    │
                                  │         距前沿 <90 帧 → 加载中
                                  │         领先恢复 ≥150 → 继续播
                                  │                    │
                                  └── done ◄───────────┘
                                         │
                                    播完 → 结束
```

---

## 四、状态对照

| 阶段 | `/api/jobs` status | manifest status | 前端 |
|------|--------------------|-----------------|------|
| 刚提交 | `queued` | 无 | 等待 |
| 转码写帧中 | `processing` | `partial` | `frames_done≥150` 后边转边播 |
| 全部完成 | `ready` | `done` | 完整播放 |
| 失败 | `failed` | — | 展示 error |
| 取消 | `cancelled` | — | 展示 error |

---

## 五、注意点

1. 进度条 max = `frame_count`；可播前沿 = `frames_available` / `frames_done`
2. `425` / 短暂缺帧 = 加载中，不是错误
3. 四路 H.264 Annex-B；某路 `is_null` / 空 AU 则跳过该路，不要整页失败
4. 解码失败先等下一关键帧（IDR），勿立刻整页报错
5. 浏览器需 WebCodecs（Chrome / Edge）
6. Base URL 指向 Master；Worker 不给浏览器供流
7. Windows 建议四路软解（DXVA 多路容易花屏）；可用 `?decode=hardware` / `?decode=software`
8. 卡顿且 CPU 不高：先查拉段路径（是否 signed-OSS、是否 `proxy=1` 绕远、是否顶到编码前沿）
9. 不要匿名请求 `play_base`（私有桶 403）；manifest 必须从 Master 拿 JSON
10. Bucket CORS 需放行前端 Origin 的 `GET`/`HEAD`（签名直连 OSS 时）
