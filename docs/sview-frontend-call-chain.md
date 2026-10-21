# 前端调用链路

Base URL 只指向 Master。本地联调是 `http://127.0.0.1:5173/sview`（Vite 反代到 Master）。

同一路稀疏播放从提交到 Seek 共用一个稳定的 `client_id`。段长以清单里的 `gop_size` 为准，当前是 30。段号 `start = Math.floor(frame / gop_size) * gop_size`，文件名是 6 位，例如 `000180.bin`。

媒体先走 `signed-media` 拿到 OSS 地址再直连。`/segments` 只在签发或直连失败时降级，不是每段都打 Master。

## 链路

**顺序转（`publish_mode` 省略或 `"progressive"`）**

- 命中缓存：`1 → 3 → 4 → 5`
- 未命中：`1 → 2 → 3 → 4 → 5`

**稀疏（`publish_mode: "sparse"`）**

- 命中缓存：`1 → 8 → 3 → 4 → 5`
- 未命中：`1 → 8 → 2 → 3 → 4 → 5`
- 播放中 Seek：`8 → 3 → 4 → 5`

`6` 在加载播放器之后每 60 秒打一次。`7` 只在关闭窗口时打。

## 接口

| 顺序 | 接口 | 接口解释 | 关键字段 | 前端处理 |
|---|---|---|---|---|
| 1 | `POST /api/jobs` | 把 OSS 地址提交到队列 | `job_id`、`status`、`cached`、`playable`、`available_ranges` | 稀疏带 `publish_mode: "sparse"` 和 `client_id`。`200` 且 `ready` 或 `cached`：跳过 2，直接去 3。`202`：去 2。稀疏不能带 `start_sec`。`job_id` 由服务端生成，前端不要自己拼 |
| 8 | `POST /api/jobs/{job_id}/encode` | 稀疏专用，告诉后端这个窗口现在要看哪一帧。只在三种时候调用：① 提交成功后一次，或者 Seek 之后；② 播放到 `available_ranges` 里还没有的段时；③ 等同一段超过 10 秒、清单一直没有新段时补一次（兜底）。顺着播、段已经生成时不调用，也不要定时轮询 | `frame_index`、`prefetch_segments`、`client_epoch`、`client_id` | `frame_index`：起播帧、Seek 目标帧，或当前正在等的段起点，不要固定传 0。`client_id`：每个标签页一个 UUID，和提交时相同。`client_epoch`：每次 Seek 加 1，只用于排查，不影响结果。`prefetch_segments`：可选，后端暂未使用，传 3。`200` 且 `ready`：这段已有，直接去 4。`202`（`queued` 或 `already_queued`）：正在转或有人在转，轮询 3 等这段出现。`404` 任务不存在，带 `force: true` 从 1 重新提交；`409` 不是稀疏任务；`416` 超出总帧数；`503` 稍后重试。顺序转不调用 |
| 2 | `GET /api/jobs/{job_id}/events` | 未命中缓存时等可播。SSE，事件名 `progress`、`done` | `playable`、`frames_done`、`status`、`available_ranges` | 稀疏：`playable === true` 或 `frames_done ≥ 1` 就去 3。顺序转：仍等 `frames_done ≥ 150`。`status` 为 `failed` / `cancelled` 则停止 |
| 3 | `GET /api/jobs/{job_id}/manifest.json` | 开播后以清单为准。稀疏在等某一段时每 400ms 再拉一次。`cache: no-store` | `available_ranges`、`frames_available`、`frame_count`、`gop_size`、`streams` | 进度条 `max = frame_count`。顺序转只拉 `start < frames_available` 的段。稀疏只拉落在 `available_ranges` 里的段（左闭右开）。`frames_available` 只是从 0 连续的前缀，Seek 出去的岛不加进这个数。流名用 `streams` 的 key：头环 bin 里是 `rr` / `fr` / `rl` / `fl`（清单里常写作 `camera_rr`），遥操是 topic 去掉开头的 `/`，例如 `camera_f_l/color/image_raw`。清单 `404` 且正文 `file not found`：OSS 清单没了，带 `force: true` 从 1 重新提交 |
| 4 | `GET /api/jobs/{job_id}/signed-media?kind=segments&start=0&count=600&expires=1800` | 按段号批量签发公网 OSS 地址，不检查段是否已生成。一次最多 600 条（`OSS_SIGN_BATCH_MAX`）。也可以 `starts=0,30,60`。`expires` 可选，不超过服务端 `OSS_SIGN_EXPIRES`（默认 7200 秒） | `items[].url`、`expires_in`、`skipped` | 打开、Seek 到已签范围外、播到已签范围末尾前约 2 分钟、或签名快过期时，一次签后面约 10 分钟的段并缓存，其余时间不再签。取段仍只取落在 3 的 `available_ranges` 里的段；`fetch(url)` 返回 `404` 当作还没生成稍后重试，`403` 先重签一次。越过 `frame_count` 或不在段边界上的起点在 `skipped` 里。`404 job not found` 表示任务不存在；`503` 表示没开 OSS 缓存或 Redis 不可用，改走 5 |
| 5 | `GET /api/jobs/{job_id}/segments/{start六位}.bin` | 4 失败时的降级，经 Master 拿同一段 | 二进制段 | 拆成单帧，按清单流名各建一个 `VideoDecoder`。路数以 `streams` 为准，不固定为 4 |
| 6 | `POST /api/jobs/{job_id}/touch` | 刷新 `last_access_at` | 无 | 进入播放后每 60 秒一次。刚访问过的 30 分钟内不会被挤掉；连续 4 小时没有访问才按空闲清除。总缓存超过约 500 GiB 时，过了 30 分钟保护也可能被挤掉 |
| 7 | `POST /api/jobs/{job_id}/cancel` | 取消任务 | `status` | 关闭窗口时调用 |

## 请求示例

提交稀疏任务：

```json
POST /api/jobs
{
  "source": "oss://bucket/path/episode113_0.mcap",
  "publish_mode": "sparse",
  "force": false,
  "client_id": "浏览器里生成一次的 id"
}
```

稀疏提交后，以及每次 Seek：

```json
POST /api/jobs/{job_id}/encode
{
  "frame_index": 180,
  "prefetch_segments": 3,
  "client_epoch": 1,
  "client_id": "和提交时同一个"
}
```

`200` 后可以直接去 4。`202` 后轮询 3，直到 `available_ranges` 盖住 `[180 所在段起点, 起点 + gop_size)`。等待期间清单超过 10 秒没有任何新段，再打一次 8；连续 2 分钟没有任何新段，才按这段超时处理。
