# SView 转码场景测试 API

正式播放接口 `POST /api/jobs`、`GET /api/jobs/{id}` **不变**。  
测试走独立命名空间 `/api/test/*`，用于按 `type` 切换 8 种转码场景并测量首帧 / 全帧耗时。

前端正式对接文档见 [`sview-frontend-api.md`](./sview-frontend-api.md)。

---

## 场景 type（1–8）

| type | 场景 | 默认源 | size | crf | force_reencode |
|-----:|------|--------|-----:|----:|---------------:|
| 1 | H264 直出 | H264 | 0 | 35 | 0 |
| 2 | H264 → 720×720 | H264 | 720 | 35 | 0 |
| 3 | H264 → CRF35（保分辨率） | H264 | 0 | 35 | 1 |
| 4 | H264 → 720×720 + CRF35 | H264 | 720 | 35 | 0 |
| 5 | H265→H264 保分辨率 | H265 | 0 | 35 | 0 |
| 6 | H265→H264 720×720 | H265 | 720 | 35 | 0 |
| 7 | H265→H264 CRF35 | H265 | 0 | 35 | 0 |
| 8 | H265→H264 720×720 + CRF35 | H265 | 720 | 35 | 0 |

- `job_id` = `{源文件名去掉.mcap}__t{type}`，不同 type 缓存互不覆盖。  
- HEVC 无 passthrough：type5 与 type7、type6 与 type8 参数相同（可作复测对照）。

---

## 接口

### `GET /api/test/types`

列出全部 type，以及服务端默认源配置。

**响应示例：**

```json
{
  "types": [
    {
      "type": 1,
      "name": "H264 直出(passthrough)",
      "source_kind": "h264",
      "transcode_size": 0,
      "transcode_crf": 35,
      "force_reencode": 0,
      "notes": "..."
    }
  ],
  "defaults": {
    "BENCH_H264_SOURCE": null,
    "BENCH_H265_SOURCE": null
  }
}
```

---

### `POST /api/test/jobs`

提交某一 type 的测试任务。

**请求体：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `type` | int | 是 | `1`–`8`（也可用 `bench_type`） |
| `source` | string | 条件 | OSS/`oss://...` 或挂载路径；不传则用 `BENCH_H264_SOURCE` / `BENCH_H265_SOURCE` |
| `force` | boolean | 否 | **默认 `true`**。`true` 时先删除该 `job_id` 本地缓存再入队；`false` 且已 ready 则直接 `200` 返回缓存 |

```json
{
  "type": 3,
  "source": "oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/.../xxx.mcap",
  "force": true
}
```

**返回：**

| HTTP | 说明 |
|------|------|
| `200` | 命中缓存（仅 `force=false`） |
| `202` | 已入队 |
| `400` | 参数错误 / 缺 source |
| `404` | 源文件本地映射不存在 |
| `503` | Redis 不可用，或缺少 `bench_types` 模块 |

**响应字段（在正式 job 字段基础上额外）：**

| 字段 | 说明 |
|------|------|
| `type` / `type_name` | 场景编号与名称 |
| `transcode` | `{ size, crf, force_reencode }` |
| `first_frame_sec` | 完成后才有：开始处理 → 首帧写出（秒） |
| `full_sec` | 完成后才有：开始处理 → 全部完成（秒） |
| `notes` | 场景备注 |

播放资源路径与正式任务相同：

- `GET /api/jobs/{job_id}/manifest.json`
- `GET /api/jobs/{job_id}/frames/000000.bin`

---

### `GET /api/test/jobs/{job_id}`

查测试任务状态。

| Redis `status` | 行为 |
|----------------|------|
| `queued` / `processing` / `failed` | **以 Redis 为准**，不会被磁盘上旧的 ready 缓存盖掉 |
| `ready`（或仅有磁盘 ready） | 返回完成状态；含 `first_frame_sec` / `full_sec`（若 worker 已写入） |

计时字段：

- `first_frame_sec`：worker 开始处理 → 写出第 1 帧  
- `full_sec`：worker 开始处理 → manifest done  

---

## 部署注意

1. **HTTP 与 worker（`sview_worker.py`）必须同一版本一起部署。**  
   - 测试队列项是 **JSON**；旧 worker 无法正确解析。  
   - 生产任务仍是 **纯路径字符串**，兼容旧行为。  
2. 镜像需包含 `bench_types.py`（见根目录 `Dockerfile` COPY 列表）。  
3. **队列名必须一致：**

```bash
REDIS_QUEUE_KEY=player:queue
```

HTTP 与 worker 任一侧仍用默认 `mcap:queue` 都会导致任务一直 `queued`。

4. 可选默认源：

```bash
BENCH_H264_SOURCE=oss://.../xxx_h264.mcap
BENCH_H265_SOURCE=oss://.../xxx_h265.mcap
```

也可每次在请求体里传完整 `source`（推荐）。

5. 资源建议与压测一致：`CPU_CONCURRENCY=2`、`CHUNK_FRAMES=150`、`TRANSCODE_PRESET=veryfast` 等。

---

## 客户端脚本

单次 / 多场景：

```bash
python scripts/bench_online.py --base-url http://139.224.211.114:8080 \
  --h264 "oss://.../h264.mcap" \
  --h265 "oss://.../h265.mcap"
```

10 轮去极值均值：

```bash
python scripts/bench_online_rounds.py --base-url http://139.224.211.114:8080 --rounds 10 \
  --h264 "oss://.../h264.mcap" \
  --h265 "oss://.../h265.mcap" \
  --out output/bench_online_10rounds
```

报告：`output/bench_online_10rounds/report_10rounds.md`、`飞书报告.md`。

镜像构建：

```bash
docker build -t mcap-processor-cpu:<tag> .
```