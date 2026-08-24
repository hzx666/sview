# OSS 播放缓存 + Master / Worker 角色

## 角色

| `SVIEW_ROLE` | 行为 |
|--------------|------|
| `all`（默认） | 兼容：同一进程 HTTP + Redis 消费 |
| `master` | HTTP API、入队、`/metrics`、缓存 GC；**不转码** |
| `worker` | 消费 Redis 转码；心跳写 Redis；默认不开 HTTP |

任务分发：Master 入队 → Redis `player:queue` → Worker `BLPOP`（**不是** Master 直连 Pod）。

扩缩：优先 [KEDA Redis list](../deploy/keda-worker.yaml)；无 KEDA 时用 [CPU HPA](../deploy/hpa-worker-cpu.yaml)。Master 暴露 `GET /metrics` 供后续自定义指标。

## OSS 播放缓存（`player-bin/`）

```bash
OSS_CACHE=1
NO_UPLOAD=1
OSS_CACHE_PREFIX=player-bin
OSS_PUBLIC_ENDPOINT=oss-cn-shanghai.aliyuncs.com
OSS_ENDPOINT=https://oss-cn-shanghai-internal.aliyuncs.com
CACHE_MAX_BYTES=536870912000   # ~500GiB
CACHE_IDLE_TTL_SEC=14400       # 4h idle
CACHE_GC_INTERVAL_SEC=300
CACHE_PIN_SEC=600
```

对象布局：

```text
oss://{bucket}/player-bin/{job_id}/manifest.json
oss://{bucket}/player-bin/{job_id}/segments/000000.bin
```

治理：

1. 前端 / status / SSE 会 `touch` → 刷新 `last_access_at` + `pin_until`
2. Master GC：空闲 TTL + 逼近 `CACHE_MAX_BYTES` 时 LRU；跳过 `processing` / pin  
   - 驱逐时 **整前缀删除** `player-bin/{job_id}/`，并清 Redis `frames_done/playable/play_base`
3. **取消 / force 重提**：同样 purge 本地 `/cache/{job_id}` + OSS 前缀 + Redis 进度，避免「manifest 虚高、早期 segment 404」
4. Worker 开转前会先 wipe 该 job 的 OSS 播放缓存，防止 Lifecycle/中断留下的尾段空洞
5. Master 读 manifest 时按 OSS **从 0 起连续 segment** 对齐 `frames_available`；发现空洞则压到真实可播帧（常为 0）并避免当 ready
6. 控制台 Lifecycle 仍可做兜底：见 [oss-lifecycle-player-bin.md](../deploy/oss-lifecycle-player-bin.md)（注意按修改时间删对象可能先删早期段，务必依赖上述对齐）

浏览器播媒体（私有 Bucket）——两套 endpoint，不要混用：

| 方向 | 变量 | 域名 | 谁用 |
|------|------|------|------|
| 上传 / Pod 回源 | `OSS_ENDPOINT` | `…-internal.aliyuncs.com` | Worker / Master（VPC 内） |
| 返回给浏览器 | `OSS_PUBLIC_ENDPOINT` | `oss-cn-shanghai.aliyuncs.com` | 签名 URL / `play_base` |

- 前端打 Master；缺本地段时 Master **302 → 公网签名 URL**（`sign_oss_url` 只用 PUBLIC）
- 若 Location 仍带 `-internal`，说明跑的还是旧镜像或 `OSS_PUBLIC_ENDPOINT` 配错；查 `GET /api/debug/oss`
- Bucket CORS 需放行前端 Origin（签名 GET 仍是跨域）

## API（Master）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/metrics` | Prometheus：queue / processing / workers / cache_bytes |
| GET | `/api/workers` | Worker 心跳列表 |
| GET | `/api/jobs/{id}/events` | SSE 进度 |
| POST | `/api/jobs/{id}/touch` | 刷新访问时间（防 GC） |
| POST | `/api/cache/gc` | 手动跑一轮 GC |

Job 状态含 `percent`、`play_base`、`cache_bytes`。

## 部署

```bash
kubectl apply -f deploy/deployment-master.yaml
kubectl apply -f deploy/deployment-worker.yaml
kubectl apply -f deploy/keda-worker.yaml   # 或 hpa-worker-cpu.yaml
```

前端 Base URL 指向 `sview-master-svc:8080`（或对外 Ingress）。

日志：容器 stdout → 阿里云 SLS（不要 Master 去刮各 Worker 文件）。
