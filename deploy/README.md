# 部署清单

完整 ACS 步骤见 **[docs/acs-deploy.md](../docs/acs-deploy.md)**。

| 文件 | 用途 |
|------|------|
| `../Dockerfile` | CPU 镜像（入口 `sview_worker.py`） |
| `deployment-master.yaml` | **推荐** Master：API + 入队 + metrics + GC |
| `deployment-worker.yaml` | **推荐** Worker：Redis 消费 + 转码 + OSS `player-bin` |
| `keda-worker.yaml` | 按 Redis 队列深度扩 Worker（需集群装 KEDA） |
| `hpa-worker-cpu.yaml` | 无 KEDA 时的 CPU HPA |
| `oss-lifecycle-player-bin.md` | OSS Lifecycle / CORS 说明 |
| `secret-template.yaml` | Redis / OSS Secret 模板 |
| `deployment.yaml` | 旧：单进程 all-in-one（兼容） |
| `deployment-cpu-redis.yaml` | 更旧的单副本清单 |
| `service.yaml` | 旧 Service（`sview-bin-svc`）；新用 master 清单内 Service |
| `job-pvc.yaml` / `job-gpu.yaml` | 一次性 Job |

```bash
# 新环境（Master + Worker）
docker build -t <registry>/playertrans-backend:<tag> .
docker push <registry>/playertrans-backend:<tag>
# 改 master/worker YAML 里的 image tag 后：
kubectl apply -f deploy/secret-template.yaml   # 先填真实 Secret，含 oss-credentials
kubectl apply -f deploy/deployment-master.yaml
kubectl apply -f deploy/deployment-worker.yaml
kubectl apply -f deploy/keda-worker.yaml       # 可选
```

注意：`REDIS_QUEUE_KEY=player:queue`（Master 与 Worker 必须一致）。
