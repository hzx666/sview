# 遗留代码

| 文件 | 说明 |
|------|------|
| `server_cpu.py` | 旧 CPU 转码入口；现网请用根目录 `sview_worker.py`（NAL 拆帧 + 渐进发布） |

GPU 路径仍使用根目录 `server_gpu.py` + `deploy/job-gpu.yaml`。
