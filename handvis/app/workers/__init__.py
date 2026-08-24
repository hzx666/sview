"""后台 worker 进程入口。

集群侧只跑 `python -m app.workers`，通过 Redis 收任务、回结果。
"""
