"""播放路由内可能用到的进程内状态。

历史曾用 dict + Semaphore 做转码任务去重 / 并发控制，已迁移到 Redis
令牌桶（app/services/transcode_queue.py），本文件保留为占位，
若未来需要进程内状态再补充。
"""
