"""SView 后端包。

目录：
  http/     Master HTTP（入队、状态、播放缓存）
  worker/   CPU / GPU 编码 worker
  ingest/   mcap / HDF5 / Orin 读取
  jobs/     Redis 任务状态和 bench 类型
  cache/    OSS 播放缓存 GC 与对齐
"""
