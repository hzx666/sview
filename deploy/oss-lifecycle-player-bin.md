# OSS Lifecycle兜底（在阿里云控制台配置，或使用 ossutil / Terraform）
#
# 前缀: player-bin/
# 规则建议:
#   1) 最后修改时间 > 2 天 → 删除（安全网；应用层 GC 默认空闲 4h + 500GiB LRU）
#   2) 勿与正在写入的 processing job 冲突：Worker 持续 PUT 会刷新 Last-Modified，
#      但空闲后仍依赖应用 touch + GC。Lifecycle 只做兜底。
#
# 控制台路径: OSS → Bucket ss-oss-data-platform → 基础设置 → 生命周期
#
# 示例策略 JSON（按控制台/API 格式微调）:
# {
#   "rules": [
#     {
#       "id": "player-bin-expire-2d",
#       "prefix": "player-bin/",
#       "status": "Enabled",
#       "expiration": { "days": 2 }
#     }
#   ]
# }
#
# CORS（浏览器直拉 play_base）:
#   AllowedOrigins: ["*"] 或前端域名
#   AllowedMethods: GET, HEAD
#   AllowedHeaders: *
#   ExposeHeaders: ETag, x-oss-request-id
#   MaxAgeSeconds: 600
