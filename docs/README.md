# 文档索引

| 文档 | 状态 | 说明 |
|------|------|------|
| [sview-frontend-api.md](./sview-frontend-api.md) | 现行 | **接口合同**（Master API / SSE / signed-media / segments） |
| [sview-frontend-call-chain.md](./sview-frontend-call-chain.md) | 现行 | **交给前端的调用链路**（顺序转 / 稀疏、signed-media、Seek） |
| [sview-playback-flow.md](./sview-playback-flow.md) | 现行 | 播放细节（顺序转起播 ≥150 帧、前沿垫）；链路以 call-chain 为准 |
| [sview-oss-cache-roles.md](./sview-oss-cache-roles.md) | 现行 | Master/Worker 角色与 OSS `player-bin` |
| [acs-deploy.md](./acs-deploy.md) | 现行 | ACS 部署与联调 |
| [sview-fullstack-flow.md](./sview-fullstack-flow.md) | 现行 | 全链路时序（细节以 frontend-api 为准） |
| [sview-test-api.md](./sview-test-api.md) | 现行 | `/api/test/*` type 1–8 压测 |
| [archive/sview-local-cache-arch.md](./archive/sview-local-cache-arch.md) | 过期 | 早期架构草案，路径与现网不一致 |

飞书可贴的压测报告见仓库根目录旁：`output/bench_online_10rounds/飞书报告.md`（跑测生成，不入库）。
