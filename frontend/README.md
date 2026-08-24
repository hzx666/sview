# SView Frontend

四路 H.264 播放器：Base URL 指向 **Master**，拉 `manifest` + `segments/*.bin`（约 30 帧一段），用 WebCodecs 解码。

支持 **边转边播**：Worker 渐进发布 `status=partial`。API `playable` 约 ≥30 帧就会为 true；**本页等到约 150 帧再起播**，播到编码前沿会暂停等待。

正式接口说明：[docs/sview-frontend-api.md](../docs/sview-frontend-api.md)  
操作流程：[docs/sview-playback-flow.md](../docs/sview-playback-flow.md)

## 要求

- Chrome / Edge（`VideoDecoder`）
- 可达的 **Master**（ACS LB / 本地 Master），不是 Worker Pod
- 源文件为 **`oss://.../*.mcap`**（不要提交 `.mp4`）

## 本地开发（推荐 Vite 反代）

Master 对私有 OSS 可能 `302` 到公网签名 URL；浏览器直连常遇 CORS / 内网域名问题。开发时用 Vite 同域代理，由 Node 跟随重定向：

```bash
# 可选：默认已指向示例 Master；按环境覆盖
# Windows PowerShell:
$env:VITE_SVIEW_PROXY_TARGET = "http://<master-host>:8080"

cd frontend
npm install
npm run dev
```

页面 **Base URL** 填：

```text
http://127.0.0.1:5173/sview
```

代理规则见 `vite.config.ts`：`/sview` → `VITE_SVIEW_PROXY_TARGET`（`followRedirects: true`）。

## 对接生产 Master

Base URL 填对外 Master，例如：

```text
http(s)://<master-host>:8080
```

需保证：

1. Master CORS 放行前端 Origin
2. **Bucket CORS** 放行前端 Origin 的 GET（签名直连 OSS）
3. Master 镜像含 `GET /api/jobs/{id}/signed-media`；`OSS_PUBLIC_ENDPOINT` 为公网域名
4. 失败自动回退 `?proxy=1`；可用 `?media=proxy` 强制 Master 中转

## 页面用法

1. 填 **Base URL**（上节）
2. 填 **OSS source**：完整 `oss://.../*.mcap`
3. **提交并播放** → 入队 → SSE 至约 150 帧（或 ready）→ 拉 manifest / segments 起播

也可只填已有 **Job ID**，点「仅加载」。

进度：`GET /api/jobs/{id}/events`（SSE）。本页默认 SSE-only，失败直接报错（排查代理）；对接方可自行加轮询回退。  
长播会 touch，避免缓存被 GC。

### API 摘要

| 方法 | 路径 |
|------|------|
| `POST` | `/api/jobs` `{"source":"oss://...mcap"}` |
| `GET` | `/api/jobs/{job_id}` / `/api/jobs/{job_id}/events` |
| `POST` | `/api/jobs/{job_id}/touch` |
| `GET` | `/api/jobs/{job_id}/manifest.json` |
| `GET` | `/api/jobs/{job_id}/segments/000000.bin`（推荐；可加 `?proxy=1`） |

字段与状态机见对接文档 §0–§6；起播阈值与前沿垫见 [播放流程](../docs/sview-playback-flow.md)。

## 解码

- Windows 默认软解四路（避免 DXVA 花屏）；URL 可加 `?decode=hardware` / `?decode=software`
- CPU 不高仍卡顿时，优先查拉段耗时 / 是否边转边播顶到前沿，而非一味怪软解

## 构建

```bash
npm run build
npm run preview
```

静态产物在 `frontend/dist/`。生产环境把 Base URL 指到 Master；注意私有 OSS 的媒体路径策略与对接文档一致。
