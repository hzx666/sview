/**
 * 首页（index）通用工具函数集合。
 * 目标：
 * 1) 统一错误格式化/文本转义/复制等通用能力
 * 2) 降低主文件长度，便于后续按功能继续拆分
 */

// 将后端/运行时不同形态的错误统一为可读文本。
export function formatError(err) {
  if (!err) return "未知错误";
  if (typeof err === "string") return err;
  if (err instanceof Error) return err.message || String(err);
  if (typeof err === "object") {
    if (typeof err.detail === "string") return err.detail;
    try {
      return JSON.stringify(err);
    } catch (_e) {
      return String(err);
    }
  }
  return String(err);
}

// 对文本做转义，避免拼接到 HTML 时产生注入问题。
export function esc(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

// 列表里缩短 ep_dir 显示，同时尽量保留末尾关键信息。
export function epTail(epDir) {
  const s = String(epDir || "");
  if (!s) return "";
  const parts = s.split("/").filter(Boolean);
  if (parts.length >= 4) {
    return `.../${parts.slice(-4).join("/")}/`;
  }
  return `...${s.slice(Math.floor(s.length / 2))}`;
}

// 复制优先走 Clipboard API，失败时回退到隐藏 textarea 方案。
export async function copyText(text) {
  const value = String(text || "");
  if (!value) return false;
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      return true;
    }
  } catch (_e) {}
  try {
    const ta = document.createElement("textarea");
    ta.value = value;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch (_e) {
    return false;
  }
}

// 数值格式化：保留 3 位小数。
export function fmtNum(n) {
  return Number.isFinite(n) ? n.toFixed(3) : "NaN";
}

// 角度转弧度工具（Three.js 欧拉角接口使用弧度）。
export function degToRad(v) {
  return (Number(v) || 0) * Math.PI / 180;
}

// 统一错误文案的 fetch 工具函数（JSON）。
export async function fetchJson(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    let detail = "";
    try {
      const data = await resp.json();
      detail = data?.detail ? `: ${formatError(data.detail)}` : "";
    } catch (_e) {}
    throw new Error(`HTTP ${resp.status} for ${url}${detail}`);
  }
  return resp.json();
}

// 二进制请求工具函数。
export async function fetchArrayBuffer(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status} for ${url}`);
  return resp.arrayBuffer();
}

// 安全拼接相对 URL。
export function joinUrl(base, rel) {
  return new URL(rel, new URL(base, window.location.origin)).toString();
}

const SUPPORTED_OSS_BUCKETS = ["ss-oss-intern", "ss-oss1", "ss-oss-external-qingyun"];

function normalizeOssDirForUi(ossDir) {
  const text = String(ossDir || "").trim();
  if (text.startsWith("oss://")) return text;
  for (const bucket of SUPPORTED_OSS_BUCKETS) {
    if (text.startsWith(`${bucket}/`)) return `oss://${text}`;
  }
  return text;
}

// 判断是否为单个 ep 目录（例如 .../ep1/）。
export function isLikelySingleEpDir(ossDir) {
  const cleaned = normalizeOssDirForUi(ossDir).replace(/\/+$/, "");
  if (!cleaned.startsWith("oss://")) return false;
  const tail = cleaned.slice(cleaned.lastIndexOf("/") + 1);
  return /^ep[\w-]*$/i.test(tail);
}

// 从 .../epX/ 回退到父目录 .../<uuid>/。
export function getParentDir(ossDir) {
  const normalized = normalizeOssDirForUi(ossDir);
  if (!normalized.startsWith("oss://") || !normalized.endsWith("/")) return "";
  const withoutTrailing = normalized.slice(0, -1);
  const lastSlash = withoutTrailing.lastIndexOf("/");
  if (lastSlash < "oss://".length) return "";
  return `${withoutTrailing.slice(0, lastSlash + 1)}`;
}
