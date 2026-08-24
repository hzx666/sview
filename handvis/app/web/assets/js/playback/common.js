/**
 * 播放页（playback）通用工具。
 * 这些函数与页面状态解耦，便于后续继续拆分渲染/交互子模块。
 */

// 将 API/运行时错误对象统一成可读文本。
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

// 健壮解析非空 API 响应体（JSON 或纯文本）。
export async function parseApiBody(resp) {
  const text = await resp.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (_e) {
    return { detail: text };
  }
}

// 调试文本格式化助手。
export function fmt(n) {
  return Number.isFinite(n) ? n.toFixed(3) : "NaN";
}

// 角度转弧度助手。
export function degToRad(v) {
  return (Number(v) || 0) * Math.PI / 180;
}

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
  // 约定：调用方拿到已解析对象，不处理流式场景。
  return resp.json();
}

export async function fetchArrayBuffer(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status} for ${url}`);
  // chunk/bin 一律走 ArrayBuffer，避免文本解码损耗。
  return resp.arrayBuffer();
}

export function joinUrl(base, rel) {
  return new URL(rel, new URL(base, window.location.origin)).toString();
}
