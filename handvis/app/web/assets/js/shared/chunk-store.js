import { pruneChunkCache, touchAccessOrder } from "./chunk-utils.js";
import { parseMeshSegment } from "./mesh-segment.js";

/**
 * 创建通用 chunk 存储器。
 * 能力：
 * 1) chunk 懒加载（请求去重）
 * 2) 失败记忆（避免重复无效请求）
 * 3) 邻近预加载 + 缓存裁剪
 * 4) 流式构建容忍：chunk.ready=false 或拉取 404 时延迟重试，不计入失败。
 */

const RETRY_BASE_MS = 800;
const RETRY_MAX_MS = 4000;
const MAX_RETRY_ATTEMPTS = 30;   // 累计 ≈ 1.5 分钟,足够覆盖典型构建耗时

function isMissingChunkError(err) {
  // fetchJson/fetchArrayBuffer 在 4xx/5xx 时抛出包含状态码的 Error。
  if (!err) return false;
  const msg = String(err.message || err || "").toLowerCase();
  return msg.includes("404") || msg.includes("not found") || msg.includes("not_found");
}

export function createChunkStore({
  getPlayback,
  fetchJson,
  fetchArrayBuffer,
  joinUrl,
  chunkCache,
  pendingChunks,
  failedChunks,
  accessOrder,
  getChunkPreloadCount,
  getMaxCachedChunks,
}) {
  // chunkIdx -> 已尝试次数，用于退避控制
  const retryAttempts = new Map();

  function scheduleRetry(chunkIdx) {
    const attempts = (retryAttempts.get(chunkIdx) || 0) + 1;
    retryAttempts.set(chunkIdx, attempts);
    if (attempts > MAX_RETRY_ATTEMPTS) {
      failedChunks.add(chunkIdx);
      return;
    }
    const delay = Math.min(RETRY_MAX_MS, RETRY_BASE_MS * Math.pow(1.3, attempts - 1));
    setTimeout(() => {
      // 触发自动重新加载,并不直接返回 promise(避免堵塞调用方)
      ensureChunkLoaded(chunkIdx).catch(() => {});
    }, delay);
  }

  async function ensureChunkLoaded(chunkIdx) {
    const playback = getPlayback();
    if (!playback) return null;
    const chunk = playback.chunks[chunkIdx];
    if (!chunk) return null;

    // 流式构建期间，chunk.ready=false 表示后端还没写完，先排队等待重试。
    if (chunk.ready === false) {
      if (!pendingChunks.has(chunkIdx) && !chunkCache.has(chunkIdx)) {
        scheduleRetry(chunkIdx);
      }
      return null;
    }

    // 命中缓存：直接触碰 LRU 顺序并返回。
    if (chunkCache.has(chunkIdx)) {
      touchAccessOrder(accessOrder, chunkIdx);
      return chunkCache.get(chunkIdx);
    }
    // 命中 pending：复用同一个 Promise，避免重复网络请求。
    if (pendingChunks.has(chunkIdx)) return pendingChunks.get(chunkIdx);
    if (failedChunks.has(chunkIdx)) return null;

    const task = (async () => {
      const binRel = String(chunk.bin || "").trim();
      if (!binRel) {
        throw new Error("chunk index missing bin path");
      }
      const buf = await fetchArrayBuffer(joinUrl(playback.chunkBaseUrl, binRel));
      const parsed = parseMeshSegment(buf);
      const loaded = {
        meta: parsed.header,
        left: parsed.left,
        right: parsed.right,
        handRoots: parsed.handRoots,
      };

      chunkCache.set(chunkIdx, loaded);
      touchAccessOrder(accessOrder, chunkIdx);
      retryAttempts.delete(chunkIdx);
      return loaded;
    })()
      .catch((err) => {
        if (isMissingChunkError(err)) {
          // 构建中文件可能尚未落盘,延迟重试,不进入 failedChunks。
          scheduleRetry(chunkIdx);
          return null;
        }
        failedChunks.add(chunkIdx);
        console.error("Chunk load failed", chunkIdx, err);
        return null;
      })
      .finally(() => {
        pendingChunks.delete(chunkIdx);
      });

    pendingChunks.set(chunkIdx, task);
    return task;
  }

  function preloadNearChunks(currentIdx) {
    const playback = getPlayback();
    const preloadCount = getChunkPreloadCount();
    for (let i = 1; i <= preloadCount; i += 1) {
      const n = currentIdx + i;
      if (playback && n < playback.chunks.length) {
        ensureChunkLoaded(n).catch(() => {});
      }
    }
    pruneChunkCache({
      chunkCache,
      accessOrder,
      currentIdx,
      maxCachedChunks: getMaxCachedChunks(),
      preloadCount,
      chunkLength: playback ? playback.chunks.length : 0,
    });
  }

  return {
    ensureChunkLoaded,
    preloadNearChunks,
    clear() {
      chunkCache.clear();
      pendingChunks.clear();
      failedChunks.clear();
      accessOrder.length = 0;
      retryAttempts.clear();
    },
  };
}
