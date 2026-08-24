/**
 * 3D chunk 通用工具（首页/播放页共享）
 * 目标：
 * 1) 去除 index.js 与 playback.js 的重复逻辑
 * 2) 将可复用的“纯函数”集中管理
 */

// 记录某个 chunk 最近被访问，用于近似 LRU 淘汰。
export function touchAccessOrder(accessOrder, idx) {
  const p = accessOrder.indexOf(idx);
  if (p >= 0) accessOrder.splice(p, 1);
  accessOrder.push(idx);
}

// 近似 LRU 裁剪，并保留当前 chunk 与预加载窗口内 chunk。
export function pruneChunkCache({
  chunkCache,
  accessOrder,
  currentIdx,
  maxCachedChunks,
  preloadCount,
  chunkLength,
}) {
  const keep = new Set();
  if (currentIdx >= 0) keep.add(currentIdx);
  for (let i = 1; i <= preloadCount; i += 1) {
    const n = currentIdx + i;
    if (n < chunkLength) keep.add(n);
  }

  while (chunkCache.size > maxCachedChunks && accessOrder.length > 0) {
    const victim = accessOrder[0];
    if (keep.has(victim)) {
      accessOrder.push(accessOrder.shift());
      continue;
    }
    accessOrder.shift();
    chunkCache.delete(victim);
  }
}

// 根据时间查找 chunk 索引，并对越界情况做两端回退。
export function findChunkByTime(chunks, t) {
  for (let i = 0; i < chunks.length; i += 1) {
    const c = chunks[i];
    if (t >= c.start_time && t < c.end_time) return i;
  }
  if (chunks.length === 0) return -1;
  if (t < chunks[0].start_time) return 0;
  return chunks.length - 1;
}

// 将帧数据写入目标缓冲；NaN/Inf 直接透传（不做回退补值）。
export function copyFrameSafe(dst, src) {
  const n = Math.min(dst.length, src.length);
  for (let i = 0; i < n; i += 1) {
    dst[i] = src[i];
  }
}

// 采样判断帧是否至少包含一个有效 xyz 点。
export function frameHasFinitePoint(frameData, sampleStep = 3) {
  if (!frameData || frameData.length < 3) return false;
  const step = Math.max(3, Number(sampleStep) || 3);
  for (let i = 0; i + 2 < frameData.length; i += step) {
    const x = frameData[i];
    const y = frameData[i + 1];
    const z = frameData[i + 2];
    if (Number.isFinite(x) && Number.isFinite(y) && Number.isFinite(z)) return true;
  }
  return false;
}
