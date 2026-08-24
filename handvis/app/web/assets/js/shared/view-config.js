// 首页与播放页共享的视图默认配置。
// 说明：这里只放两页都一致的参数，页面差异（如相机位置）由各自入口文件覆盖。
export const COMMON_VIEW_CONFIG = {
  // 预加载窗口：当前 chunk 后方预拉取数量。
  // 30 帧一段时 6 块约 6 秒前瞻，避免块变碎后边界卡顿。
  chunkPreloadCount: 6,
  // 缓存上限：与 preload 配合，控制内存占用。
  maxCachedChunks: 12,
  // 法线重算节流步长（每 N 帧重算一次）。
  // 从 2 提升到 4，降低主线程 CPU 占用；视觉差异极小。
  normalUpdateInterval: 4,
  // invalid 兜底判定的采样步长，仅用于识别整帧无有效坐标的情况。
  frameValiditySampleStep: 7,
};
