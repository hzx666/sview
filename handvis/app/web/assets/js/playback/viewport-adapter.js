/**
 * 播放页视口适配：
 * 统一处理主 WebGL 画布、浮窗位置和视频投影层在窗口变化后的尺寸同步。
 */

function getViewportSize(mountEl) {
  const rect = mountEl?.getBoundingClientRect?.();
  const width = Math.max(1, Math.floor(rect?.width || window.innerWidth || 1));
  const height = Math.max(1, Math.floor(rect?.height || window.innerHeight || 1));
  return { width, height };
}

function clampNumber(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function updatePlaybackUiScale(width, height) {
  // 以常见 16:9 播放页为设计基准；外部页面变大或变小时，控制层按同一比例同步缩放。
  const scaleByWidth = width / 1920;
  const scaleByHeight = height / 1080;
  const nextScale = clampNumber(Math.min(scaleByWidth, scaleByHeight), 0.64, 1.55);
  document.documentElement.style.setProperty("--playback-ui-scale", nextScale.toFixed(3));
  return nextScale;
}

function clampPanelIntoViewport(panel, margin = 10) {
  if (!panel || panel.offsetParent === null || panel.dataset.userPositioned !== "true") return;
  const rect = panel.getBoundingClientRect();
  const maxLeft = Math.max(margin, window.innerWidth - rect.width - margin);
  const maxTop = Math.max(margin, window.innerHeight - rect.height - margin);
  const left = Math.max(margin, Math.min(maxLeft, rect.left));
  const top = Math.max(margin, Math.min(maxTop, rect.top));
  panel.style.left = `${Math.round(left)}px`;
  panel.style.top = `${Math.round(top)}px`;
  panel.style.right = "auto";
  panel.style.bottom = "auto";
}

export function createPlaybackViewportAdapter({
  mountEl,
  renderer,
  camera,
  panels = [],
  layout,
  requestOverlayDraw,
  requestProjectionDraw,
  getTimelineState,
}) {
  let sizeKey = "";
  let resizeFrame = 0;

  function resizeMainRenderer() {
    const { width, height } = getViewportSize(mountEl);
    const uiScale = updatePlaybackUiScale(width, height);
    const dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
    const nextKey = `${width}x${height}@${dpr}@${uiScale}`;
    if (nextKey === sizeKey) return false;
    sizeKey = nextKey;
    renderer.setPixelRatio(dpr);
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
    return true;
  }

  function apply() {
    const changed = resizeMainRenderer();
    for (const panel of panels) clampPanelIntoViewport(panel);
    if (layout && typeof layout.layout === "function") layout.layout();
    if (typeof requestOverlayDraw === "function") requestOverlayDraw(getTimelineState?.());
    if (typeof requestProjectionDraw === "function") requestProjectionDraw({ force: changed });
  }

  function schedule() {
    cancelAnimationFrame(resizeFrame);
    resizeFrame = requestAnimationFrame(apply);
  }

  const observer = typeof ResizeObserver === "function" ? new ResizeObserver(schedule) : null;
  if (observer && mountEl) observer.observe(mountEl);
  window.addEventListener("resize", schedule);
  window.addEventListener("orientationchange", schedule);
  schedule();

  return {
    resizeRenderer: apply,
    dispose() {
      cancelAnimationFrame(resizeFrame);
      observer?.disconnect();
      window.removeEventListener("resize", schedule);
      window.removeEventListener("orientationchange", schedule);
    },
  };
}
