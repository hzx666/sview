/**
 * 播放页浮窗交互模块。
 *
 * 三个投影浮窗共用拖拽与缩放逻辑，避免每个面板单独维护一套事件处理。
 * 缩放时根据面板类型选择高度写入方式：3D 轨迹窗直接修改 canvas 高度，
 * 二维投影窗修改 CSS 变量后触发重绘。
 */

function clampPanelPosition(panel, left, top) {
  const rect = panel.getBoundingClientRect();
  const maxLeft = Math.max(0, window.innerWidth - rect.width);
  const maxTop = Math.max(0, window.innerHeight - rect.height);
  return {
    left: Math.max(0, Math.min(maxLeft, left)),
    top: Math.max(0, Math.min(maxTop, top)),
  };
}

function bindDrag(panel) {
  const handle = panel.querySelector(".bar");
  let dragging = false;
  let startX = 0;
  let startY = 0;
  let startLeft = 0;
  let startTop = 0;

  handle.addEventListener("pointerdown", (event) => {
    if (event.button !== 0) return;
    const rect = panel.getBoundingClientRect();
    panel.dataset.userPositioned = "true";
    dragging = true;
    startX = event.clientX;
    startY = event.clientY;
    startLeft = rect.left;
    startTop = rect.top;
    panel.style.left = `${rect.left}px`;
    panel.style.top = `${rect.top}px`;
    panel.style.bottom = "auto";
    panel.style.right = "auto";
    handle.setPointerCapture(event.pointerId);
  });

  handle.addEventListener("pointermove", (event) => {
    if (!dragging) return;
    const next = clampPanelPosition(
      panel,
      startLeft + event.clientX - startX,
      startTop + event.clientY - startY,
    );
    panel.style.left = `${Math.round(next.left)}px`;
    panel.style.top = `${Math.round(next.top)}px`;
  });

  handle.addEventListener("pointerup", (event) => {
    dragging = false;
    handle.releasePointerCapture(event.pointerId);
  });

  handle.addEventListener("pointercancel", (event) => {
    dragging = false;
    handle.releasePointerCapture(event.pointerId);
  });
}

function getResizeHeight(panel, resize) {
  if (resize.heightMode === "canvas-style") {
    // 3D 轨迹窗的 canvas 高度直接由元素样式控制。
    return resize.heightElement.getBoundingClientRect().height;
  }
  // 二维投影窗三个 canvas 等高，读取任意一个即可作为当前面板高度。
  const canvas = panel.querySelector(".projection-view canvas");
  return canvas.getBoundingClientRect().height;
}

function setResizeHeight(panel, resize, height) {
  if (resize.heightMode === "canvas-style") {
    // 3D 轨迹窗直接改变 canvas 高度，避免影响内部布局变量。
    resize.heightElement.style.height = `${Math.round(height)}px`;
    return;
  }
  // 二维投影窗通过统一 CSS 变量控制三个投影视图高度。
  panel.style.setProperty("--projection-canvas-height", `${Math.round(height)}px`);
}

function bindResize(panel, resize) {
  const handle = panel.querySelector(".resize-handle");
  let resizing = false;
  let startX = 0;
  let startY = 0;
  let startWidth = 0;
  let startHeight = 0;

  handle.addEventListener("pointerdown", (event) => {
    event.preventDefault();
    event.stopPropagation();
    const rect = panel.getBoundingClientRect();
    panel.dataset.userSized = "true";
    resizing = true;
    startX = event.clientX;
    startY = event.clientY;
    startWidth = rect.width;
    startHeight = getResizeHeight(panel, resize);
    handle.setPointerCapture(event.pointerId);
  });

  handle.addEventListener("pointermove", (event) => {
    if (!resizing) return;
    const rect = panel.getBoundingClientRect();
    const maxWidth = Math.max(resize.minWidth, window.innerWidth - rect.left - 8);
    const maxHeight = Math.max(resize.minHeight, window.innerHeight - rect.top - resize.bottomReserve);
    const nextWidth = Math.max(resize.minWidth, Math.min(maxWidth, startWidth + event.clientX - startX));
    const nextHeight = Math.max(resize.minHeight, Math.min(maxHeight, startHeight + event.clientY - startY));
    panel.style.width = `${Math.round(nextWidth)}px`;
    setResizeHeight(panel, resize, nextHeight);
    // 当前帧投影和轨迹投影依赖 canvas 尺寸，缩放过程中需要同步重绘。
    resize.onResize();
  });

  handle.addEventListener("pointerup", (event) => {
    resizing = false;
    handle.releasePointerCapture(event.pointerId);
    resize.onResize();
  });

  handle.addEventListener("pointercancel", (event) => {
    resizing = false;
    handle.releasePointerCapture(event.pointerId);
    resize.onResize();
  });
}

export function bindFloatingPanelInteractions(configs) {
  for (const config of configs) {
    bindDrag(config.panel);
    bindResize(config.panel, config.resize);
  }
}

function setDefaultPosition(panel, left, top) {
  const next = clampPanelPosition(panel, left, top);
  panel.style.left = `${Math.round(next.left)}px`;
  panel.style.top = `${Math.round(next.top)}px`;
  panel.style.right = "auto";
  panel.style.bottom = "auto";
}

function panelChromeHeight(panel, content) {
  return Math.max(0, panel.getBoundingClientRect().height - content.getBoundingClientRect().height);
}

function clampNumber(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function setPanelWidth(panel, width) {
  panel.style.width = `${Math.round(width)}px`;
  panel.style.minWidth = "0";
  panel.style.maxWidth = "none";
}

export function createFloatingPanelLayout({
  hud,
  trajectoryPanel,
  trajectoryCanvas,
  debugValues,
  videoPanel,
  projectionPanels,
  onProjectionResize,
}) {
  const baseMargin = 16;
  const baseGap = 12;
  let layoutFrame = 0;
  let lastViewportKey = "";

  function getUiScale() {
    const raw = getComputedStyle(document.documentElement).getPropertyValue("--playback-ui-scale");
    const scale = Number(raw);
    return Number.isFinite(scale) && scale > 0 ? scale : 1;
  }

  function getScaledLayoutMetrics() {
    const scale = getUiScale();
    return {
      scale,
      margin: baseMargin * scale,
      gap: baseGap * scale,
    };
  }

  function layoutLeftPanel() {
    const { scale, margin, gap } = getScaledLayoutMetrics();
    const hudRect = hud.getBoundingClientRect();
    const debugRect = debugValues.getBoundingClientRect();
    const top = Math.max(margin, hudRect.bottom + gap);
    const bottomLimit = Math.min(window.innerHeight - margin, debugRect.top - gap);
    const maxWidth = Math.max(280 * scale, Math.min(760 * scale, window.innerWidth - margin * 2));
    const preferredWidth = window.innerWidth >= 1280
      ? Math.min(620 * scale, Math.max(360 * scale, window.innerWidth * 0.4))
      : Math.min(560 * scale, Math.max(300 * scale, window.innerWidth - margin * 2));
    const chromeHeight = panelChromeHeight(trajectoryPanel, trajectoryCanvas);
    const canvasHeight = Math.max(120 * scale, Math.min(380 * scale, bottomLimit - top - chromeHeight));
    setPanelWidth(trajectoryPanel, Math.min(preferredWidth, maxWidth));
    trajectoryCanvas.style.height = `${Math.round(canvasHeight)}px`;
    setDefaultPosition(trajectoryPanel, margin, top);
  }

  function computeRightColumn(hudRect, metrics) {
    const { scale, margin, gap } = metrics;
    const viewportWidth = window.innerWidth;
    const maxWidth = Math.min(Math.max(560 * scale, viewportWidth * 0.34), viewportWidth - margin * 2);
    const rightSpace = viewportWidth - hudRect.right - gap - margin;
    const canSitBesideHud = rightSpace >= 420 * scale;
    const width = canSitBesideHud
      ? clampNumber(rightSpace, 420 * scale, maxWidth)
      : window.innerWidth <= 980
        ? Math.max(280 * scale, viewportWidth - margin * 2)
        : clampNumber(Math.min(560 * scale, viewportWidth * 0.46), 360 * scale, maxWidth);
    return {
      width,
      left: viewportWidth - width - margin,
      top: canSitBesideHud ? margin : Math.round(hudRect.bottom + gap),
    };
  }

  function layoutRightPanels() {
    const metrics = getScaledLayoutMetrics();
    const { scale, margin, gap } = metrics;
    const [orthographicPanel, framePanel] = projectionPanels;
    const hudRect = hud.getBoundingClientRect();
    const column = computeRightColumn(hudRect, metrics);
    const maxVideoHeight = Math.max(120 * scale, Math.min(560 * scale, Math.floor(window.innerHeight * 0.42)));
    document.documentElement.style.setProperty("--video-display-max-height", `${maxVideoHeight}px`);
    setPanelWidth(videoPanel, column.width);
    videoPanel.style.left = `${Math.round(column.left)}px`;
    videoPanel.style.top = `${Math.round(column.top)}px`;
    videoPanel.style.right = "auto";
    videoPanel.style.bottom = "auto";

    requestAnimationFrame(() => {
      const videoRect = videoPanel.getBoundingClientRect();
      const right = Math.max(margin, window.innerWidth - column.width - margin);
      const firstTop = videoRect.bottom + gap;
      const remainingHeight = Math.max(150, window.innerHeight - margin - firstTop - gap);
      const firstCanvas = orthographicPanel.querySelector(".projection-view canvas");
      const secondCanvas = framePanel.querySelector(".projection-view canvas");
      const chromeHeight = Math.max(
        panelChromeHeight(orthographicPanel, firstCanvas),
        panelChromeHeight(framePanel, secondCanvas),
      );
      const canvasHeight = clampNumber((remainingHeight / 2) - chromeHeight, 68 * scale, 220 * scale);

      for (const panel of projectionPanels) {
        setPanelWidth(panel, column.width);
        panel.style.setProperty("--projection-canvas-height", `${Math.floor(canvasHeight)}px`);
      }
      setDefaultPosition(orthographicPanel, right, firstTop);
      const firstRect = orthographicPanel.getBoundingClientRect();
      setDefaultPosition(framePanel, right, firstRect.bottom + gap);
      onProjectionResize();
    });
  }

  function layout() {
    cancelAnimationFrame(layoutFrame);
    layoutFrame = requestAnimationFrame(() => {
      const viewportKey = `${window.innerWidth}x${window.innerHeight}`;
      if (viewportKey !== lastViewportKey) {
        lastViewportKey = viewportKey;
        for (const panel of [trajectoryPanel, ...projectionPanels]) {
          panel.dataset.userPositioned = "";
          panel.dataset.userSized = "";
        }
      }
      layoutLeftPanel();
      layoutRightPanels();
    });
  }

  const observer = new ResizeObserver(layout);
  observer.observe(hud);
  observer.observe(videoPanel);
  window.addEventListener("resize", layout);
  layout();
  return { layout };
}
