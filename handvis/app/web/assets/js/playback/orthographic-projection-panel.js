import { getCameraFrameTransform, transformWorldToCameraRaw } from "./camera-projection-utils.js";
import {
  clearProjectionCanvas,
  collectValidPoints,
  renderTrajectoryProjectionViews,
} from "./trajectory-projection-plot.js";

// 手部轨迹使用整只手 mesh 的相机系中心点，不直接画所有顶点，避免轨迹窗口过载。
function getInvalidState(playback, absFrame) {
  const invalidLeftMask = playback.invalidLeftMask;
  const invalidRightMask = playback.invalidRightMask;
  return {
    left: !Array.isArray(invalidLeftMask) || Number(invalidLeftMask[absFrame]) > 0,
    right: !Array.isArray(invalidRightMask) || Number(invalidRightMask[absFrame]) > 0,
  };
}

/**
 * 计算某一帧单只手在相机坐标系下的中心点。
 *
 * 顶点先从 world 转到当前帧相机坐标系，再求平均；
 * invalid mask 命中的帧直接跳过，避免异常帧把轨迹拉偏。
 */
function computeHandTrackPoint({ src, localFrame, vertexCount, transform, invalid }) {
  if (!src || !transform || invalid || localFrame < 0 || vertexCount <= 0) return null;
  const frameOffset = localFrame * vertexCount * 3;
  if (frameOffset + vertexCount * 3 > src.length) return null;

  let sx = 0;
  let sy = 0;
  let sz = 0;
  let count = 0;
  for (let i = 0; i < vertexCount; i += 1) {
    const srcIdx = frameOffset + i * 3;
    const wx = src[srcIdx];
    const wy = src[srcIdx + 1];
    const wz = src[srcIdx + 2];
    if (!Number.isFinite(wx) || !Number.isFinite(wy) || !Number.isFinite(wz)) continue;

    const cam = transformWorldToCameraRaw(wx, wy, wz, transform);
    if (!Number.isFinite(cam.x) || !Number.isFinite(cam.y) || !Number.isFinite(cam.z)) continue;
    sx += cam.x;
    sy += cam.y;
    sz += cam.z;
    count += 1;
  }
  if (count <= 0) return null;
  return { x: sx / count, y: sy / count, z: sz / count };
}

export function createOrthographicProjectionPanel({ dom, state, chunkCache, ensureChunkLoaded }) {
  const canvases = {
    xy: dom.xyProjectionCanvas,
    xz: dom.xzProjectionCanvas,
    yz: dom.yzProjectionCanvas,
  };
  let enabled = true;
  let vertexCount = 0;
  let lastDrawnAbsFrame = -1;
  let syncedThroughFrame = -1;
  // leftTrack/rightTrack 按绝对帧号缓存中心点，播放推进时只补齐新增帧。
  let leftTrack = [];
  let rightTrack = [];
  const pendingTrackChunks = new Set();

  function setVisible(nextVisible) {
    enabled = Boolean(nextVisible);
    if (dom.orthographicProjectionPanel) {
      dom.orthographicProjectionPanel.style.display = enabled ? "block" : "none";
    }
  }

  function clearCanvases() {
    for (const canvas of Object.values(canvases)) clearProjectionCanvas(canvas);
  }

  function init(nextVertexCount) {
    vertexCount = Number(nextVertexCount || 0);
    const totalFrames = Number(state.get("totalFrames") || 0);
    leftTrack = new Array(Math.max(0, totalFrames)).fill(null);
    rightTrack = new Array(Math.max(0, totalFrames)).fill(null);
    pendingTrackChunks.clear();
    lastDrawnAbsFrame = -1;
    syncedThroughFrame = -1;
    clearCanvases();
  }

  function reset() {
    vertexCount = 0;
    lastDrawnAbsFrame = -1;
    syncedThroughFrame = -1;
    leftTrack = [];
    rightTrack = [];
    pendingTrackChunks.clear();
    setVisible(Boolean(dom.projectionPanelToggle?.checked ?? true));
    clearCanvases();
  }

  function updateFrameFromChunk(chunk, absFrame, videoProjection) {
    const playback = state.get("playback");
    if (!playback || !chunk || !videoProjection || absFrame < 0) return false;

    const transform = getCameraFrameTransform(videoProjection, absFrame);
    const localFrame = absFrame - Number(chunk.meta?.frame_start || 0);
    const invalid = getInvalidState(playback, absFrame);
    leftTrack[absFrame] = computeHandTrackPoint({
      src: chunk.left,
      localFrame,
      vertexCount,
      transform,
      invalid: invalid.left,
    });
    rightTrack[absFrame] = computeHandTrackPoint({
      src: chunk.right,
      localFrame,
      vertexCount,
      transform,
      invalid: invalid.right,
    });
    return true;
  }

  /**
   * 将轨迹缓存同步到目标帧。
   *
   * 轨迹窗口需要显示“从开始到当前帧”的曲线，因此会按帧递增补齐缓存；
   * 如果目标 chunk 尚未加载，只触发加载并等待后续动画帧重绘，不在这里阻塞 UI。
   */
  function syncTrackToFrame(absFrame) {
    const playback = state.get("playback");
    const videoProjection = playback?.videoProjection;
    if (!playback || !videoProjection || absFrame < 0) return false;

    const targetFrame = Math.min(absFrame, Math.max(0, leftTrack.length - 1));
    // 用户向前跳帧时，丢弃旧缓存后重新从头同步，保证曲线和时间轴一致。
    if (targetFrame < syncedThroughFrame) {
      leftTrack.fill(null);
      rightTrack.fill(null);
      syncedThroughFrame = -1;
    }
    let wroteAny = false;
    for (let frame = Math.max(0, syncedThroughFrame + 1); frame <= targetFrame; frame += 1) {
      let chunk = null;
      let missingChunkIdx = -1;
      for (const loadedChunk of chunkCache.values()) {
        const start = Number(loadedChunk?.meta?.frame_start || 0);
        const count = Number(loadedChunk?.meta?.frame_count || 0);
        if (frame >= start && frame < start + count) {
          chunk = loadedChunk;
          break;
        }
      }
      if (!chunk) {
        const frameStepSec = Number(state.get("frameStepSec") || 0);
        const tSec = frame * frameStepSec;
        missingChunkIdx = playback.chunks.findIndex((item) => (
          tSec >= Number(item.start_time || 0) && tSec < Number(item.end_time || 0)
        ));
        if (missingChunkIdx < 0 && frame === targetFrame) {
          missingChunkIdx = playback.chunks.length - 1;
        }
        if (missingChunkIdx >= 0 && typeof ensureChunkLoaded === "function" && !pendingTrackChunks.has(missingChunkIdx)) {
          pendingTrackChunks.add(missingChunkIdx);
          ensureChunkLoaded(missingChunkIdx)
            .then((loaded) => {
              if (loaded) lastDrawnAbsFrame = -1;
            })
            .catch(() => {})
            .finally(() => {
              pendingTrackChunks.delete(missingChunkIdx);
            });
        }
        break;
      }
      wroteAny = updateFrameFromChunk(chunk, frame, videoProjection) || wroteAny;
      syncedThroughFrame = frame;
    }
    return wroteAny || syncedThroughFrame >= targetFrame;
  }

  function requestDraw(timelineState, { force = false } = {}) {
    if (!enabled || vertexCount <= 0) return;
    const absFrame = Number(timelineState?.absFrame ?? state.get("frame") ?? -1);
    if (!force && absFrame >= 0 && absFrame === lastDrawnAbsFrame) return;
    if (!syncTrackToFrame(absFrame)) return;

    // collectValidPoints 会过滤无效帧；当前帧点单独传入，用绿色高亮。
    const leftPoints = collectValidPoints(leftTrack);
    const rightPoints = collectValidPoints(rightTrack);
    renderTrajectoryProjectionViews({
      canvases,
      leftPoints,
      rightPoints,
      currentLeftPoint: leftTrack[absFrame] || null,
      currentRightPoint: rightTrack[absFrame] || null,
    });
    lastDrawnAbsFrame = absFrame;
  }

  return {
    init,
    reset,
    requestDraw,
    setVisible,
  };
}
