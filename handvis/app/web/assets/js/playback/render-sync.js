/**
 * 播放页“时间轴 -> chunk -> 网格”渲染同步模块。
 *
 * 职责：
 * 1) 将 video 当前时间映射到 chunk 与 localFrame
 * 2) 将帧顶点写入 handRuntime mesh
 * 3) 更新 UI 与渲染状态
 */

import { mapTimelineToLocalFrame, resolveFrameStepSec } from "../shared/playback-timing.js";

export function createPlaybackRenderSync({
  dom,
  viewConfig,
  timelineState,
  chunkCache,
  setChunkState,
  updateMetricsUI,
  ensureChunkLoaded,
  preloadNearChunks,
  findChunkByTime,
  copyFrameSafe,
  frameHasFinitePoint,
  handRuntime,
  state,
}) {
  const pendingRenderRetries = new Set();

  function getEffectiveNormalUpdateInterval() {
    // scrubbing 时会放大该值，减少拖拽过程中法线重算成本。
    const factor = state.get("normalIntervalFactor") || 1;
    return Math.max(1, Math.floor(viewConfig.normalUpdateInterval * factor));
  }

  function updateFrameUI(absFrame = state.get("frame") || 0) {
    state.set("frame", absFrame);
    if (dom.frame) dom.frame.value = String(absFrame);
    if (dom.frameText) dom.frameText.textContent = `${absFrame + 1}/${state.get("totalFrames")}`;
  }

  function clampAbsFrame(absFrame) {
    const totalFrames = Number(state.get("totalFrames") || 0);
    if (totalFrames <= 0) return 0;
    const frame = Math.round(Number(absFrame) || 0);
    return Math.max(0, Math.min(totalFrames - 1, frame));
  }

  function getEffectiveFrameStepSec() {
    const videoDuration = dom.sourceVideo?.readyState >= 1
      ? Number(dom.sourceVideo.duration)
      : NaN;
    return resolveFrameStepSec({
      metaFrameStepSec: state.get("frameStepSec"),
      totalFrames: state.get("totalFrames"),
      videoDuration,
    });
  }

  function frameToTimelineTimeSec(absFrame) {
    return clampAbsFrame(absFrame) * getEffectiveFrameStepSec();
  }

  function shouldInterpolatePlayback() {
    return Boolean(state.get("videoReady") && dom.sourceVideo && !dom.sourceVideo.paused && !dom.sourceVideo.ended);
  }

  /**
   * 将单个 chunk 的 localFrame 应用到左右手 mesh。
   * absFrame 为全局 0-based 帧号，用于 invalid mask 判定。
   */
  function renderChunkFrame(chunkData, localFrame, absFrame = null) {
    const { leftMesh, rightMesh } = handRuntime.getMeshes();
    if (!leftMesh || !rightMesh) return;

    const { meta, left, right } = chunkData;
    const actualAbsFrame = Number.isFinite(absFrame) ? absFrame : Number(meta.frame_start || 0) + localFrame;
    // 顶点缓冲布局：[frame][vertex][xyz]，每顶点 3 个 float。
    const frameStride = meta.vertex_count * 3;
    const frameOffset = localFrame * frameStride;

    const leftPos = leftMesh.geometry.attributes.position.array;
    const rightPos = rightMesh.geometry.attributes.position.array;
    const leftFrame = left.subarray(frameOffset, frameOffset + frameStride);
    const rightFrame = right.subarray(frameOffset, frameOffset + frameStride);
    const playback = state.get("playback");
    const invalidLeftMask = playback?.invalidLeftMask || playback?.invalidMask;
    const invalidRightMask = playback?.invalidRightMask || playback?.invalidMask;
    const leftInvalid = Array.isArray(invalidLeftMask) && actualAbsFrame >= 0 && actualAbsFrame < invalidLeftMask.length
      ? Number(invalidLeftMask[actualAbsFrame]) > 0
      : false;
    const rightInvalid = Array.isArray(invalidRightMask) && actualAbsFrame >= 0 && actualAbsFrame < invalidRightMask.length
      ? Number(invalidRightMask[actualAbsFrame]) > 0
      : false;
    const leftValid = !leftInvalid && frameHasFinitePoint(leftFrame, viewConfig.frameValiditySampleStep);
    const rightValid = !rightInvalid && frameHasFinitePoint(rightFrame, viewConfig.frameValiditySampleStep);
    if (!leftValid && !rightValid) {
      leftMesh.visible = false;
      rightMesh.visible = false;
      return;
    }

    if (leftValid) copyFrameSafe(leftPos, leftFrame);
    if (rightValid) copyFrameSafe(rightPos, rightFrame);

    leftMesh.geometry.attributes.position.needsUpdate = leftValid;
    rightMesh.geometry.attributes.position.needsUpdate = rightValid;
    leftMesh.visible = leftValid;
    rightMesh.visible = rightValid;

    const nextTick = (state.get("normalTick") || 0) + 1;
    state.set("normalTick", nextTick);
    if (nextTick % getEffectiveNormalUpdateInterval() === 0) {
      leftMesh.geometry.computeVertexNormals();
      rightMesh.geometry.computeVertexNormals();
    }
  }

  function applyInterpolatedFrame(mesh, fromFrame, toFrame, alpha, valid) {
    if (!mesh) return;
    if (!valid || !fromFrame || !toFrame) {
      mesh.visible = false;
      return;
    }
    const dst = mesh.geometry.attributes.position.array;
    const n = Math.min(dst.length, fromFrame.length, toFrame.length);
    const t = Math.max(0, Math.min(1, Number(alpha) || 0));
    const invT = 1 - t;
    for (let i = 0; i < n; i += 1) {
      dst[i] = fromFrame[i] * invT + toFrame[i] * t;
    }
    mesh.geometry.attributes.position.needsUpdate = true;
    mesh.visible = true;
  }

  function renderChunkFrameInterpolated(chunkData, localFrame, alpha, absFrame = null) {
    const { leftMesh, rightMesh } = handRuntime.getMeshes();
    if (!leftMesh || !rightMesh) return;

    const { meta, left, right } = chunkData;
    const currentLocal = Math.max(0, Math.min(meta.frame_count - 1, localFrame));
    const nextLocal = Math.max(0, Math.min(meta.frame_count - 1, currentLocal + 1));
    const actualAbsFrame = Number.isFinite(absFrame) ? absFrame : Number(meta.frame_start || 0) + currentLocal;
    const frameStride = meta.vertex_count * 3;
    const currentOffset = currentLocal * frameStride;
    const nextOffset = nextLocal * frameStride;
    const leftFrame = left.subarray(currentOffset, currentOffset + frameStride);
    const rightFrame = right.subarray(currentOffset, currentOffset + frameStride);
    const leftNextFrame = left.subarray(nextOffset, nextOffset + frameStride);
    const rightNextFrame = right.subarray(nextOffset, nextOffset + frameStride);
    const playback = state.get("playback");
    const invalidLeftMask = playback?.invalidLeftMask || playback?.invalidMask;
    const invalidRightMask = playback?.invalidRightMask || playback?.invalidMask;
    const leftInvalid = Array.isArray(invalidLeftMask) && actualAbsFrame >= 0 && actualAbsFrame < invalidLeftMask.length
      ? Number(invalidLeftMask[actualAbsFrame]) > 0
      : false;
    const rightInvalid = Array.isArray(invalidRightMask) && actualAbsFrame >= 0 && actualAbsFrame < invalidRightMask.length
      ? Number(invalidRightMask[actualAbsFrame]) > 0
      : false;
    const leftValid = !leftInvalid && frameHasFinitePoint(leftFrame, viewConfig.frameValiditySampleStep);
    const rightValid = !rightInvalid && frameHasFinitePoint(rightFrame, viewConfig.frameValiditySampleStep);

    applyInterpolatedFrame(leftMesh, leftFrame, leftNextFrame, alpha, leftValid);
    applyInterpolatedFrame(rightMesh, rightFrame, rightNextFrame, alpha, rightValid);

    const nextTick = (state.get("normalTick") || 0) + 1;
    state.set("normalTick", nextTick);
    if (nextTick % getEffectiveNormalUpdateInterval() === 0) {
      leftMesh.geometry.computeVertexNormals();
      rightMesh.geometry.computeVertexNormals();
    }
  }

  function clampTimeSec(tSec) {
    const frameStepSec = getEffectiveFrameStepSec();
    const totalFrames = state.get("totalFrames") || 0;
    const videoDuration = Number.isFinite(dom.sourceVideo?.duration) ? dom.sourceVideo.duration : NaN;
    const timelineDuration = Math.max(0, (totalFrames - 1) * frameStepSec);
    // 优先以视频时长为上界；metadata 不可用时回退时间轴估算值。
    const maxT = Number.isFinite(videoDuration) && videoDuration > 0 ? videoDuration : timelineDuration;
    if (!(maxT > 0)) return 0;
    return Math.max(0, Math.min(maxT, tSec));
  }

  function renderAtTime(rawTimeSec) {
    const playback = state.get("playback");
    const { leftMesh, rightMesh } = handRuntime.getMeshes();
    if (!playback || !leftMesh || !rightMesh) return;

    const frameStepSec = getEffectiveFrameStepSec();
    const tSec = clampTimeSec(rawTimeSec);
    timelineState.timeSec = tSec;

    const chunkIdx = findChunkByTime(playback.chunks, tSec);
    if (chunkIdx < 0) return;

    state.set("currentChunkIdx", chunkIdx);
    preloadNearChunks(chunkIdx);

    const chunk = chunkCache.get(chunkIdx);
    if (!chunk) {
      if (!pendingRenderRetries.has(chunkIdx)) {
        pendingRenderRetries.add(chunkIdx);
        // 跳帧可能命中尚未加载的 chunk：只发起一次异步加载，避免每帧重复请求。
        ensureChunkLoaded(chunkIdx)
          .then((loaded) => {
            if (!loaded) return;
            // 帧跳转可能只触发一次 renderAtTime；目标 chunk 异步到达后主动补渲染。
            if (Math.abs((timelineState.timeSec || 0) - tSec) < 1e-6) {
              renderAtTime(tSec);
            }
          })
          .catch(() => {})
          .finally(() => {
            pendingRenderRetries.delete(chunkIdx);
          });
      }
      setChunkState(`3D加载中 chunk ${chunkIdx + 1}/${playback.chunks.length}`);
      timelineState.renderState = "等待chunk";
      updateMetricsUI();
      return;
    }

    const { localFrame, alpha, frameFloat } = mapTimelineToLocalFrame({
      tSec,
      chunkStartTime: chunk.meta.start_time,
      frameStepSec,
      frameCount: chunk.meta.frame_count,
    });
    const absFrame = chunk.meta.frame_start + localFrame;
    const nextAbsFrame = Math.min(
      (state.get("totalFrames") || 1) - 1,
      absFrame + (localFrame + 1 < chunk.meta.frame_count ? 1 : 0),
    );
    const shouldInterpolate = shouldInterpolatePlayback();
    if (shouldInterpolate) {
      renderChunkFrameInterpolated(chunk, localFrame, alpha, absFrame);
      updateFrameUI(absFrame);
      state.set("lastRenderedAbsFrame", absFrame);
    } else if (absFrame !== state.get("lastRenderedAbsFrame")) {
      renderChunkFrame(chunk, localFrame, absFrame);
      updateFrameUI(absFrame);
      state.set("lastRenderedAbsFrame", absFrame);
    }

    setChunkState(`3D就绪 chunk ${chunkIdx + 1}/${playback.chunks.length}`);
    timelineState.absFrame = absFrame;
    timelineState.nextAbsFrame = nextAbsFrame;
    timelineState.alpha = alpha;
    timelineState.frameFloat = chunk.meta.frame_start + frameFloat;
    timelineState.renderState = "同步渲染";
    updateMetricsUI();
  }

  function renderAtFrame(rawAbsFrame) {
    const absFrame = clampAbsFrame(rawAbsFrame);
    const tSec = frameToTimelineTimeSec(absFrame);
    updateFrameUI(absFrame);
    timelineState.absFrame = absFrame;
    timelineState.timeSec = tSec;
    updateMetricsUI();
    renderAtTime(tSec);
  }

  function renderAtVideoTime() {
    renderAtTime(dom.sourceVideo.currentTime || 0);
  }

  return {
    updateFrameUI,
    renderChunkFrame,
    clampTimeSec,
    clampAbsFrame,
    frameToTimelineTimeSec,
    getEffectiveFrameStepSec,
    renderAtTime,
    renderAtFrame,
    renderAtVideoTime,
  };
}
