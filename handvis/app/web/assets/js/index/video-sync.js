// 首页视频时间 -> 3D 网格同步模块（基于 handRuntime + state）。
export function createIndexVideoSync({
  dom,
  viewConfig,
  chunkCache,
  ensureChunkLoaded,
  preloadNearChunks,
  findChunkByTime,
  copyFrameSafe,
  frameHasFinitePoint,
  handRuntime,
  state,
}) {
  function updateMeshFrame(chunkData, tSec) {
    const playback = state.get("playback");
    const { leftMesh, rightMesh } = handRuntime.getMeshes();
    if (!leftMesh || !rightMesh || !playback) return;

    const { meta, left, right } = chunkData;
    // 使用 round 对齐帧索引，减少 currentTime 小数边界导致的抖动。
    const localFrame = Math.round((tSec - meta.start_time) / playback.frameStepSec);
    const clampedLocal = Math.max(0, Math.min(meta.frame_count - 1, localFrame));
    const absFrame = meta.frame_start + clampedLocal;
    if (absFrame === state.get("lastRenderedAbsFrame")) return;

    const frameStride = meta.vertex_count * 3;
    const frameOffset = clampedLocal * frameStride;

    const leftPos = leftMesh.geometry.attributes.position.array;
    const rightPos = rightMesh.geometry.attributes.position.array;
    const leftFrame = left.subarray(frameOffset, frameOffset + frameStride);
    const rightFrame = right.subarray(frameOffset, frameOffset + frameStride);
    const invalidLeftMask = playback?.invalidLeftMask || playback?.invalidMask;
    const invalidRightMask = playback?.invalidRightMask || playback?.invalidMask;
    const leftInvalid = Array.isArray(invalidLeftMask) && absFrame >= 0 && absFrame < invalidLeftMask.length
      ? Number(invalidLeftMask[absFrame]) > 0
      : false;
    const rightInvalid = Array.isArray(invalidRightMask) && absFrame >= 0 && absFrame < invalidRightMask.length
      ? Number(invalidRightMask[absFrame]) > 0
      : false;
    const leftValid = !leftInvalid && frameHasFinitePoint(leftFrame, viewConfig.frameValiditySampleStep);
    const rightValid = !rightInvalid && frameHasFinitePoint(rightFrame, viewConfig.frameValiditySampleStep);
    if (!leftValid && !rightValid) {
      leftMesh.visible = false;
      rightMesh.visible = false;
      state.set("lastRenderedAbsFrame", absFrame);
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
    if (nextTick % viewConfig.normalUpdateInterval === 0) {
      leftMesh.geometry.computeVertexNormals();
      rightMesh.geometry.computeVertexNormals();
    }

    state.set("lastRenderedAbsFrame", absFrame);
  }

  function renderAtVideoTime() {
    const playback = state.get("playback");
    const { leftMesh, rightMesh } = handRuntime.getMeshes();
    if (!playback || !leftMesh || !rightMesh || !state.get("videoReady")) return;

    const t = dom.player.currentTime || 0;
    const chunkIdx = findChunkByTime(playback.chunks, t);
    if (chunkIdx < 0) return;

    const cached = chunkCache.get(chunkIdx);
    if (!cached) {
      // 首页以“可用优先”为目标：缺 chunk 时只异步拉取，不阻塞视频播放。
      ensureChunkLoaded(chunkIdx).catch(() => {});
      return;
    }

    updateMeshFrame(cached, t);
    if (!state.get("boardPlaced")) handRuntime.placeBoardUnderHandsOnce();
    preloadNearChunks(chunkIdx);
  }

  function bindVideoEvents() {
    dom.player.addEventListener("loadedmetadata", () => {
      state.set("videoReady", true);
    });
    dom.player.addEventListener("canplay", () => {
      state.set("videoReady", true);
    });
    dom.player.addEventListener("seeking", renderAtVideoTime);
    dom.player.addEventListener("seeked", renderAtVideoTime);
    dom.player.addEventListener("timeupdate", renderAtVideoTime);
  }

  return {
    updateMeshFrame,
    renderAtVideoTime,
    bindVideoEvents,
  };
}
