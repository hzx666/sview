// 播放页拖拽与播放控制器：接管 scrub 状态机与播放开关。
export function createScrubController({
  dom,
  viewConfig,
  syncState,
  state,
  renderSync,
  setStatus,
}) {
  function requestPreviewRender() {
    if (syncState.pendingPreviewRaf) return;
    syncState.pendingPreviewRaf = true;
    requestAnimationFrame(() => {
      syncState.pendingPreviewRaf = false;
      if (syncState.mode === "scrubbing") renderSync.renderAtFrame(syncState.previewFrame);
    });
  }

  function setPlaybackState(shouldPlay) {
    const videoReady = Boolean(state.get("videoReady") || dom.sourceVideo.readyState >= 2);
    if (!videoReady) {
      state.set("play", false);
      dom.playPause.textContent = "播放";
      return false;
    }
    state.set("videoReady", true);
    if (syncState.mode === "scrubbing") commitScrub();
    if (syncState.mode === "seeking") {
      syncState.resumePlaybackAfterSeek = Boolean(shouldPlay);
      return true;
    }

    if (shouldPlay) {
      if (dom.sourceVideo.ended) dom.sourceVideo.currentTime = 0;
      syncState.clockMediaTimeSec = Number(dom.sourceVideo.currentTime || 0);
      syncState.clockPerfMs = performance.now();
      dom.sourceVideo.playbackRate = state.get("speed") || 1;
      dom.sourceVideo.play().catch(() => {
        setStatus("浏览器阻止了自动播放，请手动点击视频播放", false);
        state.set("play", false);
        dom.playPause.textContent = "播放";
      });
      state.set("play", true);
      dom.playPause.textContent = "暂停";
    } else {
      dom.sourceVideo.pause();
      state.set("play", false);
      dom.playPause.textContent = "播放";
    }
    return true;
  }

  function frameToTimeSec(absFrame) {
    return renderSync.frameToTimelineTimeSec(absFrame);
  }

  function seekToAbsFrame(absFrame, { resumePlayback = false } = {}) {
    const nextFrame = renderSync.clampAbsFrame(absFrame);
    const nextTime = renderSync.clampTimeSec(frameToTimeSec(nextFrame));
    const videoReady = Boolean(state.get("videoReady"));
    if (videoReady) {
      syncState.mode = "seeking";
      dom.sourceVideo.pause();
    }
    syncState.previewFrame = nextFrame;
    syncState.wasPlayingBeforeScrub = false;
    syncState.resumePlaybackAfterSeek = Boolean(resumePlayback);
    state.set("normalIntervalFactor", 1);
    renderSync.renderAtFrame(nextFrame);
    if (videoReady) {
      dom.sourceVideo.playbackRate = state.get("speed") || 1;
      dom.sourceVideo.currentTime = nextTime;
    } else {
      syncState.mode = "idle";
      syncState.resumePlaybackAfterSeek = false;
    }
    if (resumePlayback && !state.get("videoReady")) {
      state.set("play", true);
      dom.playPause.textContent = "暂停";
      setPlaybackState(true);
    }
  }

  function completeVideoSeek() {
    if (syncState.mode !== "seeking") return;
    const shouldResume = Boolean(syncState.resumePlaybackAfterSeek);
    syncState.mode = "idle";
    syncState.resumePlaybackAfterSeek = false;
    syncState.clockMediaTimeSec = Number(dom.sourceVideo.currentTime || 0);
    syncState.clockPerfMs = performance.now();
    renderSync.renderAtFrame(syncState.previewFrame);
    if (shouldResume) {
      state.set("play", true);
      dom.playPause.textContent = "暂停";
      setPlaybackState(true);
    }
  }

  function enterScrubMode(initialTimeSec) {
    if (syncState.mode === "scrubbing") return;

    // 进入拖拽态时先暂停视频，所有渲染改由手动时间驱动。
    syncState.mode = "scrubbing";
    syncState.wasPlayingBeforeScrub = Boolean(state.get("videoReady") && !dom.sourceVideo.paused && !dom.sourceVideo.ended);
    syncState.previewFrame = renderSync.clampAbsFrame(initialTimeSec / renderSync.getEffectiveFrameStepSec());
    state.set("normalIntervalFactor", viewConfig.scrubNormalIntervalFactor);
    if (state.get("videoReady")) dom.sourceVideo.pause();
    renderSync.renderAtFrame(syncState.previewFrame);
  }

  function updateScrubPreview(nextTimeSec) {
    if (syncState.mode !== "scrubbing") return;
    syncState.previewFrame = renderSync.clampAbsFrame(nextTimeSec / renderSync.getEffectiveFrameStepSec());
    requestPreviewRender();
  }

  function commitScrub() {
    if (syncState.mode !== "scrubbing") return;
    seekToAbsFrame(syncState.previewFrame, {
      resumePlayback: syncState.wasPlayingBeforeScrub,
    });
  }

  return {
    requestPreviewRender,
    setPlaybackState,
    frameToTimeSec,
    enterScrubMode,
    updateScrubPreview,
    commitScrub,
    seekToAbsFrame,
    completeVideoSeek,
  };
}
