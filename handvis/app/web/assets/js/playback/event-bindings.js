// 播放页事件绑定器：只负责绑定，不承载业务编排。
export function createPlaybackEventBindings({
  dom,
  state,
  syncState,
  timelineState,
  pending,
  renderSync,
  scrubController,
  setStatus,
  formatError,
  buildAndPlay,
  refreshAndBuild,
  resizeRenderer,
  onPanelToggle,
  onCanvasToggle,
  onProjectionToggle,
  onFrameMeshProjectionToggle,
  onCameraMarkerToggle,
}) {
  function syncPlaybackClock() {
    syncState.clockMediaTimeSec = Number(dom.sourceVideo.currentTime || 0);
    syncState.clockPerfMs = performance.now();
  }

  function bindHudEvents() {
    dom.playPause.addEventListener("click", () => {
      const video = dom.sourceVideo;
      const nextPlay = video ? (video.paused || video.ended) : !state.get("play");
      scrubController.setPlaybackState(nextPlay);
    });

    dom.speed.addEventListener("input", () => {
      const nextSpeed = Number(dom.speed.value || 1);
      const safeSpeed = Number.isFinite(nextSpeed) ? nextSpeed : 1;
      state.set("speed", safeSpeed);
      dom.sourceVideo.playbackRate = safeSpeed;
    });

    if (typeof onPanelToggle === "function") {
      if (dom.trajectoryPanelToggle) {
        dom.trajectoryPanelToggle.addEventListener("change", () => {
          onPanelToggle("trajectory", dom.trajectoryPanelToggle.checked);
        });
      }
    }

    if (typeof onCanvasToggle === "function" && dom.canvasRenderToggle) {
      dom.canvasRenderToggle.addEventListener("change", () => {
        onCanvasToggle(dom.canvasRenderToggle.checked);
      });
    }

    if (typeof onProjectionToggle === "function" && dom.projectionPanelToggle) {
      dom.projectionPanelToggle.addEventListener("change", () => {
        onProjectionToggle(dom.projectionPanelToggle.checked);
      });
    }

    if (typeof onFrameMeshProjectionToggle === "function" && dom.frameMeshProjectionPanelToggle) {
      dom.frameMeshProjectionPanelToggle.addEventListener("change", () => {
        onFrameMeshProjectionToggle(dom.frameMeshProjectionPanelToggle.checked);
      });
    }

    if (typeof onCameraMarkerToggle === "function" && dom.cameraMarkerToggle) {
      dom.cameraMarkerToggle.addEventListener("change", () => {
        onCameraMarkerToggle(dom.cameraMarkerToggle.checked);
      });
    }

    dom.frame.addEventListener("mousedown", () => {
      const absFrame = Number(dom.frame.value || 0);
      scrubController.enterScrubMode(scrubController.frameToTimeSec(absFrame));
    });

    dom.frame.addEventListener("touchstart", () => {
      const absFrame = Number(dom.frame.value || 0);
      scrubController.enterScrubMode(scrubController.frameToTimeSec(absFrame));
    }, { passive: true });

    dom.frame.addEventListener("input", () => {
      const absFrame = Number(dom.frame.value || 0);
      renderSync.updateFrameUI(absFrame);
      const tSec = scrubController.frameToTimeSec(absFrame);
      // 拖动滑杆时统一进入 scrubbing，以同一逻辑处理预览与提交。
      if (syncState.mode !== "scrubbing") scrubController.enterScrubMode(tSec);
      scrubController.updateScrubPreview(tSec);
    });

    dom.frame.addEventListener("change", () => {
      const absFrame = Number(dom.frame.value || 0);
      if (syncState.mode === "scrubbing") {
        scrubController.commitScrub();
        return;
      }
      scrubController.seekToAbsFrame(absFrame);
    });

    window.addEventListener("mouseup", () => {
      scrubController.commitScrub();
    });

    window.addEventListener("touchend", () => {
      scrubController.commitScrub();
    }, { passive: true });

    const frameStatEl = dom.frameNow?.closest("span");
    let frameEditOpen = false;
    function clearFrameInputInvalid() {
      dom.frameJumpInput?.classList.remove("input-invalid");
    }
    function markFrameInputInvalid() {
      dom.frameJumpInput?.classList.add("input-invalid");
    }
    function isValidFrameInputValue(rawValue, totalFrames) {
      if (!Number.isFinite(totalFrames) || totalFrames <= 0) return false;
      const value = Number.parseInt(String(rawValue || "").trim(), 10);
      return Number.isFinite(value) && value >= 1 && value <= totalFrames;
    }
    function closeFrameEditor() {
      if (!frameStatEl || !dom.frameJumpInput) return;
      frameEditOpen = false;
      frameStatEl.classList.remove("frame-editing");
      clearFrameInputInvalid();
    }
    function openFrameEditor() {
      if (!frameStatEl || !dom.frameJumpInput) return;
      const totalFrames = Number(state.get("totalFrames") || 0);
      if (totalFrames <= 0) return;
      const currentFrame = Number.isFinite(timelineState.absFrame) && timelineState.absFrame >= 0
        ? timelineState.absFrame
        : Number(state.get("frame") || 0);
      frameEditOpen = true;
      frameStatEl.classList.add("frame-editing");
      clearFrameInputInvalid();
      dom.frameJumpInput.value = String(currentFrame + 1);
      dom.frameJumpInput.focus();
      dom.frameJumpInput.select();
    }
    function commitFrameJump() {
      if (!dom.frameJumpInput) return;
      const totalFrames = Number(state.get("totalFrames") || 0);
      if (totalFrames <= 0) {
        closeFrameEditor();
        return;
      }
      const raw = Number.parseInt((dom.frameJumpInput.value || "").trim(), 10);
      if (!Number.isFinite(raw)) {
        markFrameInputInvalid();
        setStatus(`请输入有效帧号（1-${totalFrames}）`, false);
        return;
      }
      const frameNo = Math.max(1, Math.min(totalFrames, raw));
      if (frameNo !== raw) {
        markFrameInputInvalid();
        setStatus(`请输入有效帧号（1-${totalFrames}）`, false);
        return;
      }
      clearFrameInputInvalid();
      // 输入框是 1-based，内部渲染是 0-based。
      scrubController.seekToAbsFrame(frameNo - 1);
      closeFrameEditor();
    }

    dom.frameNow?.addEventListener("click", () => {
      openFrameEditor();
    });
    dom.frameJumpInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        commitFrameJump();
      } else if (event.key === "Escape") {
        event.preventDefault();
        closeFrameEditor();
      }
    });
    dom.frameJumpInput?.addEventListener("input", () => {
      const totalFrames = Number(state.get("totalFrames") || 0);
      if (isValidFrameInputValue(dom.frameJumpInput.value, totalFrames)) {
        clearFrameInputInvalid();
      }
    });
    dom.frameJumpInput?.addEventListener("blur", () => {
      if (!frameEditOpen) return;
      closeFrameEditor();
    });

    function stepFrame(delta) {
      const totalFrames = Number(state.get("totalFrames") || 0);
      if (totalFrames <= 0) return;
      const current = renderSync.clampAbsFrame(Number(dom.frame?.value || 0));
      const next = Math.max(0, Math.min(totalFrames - 1, current + delta));
      if (next === current) return;
      scrubController.seekToAbsFrame(next);
    }
    dom.frameStepDown?.addEventListener("click", () => {
      stepFrame(-1);
    });
    dom.frameStepUp?.addEventListener("click", () => {
      stepFrame(1);
    });
  }

  function bindVideoEvents(timelineState) {
    dom.sourceVideo.addEventListener("loadedmetadata", () => {
      state.set("videoReady", true);
      syncPlaybackClock();
      if (pending.initialSeekSec !== null) {
        const next = renderSync.clampTimeSec(pending.initialSeekSec);
        pending.initialSeekSec = null;
        dom.sourceVideo.currentTime = next;
        timelineState.timeSec = next;
        syncPlaybackClock();
      }
      setStatus("视频已加载，可播放", true);
      renderSync.renderAtVideoTime();
    });

    dom.sourceVideo.addEventListener("canplay", () => {
      state.set("videoReady", true);
      if (pending.initialAutoPlay) {
        pending.initialAutoPlay = false;
        state.set("play", true);
        dom.playPause.textContent = "暂停";
        scrubController.setPlaybackState(true);
      }
    });

    dom.sourceVideo.addEventListener("seeking", () => {
      if (syncState.mode === "scrubbing" || syncState.mode === "seeking") return;
      renderSync.renderAtVideoTime();
    });

    dom.sourceVideo.addEventListener("seeked", () => {
      if (syncState.mode === "seeking") {
        scrubController.completeVideoSeek();
        syncPlaybackClock();
        return;
      }
      if (syncState.mode === "scrubbing") return;
      syncPlaybackClock();
      renderSync.renderAtVideoTime();
    });

    dom.sourceVideo.addEventListener("timeupdate", () => {
      if (syncState.mode === "scrubbing" || syncState.mode === "seeking") return;
      syncPlaybackClock();
    });

    dom.sourceVideo.addEventListener("play", () => {
      syncPlaybackClock();
      state.set("play", true);
      dom.playPause.textContent = "暂停";
    });

    dom.sourceVideo.addEventListener("pause", () => {
      syncPlaybackClock();
      if (syncState.mode !== "scrubbing") {
        state.set("play", false);
        dom.playPause.textContent = "播放";
      }
    });

    dom.sourceVideo.addEventListener("ended", () => {
      syncPlaybackClock();
      syncState.mode = "idle";
      state.set("play", false);
      dom.playPause.textContent = "播放";
      renderSync.renderAtVideoTime();
    });

    dom.sourceVideo.addEventListener("ratechange", () => {
      const v = dom.sourceVideo.playbackRate;
      if (Number.isFinite(v) && v >= 0.25 && v <= 2) {
        state.set("speed", v);
        dom.speed.value = String(v);
      }
    });

    dom.sourceVideo.addEventListener("error", () => {
      const code = dom.sourceVideo.error?.code ?? "unknown";
      setStatus(`视频加载失败(code=${code})`, false);
    });
  }

  function bindActionEvents() {
    dom.queryBtn.addEventListener("click", async () => {
      try {
        await buildAndPlay((dom.epDir.value || "").trim());
      } catch (e) {
        setStatus(formatError(e), false);
      }
    });

    dom.refreshBtn.addEventListener("click", async () => {
      const epDir = (dom.epDir.value || "").trim();
      const itemName = (dom.itemName?.value || "").trim();
      if (!epDir) {
        setStatus("请输入 ep_dir", false);
        return;
      }
      if (!itemName) {
        setStatus("请输入 item_name", false);
        return;
      }
      try {
        // 强刷前先校验 item_name，避免刷新后仍无法定位配对组。
        await refreshAndBuild(epDir, itemName);
      } catch (e) {
        setStatus(formatError(e), false);
      }
    });
  }

  function bindWindowEvents() {
    window.addEventListener("resize", () => {
      resizeRenderer();
    });
  }

  return {
    bindHudEvents,
    bindVideoEvents,
    bindActionEvents,
    bindWindowEvents,
  };
}
