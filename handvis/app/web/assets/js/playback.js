/**
 * 播放页入口（playback.js）。
 * 职责：
 * 1) 组织视频时间轴与 3D 渲染同步
 * 2) 发起构建任务并加载 index/chunk
 * 3) 维护播放态、拖拽态和性能指标
 */

import * as THREE from "three";
import { fmt, fetchArrayBuffer, fetchJson, formatError, joinUrl, parseApiBody } from "./playback/common.js";
import { copyFrameSafe, findChunkByTime, frameHasFinitePoint } from "./shared/chunk-utils.js";
import { createChunkStore } from "./shared/chunk-store.js";
import { createBoardController } from "./playback/board-controller.js";
import { bootstrapPlaybackScene } from "./playback/scene-bootstrap.js";
import { createPlaybackRenderSync } from "./playback/render-sync.js";
import { createHandRuntime } from "./shared/hand-runtime.js";
import { createMutableState } from "./shared/state-helpers.js";
import { COMMON_VIEW_CONFIG } from "./shared/view-config.js";
import { BOARD_CONFIG, SHOW_WHITEBOARD } from "./shared/scene-config.js";
import { createScrubController } from "./playback/scrub-controller.js";
import { createPlaybackEventBindings } from "./playback/event-bindings.js";
import { createTrajectoryMiniWorld } from "./playback/trajectory-mini-world.js";
import { createCameraMarker } from "./playback/camera-marker.js";
import { createCameraPositionView } from "./playback/camera-position-view.js";
import { createCanvasOverlay } from "./playback/canvas-overlay.js";
import { createOrthographicProjectionPanel } from "./playback/orthographic-projection-panel.js";
import { createFrameMeshProjectionPanel } from "./playback/frame-mesh-projection-panel.js";
import { createPlaybackScorePanel } from "./playback/score-panel.js";
import { bindFloatingPanelInteractions, createFloatingPanelLayout } from "./playback/floating-panel-interactions.js";
import { createPlaybackViewportAdapter } from "./playback/viewport-adapter.js";
import { HAND_VISUAL_COLORS } from "./playback/hand-visual-style.js";
import { loadVideoProjection } from "./shared/video-projection-loader.js";

const VIEW_CONFIG = {
  // 公共显示参数来自共享模块；播放页额外覆盖拖拽/相机参数。
  ...COMMON_VIEW_CONFIG,
  fov: 70,
  scrubNormalIntervalFactor: 2,
  cameraPos: [-0.023, -1.179, -1.102],
  cameraTarget: [-0.008, 0.544, -0.074],
};

const TRAJECTORY_VIEW_CONFIG = {
  ...VIEW_CONFIG,
  // 3D 小窗固定使用人工确认后的可视化参数。
  cameraPos: [-0.004, -0.089, -0.048],
  cameraTarget: [0.0, 0.32, 0.18],
  cameraFov: 70,
  cameraNear: 0.001,
  cameraFar: 100.858,
};

const dom = {
  app: document.getElementById("app"), hud: document.getElementById("hud"), status: document.getElementById("status"), epDir: document.getElementById("epDir"),
  itemName: document.getElementById("itemName"),
  videoPanel: document.getElementById("videoPanel"),
  queryBtn: document.getElementById("queryBtn"), refreshBtn: document.getElementById("refreshBtn"), sourceVideo: document.getElementById("sourceVideo"),
  playPause: document.getElementById("playPause"), speed: document.getElementById("speed"), frame: document.getElementById("frame"), frameText: document.getElementById("frameText"),
  trajectoryPanelToggle: document.getElementById("trajectoryPanelToggle"),
  cameraMarkerToggle: document.getElementById("cameraMarkerToggle"),
  cameraFollowToggle: document.getElementById("cameraFollowToggle"),
  canvasRenderToggle: document.getElementById("canvasRenderToggle"),
  projectionPanelToggle: document.getElementById("projectionPanelToggle"),
  frameMeshProjectionPanelToggle: document.getElementById("frameMeshProjectionPanelToggle"),
  trajectoryPanel: document.getElementById("trajectoryPanel"),
  trajectoryCanvas: document.getElementById("trajectoryCanvas"),
  chunkState: document.getElementById("chunkState"), debugValues: document.getElementById("debugValues"), renderFps: document.getElementById("renderFps"),
  frameNow: document.getElementById("frameNow"), frameStepDown: document.getElementById("frameStepDown"), frameStepUp: document.getElementById("frameStepUp"), frameJumpInput: document.getElementById("frameJumpInput"), videoTime: document.getElementById("videoTime"), chunkNow: document.getElementById("chunkNow"), cacheNow: document.getElementById("cacheNow"), renderState: document.getElementById("renderState"),
  videoStage: document.getElementById("videoStage"), canvasOverlay: document.getElementById("canvasOverlay"),
  orthographicProjectionPanel: document.getElementById("orthographicProjectionPanel"),
  xyProjectionCanvas: document.getElementById("xyProjectionCanvas"),
  xzProjectionCanvas: document.getElementById("xzProjectionCanvas"),
  yzProjectionCanvas: document.getElementById("yzProjectionCanvas"),
  frameMeshProjectionPanel: document.getElementById("frameMeshProjectionPanel"),
  xyFrameMeshCanvas: document.getElementById("xyFrameMeshCanvas"),
  xzFrameMeshCanvas: document.getElementById("xzFrameMeshCanvas"),
  yzFrameMeshCanvas: document.getElementById("yzFrameMeshCanvas"),
  boardYOffset: document.getElementById("boardYOffset"), boardYOffsetVal: document.getElementById("boardYOffsetVal"), boardXOffset: document.getElementById("boardXOffset"), boardXOffsetVal: document.getElementById("boardXOffsetVal"),
  boardZOffset: document.getElementById("boardZOffset"), boardZOffsetVal: document.getElementById("boardZOffsetVal"), boardRotX: document.getElementById("boardRotX"), boardRotXVal: document.getElementById("boardRotXVal"),
  boardRotY: document.getElementById("boardRotY"), boardRotYVal: document.getElementById("boardRotYVal"), boardRotZ: document.getElementById("boardRotZ"), boardRotZVal: document.getElementById("boardRotZVal"), boardResetBtn: document.getElementById("boardResetBtn"),
};

const urlParams = new URLSearchParams(window.location.search);
const initialEpDirFromUrl = (urlParams.get("ep_dir") || "").trim();
const initialItemNameFromUrl = (urlParams.get("item_name") || "").trim();
const initialVideoPathFromUrl = (urlParams.get("video_path") || "").trim();
const initialPosehandPathFromUrl = (urlParams.get("posehand_path") || "").trim();
const initialIndexUrlFromUrl = (urlParams.get("index_url") || "").trim();
const initialVideoUrlFromUrl = (urlParams.get("video_url") || "").trim();
const initialAutoPlay = (urlParams.get("auto_play") || "").trim() === "1";
const initialTimeSecFromUrl = Number(urlParams.get("t") || "");
const initialSpeedFromUrl = Number(urlParams.get("speed") || "");
const initialStartTimeSec = Number.isFinite(initialTimeSecFromUrl) && initialTimeSecFromUrl >= 0 ? initialTimeSecFromUrl : 0;
const initialSpeed = Number.isFinite(initialSpeedFromUrl) && initialSpeedFromUrl >= 0.25 && initialSpeedFromUrl <= 2 ? initialSpeedFromUrl : 1;

// Master 模式：页面由 SView Master 托管（注入 window.HANDVIS_MASTER），或显式
// ?master=1 / ?base=http://master:8080。此时提交/进度/媒体全部走 Master /api/jobs，
// 不再依赖 HandVis FastAPI（无 PG、无转码、无评分）。
const masterBase = (urlParams.get("base") || window.HANDVIS_MASTER_BASE || "").trim().replace(/\/+$/, "");
const MASTER_MODE = Boolean(window.HANDVIS_MASTER) || urlParams.has("master") || Boolean(masterBase);
const apiUrl = (p) => `${masterBase}${p}`;
const jobMediaUrl = (jobId, rel) => apiUrl(`/api/jobs/${encodeURIComponent(jobId)}/${rel}`);
// Master 模式下跨域拉媒体统一加 ?proxy=1，让 Master 代理 OSS，
// 避免浏览器跟随 302 到 OSS 时受 bucket CORS 配置影响。
const wrapProxy = (fn) => (url, ...args) => {
  if (MASTER_MODE && typeof url === "string" && url.includes("/api/jobs/") && !url.includes("proxy=")) {
    url += (url.includes("?") ? "&" : "?") + "proxy=1";
  }
  return fn(url, ...args);
};
const fetchJsonM = wrapProxy(fetchJson);
const fetchArrayBufferM = wrapProxy(fetchArrayBuffer);

const pending = { initialSeekSec: null, initialAutoPlay: false };
// scrub 状态机（时间轴交互）：idle -> scrubbing -> seeking。
// - idle: 正常播放/暂停
// - scrubbing: 用户拖拽预览
// - seeking: 已提交跳帧，等待 video seeked 后恢复 idle
const syncState = {
  mode: "idle",
  previewFrame: 0,
  wasPlayingBeforeScrub: false,
  pendingPreviewRaf: false,
  resumePlaybackAfterSeek: false,
  clockMediaTimeSec: 0,
  clockPerfMs: 0,
};
const timelineState = { absFrame: -1, timeSec: 0, renderState: "等待加载" };
const perfState = { fps: 0, frameCounter: 0, sampleStartTs: performance.now() };

const state = createMutableState({
  leftMesh: null, rightMesh: null, playback: null, videoReady: false, play: false, speed: initialSpeed, frame: 0, totalFrames: 0, frameStepSec: 1 / 30,
  lastRenderedAbsFrame: -1, normalTick: 0, leftPrevFrame: null, rightPrevFrame: null, currentChunkIdx: -1, normalIntervalFactor: 1,
  handLayoutApplied: false, buildInFlight: false,
  showTrajectoryPanel: true, showProjectionPanel: true, showFrameMeshProjectionPanel: true, showCameraMarker: true, followCameraView: false,
});

function setStatus(text, ok = false) { dom.status.textContent = text; dom.status.className = ok ? "ok" : ""; }
function setChunkState(text) { if (dom.chunkState) dom.chunkState.textContent = text; }
function setMetricValue(node, value) { if (node) node.textContent = String(value); }
function updateMetricsUI() {
  setMetricValue(dom.renderFps, perfState.fps.toFixed(1));
  const totalFrames = state.get("totalFrames") || 0;
  setMetricValue(dom.frameNow, timelineState.absFrame >= 0 && totalFrames > 0 ? `${timelineState.absFrame + 1}/${totalFrames}` : `0/${totalFrames}`);
  setMetricValue(dom.videoTime, `${timelineState.timeSec.toFixed(2)}s`);
  const playback = state.get("playback"); const currentChunkIdx = state.get("currentChunkIdx");
  setMetricValue(dom.chunkNow, currentChunkIdx >= 0 && playback?.chunks?.length ? `${currentChunkIdx + 1}/${playback.chunks.length}` : "-/-");
  setMetricValue(dom.cacheNow, chunkCache.size); setMetricValue(dom.renderState, timelineState.renderState);
}

const sceneBoot = bootstrapPlaybackScene({
  mountEl: dom.app,
  viewConfig: VIEW_CONFIG,
  boardConfig: BOARD_CONFIG,
  showWhiteboard: SHOW_WHITEBOARD,
});
const { scene, camera, renderer, controls, board, handRoot } = sceneBoot;
const followTargetBox = new THREE.Box3();
const followTargetCenter = new THREE.Vector3();
let viewportAdapter = null;
function resizeRenderer() {
  viewportAdapter?.resizeRenderer();
}
function resetMainCameraView() {
  camera.position.set(...VIEW_CONFIG.cameraPos);
  camera.up.set(0, 1, 0);
  controls.target.set(...VIEW_CONFIG.cameraTarget);
  camera.fov = VIEW_CONFIG.fov;
  camera.updateProjectionMatrix();
  controls.update();
}
function setPanelVisible(panelEl, visible) {
  if (!panelEl) return;
  panelEl.style.display = visible ? "block" : "none";
}
function applyPanelVisibility() {
  setPanelVisible(dom.trajectoryPanel, Boolean(state.get("showTrajectoryPanel")));
  trajectoryWorld.setVisible(Boolean(state.get("showTrajectoryPanel")));
  projectionPanel.setVisible(Boolean(state.get("showProjectionPanel")));
  setPanelVisible(dom.frameMeshProjectionPanel, Boolean(state.get("showFrameMeshProjectionPanel")));
}

function syncBoardVisibility() {
  if (!board) return;
  board.visible = Boolean(SHOW_WHITEBOARD && !state.get("followCameraView"));
}

function bindFollowDistanceZoom() {
  const canvas = renderer?.domElement;
  if (!canvas) return;
  canvas.addEventListener("wheel", (event) => {
    if (!state.get("followCameraView")) return;
    event.preventDefault();
    const scale = event.deltaY > 0 ? 1.08 : 0.92;
    cameraPositionView.setDistance(cameraPositionView.getDistance() * scale);
  }, { passive: false });
}

const chunkCache = new Map(); const pendingChunks = new Map(); const failedChunks = new Set(); const accessOrder = [];
// chunkStore 封装 chunk 拉取、请求去重与近似 LRU 淘汰。
const chunkStore = createChunkStore({
  getPlayback: () => state.get("playback"), fetchJson: fetchJsonM, fetchArrayBuffer: fetchArrayBufferM, joinUrl, chunkCache, pendingChunks, failedChunks, accessOrder,
  getChunkPreloadCount: () => VIEW_CONFIG.chunkPreloadCount, getMaxCachedChunks: () => VIEW_CONFIG.maxCachedChunks,
});

const boardController = createBoardController({
  dom, boardConfig: BOARD_CONFIG, resetValues: { belowPalmDistance: 1.5, offsetX: 0.06, offsetZ: 0.72, rotXDeg: -39, rotYDeg: 4, rotZDeg: 0 },
  fmtNumber: fmt, degToRad: (v) => (v * Math.PI) / 180, getBoardMesh: () => board, showWhiteboard: SHOW_WHITEBOARD,
});
const { applyBoardTransformFromAnchor, bindBoardControls } = boardController;
applyBoardTransformFromAnchor();

const handRuntime = createHandRuntime({
  THREE,
  handRoot,
  showWhiteboard: false,
  materialPreset: {
    leftColor: HAND_VISUAL_COLORS.left.hex,
    rightColor: HAND_VISUAL_COLORS.right.hex,
    roughness: 0.7,
    metalness: 0.1,
  },
  sanitizeMeshPositions: () => {},
  applyBoardTransformFromAnchor: () => {},
  getBoardMesh: () => null,
  getBoardPlaced: () => true,
  setBoardPlaced: () => {},
  setBoardAnchor: () => {},
  getCameraTarget: () => ({ x: VIEW_CONFIG.cameraTarget[0], y: VIEW_CONFIG.cameraTarget[1], z: VIEW_CONFIG.cameraTarget[2] }),
  getHandLayoutApplied: () => state.get("handLayoutApplied"),
  setHandLayoutApplied: (v) => state.set("handLayoutApplied", v),
  getLeftMesh: () => state.get("leftMesh"), setLeftMesh: (v) => state.set("leftMesh", v),
  getRightMesh: () => state.get("rightMesh"), setRightMesh: (v) => state.set("rightMesh", v),
  setLeftPrevFrame: (v) => state.set("leftPrevFrame", v), setRightPrevFrame: (v) => state.set("rightPrevFrame", v),
});
const trajectoryWorld = createTrajectoryMiniWorld({
  THREE,
  canvas: dom.trajectoryCanvas,
  state,
  chunkCache,
  viewConfig: TRAJECTORY_VIEW_CONFIG,
  boardConfig: BOARD_CONFIG,
});
const cameraMarker = createCameraMarker({ THREE, scene });
function getFollowTargetCenter() {
  const leftMesh = state.get("leftMesh");
  const rightMesh = state.get("rightMesh");
  let hasBox = false;
  followTargetBox.makeEmpty();
  for (const mesh of [leftMesh, rightMesh]) {
    if (!mesh || !mesh.visible) continue;
    mesh.updateMatrixWorld(true);
    followTargetBox.expandByObject(mesh);
    hasBox = true;
  }
  if (!hasBox || followTargetBox.isEmpty()) return null;
  return followTargetBox.getCenter(followTargetCenter);
}
const cameraPositionView = createCameraPositionView({
  THREE,
  camera,
  controls,
  getObjectCenter: getFollowTargetCenter,
});
// canvasOverlay：视频上方透明 WebGL 投影层，渲染与视频同帧的 3D 手模叠加。
const canvasOverlay = createCanvasOverlay({
  dom,
  state,
  chunkCache,
  ensureChunkLoaded: (idx) => chunkStore.ensureChunkLoaded(idx),
});
// projectionPanel：相机坐标系下 XY/XZ/YZ 三个正交平面投影小窗。
const projectionPanel = createOrthographicProjectionPanel({
  dom,
  state,
  chunkCache,
  ensureChunkLoaded: (idx) => chunkStore.ensureChunkLoaded(idx),
});
const frameMeshProjectionPanel = createFrameMeshProjectionPanel({ dom, state, chunkCache });
function requestProjectionDraw(options) {
  try {
    if (state.get("showProjectionPanel")) projectionPanel.requestDraw(timelineState, options);
    if (state.get("showFrameMeshProjectionPanel")) frameMeshProjectionPanel.requestDraw(timelineState, options);
  } catch (error) {
    console.warn("projection panel draw failed", error);
  }
}

const renderSync = createPlaybackRenderSync({
  dom, viewConfig: VIEW_CONFIG, timelineState, chunkCache, setChunkState, updateMetricsUI,
  ensureChunkLoaded: (idx) => chunkStore.ensureChunkLoaded(idx), preloadNearChunks: (idx) => chunkStore.preloadNearChunks(idx),
  findChunkByTime, copyFrameSafe, frameHasFinitePoint, handRuntime, state,
});

const scrubController = createScrubController({ dom, viewConfig: VIEW_CONFIG, syncState, state, renderSync, setStatus });
const eventBindings = createPlaybackEventBindings({
  dom, state, syncState, timelineState, pending, renderSync, scrubController, setStatus, formatError,
  buildAndPlay, refreshAndBuild, resizeRenderer,
  onPanelToggle: (panel, nextVisible) => {
    if (panel === "trajectory") state.set("showTrajectoryPanel", nextVisible);
    applyPanelVisibility();
  },
  onCanvasToggle: (nextEnabled) => {
    if (nextEnabled && !state.get("playback")) {
      setStatus("请先加载播放资产后再开启视频投影渲染", false);
    }
    canvasOverlay.setEnabled(nextEnabled);
    canvasOverlay.requestDraw(timelineState);
  },
  onProjectionToggle: (nextVisible) => {
    state.set("showProjectionPanel", Boolean(nextVisible));
    projectionPanel.setVisible(nextVisible);
    requestProjectionDraw();
  },
  onFrameMeshProjectionToggle: (nextVisible) => {
    state.set("showFrameMeshProjectionPanel", Boolean(nextVisible));
    setPanelVisible(dom.frameMeshProjectionPanel, nextVisible);
    requestProjectionDraw({ force: true });
  },
  onCameraMarkerToggle: (nextVisible) => {
    state.set("showCameraMarker", Boolean(nextVisible));
    if (!nextVisible) cameraMarker.reset();
  },
});

function resetRuntimeState() {
  const keepTrajectoryPanelVisible = Boolean(
    dom.trajectoryPanelToggle ? dom.trajectoryPanelToggle.checked : state.get("showTrajectoryPanel"),
  );
  const keepCameraMarkerVisible = Boolean(
    dom.cameraMarkerToggle ? dom.cameraMarkerToggle.checked : state.get("showCameraMarker"),
  );
  const keepProjectionPanelVisible = Boolean(
    dom.projectionPanelToggle ? dom.projectionPanelToggle.checked : state.get("showProjectionPanel"),
  );
  const keepFrameMeshProjectionPanelVisible = Boolean(
    dom.frameMeshProjectionPanelToggle
      ? dom.frameMeshProjectionPanelToggle.checked
      : state.get("showFrameMeshProjectionPanel"),
  );
  const keepCameraFollowEnabled = Boolean(
    dom.cameraFollowToggle ? dom.cameraFollowToggle.checked : state.get("followCameraView"),
  );
  // 切换数据源时必须清空缓存与渲染态，避免旧 mesh 与新 index 混用。
  chunkStore.clear();
  stopIndexRefresh();
  state.patch({ lastRenderedAbsFrame: -1, normalTick: 0, leftPrevFrame: null, rightPrevFrame: null, playback: null, currentChunkIdx: -1, handLayoutApplied: false, videoReady: false, totalFrames: 0, frame: 0, play: false, speed: initialSpeed, normalIntervalFactor: 1 });
  trajectoryWorld.reset();
  cameraMarker.reset();
  canvasOverlay.reset();
  projectionPanel.reset();
  frameMeshProjectionPanel.reset();
  state.set("showTrajectoryPanel", keepTrajectoryPanelVisible);
  state.set("showProjectionPanel", keepProjectionPanelVisible);
  state.set("showFrameMeshProjectionPanel", keepFrameMeshProjectionPanelVisible);
  state.set("showCameraMarker", keepCameraMarkerVisible);
  state.set("followCameraView", keepCameraFollowEnabled);
  handRoot.position.set(0, 0, 0);
  if (!keepCameraFollowEnabled) resetMainCameraView();
  applyBoardTransformFromAnchor();
  syncBoardVisibility();
  syncState.mode = "idle"; syncState.previewFrame = 0; syncState.pendingPreviewRaf = false; syncState.resumePlaybackAfterSeek = false; syncState.clockMediaTimeSec = 0; syncState.clockPerfMs = 0;
  perfState.fps = 0; perfState.frameCounter = 0; perfState.sampleStartTs = performance.now();
  timelineState.absFrame = -1; timelineState.timeSec = 0; timelineState.renderState = "等待加载";
  dom.playPause.textContent = "播放"; dom.speed.value = String(initialSpeed);
  if (dom.trajectoryPanelToggle) dom.trajectoryPanelToggle.checked = keepTrajectoryPanelVisible;
  if (dom.projectionPanelToggle) dom.projectionPanelToggle.checked = keepProjectionPanelVisible;
  if (dom.frameMeshProjectionPanelToggle) dom.frameMeshProjectionPanelToggle.checked = keepFrameMeshProjectionPanelVisible;
  if (dom.cameraMarkerToggle) dom.cameraMarkerToggle.checked = keepCameraMarkerVisible;
  if (dom.cameraFollowToggle) dom.cameraFollowToggle.checked = keepCameraFollowEnabled;
  if (dom.frame) { dom.frame.max = "0"; dom.frame.value = "0"; }
  if (dom.frameText) dom.frameText.textContent = "0/0";
  applyPanelVisibility();
  setChunkState("3D未加载"); updateMetricsUI();
}

async function setupPlayback(indexUrl, videoUrl) {
  setStatus("处理中首次加载需要稍等片刻（加载 index/chunks）...", false);
  resetRuntimeState();

  const index = await fetchJsonM(indexUrl); const preset = index.presets?.balanced; if (!preset) throw new Error("balanced preset missing in index.json");
  const chunkBaseUrl = indexUrl.slice(0, indexUrl.lastIndexOf("/") + 1);
  // video_projection 大数组使用 video_projection.bin 二进制文件，
  // index.json 只保留内参和二进制引用 URL。
  let videoProjection = null;
  try {
    videoProjection = await loadVideoProjection({
      meta: index.video_projection,
      chunkBase: chunkBaseUrl,
      fetchArrayBuffer: fetchArrayBufferM,
      joinUrl,
    });
  } catch (e) {
    console.warn("加载 video_projection 失败,将禁用投影叠加", e);
  }
  const playback = {
    chunks: preset.chunks,
    fps: preset.fps,
    stride: preset.stride,
    frameStepSec: preset.stride / preset.fps,
    defaultQuality: index.default_quality || "balanced",
    chunkBaseUrl,
    invalidMask: Array.isArray(preset.invalid_mask) ? preset.invalid_mask : null,
    invalidLeftMask: Array.isArray(preset.invalid_left_mask) ? preset.invalid_left_mask : null,
    invalidRightMask: Array.isArray(preset.invalid_right_mask) ? preset.invalid_right_mask : null,
    videoProjection,
    // 流式构建状态：是否还在后台生成,以及当前已就绪的 chunk 数量。
    stillBuilding: Boolean(index.still_building),
    readyChunks: Number(index.ready_chunks || preset.ready_chunks || 0),
    totalChunks: Number(index.total_chunks || preset.total_chunks || preset.chunks.length),
    indexUrl,
  };
  state.patch({ playback, frameStepSec: playback.frameStepSec, totalFrames: Number(preset.frame_count || 0) });
  trajectoryWorld.setCameraData(videoProjection);
  dom.frame.max = String(Math.max(0, (state.get("totalFrames") || 0) - 1)); renderSync.updateFrameUI(0); timelineState.renderState = "加载首帧"; setChunkState("3D加载中 chunk 1");

  // 启动顺序：
  // 1) 加载 faces + 首 chunk
  // 2) 初始化 Three 主场景 mesh 与视频投影 overlay mesh
  // 3) 输出首帧，避免首帧闪空
  const facesBuf = await fetchArrayBufferM(joinUrl(playback.chunkBaseUrl, index.faces_bin));
  const first = await chunkStore.ensureChunkLoaded(0); if (!first) throw new Error("首个 chunk 加载失败");
  handRuntime.init(first.meta.vertex_count, new Uint32Array(facesBuf));
  canvasOverlay.init(first.meta.vertex_count, new Uint32Array(facesBuf));
  projectionPanel.init(first.meta.vertex_count, new Uint32Array(facesBuf));
  frameMeshProjectionPanel.init(first.meta.vertex_count, new Uint32Array(facesBuf));
  renderSync.renderChunkFrame(first, 0, 0); handRuntime.applyBestHandLayout();
  state.set("lastRenderedAbsFrame", 0); timelineState.absFrame = 0; timelineState.timeSec = 0; timelineState.renderState = "3D已就绪";
  setChunkState(`3D就绪 chunk 1/${playback.chunks.length}`); updateMetricsUI();

  // 视频可能尚未就绪：videoUrl 为空时先不加载，视频转码完成后由 pollVideoStatus 触发。
  // 如果 video_url_hint 已预加载(src 已设置),不重复 load。
  const videoAlreadyLoading = Boolean(dom.sourceVideo.src && dom.sourceVideo.readyState >= 1);
  if (videoUrl && !videoAlreadyLoading) {
    dom.sourceVideo.src = videoUrl;
    dom.sourceVideo.playbackRate = state.get("speed");
    dom.sourceVideo.load();
    setStatus("3D已就绪，等待视频可播放", true);
  } else if (videoAlreadyLoading) {
    setStatus("3D已就绪，视频加载中", true);
  } else if (!videoUrl) {
    setStatus("3D已就绪，视频转码中，请稍等...", false);
  }
}

function loadVideoOnly(videoUrl) {
  if (!videoUrl) return;
  dom.sourceVideo.src = videoUrl;
  dom.sourceVideo.playbackRate = state.get("speed");
  dom.sourceVideo.load();
  // 视频加载后尝试自动播放(可能被浏览器策略阻止)。
  dom.sourceVideo.addEventListener("canplay", function onCanPlay() {
    dom.sourceVideo.removeEventListener("canplay", onCanPlay);
    state.set("videoReady", true);
    if (state.get("play")) {
      dom.sourceVideo.play().catch(() => {
        setStatus("视频已就绪，请手动点击播放", true);
      });
    }
  }, { once: true });
  dom.sourceVideo.addEventListener("error", function onError() {
    dom.sourceVideo.removeEventListener("error", onError);
    const err = dom.sourceVideo.error;
    console.error("video load error:", err?.code, err?.message);
    setStatus(`视频加载失败：${err?.message || "unknown"}`, false);
  }, { once: true });
}

let activeVideoStreamToken = 0;
let activeVideoStatusSource = null;

function stopVideoStatusStream() {
  if (activeVideoStatusSource) {
    activeVideoStatusSource.close();
    activeVideoStatusSource = null;
  }
}

function pollVideoStatus(mediaId, { timeoutMs = 15 * 60 * 1000 } = {}) {
  if (!mediaId) return;

  stopVideoStatusStream();
  const token = ++activeVideoStreamToken;
  const source = new EventSource(`/api/playback/video-status/stream/${encodeURIComponent(mediaId)}`);
  activeVideoStatusSource = source;

  const timeoutId = setTimeout(() => {
    if (token !== activeVideoStreamToken) return;
    stopVideoStatusStream();
    setStatus("视频转码等待超时，请稍后重试", false);
  }, timeoutMs);

  const closeCurrent = () => {
    clearTimeout(timeoutId);
    if (token !== activeVideoStreamToken) {
      source.close();
      return false;
    }
    stopVideoStatusStream();
    return true;
  };

  source.addEventListener("status", (event) => {
    let data = null;
    try {
      data = JSON.parse(event.data || "{}");
    } catch {
      return;
    }
    if (token !== activeVideoStreamToken) return;

    const status = String(data.status || "").toLowerCase();
    if (status === "queued" || status === "running" || status === "pending") {
      setStatus("正在视频转码，请稍等...", false);
      return;
    }
    if (status === "success" && data.video_url) {
      if (!closeCurrent()) return;
      loadVideoOnly(data.video_url);
      setStatus("视频转码完成，已自动加载", true);
      return;
    }
    if (status === "failed") {
      if (!closeCurrent()) return;
      setStatus(`视频转码失败：${data.error || "unknown"}`, false);
    }
  });

  source.onerror = () => {
    if (token !== activeVideoStreamToken) {
      source.close();
    }
  };
}

function setupVideoOnlyPlayback(videoUrl) {
  resetRuntimeState();
  handRuntime.dispose();
  cameraMarker.reset();
  canvasOverlay.setEnabled(false);
  projectionPanel.reset();
  frameMeshProjectionPanel.reset();
  trajectoryWorld.setCameraData(null);
  timelineState.renderState = "纯视频模式";
  setChunkState("无3D数据");
  updateMetricsUI();
  if (videoUrl) {
    dom.sourceVideo.src = videoUrl;
    dom.sourceVideo.playbackRate = state.get("speed");
    dom.sourceVideo.load();
  }
}

let activeBuildStreamToken = 0;
let activeBuildTaskSource = null;

function stopBuildTaskStream() {
  if (activeBuildTaskSource) {
    activeBuildTaskSource.close();
    activeBuildTaskSource = null;
  }
}

function waitBuildTask(taskId, { timeoutMs = 15 * 60 * 1000, acceptPartial = false } = {}) {
  if (MASTER_MODE) return waitMasterJob(taskId, { timeoutMs });
  return new Promise((resolve, reject) => {
    stopBuildTaskStream();
    const token = ++activeBuildStreamToken;
    const source = new EventSource(`/api/playback/build-chunks/stream/${encodeURIComponent(taskId)}`);
    activeBuildTaskSource = source;

    const timeoutId = setTimeout(() => {
      if (token !== activeBuildStreamToken) return;
      stopBuildTaskStream();
      reject(new Error("build task timeout"));
    }, timeoutMs);

    const closeCurrent = () => {
      clearTimeout(timeoutId);
      if (token !== activeBuildStreamToken) {
        source.close();
        return false;
      }
      stopBuildTaskStream();
      return true;
    };

    source.addEventListener("status", (event) => {
      let taskData = null;
      try {
        taskData = JSON.parse(event.data || "{}");
      } catch {
        return;
      }
      if (token !== activeBuildStreamToken) return;

      const status = String(taskData.status || "").toLowerCase();
      if (status === "succeeded") {
        if (!closeCurrent()) return;
        resolve({ result: taskData.result, partial: false });
        return;
      }
      if (status === "failed") {
        if (!closeCurrent()) return;
        reject(new Error(taskData.error || "build task failed"));
        return;
      }
      if (acceptPartial && taskData.result && taskData.result.has_3d) {
        const partial = taskData.result;
        const partialStatus = String(partial.video_status || "").toLowerCase();
        const partialMediaId = Number(partial.media_id || 0);
        if (partialMediaId > 0 || partialStatus === "ready" || partialStatus === "failed") {
          if (!closeCurrent()) return;
          resolve({ result: partial, partial: Boolean(partial.still_building) });
        }
      }
    });

    source.onerror = () => {
      if (token !== activeBuildStreamToken) {
        source.close();
        return;
      }
      if (!closeCurrent()) return;
      reject(new Error("build task stream disconnected"));
    };
  });
}

/**
 * Master 模式下等待 handvis 任务：SSE /api/jobs/{id}/events。
 * 事件 payload: {status, frames_done, frame_count, playable, video_url, error}。
 * playable（≥30 帧）即 resolve 出首段，ready 才视为全部完成。
 */
function waitMasterJob(jobId, { timeoutMs = 15 * 60 * 1000 } = {}) {
  return new Promise((resolve, reject) => {
    stopBuildTaskStream();
    const token = ++activeBuildStreamToken;
    const source = new EventSource(apiUrl(`/api/jobs/${encodeURIComponent(jobId)}/events`));
    activeBuildTaskSource = source;

    const timeoutId = setTimeout(() => {
      if (token !== activeBuildStreamToken) return;
      stopBuildTaskStream();
      reject(new Error("master job timeout"));
    }, timeoutMs);

    const closeCurrent = () => {
      clearTimeout(timeoutId);
      if (token !== activeBuildStreamToken) {
        source.close();
        return false;
      }
      stopBuildTaskStream();
      return true;
    };

    const buildResult = (s) => {
      const status = String(s.status || "").toLowerCase();
      const done = Number(s.frames_done || 0);
      const videoUrl = String(s.video_url || "").trim();
      return {
        result: {
          has_3d: done > 0,
          index_url: jobMediaUrl(jobId, "index.json"),
          video_url: videoUrl,
          video_status: videoUrl ? "ready" : "failed",
          video_error: videoUrl ? null : "Master 模式不转码；请提供浏览器可播的 H.264 视频",
          media_id: 0,
          still_building: status !== "ready",
        },
        partial: status !== "ready",
      };
    };

    const onPayload = (raw) => {
      let s = null;
      try {
        s = JSON.parse(raw || "{}");
      } catch {
        return;
      }
      if (token !== activeBuildStreamToken) return;

      // 视频签名 URL 一到就预加载，不等 mesh。
      const vurl = String(s.video_url || "").trim();
      if (vurl && !dom.sourceVideo.src) {
        dom.sourceVideo.src = vurl;
        dom.sourceVideo.playbackRate = state.get("speed");
        dom.sourceVideo.load();
      }

      const status = String(s.status || "").toLowerCase();
      if (status === "failed" || status === "cancelled") {
        if (!closeCurrent()) return;
        reject(new Error(s.error || `job ${status}`));
        return;
      }
      const done = Number(s.frames_done || 0);
      if (status === "ready" || s.playable || done >= 30) {
        if (!closeCurrent()) return;
        resolve(buildResult(s));
      }
    };

    source.addEventListener("progress", (ev) => onPayload(ev.data));
    source.addEventListener("done", (ev) => onPayload(ev.data));
    source.onerror = () => {
      // Master SSE 5 分钟一轮会主动断开，EventSource 自动重连；继续等。
      if (token !== activeBuildStreamToken) source.close();
    };
  });
}

async function refreshIndexWhileBuilding(indexUrl) {
  // 流式构建期间,定期重新拉取 index.json,把新就绪的 chunk 同步到前端 playback.chunks。
  // video_projection 是整段视频一次写完的(基于 slam_data),不需要重拉 .bin。
  const playback = state.get("playback");
  if (!playback || !playback.stillBuilding) return;
  try {
    const index = await fetchJsonM(indexUrl);
    const preset = index.presets?.balanced;
    if (!preset || !Array.isArray(preset.chunks)) return;
    playback.chunks = preset.chunks;
    playback.stillBuilding = Boolean(index.still_building);
    playback.readyChunks = Number(index.ready_chunks || preset.ready_chunks || 0);
    playback.totalChunks = Number(index.total_chunks || preset.total_chunks || preset.chunks.length);
  } catch (e) {
    console.warn("refresh index failed", e);
  }
}

let activeIndexRefreshTimer = null;

function stopIndexRefresh() {
  if (activeIndexRefreshTimer) {
    clearInterval(activeIndexRefreshTimer);
    activeIndexRefreshTimer = null;
  }
}

function startIndexRefresh(indexUrl, { intervalMs = 2000 } = {}) {
  stopIndexRefresh();
  activeIndexRefreshTimer = setInterval(async () => {
    await refreshIndexWhileBuilding(indexUrl);
    const playback = state.get("playback");
    if (!playback || !playback.stillBuilding) {
      stopIndexRefresh();
    }
  }, intervalMs);
}

async function buildAndPlay(epDir, { forceRebuild = false } = {}) {
  if (state.get("buildInFlight")) return;
  const itemName = String(dom.itemName?.value || "").trim();
  if (!epDir) { setStatus("请输入 ep_dir", false); return; }
  if (!itemName) { setStatus("请输入 item_name", false); return; }
  try {
    state.set("buildInFlight", true);
    activeBuildStreamToken += 1;
    stopBuildTaskStream();
    activeVideoStreamToken += 1;  // 每次新构建打断旧的视频状态监听。
    stopVideoStatusStream();
    stopIndexRefresh();
    setStatus(`处理中首次加载需要稍等片刻（item=${itemName}）...`, false);
    let taskId;
    if (MASTER_MODE) {
      const submitResp = await fetch(apiUrl("/api/jobs"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          kind: "handvis",
          ep_dir: epDir,
          item_name: itemName,
          force: !!forceRebuild,
          video_path: initialVideoPathFromUrl || undefined,
          posehand_path: initialPosehandPathFromUrl || undefined,
        }),
      });
      const submitData = await parseApiBody(submitResp);
      if (!submitResp.ok && submitResp.status !== 202) throw new Error(submitData.error || "master submit failed");
      taskId = submitData.job_id;
      if (!taskId) throw new Error("master 未返回 job_id");
    } else {
      const submitResp = await fetch("/api/playback/build-chunks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ep_dir: epDir,
          item_name: itemName,
          force_rebuild: !!forceRebuild,
          video_path: initialVideoPathFromUrl,
          posehand_path: initialPosehandPathFromUrl,
        }),
      });
      const submitData = await parseApiBody(submitResp);
      if (!submitResp.ok) throw new Error(submitData.detail || "build-chunks submit failed");
      if (!submitData.task_id) throw new Error("missing task_id");
      taskId = submitData.task_id;

      // 视频预加载：大部分视频可直接播放,拿到 hint 后立即加载,不等 build task。
      if (submitData.video_url_hint) {
        dom.sourceVideo.src = submitData.video_url_hint;
        dom.sourceVideo.playbackRate = state.get("speed");
        dom.sourceVideo.load();
      }
    }

    // 流式优化：首 chunk 就绪即返回,提前开始 3D 渲染。
    const { result: data, partial } = await waitBuildTask(taskId, { acceptPartial: true });
    const videoStatus = String(data.video_status || "").toLowerCase();
    const videoReady = videoStatus === "ready" && data.video_url;

    if (data.has_3d && data.index_url) {
      setStatus(partial ? "首段3D已就绪,后续构建中..." : "构建完成,正在加载3D...", false);
      await setupPlayback(data.index_url, videoReady ? data.video_url : "");

      // 流式期间持续刷新 index.json,把新就绪的 chunk 注入 playback.chunks。
      if (partial) {
        startIndexRefresh(data.index_url);
      }

      if (videoReady) {
        setStatus(partial
          ? `首段3D+视频已就绪,后台继续构建中（item=${itemName}）`
          : `构建完成,视频与3D已就绪（item=${itemName}）`, true);
      } else if (videoStatus === "failed") {
        setStatus(`3D 已就绪,但视频处理失败：${data.video_error || "unknown"}`, false);
      } else {
        pollVideoStatus(data.media_id);
      }
    } else if (videoReady) {
      setStatus("构建完成,正在加载视频...", false);
      setupVideoOnlyPlayback(data.video_url);
      setStatus(`当前素材无posehand,已切换纯视频播放（item=${itemName}）`, true);
    } else {
      setupVideoOnlyPlayback("");
      setStatus("视频转码中,请稍等...", false);
      pollVideoStatus(data.media_id);
    }
  } finally { state.set("buildInFlight", false); }
}

async function refreshAndBuild(epDir, itemName) {
  if (MASTER_MODE) {
    // Master 模式：worker 每次都现扫 OSS，「强制刷新」就是强制重建。
    await buildAndPlay(epDir, { forceRebuild: true });
    return;
  }
  setStatus("正在强制刷新远程索引...", false);
  const refreshResp = await fetch("/api/ep-assets/refresh", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ep_dir: epDir }) });
  const refreshData = await parseApiBody(refreshResp); if (!refreshResp.ok) throw new Error(refreshData.detail || "refresh failed");
  if (!Array.isArray(refreshData.items) || !refreshData.items.some((x) => String(x?.item_name || "").trim() === itemName)) {
    throw new Error(`item_name 不存在: ${itemName}`);
  }
  await buildAndPlay(epDir, { forceRebuild: true });
}

function updateDebugValues() {
  if (!dom.debugValues) return;
  const cp = camera.position; const t = controls.target;
  dom.debugValues.textContent = [
    `cameraPos: [${fmt(cp.x)}, ${fmt(cp.y)}, ${fmt(cp.z)}]`,
    `target: [${fmt(t.x)}, ${fmt(t.y)}, ${fmt(t.z)}]`,
    `polar(rad): ${fmt(controls.getPolarAngle())}  azimuth(rad): ${fmt(controls.getAzimuthalAngle())}`,
    `distance: ${fmt(camera.position.distanceTo(t))}  fov: ${fmt(camera.fov)}`,
    `videoTime: ${fmt(dom.sourceVideo?.currentTime || 0)}s  frame: ${(state.get("frame") || 0) + 1}/${Math.max(state.get("totalFrames") || 0, 0)}`,
  ].join("\n");
}

function animate() {
  requestAnimationFrame(animate);
  perfState.frameCounter += 1;
  // 暂停后的逐帧跳转以 renderAtFrame 指定的帧为准，不能再被视频 seek 的实际落点覆盖。
  if (syncState.mode === "idle" && !dom.sourceVideo.paused && !dom.sourceVideo.ended) {
    renderSync.renderAtVideoTime();
  }
  if (state.get("showTrajectoryPanel")) trajectoryWorld.update(timelineState.absFrame);
  if (state.get("showCameraMarker")) {
    cameraMarker.update(state.get("playback")?.videoProjection || null, timelineState.absFrame);
  } else {
    cameraMarker.reset();
  }
  if (state.get("followCameraView")) {
    cameraPositionView.apply(state.get("playback")?.videoProjection || null, timelineState.absFrame);
  }
  canvasOverlay.requestDraw(timelineState);
  requestProjectionDraw();
  controls.update(); updateDebugValues(); renderer.render(scene, camera);
  const now = performance.now(); const elapsed = now - perfState.sampleStartTs;
  if (elapsed >= 500) { perfState.fps = (perfState.frameCounter * 1000) / elapsed; perfState.frameCounter = 0; perfState.sampleStartTs = now; updateMetricsUI(); }
}

async function bootstrapFromQuery() {
  // 优先使用首页直传的 index/video（无后端二次构建），失败再退回手动查询。
  if (initialIndexUrlFromUrl && initialVideoUrlFromUrl) {
    try { pending.initialSeekSec = initialStartTimeSec; pending.initialAutoPlay = initialAutoPlay; await setupPlayback(initialIndexUrlFromUrl, initialVideoUrlFromUrl); setStatus("已自动加载首页当前视频与3D", true); return; }
    catch (e) { pending.initialSeekSec = null; pending.initialAutoPlay = false; setStatus(`自动加载失败，已回退手动查询: ${formatError(e)}`, false); return; }
  }
  if (initialEpDirFromUrl) {
    if (!initialItemNameFromUrl) {
      setStatus("URL 缺少 item_name，请手动补充后查询", false);
      return;
    }
    try { pending.initialSeekSec = initialStartTimeSec; pending.initialAutoPlay = true; await buildAndPlay(initialEpDirFromUrl); setStatus("已自动加载 ep_dir 对应视频与3D", true); }
    catch (e) { pending.initialSeekSec = null; pending.initialAutoPlay = false; setStatus(`自动加载 ep_dir 失败，可手动查询: ${formatError(e)}`, false); }
  }
}

if (initialEpDirFromUrl) dom.epDir.value = initialEpDirFromUrl;
if (initialItemNameFromUrl && dom.itemName) dom.itemName.value = initialItemNameFromUrl;
if (MASTER_MODE) {
  // Master 无评分库与 HandVis 首页：隐藏评分表单和返回链接。
  const scoreForm = document.getElementById("playbackScoreForm");
  if (scoreForm) scoreForm.style.display = "none";
  const backLink = document.querySelector("a.back");
  if (backLink) backLink.style.display = "none";
} else {
  createPlaybackScorePanel({ epDirInput: dom.epDir, itemNameInput: dom.itemName });
}
bindBoardControls();
bindFloatingPanelInteractions([
  {
    panel: dom.trajectoryPanel,
    resize: {
      heightMode: "canvas-style",
      heightElement: dom.trajectoryCanvas,
      minWidth: 320,
      minHeight: 220,
      bottomReserve: 44,
      onResize: () => {},
    },
  },
  {
    panel: dom.orthographicProjectionPanel,
    resize: {
      heightMode: "css-var",
      minWidth: 320,
      minHeight: 120,
      bottomReserve: 72,
      onResize: () => requestProjectionDraw({ force: true }),
    },
  },
  {
    panel: dom.frameMeshProjectionPanel,
    resize: {
      heightMode: "css-var",
      minWidth: 320,
      minHeight: 120,
      bottomReserve: 72,
      onResize: () => requestProjectionDraw({ force: true }),
    },
  },
]);
const floatingPanelLayout = createFloatingPanelLayout({
  hud: dom.hud,
  trajectoryPanel: dom.trajectoryPanel,
  trajectoryCanvas: dom.trajectoryCanvas,
  debugValues: dom.debugValues,
  videoPanel: dom.videoPanel,
  projectionPanels: [dom.orthographicProjectionPanel, dom.frameMeshProjectionPanel],
  onProjectionResize: () => requestProjectionDraw({ force: true }),
});
viewportAdapter = createPlaybackViewportAdapter({
  mountEl: dom.app,
  renderer,
  camera,
  panels: [dom.trajectoryPanel, dom.orthographicProjectionPanel, dom.frameMeshProjectionPanel],
  layout: floatingPanelLayout,
  requestOverlayDraw: (nextTimelineState) => canvasOverlay.requestDraw(nextTimelineState || timelineState),
  requestProjectionDraw,
  getTimelineState: () => timelineState,
});
bindFollowDistanceZoom();
eventBindings.bindHudEvents();
eventBindings.bindVideoEvents(timelineState);
eventBindings.bindActionEvents();
eventBindings.bindWindowEvents();
if (dom.cameraFollowToggle) {
  dom.cameraFollowToggle.addEventListener("change", () => {
    const enabled = dom.cameraFollowToggle.checked;
    state.set("followCameraView", enabled);
    if (enabled) {
      state.set("showCameraMarker", false);
      if (dom.cameraMarkerToggle) dom.cameraMarkerToggle.checked = false;
      cameraMarker.reset();
    } else {
      resetMainCameraView();
    }
    syncBoardVisibility();
  });
}
resetRuntimeState(); updateDebugValues(); animate();
bootstrapFromQuery();
