/**
 * 首页入口（index.js）。
 * 职责：
 * 1) 组织查询模块、列表模块、3D 预览模块
 * 2) 管理首页级共享状态
 * 3) 暴露 window 事件入口给 HTML 按钮
 */

import * as THREE from "three";
import {
  copyText,
  epTail,
  esc,
  formatError,
  fetchArrayBuffer,
  fetchJson,
  fmtNum,
  getParentDir,
  isLikelySingleEpDir,
  joinUrl,
} from "./index/common.js";
import { copyFrameSafe, findChunkByTime, frameHasFinitePoint } from "./shared/chunk-utils.js";
import { createChunkStore } from "./shared/chunk-store.js";
import { createBoardController, sanitizeMeshPositions } from "./index/board-controller.js";
import { bootstrapIndexScene } from "./index/scene-bootstrap.js";
import { createIndexVideoSync } from "./index/video-sync.js";
import { createQueryController } from "./index/query-controller.js";
import { createScoreDialog } from "./index/score-dialog.js";
import { createHandRuntime } from "./shared/hand-runtime.js";
import { createMutableState } from "./shared/state-helpers.js";
import { COMMON_VIEW_CONFIG } from "./shared/view-config.js";
import { BOARD_CONFIG, SHOW_WHITEBOARD } from "./shared/scene-config.js";

const dom = {
  epDir: document.getElementById("epDir"),
  directPairToggle: document.getElementById("directPairToggle"),
  directPairPanel: document.getElementById("directPairPanel"),
  directPosehandPath: document.getElementById("directPosehandPath"),
  directVideoPath: document.getElementById("directVideoPath"),
  resultText: document.getElementById("resultText"),
  backParentBtn: document.getElementById("backParentBtn"),
  parentEpWrap: document.getElementById("parentEpWrap"),
  parentEpTitle: document.getElementById("parentEpTitle"),
  parentEpHint: document.getElementById("parentEpHint"),
  parentEpBody: document.getElementById("parentEpBody"),
  parentPrevPageBtn: document.getElementById("parentPrevPageBtn"),
  parentNextPageBtn: document.getElementById("parentNextPageBtn"),
  parentPageInfo: document.getElementById("parentPageInfo"),
  parentPageSize: document.getElementById("parentPageSize"),
  pairWrap: document.getElementById("pairWrap"),
  pairTitle: document.getElementById("pairTitle"),
  pairHint: document.getElementById("pairHint"),
  pairBody: document.getElementById("pairBody"),
  pairPrevPageBtn: document.getElementById("pairPrevPageBtn"),
  pairNextPageBtn: document.getElementById("pairNextPageBtn"),
  pairPageInfo: document.getElementById("pairPageInfo"),
  pairPageSize: document.getElementById("pairPageSize"),
  playerWrap: document.getElementById("playerWrap"),
  player: document.getElementById("videoPlayer"),
  listBody: document.getElementById("listBody"),
  listPageInfo: document.getElementById("listPageInfo"),
  listPageSize: document.getElementById("listPageSize"),
  prevListPageBtn: document.getElementById("prevListPageBtn"),
  nextListPageBtn: document.getElementById("nextListPageBtn"),
  threeReserve: document.getElementById("threeReserve"),
  boardYOffset: document.getElementById("boardYOffset"),
  boardYOffsetVal: document.getElementById("boardYOffsetVal"),
  boardXOffset: document.getElementById("boardXOffset"),
  boardXOffsetVal: document.getElementById("boardXOffsetVal"),
  boardZOffset: document.getElementById("boardZOffset"),
  boardZOffsetVal: document.getElementById("boardZOffsetVal"),
  boardRotX: document.getElementById("boardRotX"),
  boardRotXVal: document.getElementById("boardRotXVal"),
  boardRotY: document.getElementById("boardRotY"),
  boardRotYVal: document.getElementById("boardRotYVal"),
  boardRotZ: document.getElementById("boardRotZ"),
  boardRotZVal: document.getElementById("boardRotZVal"),
  boardResetBtn: document.getElementById("boardResetBtn"),
  scoreDialog: document.getElementById("scoreDialog"),
  scoreForm: document.getElementById("scoreForm"),
  scoreOssPath: document.getElementById("scoreOssPath"),
  scoreEgoChoice: document.getElementById("scoreEgoChoice"),
  scoreRating: document.getElementById("scoreRating"),
  scoreZeroButton: document.getElementById("scoreZeroButton"),
  scoreValueLabel: document.getElementById("scoreValueLabel"),
  scoreComment: document.getElementById("scoreComment"),
  scoreMessage: document.getElementById("scoreMessage"),
  scoreCloseBtn: document.getElementById("scoreCloseBtn"),
  scoreCancelBtn: document.getElementById("scoreCancelBtn"),
  scoreSaveBtn: document.getElementById("scoreSaveBtn"),
};

function listFeatureDisabled() {
  setResult("列表查询功能已前端关闭", false);
}

const VIEW_CONFIG = {
  // 公共判定参数来自共享模块；首页只覆盖相机参数。
  ...COMMON_VIEW_CONFIG,
  cameraPos: [-0.029, -1.620, -1.246],
  cameraTarget: [0, 0.26, 0],
};

function updateBackParentButton(ok) {
  if (!dom.backParentBtn) return;
  const epDir = (dom.epDir.value || "").trim();
  const parentDir = getParentDir(epDir);
  const visible = Boolean(ok && isLikelySingleEpDir(epDir) && parentDir);
  dom.backParentBtn.hidden = !visible;
  dom.backParentBtn.style.display = visible ? "inline-flex" : "none";
}

function setResult(text, ok = false) {
  const normalizedText = String(text ?? "").replaceAll("\n", "  ");
  dom.resultText.textContent = normalizedText;
  dom.resultText.className = ok ? "ok" : "err";
  updateBackParentButton(ok);
}

const state = createMutableState({
  videoReady: false,
  playback: null,
  lastRenderedAbsFrame: -1,
  normalTick: 0,
  leftPrevFrame: null,
  rightPrevFrame: null,
  handLayoutApplied: false,
  boardPlaced: false,
  boardAnchor: null,
  lastResultData: null,
  selectedItemName: "",
  currentPairs: [],
  queryInFlight: false,
});

// 三维运行时对象在初始化后复用，避免重复创建 WebGL 上下文。
let scene = null;
let camera = null;
let renderer = null;
let controls = null;
let handRoot = null;
let boardMesh = null;
let handRuntime = null;
let videoSync = null;

const chunkCache = new Map();
const pendingChunks = new Map();
const failedChunks = new Set();
const accessOrder = [];
const chunkStore = createChunkStore({
  getPlayback: () => state.get("playback"),
  fetchJson,
  fetchArrayBuffer,
  joinUrl,
  chunkCache,
  pendingChunks,
  failedChunks,
  accessOrder,
  getChunkPreloadCount: () => VIEW_CONFIG.chunkPreloadCount,
  getMaxCachedChunks: () => VIEW_CONFIG.maxCachedChunks,
});

const boardController = createBoardController({
  dom,
  boardConfig: BOARD_CONFIG,
  resetValues: { belowPalmDistance: 1.5, offsetX: 0.06, offsetZ: 0.72, rotXDeg: -39, rotYDeg: 4, rotZDeg: 0 },
  fmtNumber: fmtNum,
  degToRad: (v) => (v * Math.PI) / 180,
  getBoardMesh: () => boardMesh,
  showWhiteboard: SHOW_WHITEBOARD,
});
const { applyBoardTransformFromAnchor, bindBoardControls } = boardController;

function initThreeScene() {
  // 幂等初始化：页面生命周期内只创建一次场景。
  if (renderer) return;
  const boot = bootstrapIndexScene({ mountEl: dom.threeReserve, viewConfig: VIEW_CONFIG, boardConfig: BOARD_CONFIG, showWhiteboard: SHOW_WHITEBOARD });
  scene = boot.scene; camera = boot.camera; renderer = boot.renderer; controls = boot.controls; handRoot = boot.handRoot; boardMesh = boot.boardMesh;
  applyBoardTransformFromAnchor();

  handRuntime = createHandRuntime({
    THREE,
    handRoot,
    showWhiteboard: SHOW_WHITEBOARD,
    materialPreset: { leftColor: 0xdcb6ff, rightColor: 0x31b9e6, roughness: 0.72, metalness: 0.05 },
    sanitizeMeshPositions,
    applyBoardTransformFromAnchor,
    getBoardMesh: () => boardMesh,
    getBoardPlaced: () => state.get("boardPlaced"),
    setBoardPlaced: (v) => state.set("boardPlaced", v),
    setBoardAnchor: (v) => state.set("boardAnchor", v),
    getCameraTarget: () => ({ x: VIEW_CONFIG.cameraTarget[0], y: VIEW_CONFIG.cameraTarget[1], z: VIEW_CONFIG.cameraTarget[2] }),
    getHandLayoutApplied: () => state.get("handLayoutApplied"),
    setHandLayoutApplied: (v) => state.set("handLayoutApplied", v),
    getLeftMesh: () => state.get("leftMesh"),
    setLeftMesh: (v) => state.set("leftMesh", v),
    getRightMesh: () => state.get("rightMesh"),
    setRightMesh: (v) => state.set("rightMesh", v),
    setLeftPrevFrame: (v) => state.set("leftPrevFrame", v),
    setRightPrevFrame: (v) => state.set("rightPrevFrame", v),
  });

  videoSync = createIndexVideoSync({
    dom,
    viewConfig: VIEW_CONFIG,
    chunkCache,
    ensureChunkLoaded: (idx) => chunkStore.ensureChunkLoaded(idx),
    preloadNearChunks: (idx) => chunkStore.preloadNearChunks(idx),
    findChunkByTime,
    copyFrameSafe,
    frameHasFinitePoint,
    handRuntime,
    state,
  });

  videoSync.bindVideoEvents();
  resizeThree();
  animateThree();
}

function resizeThree() {
  if (!renderer || !camera) return;
  const w = Math.max(1, dom.threeReserve.clientWidth);
  const h = Math.max(1, dom.threeReserve.clientHeight);
  renderer.setSize(w, h, false);
  camera.aspect = w / h;
  camera.updateProjectionMatrix();
}

function animateThree() {
  requestAnimationFrame(animateThree);
  if (videoSync) videoSync.renderAtVideoTime();
  if (controls) controls.update();
  if (renderer && scene && camera) renderer.render(scene, camera);
}

function resetPlaybackState() {
  // 每次切换查询结果前清理缓存与状态，避免旧帧残留。
  chunkStore.clear();
  state.patch({ lastRenderedAbsFrame: -1, normalTick: 0, leftPrevFrame: null, rightPrevFrame: null, playback: null, handLayoutApplied: false, boardPlaced: false, boardAnchor: null });
  if (handRoot) handRoot.position.set(0, 0, 0);
  if (boardMesh) {
    applyBoardTransformFromAnchor();
    boardMesh.visible = SHOW_WHITEBOARD;
  }
}

function clearThreePlayback() {
  resetPlaybackState();
  handRuntime.dispose();
}

async function setupThreePlayback(indexUrl) {
  initThreeScene();
  resetPlaybackState();

  const index = await fetchJson(indexUrl);
  const preset = index.presets?.balanced;
  if (!preset) throw new Error("index.json 缺少 balanced 预设");

  state.set("playback", {
    chunks: preset.chunks,
    fps: preset.fps,
    stride: preset.stride,
    frameStepSec: preset.stride / preset.fps,
    defaultQuality: index.default_quality || "balanced",
    chunkBaseUrl: indexUrl.slice(0, indexUrl.lastIndexOf("/") + 1),
    // 后端 invalid 帧（1=invalid）优先隐藏，避免无效帧被渲染。
    invalidMask: Array.isArray(preset.invalid_mask) ? preset.invalid_mask : null,
    invalidLeftMask: Array.isArray(preset.invalid_left_mask) ? preset.invalid_left_mask : null,
    invalidRightMask: Array.isArray(preset.invalid_right_mask) ? preset.invalid_right_mask : null,
  });

  const facesBuf = await fetchArrayBuffer(joinUrl(state.get("playback").chunkBaseUrl, index.faces_bin));
  const facesU32 = new Uint32Array(facesBuf);
  const first = await chunkStore.ensureChunkLoaded(0);
  if (!first) throw new Error("首个 chunk 加载失败");
  handRuntime.init(first.meta.vertex_count, facesU32);
  videoSync.updateMeshFrame(first, 0);
  handRuntime.applyBestHandLayout();
  requestAnimationFrame(() => resizeThree());
}

function pickEpDir(epDir) { dom.epDir.value = epDir; }
function openPlaybackByEp(epDir, itemName = "", directPairPaths = null) {
  const params = new URLSearchParams();
  const v = String(epDir || "").trim();
  const n = String(itemName || "").trim();
  if (v) params.set("ep_dir", v);
  if (n) params.set("item_name", n);
  const videoPath = String(directPairPaths?.video_path || "").trim();
  const posehandPath = String(directPairPaths?.posehand_path || "").trim();
  if (videoPath) params.set("video_path", videoPath);
  if (posehandPath) params.set("posehand_path", posehandPath);
  const q = params.toString();
  window.location.href = q ? `/playback?${q}` : "/playback";
}

const scoreDialog = createScoreDialog({ dom });

const queryController = createQueryController({
  dom,
  state,
  esc,
  epTail,
  setResult,
  formatError,
  copyText,
  setupThreePlayback,
  clearThreePlayback,
  refreshList: null,
  openPlaybackByEp,
  openScoreDialog: scoreDialog.open,
});

dom.directPairToggle?.addEventListener("click", () => {
  const nextExpanded = dom.directPairToggle.getAttribute("aria-expanded") !== "true";
  dom.directPairToggle.setAttribute("aria-expanded", String(nextExpanded));
  if (dom.directPairPanel) dom.directPairPanel.hidden = !nextExpanded;
  if (nextExpanded) dom.directPosehandPath?.focus();
});

window.pickEpDir = pickEpDir;
window.openPlaybackByEp = openPlaybackByEp;
window.copyEpDir = queryController.copyEpDir;
window.copyLastResult = queryController.copyLastResult;
window.submitQuery = queryController.submitQuery;
window.submitRefresh = queryController.submitRefresh;
window.openPlayback = queryController.openPlayback;
// 前端先关掉列表查询，保留同名入口避免历史 onclick 报错。
window.loadList = listFeatureDisabled;
window.prevListPage = listFeatureDisabled;
window.nextListPage = listFeatureDisabled;
window.changeListPageSize = listFeatureDisabled;
window.saveAssetName = listFeatureDisabled;

dom.backParentBtn?.addEventListener("click", () => {
  const current = (dom.epDir.value || "").trim();
  const parentDir = getParentDir(current);
  if (!parentDir) {
    setResult("当前目录无法继续回退", false);
    return;
  }
  dom.epDir.value = parentDir;
  queryController.submitQuery();
});

// 降级策略：3D 初始化失败时保留查询/列表能力，不影响主流程排查。
try {
  initThreeScene();
  bindBoardControls();
  window.addEventListener("resize", resizeThree);
} catch (err) {
  setResult(`3D 初始化失败，已降级为仅查询模式: ${formatError(err)}`, false);
  console.error("index 3D init failed:", err);
}

// 浏览器回退/前进后，尽量恢复首页查询上下文（ep_dir/分页/游标/选中项）。
queryController.restoreFromSession().catch((err) => {
  console.warn("restore preview state failed:", err);
});
