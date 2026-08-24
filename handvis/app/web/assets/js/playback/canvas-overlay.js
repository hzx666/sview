import * as THREE from "three";
import { getCameraFrameTransform, getCameraFrameTransformInterpolated, transformWorldToCameraRaw } from "./camera-projection-utils.js";
import { createVideoRootTrail } from "./video-root-trail.js";

/**
 * 播放页视频投影渲染层。
 *
 * 输入：
 * 1) chunk 的 world 顶点帧（左右手 Float32Array）
 * 2) playback.videoProjection: 相机轨迹 + OpenCV pinhole 内参 + 基准分辨率
 * 3) 当前 video 的 CSS 显示尺寸
 * 输出：
 * - 叠加到视频上的透明 WebGL 手模，以及左右腕部 0 号根节点最近 30 帧的等强度轨迹。
 *
 * 投影链路（前端实时计算）：
 * - 四元数归一化转旋转矩阵 R
 * - 世界顶点先减去相机位置 t（×scale），再以行向量右乘 R 进入相机系
 * - 透视除法配合焦距 + 主点偏移落到视频基准像素平面
 * - 最后按当前 video CSS 尺寸做线性缩放，自适应任意展示分辨率
 */

const HIDE_X = -99999;
const HIDE_Y = -99999;
const HIDE_Z = -99999;

/**
 * 计算 video 元素内真实视频内容区域。
 *
 * 当 video 受到 max-height 或 object-fit 影响时，元素外框和真实画面区域可能不一致；
 * 投影必须落在真实画面区域上，才能和离线原始帧叠图保持同一坐标基准。
 */
function getVideoContentRect(video) {
  const rect = video.getBoundingClientRect();
  const intrinsicW = Number(video.videoWidth || 0);
  const intrinsicH = Number(video.videoHeight || 0);
  if (!(intrinsicW > 0) || !(intrinsicH > 0) || !(rect.width > 0) || !(rect.height > 0)) {
    return rect;
  }

  const scale = Math.min(rect.width / intrinsicW, rect.height / intrinsicH);
  const width = intrinsicW * scale;
  const height = intrinsicH * scale;
  return {
    left: rect.left + (rect.width - width) * 0.5,
    top: rect.top + (rect.height - height) * 0.5,
    width,
    height,
  };
}

/**
 * 将 overlay canvas 与 video 真实内容区域对齐，保证像素坐标投影一致。
 */
function resizeRendererToVideo(renderer, camera, canvas, video) {
  const rect = getVideoContentRect(video);
  const stageRect = (canvas.parentElement || video).getBoundingClientRect();
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const cssW = Math.max(1, Math.round(rect.width));
  const cssH = Math.max(1, Math.round(rect.height));
  const nextW = Math.max(1, Math.round(cssW * dpr));
  const nextH = Math.max(1, Math.round(cssH * dpr));

  canvas.style.left = `${Math.round(rect.left - stageRect.left)}px`;
  canvas.style.top = `${Math.round(rect.top - stageRect.top)}px`;
  canvas.style.right = "auto";
  canvas.style.bottom = "auto";
  canvas.style.width = `${cssW}px`;
  canvas.style.height = `${cssH}px`;

  if (canvas.width !== nextW || canvas.height !== nextH) {
    renderer.setPixelRatio(dpr);
    renderer.setSize(cssW, cssH, false);
  }
  camera.left = 0;
  camera.right = cssW;
  camera.top = 0;
  camera.bottom = cssH;
  camera.near = 0.001;
  camera.far = 10000;
  camera.updateProjectionMatrix();
  return {
    cssW,
    cssH,
    layoutKey: `${cssW}x${cssH}@${Math.round(rect.left - stageRect.left)},${Math.round(rect.top - stageRect.top)}`,
  };
}

/**
 * 把单帧 world 顶点投影到当前 CSS 像素坐标，并写入 overlay mesh 的 position buffer。
 *
 * 投影分两步：
 *  1) world → 视频基准像素：四元数→旋转矩阵 + 行向量乘法 + 透视除法 + 内参缩放
 *  2) 基准像素 → 当前 CSS 像素：按 cssW/baseWidth 与 cssH/baseHeight 等比缩放
 */
function projectFrameToPixels({
  src,
  dst,
  localFrame,
  vertexCount,
  transform,
  cssW,
  cssH,
  videoProjection,
}) {
  if (!src || !dst || localFrame < 0 || vertexCount <= 0) return false;
  if (!transform || !videoProjection) return false;

  const frameOffset = localFrame * vertexCount * 3;
  if (frameOffset + vertexCount * 3 > src.length) return false;

  const focal = Number(videoProjection.img_focal || 0);
  const center = videoProjection.img_center || [];
  const baseW = Number(videoProjection.base_width || 0);
  const baseH = Number(videoProjection.base_height || 0);
  const cx = Number(center[0] || 0);
  const cy = Number(center[1] || 0);
  if (!(focal > 0) || !(baseW > 0) || !(baseH > 0)) return false;

  const sx = cssW / baseW;
  const sy = cssH / baseH;

  let finiteCount = 0;
  for (let i = 0; i < vertexCount; i += 1) {
    const srcIdx = frameOffset + i * 3;
    const wx = src[srcIdx];
    const wy = src[srcIdx + 1];
    const wz = src[srcIdx + 2];
    const dstIdx = i * 3;

    if (!Number.isFinite(wx) || !Number.isFinite(wy) || !Number.isFinite(wz)) {
      dst[dstIdx] = HIDE_X;
      dst[dstIdx + 1] = HIDE_Y;
      dst[dstIdx + 2] = HIDE_Z;
      continue;
    }

    const cam = transformWorldToCameraRaw(wx, wy, wz, transform);
    const camX = cam.x;
    const camY = cam.y;
    const camZ = cam.z;

    if (!(camZ > 1e-5)) {
      dst[dstIdx] = HIDE_X;
      dst[dstIdx + 1] = HIDE_Y;
      dst[dstIdx + 2] = HIDE_Z;
      continue;
    }

    const baseX = focal * (camX / camZ) + cx;
    const baseY = focal * (camY / camZ) + cy;

    dst[dstIdx] = baseX * sx;
    dst[dstIdx + 1] = baseY * sy;
    dst[dstIdx + 2] = -camZ;
    finiteCount += 1;
  }
  return finiteCount > 0;
}

function blendFrameVertices(dst, fromFrame, toFrame, alpha) {
  const n = Math.min(dst.length, fromFrame.length, toFrame.length);
  const t = Math.max(0, Math.min(1, Number(alpha) || 0));
  const invT = 1 - t;
  for (let i = 0; i < n; i += 1) {
    dst[i] = fromFrame[i] * invT + toFrame[i] * t;
  }
  return dst;
}

/**
 * 创建投影 mesh（屏幕像素空间）。
 */
function createProjectedMesh(THREE, vertexCount, facesU32, color) {
  const geometry = new THREE.BufferGeometry();
  geometry.setAttribute("position", new THREE.BufferAttribute(new Float32Array(vertexCount * 3), 3));
  geometry.setIndex(new THREE.BufferAttribute(facesU32, 1));
  const material = new THREE.MeshBasicMaterial({
    color,
    transparent: true,
    opacity: 0.5,
    side: THREE.DoubleSide,
    depthTest: true,
    depthWrite: true,
  });
  return new THREE.Mesh(geometry, material);
}

export function createCanvasOverlay({ dom, state, chunkCache, ensureChunkLoaded }) {
  const canvas = dom.canvasOverlay;
  const renderer = canvas
    ? new THREE.WebGLRenderer({ canvas, alpha: true, antialias: true, powerPreference: "high-performance" })
    : null;
  const scene = new THREE.Scene();
  const camera = new THREE.OrthographicCamera(0, 1, 0, 1, 0.001, 10000);
  let enabled = false;
  let leftMesh = null;
  let rightMesh = null;
  let vertexCount = 0;
  // 帧级跳过：只在时间轴采样变化时才执行投影写入与渲染，避免每个 RAF 空转浪费。
  let lastDrawnFrameFloat = -1;
  let lastDrawnLayoutKey = "";
  const rootTrail = createVideoRootTrail({
    THREE,
    scene,
    state,
    chunkCache,
    ensureChunkLoaded,
    requestRedraw: () => {
      lastDrawnFrameFloat = -1;
    },
  });

  if (renderer) {
    renderer.setClearColor(0x000000, 0);
    renderer.outputColorSpace = THREE.SRGBColorSpace;
  }

  function disposeMeshes() {
    for (const mesh of [leftMesh, rightMesh]) {
      if (!mesh) continue;
      scene.remove(mesh);
      mesh.geometry.dispose();
      mesh.material.dispose();
    }
    leftMesh = null;
    rightMesh = null;
    vertexCount = 0;
    rootTrail.dispose();
  }

  /**
   * 根据当前 index 初始化 overlay mesh。
   * 注意：faces 与 vertex_count 必须和 chunk 顶点帧严格一致。
   */
  function init(nextVertexCount, facesU32) {
    disposeMeshes();
    lastDrawnFrameFloat = -1;
    lastDrawnLayoutKey = "";
    if (!renderer || !facesU32 || !nextVertexCount) return;
    vertexCount = Number(nextVertexCount || 0);
    leftMesh = createProjectedMesh(THREE, vertexCount, facesU32, 0xef4444);
    rightMesh = createProjectedMesh(THREE, vertexCount, facesU32, 0x3b82f6);
    scene.add(leftMesh);
    scene.add(rightMesh);
    rootTrail.init();
  }

  function setEnabled(nextEnabled) {
    enabled = Boolean(nextEnabled);
    lastDrawnFrameFloat = -1;
    lastDrawnLayoutKey = "";
    if (dom.videoStage) dom.videoStage.classList.toggle("canvas-enabled", enabled);
    if (canvas) canvas.hidden = !enabled;
    if (renderer && !enabled) renderer.clear();
  }

  /**
   * 将指定手的当前帧 world 顶点（可含子帧插值）投影后写入 GPU buffer；invalid 帧直接隐藏。
   */
  function updateMeshFrame(mesh, frameData, localFrame, nextLocalFrame, alpha, videoProjection, cssW, cssH, invalid, absFrame, nextAbsFrame) {
    if (!mesh || invalid) {
      if (mesh) mesh.visible = false;
      return;
    }
    const pos = mesh.geometry.attributes.position.array;
    const frameStride = vertexCount * 3;
    const currentOffset = localFrame * frameStride;
    const nextOffset = nextLocalFrame * frameStride;
    const currentFrame = frameData.subarray(currentOffset, currentOffset + frameStride);
    const nextFrame = frameData.subarray(nextOffset, nextOffset + frameStride);
    const blended = alpha > 1e-6 && nextLocalFrame !== localFrame
      ? blendFrameVertices(new Float32Array(frameStride), currentFrame, nextFrame, alpha)
      : currentFrame;

    const t = Math.max(0, Math.min(1, Number(alpha) || 0));
    const transform = t > 1e-6 && nextAbsFrame !== absFrame
      ? getCameraFrameTransformInterpolated(videoProjection, absFrame, nextAbsFrame, t)
      : getCameraFrameTransform(videoProjection, absFrame);
    const ok = projectFrameToPixels({
      src: blended,
      dst: pos,
      localFrame: 0,
      vertexCount,
      transform,
      cssW,
      cssH,
      videoProjection,
    });
    mesh.visible = ok;
    if (ok) {
      mesh.geometry.attributes.position.needsUpdate = true;
      mesh.geometry.computeBoundingSphere();
    }
  }

  /**
   * 每一帧同步入口：
   * 1) 读取当前 absFrame 对应 chunk 的 world 顶点
   * 2) 通过 videoProjection 实时算出视频像素坐标
   * 3) 渲染到透明 canvas
   */
  function requestDraw(timelineState) {
    if (!enabled || !renderer || !canvas || !dom.sourceVideo) return;
    const video = dom.sourceVideo;
    if (video.readyState < 2 || vertexCount <= 0) return;

    const absFrame = Number(timelineState?.absFrame ?? state.get("frame") ?? -1);
    const nextAbsFrame = Number.isFinite(timelineState?.nextAbsFrame)
      ? Number(timelineState.nextAbsFrame)
      : absFrame;
    const alpha = Number(timelineState?.alpha) || 0;
    const frameFloat = Number.isFinite(timelineState?.frameFloat)
      ? Number(timelineState.frameFloat)
      : absFrame;
    const { cssW, cssH, layoutKey } = resizeRendererToVideo(renderer, camera, canvas, video);
    if (frameFloat >= 0 && Math.abs(frameFloat - lastDrawnFrameFloat) < 1e-5 && layoutKey === lastDrawnLayoutKey) return;

    const playback = state.get("playback");
    const chunkIdx = Number(state.get("currentChunkIdx") ?? -1);
    const chunk = chunkIdx >= 0 ? chunkCache.get(chunkIdx) : null;
    if (!playback || !chunk || absFrame < 0) {
      rootTrail.clear();
      renderer.clear();
      return;
    }

    const localFrame = absFrame - Number(chunk.meta?.frame_start || 0);
    const nextLocalFrame = nextAbsFrame - Number(chunk.meta?.frame_start || 0);
    const invalidLeftMask = playback.invalidLeftMask || playback.invalidMask;
    const invalidRightMask = playback.invalidRightMask || playback.invalidMask;
    const leftInvalid = Array.isArray(invalidLeftMask) && Number(invalidLeftMask[absFrame]) > 0;
    const rightInvalid = Array.isArray(invalidRightMask) && Number(invalidRightMask[absFrame]) > 0;
    const videoProjection = playback.videoProjection;

    updateMeshFrame(
      leftMesh, chunk.left, localFrame, nextLocalFrame, alpha,
      videoProjection, cssW, cssH, leftInvalid, absFrame, nextAbsFrame,
    );
    updateMeshFrame(
      rightMesh, chunk.right, localFrame, nextLocalFrame, alpha,
      videoProjection, cssW, cssH, rightInvalid, absFrame, nextAbsFrame,
    );
    rootTrail.update(absFrame, cssW, cssH);
    renderer.clear();
    renderer.render(scene, camera);
    lastDrawnFrameFloat = frameFloat;
    lastDrawnLayoutKey = layoutKey;
  }

  return {
    init,
    setEnabled,
    requestDraw,
    reset() {
      disposeMeshes();
      lastDrawnFrameFloat = -1;
      lastDrawnLayoutKey = "";
      setEnabled(Boolean(dom.canvasRenderToggle?.checked));
    },
  };
}
