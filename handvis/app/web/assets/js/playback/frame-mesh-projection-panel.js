import { getCameraFrameTransform, transformWorldToCameraRaw } from "./camera-projection-utils.js";
import {
  computeProjectionBounds,
  createOrthographicProjector,
} from "./orthographic-projector.js";
import { drawProjectionAxes } from "./projection-axis-overlay.js";
import { CAMERA_ORTHOGRAPHIC_VIEWS } from "./projection-view-config.js";
import { HAND_VISUAL_COLORS } from "./hand-visual-style.js";

// 当前帧完整手部投影用于校验 mesh 朝向，颜色与主 3D mesh 保持一致，并使用不透明填充。
const HAND_STYLE = {
  left: { stroke: "rgb(185, 28, 28)", fill: `#${HAND_VISUAL_COLORS.left.hex.toString(16).padStart(6, "0")}` },
  right: { stroke: "rgb(29, 78, 216)", fill: `#${HAND_VISUAL_COLORS.right.hex.toString(16).padStart(6, "0")}` },
};
const FRAME_MESH_EXTRA_PADDING_RATIO = 0.24;

function getInvalidState(playback, absFrame) {
  const invalidLeftMask = playback.invalidLeftMask;
  const invalidRightMask = playback.invalidRightMask;
  return {
    left: !Array.isArray(invalidLeftMask) || Number(invalidLeftMask[absFrame]) > 0,
    right: !Array.isArray(invalidRightMask) || Number(invalidRightMask[absFrame]) > 0,
  };
}

function resizeCanvasToDisplay(canvas) {
  const rect = canvas.getBoundingClientRect();
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const cssW = Math.max(1, Math.round(rect.width));
  const cssH = Math.max(1, Math.round(rect.height));
  const nextW = Math.max(1, Math.round(cssW * dpr));
  const nextH = Math.max(1, Math.round(cssH * dpr));
  if (canvas.width !== nextW || canvas.height !== nextH) {
    canvas.width = nextW;
    canvas.height = nextH;
  }
  return { cssW, cssH, dpr };
}

function drawGrid(ctx, cssW, cssH) {
  ctx.fillStyle = "rgba(7, 10, 15, 0.96)";
  ctx.fillRect(0, 0, cssW, cssH);
  ctx.strokeStyle = "rgba(170, 201, 255, 0.08)";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const x = (cssW * i) / 4;
    const y = (cssH * i) / 4;
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, cssH);
    ctx.moveTo(0, y);
    ctx.lineTo(cssW, y);
    ctx.stroke();
  }
}

function expandFrameMeshBounds(bounds) {
  if (!bounds) return null;
  const spanA = bounds.maxA - bounds.minA;
  const spanB = bounds.maxB - bounds.minB;
  return {
    minA: bounds.minA - spanA * FRAME_MESH_EXTRA_PADDING_RATIO,
    maxA: bounds.maxA + spanA * FRAME_MESH_EXTRA_PADDING_RATIO,
    minB: bounds.minB - spanB * FRAME_MESH_EXTRA_PADDING_RATIO,
    maxB: bounds.maxB + spanB * FRAME_MESH_EXTRA_PADDING_RATIO,
  };
}

/**
 * 从 chunk 顶点缓冲中取出某一帧，并转换到相机坐标系。
 *
 * src 布局为 [frame][vertex][xyz]，localFrame 是当前 chunk 内的帧号。
 * 输出使用数组点 [x,y,z]，后续由共享投影器处理轴选择和方向。
 */
function buildCameraFramePoints({ src, localFrame, vertexCount, transform }) {
  if (!src || !transform || localFrame < 0 || vertexCount <= 0) return [];
  const frameOffset = localFrame * vertexCount * 3;
  if (frameOffset + vertexCount * 3 > src.length) return [];
  const points = new Array(vertexCount);
  for (let i = 0; i < vertexCount; i += 1) {
    const idx = frameOffset + i * 3;
    const wx = src[idx];
    const wy = src[idx + 1];
    const wz = src[idx + 2];
    if (!Number.isFinite(wx) || !Number.isFinite(wy) || !Number.isFinite(wz)) continue;
    const cam = transformWorldToCameraRaw(wx, wy, wz, transform);
    points[i] = [cam.x, cam.y, cam.z];
  }
  return points;
}

// 按 MANO faces 逐三角面绘制当前帧 mesh 的投影轮廓。
function drawHandMesh(ctx, points, faces, project, style) {
  if (points.length === 0 || faces.length === 0) return;
  ctx.strokeStyle = style.stroke;
  ctx.fillStyle = style.fill;
  ctx.lineWidth = 0.75;
  for (let i = 0; i + 2 < faces.length; i += 3) {
    const p0 = project(points[faces[i]]);
    const p1 = project(points[faces[i + 1]]);
    const p2 = project(points[faces[i + 2]]);
    if (!p0 || !p1 || !p2) continue;
    ctx.beginPath();
    ctx.moveTo(p0.x, p0.y);
    ctx.lineTo(p1.x, p1.y);
    ctx.lineTo(p2.x, p2.y);
    ctx.closePath();
    ctx.fill();
    ctx.stroke();
  }
}

export function createFrameMeshProjectionPanel({ dom, state, chunkCache }) {
  const canvases = {
    xy: dom.xyFrameMeshCanvas,
    xz: dom.xzFrameMeshCanvas,
    yz: dom.yzFrameMeshCanvas,
  };
  let vertexCount = 0;
  let faces = null;
  let lastDrawnAbsFrame = -1;

  function clearCanvases() {
    for (const canvas of Object.values(canvases)) {
      if (!canvas) continue;
      const ctx = canvas.getContext("2d");
      const { cssW, cssH, dpr } = resizeCanvasToDisplay(canvas);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      drawGrid(ctx, cssW, cssH);
    }
  }

  function init(nextVertexCount, facesU32) {
    vertexCount = Number(nextVertexCount || 0);
    faces = facesU32 || null;
    lastDrawnAbsFrame = -1;
    clearCanvases();
  }

  function reset() {
    vertexCount = 0;
    faces = null;
    lastDrawnAbsFrame = -1;
    clearCanvases();
  }

  function requestDraw(timelineState, { force = false } = {}) {
    if (!faces || vertexCount <= 0) return;
    const absFrame = Number(timelineState?.absFrame ?? state.get("frame") ?? -1);
    if (!force && absFrame >= 0 && absFrame === lastDrawnAbsFrame) return;
    const playback = state.get("playback");
    const videoProjection = playback?.videoProjection;
    const chunkIdx = Number(state.get("currentChunkIdx") ?? -1);
    const chunk = chunkIdx >= 0 ? chunkCache.get(chunkIdx) : null;
    if (!playback || !videoProjection || !chunk || absFrame < 0) return;

    const transform = getCameraFrameTransform(videoProjection, absFrame);
    const localFrame = absFrame - Number(chunk.meta?.frame_start || 0);
    const invalid = getInvalidState(playback, absFrame);
    const leftPoints = invalid.left
      ? []
      : buildCameraFramePoints({ src: chunk.left, localFrame, vertexCount, transform });
    const rightPoints = invalid.right
      ? []
      : buildCameraFramePoints({ src: chunk.right, localFrame, vertexCount, transform });
    const allPoints = [...leftPoints, ...rightPoints];
    for (const view of CAMERA_ORTHOGRAPHIC_VIEWS) {
      const canvas = canvases[view.key];
      if (!canvas) continue;
      const ctx = canvas.getContext("2d");
      const { cssW, cssH, dpr } = resizeCanvasToDisplay(canvas);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      drawGrid(ctx, cssW, cssH);
      // 左右手共同计算 bounds，确保同一平面内两只手不会被分别缩放。
      const bounds = expandFrameMeshBounds(computeProjectionBounds(allPoints, view, 0.04, false));
      if (!bounds) continue;
      const project = createOrthographicProjector(view, bounds, cssW, cssH);
      drawProjectionAxes(ctx, view, bounds, cssW, cssH);
      drawHandMesh(ctx, leftPoints, faces, project, HAND_STYLE.left);
      drawHandMesh(ctx, rightPoints, faces, project, HAND_STYLE.right);
    }
    lastDrawnAbsFrame = absFrame;
  }

  return {
    init,
    reset,
    requestDraw,
  };
}
