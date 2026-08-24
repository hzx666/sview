import { CAMERA_ORTHOGRAPHIC_VIEWS } from "./projection-view-config.js";
import {
  computeProjectionBounds,
  createOrthographicProjector,
} from "./orthographic-projector.js";
import { drawProjectionAxes } from "./projection-axis-overlay.js";
import { HAND_VISUAL_COLORS } from "./hand-visual-style.js";

// 轨迹小窗只画左右手中心点轨迹，颜色和主 3D 轨迹保持左右手语义一致。
const HAND_STYLE = {
  left: {
    line: HAND_VISUAL_COLORS.left.line,
    point: HAND_VISUAL_COLORS.left.point,
  },
  right: {
    line: HAND_VISUAL_COLORS.right.line,
    point: HAND_VISUAL_COLORS.right.point,
  },
};

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

export function collectValidPoints(track) {
  return track.filter((point) => point && Number.isFinite(point.x) && Number.isFinite(point.y) && Number.isFinite(point.z));
}

// 黑色底 + 轻量网格用于观察轨迹形态，不承载实际坐标换算。
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

function drawTrack(ctx, points, project, style) {
  const projected = points.map(project);
  ctx.strokeStyle = style.line;
  ctx.lineWidth = 1.2;
  ctx.beginPath();
  let drawing = false;
  for (const p of projected) {
    if (!p) {
      drawing = false;
      continue;
    }
    if (!drawing) {
      ctx.moveTo(p.x, p.y);
      drawing = true;
    } else {
      ctx.lineTo(p.x, p.y);
    }
  }
  ctx.stroke();

  ctx.fillStyle = style.point;
  for (const p of projected) {
    if (!p) continue;
    ctx.beginPath();
    ctx.arc(p.x, p.y, 2, 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawCurrentPoint(ctx, point, project) {
  const p = project(point);
  if (!p) return;
  ctx.fillStyle = "rgba(34, 197, 94, 0.96)";
  ctx.strokeStyle = "rgba(255, 255, 255, 0.9)";
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  ctx.arc(p.x, p.y, 4.5, 0, Math.PI * 2);
  ctx.fill();
  ctx.stroke();
}

export function clearProjectionCanvas(canvas) {
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const { cssW, cssH, dpr } = resizeCanvasToDisplay(canvas);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  drawGrid(ctx, cssW, cssH);
}

/**
 * 绘制 XY/XZ/YZ 三个相机坐标系轨迹视图。
 *
 * bounds 使用左右手历史轨迹共同计算，保证同一个小窗内两只手处在同一比例尺下；
 * project 来自共享正交投影器，因此这里不关心 YZ 是否旋转，只消费统一 view 配置。
 */
export function renderTrajectoryProjectionViews({ canvases, leftPoints, rightPoints, currentLeftPoint, currentRightPoint }) {
  for (const view of CAMERA_ORTHOGRAPHIC_VIEWS) {
    const canvas = canvases[view.key];
    if (!canvas) continue;
    const ctx = canvas.getContext("2d");
    const { cssW, cssH, dpr } = resizeCanvasToDisplay(canvas);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const bounds = computeProjectionBounds([...leftPoints, ...rightPoints], view, 0.08, true);
    const project = createOrthographicProjector(view, bounds, cssW, cssH);
    drawGrid(ctx, cssW, cssH);
    drawProjectionAxes(ctx, view, bounds, cssW, cssH);
    drawTrack(ctx, leftPoints, project, HAND_STYLE.left);
    drawTrack(ctx, rightPoints, project, HAND_STYLE.right);
    drawCurrentPoint(ctx, currentLeftPoint, project);
    drawCurrentPoint(ctx, currentRightPoint, project);
  }
}
