import { createPlaneCoordinateProjector } from "./orthographic-projector.js";

const AXIS_NAMES = ["X", "Y", "Z"];

// 轴标签直接读取当前 view 配置，YZ 旋转或翻转后标签会自动同步。
function getAxisLabel(view, index) {
  const axisName = AXIS_NAMES[view.axes[index]] || "?";
  const sign = Number(view.signs?.[index] ?? 1);
  return `${sign >= 0 ? "+" : "-"}${axisName}`;
}

function drawArrowHead(ctx, from, to) {
  const angle = Math.atan2(to.y - from.y, to.x - from.x);
  const size = 6;
  ctx.beginPath();
  ctx.moveTo(to.x, to.y);
  ctx.lineTo(to.x - size * Math.cos(angle - Math.PI / 6), to.y - size * Math.sin(angle - Math.PI / 6));
  ctx.lineTo(to.x - size * Math.cos(angle + Math.PI / 6), to.y - size * Math.sin(angle + Math.PI / 6));
  ctx.closePath();
  ctx.fill();
}

function drawAxisLine(ctx, start, end, label) {
  if (!start || !end) return;
  ctx.beginPath();
  ctx.moveTo(start.x, start.y);
  ctx.lineTo(end.x, end.y);
  ctx.stroke();
  drawArrowHead(ctx, start, end);
  ctx.fillText(label, end.x + 5, end.y - 5);
}

// 在投影平面内绘制两个坐标轴：横轴对应 view.axes[0]，纵轴对应 view.axes[1]。
export function drawProjectionAxes(ctx, view, bounds, cssW, cssH) {
  if (!bounds) return;
  const projectPlane = createPlaneCoordinateProjector(view, bounds, cssW, cssH);
  // 如果 0 不在当前数据范围内，就把轴吸附到可见边界，避免轴完全画到画布外。
  const zeroA = Math.max(bounds.minA, Math.min(bounds.maxA, 0));
  const zeroB = Math.max(bounds.minB, Math.min(bounds.maxB, 0));
  const horizontalStart = projectPlane(bounds.minA, zeroB);
  const horizontalEnd = projectPlane(bounds.maxA, zeroB);
  const verticalStart = projectPlane(zeroA, bounds.minB);
  const verticalEnd = projectPlane(zeroA, bounds.maxB);

  ctx.save();
  ctx.strokeStyle = "rgba(226, 241, 255, 0.45)";
  ctx.fillStyle = "rgba(226, 241, 255, 0.82)";
  ctx.lineWidth = 1.1;
  ctx.font = "11px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
  ctx.textBaseline = "middle";
  drawAxisLine(ctx, horizontalStart, horizontalEnd, getAxisLabel(view, 0));
  drawAxisLine(ctx, verticalStart, verticalEnd, getAxisLabel(view, 1));
  ctx.restore();
}
