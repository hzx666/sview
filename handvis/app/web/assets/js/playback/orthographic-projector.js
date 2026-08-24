/**
 * 相机坐标系正交投影的共享工具。
 *
 * 轨迹点使用 {x,y,z}，完整 mesh 顶点使用 [x,y,z]，这里统一处理两种结构，
 * 确保两个小窗在轴选择、方向翻转和等比例缩放上完全一致。
 */
function getAxisValue(point, axis) {
  if (Array.isArray(point)) return point[axis];
  if (axis === 0) return point.x;
  if (axis === 1) return point.y;
  return point.z;
}

export function getProjectedPlanePoint(point, view) {
  if (!point) return null;
  const signA = Number(view.signs?.[0] ?? 1);
  const signB = Number(view.signs?.[1] ?? 1);
  const a = getAxisValue(point, view.axes[0]) * signA;
  const b = getAxisValue(point, view.axes[1]) * signB;
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  return { a, b };
}

/**
 * 计算某个平面视图下的绘制边界。
 *
 * includeOrigin 用于轨迹窗口：需要把相机原点纳入范围，避免轨迹相对相机的位置被裁掉。
 * 完整 mesh 小窗只看当前手部，因此不强制包含原点。
 */
export function computeProjectionBounds(points, view, minSpan = 0.04, includeOrigin = false) {
  let minA = includeOrigin ? 0 : Infinity;
  let maxA = includeOrigin ? 0 : -Infinity;
  let minB = includeOrigin ? 0 : Infinity;
  let maxB = includeOrigin ? 0 : -Infinity;
  for (const point of points) {
    const projected = getProjectedPlanePoint(point, view);
    if (!projected) continue;
    minA = Math.min(minA, projected.a);
    maxA = Math.max(maxA, projected.a);
    minB = Math.min(minB, projected.b);
    maxB = Math.max(maxB, projected.b);
  }
  if (!Number.isFinite(minA) || !Number.isFinite(minB)) return null;
  const spanA = Math.max(minSpan, maxA - minA);
  const spanB = Math.max(minSpan, maxB - minB);
  return {
    minA: minA - spanA * 0.12,
    maxA: maxA + spanA * 0.12,
    minB: minB - spanB * 0.12,
    maxB: maxB + spanB * 0.12,
  };
}

/**
 * 创建三维点到 canvas 像素的投影函数。
 * 这里不单独拉伸横纵轴，而是取同一个 scale，保证米制比例不变。
 */
export function createOrthographicProjector(view, bounds, cssW, cssH) {
  if (!bounds) return () => null;
  const projectPlane = createPlaneCoordinateProjector(view, bounds, cssW, cssH);
  return (point) => {
    const projected = getProjectedPlanePoint(point, view);
    if (!projected) return null;
    return projectPlane(projected.a, projected.b);
  };
}

/**
 * 创建平面坐标到 canvas 像素的投影函数。
 * 坐标轴绘制已经处在投影平面坐标中，所以单独暴露这个函数避免重复换轴逻辑。
 */
export function createPlaneCoordinateProjector(view, bounds, cssW, cssH) {
  if (!bounds) return () => null;
  const pad = 14;
  const plotW = Math.max(1, cssW - pad * 2);
  const plotH = Math.max(1, cssH - pad * 2);
  const spanA = Math.max(1e-9, bounds.maxA - bounds.minA);
  const spanB = Math.max(1e-9, bounds.maxB - bounds.minB);
  const scale = Math.min(plotW / spanA, plotH / spanB);
  const centerA = (bounds.minA + bounds.maxA) / 2;
  const centerB = (bounds.minB + bounds.maxB) / 2;
  return (a, b) => {
    if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
    const dy = (b - centerB) * scale;
    return {
      x: cssW / 2 + (a - centerA) * scale,
      y: view.yDown ? cssH / 2 + dy : cssH / 2 - dy,
    };
  };
}
