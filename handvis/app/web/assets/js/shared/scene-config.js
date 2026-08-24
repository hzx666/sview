// BOARD_CONFIG：白板平面默认位置与姿态（世界坐标系）。
export const BOARD_CONFIG = {
  width: 1.8,
  depth: 1.35,
  belowPalmDistance: 1.5,
  offsetX: 0.06,
  offsetZ: 0.72,
  rotXDeg: -39,
  rotYDeg: 4,
  rotZDeg: 0,
};

// 调试开关：关闭后白板 mesh 完全不参与渲染与定位。
export const SHOW_WHITEBOARD = true;
