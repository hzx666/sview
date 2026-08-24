// 三个相机坐标系正交投影视图的统一定义。
// axes 表示屏幕横轴/纵轴分别取相机坐标的哪个分量：0=X、1=Y、2=Z。
// signs 表示对应屏幕轴是否取反；yDown 表示纵轴是否按 canvas 向下为正。
export const CAMERA_ORTHOGRAPHIC_VIEWS = [
  { key: "xz", axes: [0, 2], signs: [1, 1], yDown: false },
  { key: "xy", axes: [0, 1], signs: [1, 1], yDown: true },
  // YZ 已按当前视觉确认过方向：基础 YZ 视图旋转后显示，轨迹和完整 mesh 共用此配置。
  { key: "yz", axes: [2, 1], signs: [1, -1], yDown: false },
];
