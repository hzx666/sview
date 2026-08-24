/**
 * 播放页相机系投影工具。
 *
 * 这里保留 video_projection 使用的坐标约定：world 顶点先减去相机位置，
 * 再按四元数旋转矩阵进入相机坐标系。
 */

/**
 * 四元数转旋转矩阵（接受散开的 4 个分量，避免数组寻址开销）。
 */
export function quatToRotmatRaw(qx, qy, qz, qw) {
  const n = Math.hypot(qx, qy, qz, qw);
  if (!Number.isFinite(n) || n <= 1e-8) return null;
  const x = qx / n;
  const y = qy / n;
  const z = qz / n;
  const w = qw / n;
  return [
    [1 - 2 * (y * y + z * z), 2 * (x * y - z * w), 2 * (x * z + y * w)],
    [2 * (x * y + z * w), 1 - 2 * (x * x + z * z), 2 * (y * z - x * w)],
    [2 * (x * z - y * w), 2 * (y * z + x * w), 1 - 2 * (x * x + y * y)],
  ];
}

function slerpScalar(a, b, t) {
  return a + (b - a) * t;
}

/** 四元数球面插值（qx, qy, qz, qw）。 */
export function slerpQuaternionRaw(ax, ay, az, aw, bx, by, bz, bw, alpha) {
  let t = Math.max(0, Math.min(1, Number(alpha) || 0));
  let cosHalf = ax * bx + ay * by + az * bz + aw * bw;
  if (cosHalf < 0) {
    cosHalf = -cosHalf;
    bx = -bx;
    by = -by;
    bz = -bz;
    bw = -bw;
  }
  if (cosHalf >= 0.9995) {
    return [
      slerpScalar(ax, bx, t),
      slerpScalar(ay, by, t),
      slerpScalar(az, bz, t),
      slerpScalar(aw, bw, t),
    ];
  }
  const halfTheta = Math.acos(Math.max(-1, Math.min(1, cosHalf)));
  const sinHalf = Math.sin(halfTheta);
  if (!(sinHalf > 1e-8)) {
    return [ax, ay, az, aw];
  }
  const wA = Math.sin((1 - t) * halfTheta) / sinHalf;
  const wB = Math.sin(t * halfTheta) / sinHalf;
  return [
    ax * wA + bx * wB,
    ay * wA + by * wB,
    az * wA + bz * wB,
    aw * wA + bw * wB,
  ];
}

function readCameraFramePose(videoProjection, absFrame) {
  if (!videoProjection || absFrame < 0) return null;
  const positions = videoProjection.positions;
  const quaternions = videoProjection.quaternions;
  const validMask = videoProjection.valid_mask;
  if (!positions || !quaternions) return null;

  const trajValid = validMask ? Number(validMask[absFrame]) > 0 : true;
  if (!trajValid) return null;

  const pIdx = absFrame * 3;
  const qIdx = absFrame * 4;
  if (pIdx + 2 >= positions.length || qIdx + 3 >= quaternions.length) return null;

  const px = positions[pIdx];
  const py = positions[pIdx + 1];
  const pz = positions[pIdx + 2];
  const qx = quaternions[qIdx];
  const qy = quaternions[qIdx + 1];
  const qz = quaternions[qIdx + 2];
  const qw = quaternions[qIdx + 3];
  if (!Number.isFinite(px) || !Number.isFinite(py) || !Number.isFinite(pz)) return null;
  if (!Number.isFinite(qx) || !Number.isFinite(qy) || !Number.isFinite(qz) || !Number.isFinite(qw)) return null;
  return { px, py, pz, qx, qy, qz, qw };
}

function buildCameraFrameTransform(pose, videoProjection) {
  const rot = quatToRotmatRaw(pose.qx, pose.qy, pose.qz, pose.qw);
  if (!rot) return null;
  const scale = Number(videoProjection.scale || 1);
  return {
    rot,
    tx: pose.px * scale,
    ty: pose.py * scale,
    tz: pose.pz * scale,
  };
}

export function getCameraFrameTransform(videoProjection, absFrame) {
  const pose = readCameraFramePose(videoProjection, absFrame);
  if (!pose) return null;
  return buildCameraFrameTransform(pose, videoProjection);
}

export function getCameraFrameTransformInterpolated(videoProjection, absFrame, nextAbsFrame, alpha) {
  const from = readCameraFramePose(videoProjection, absFrame);
  const to = readCameraFramePose(videoProjection, nextAbsFrame);
  if (!from) return to ? buildCameraFrameTransform(to, videoProjection) : null;
  if (!to) return buildCameraFrameTransform(from, videoProjection);
  const t = Math.max(0, Math.min(1, Number(alpha) || 0));
  if (t <= 0) return buildCameraFrameTransform(from, videoProjection);
  if (t >= 1) return buildCameraFrameTransform(to, videoProjection);

  const [qx, qy, qz, qw] = slerpQuaternionRaw(
    from.qx, from.qy, from.qz, from.qw,
    to.qx, to.qy, to.qz, to.qw,
    t,
  );
  const rot = quatToRotmatRaw(qx, qy, qz, qw);
  if (!rot) return buildCameraFrameTransform(from, videoProjection);
  const scale = Number(videoProjection.scale || 1);
  return {
    rot,
    tx: slerpScalar(from.px, to.px, t) * scale,
    ty: slerpScalar(from.py, to.py, t) * scale,
    tz: slerpScalar(from.pz, to.pz, t) * scale,
  };
}

/**
 * 将 world 顶点变换到相机坐标系。
 */
export function transformWorldToCameraRaw(wx, wy, wz, transform) {
  const dx = wx - transform.tx;
  const dy = wy - transform.ty;
  const dz = wz - transform.tz;
  const rot = transform.rot;
  return {
    x: dx * rot[0][0] + dy * rot[1][0] + dz * rot[2][0],
    y: dx * rot[0][1] + dy * rot[1][1] + dz * rot[2][1],
    z: dx * rot[0][2] + dy * rot[1][2] + dz * rot[2][2],
  };
}
