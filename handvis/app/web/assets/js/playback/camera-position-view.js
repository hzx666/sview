/**
 * SLAM 相机位置应用器。
 *
 * 用于第一帧初始化，也可用于逐帧跟随位置；
 * 不应用 SLAM 四元数旋转；只用四元数计算图像中心射线。
 * 主相机位于“图像中心世界点 -> SLAM 相机点”的方向上，并与图像中心点保持固定距离。
 */
const DEFAULT_FOLLOW_DISTANCE = 2;

export function createCameraPositionView({ THREE, camera, controls, getObjectCenter }) {
  const displayQuat = new THREE.Quaternion().setFromEuler(new THREE.Euler(Math.PI / 2, Math.PI, 0, "XYZ"));
  let followDistance = DEFAULT_FOLLOW_DISTANCE;
  const tempPos = new THREE.Vector3();
  const tempQuat = new THREE.Quaternion();
  const tempDir = new THREE.Vector3();
  const tempForward = new THREE.Vector3();
  const tempUp = new THREE.Vector3();
  const tempTarget = new THREE.Vector3();

  function readPosition(positions, frame) {
    const base = frame * 3;
    if (!positions || base + 2 >= positions.length) return null;
    const x = positions[base];
    const y = positions[base + 1];
    const z = positions[base + 2];
    if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) return null;
    return [x, y, z];
  }

  function readQuaternion(quaternions, frame) {
    const base = frame * 4;
    if (!quaternions || base + 3 >= quaternions.length) return null;
    const x = quaternions[base];
    const y = quaternions[base + 1];
    const z = quaternions[base + 2];
    const w = quaternions[base + 3];
    if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z) || !Number.isFinite(w)) return null;
    return [x, y, z, w];
  }

  function apply(videoProjection, frame = 0) {
    if (!videoProjection || frame < 0) return false;

    const validMask = videoProjection.valid_mask;
    if (validMask instanceof Uint8Array && validMask[frame] <= 0) return false;

    const p = readPosition(videoProjection.positions, frame);
    const q = readQuaternion(videoProjection.quaternions, frame);
    if (!p || !q) return false;

    const scale = Number(videoProjection.scale || 1);
    tempPos.set(p[0] * scale, p[1] * scale, p[2] * scale).applyQuaternion(displayQuat);
    const center = getObjectCenter();
    if (!center) return false;

    tempQuat.set(q[0], q[1], q[2], q[3]).normalize().premultiply(displayQuat);
    tempForward.set(0, 0, 1).applyQuaternion(tempQuat).normalize();
    tempUp.set(0, -1, 0).applyQuaternion(tempQuat).normalize();

    // 手模中心只用于确定图像中心射线深度，不直接作为 target。
    const depth = tempDir.copy(center).sub(tempPos).dot(tempForward);
    if (!(depth > 1e-6)) return false;

    tempTarget.copy(tempPos).addScaledVector(tempForward, depth);
    tempDir.copy(tempPos).sub(tempTarget);
    if (tempDir.lengthSq() <= 1e-10) return false;
    tempPos.copy(tempTarget).addScaledVector(tempDir.normalize(), followDistance);

    camera.position.copy(tempPos);
    camera.up.copy(tempUp);
    controls.target.copy(tempTarget);
    controls.enabled = true;
    controls.update();
    return true;
  }

  return {
    apply,
    getDistance() {
      return followDistance;
    },
    setDistance(nextDistance) {
      const value = Number(nextDistance);
      if (!Number.isFinite(value)) return;
      followDistance = Math.max(0.05, Math.min(100, value));
    },
  };
}
