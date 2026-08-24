/**
 * 主 3D 场景中的相机位置四棱锥标记。
 * - 数据源：playback.videoProjection（slam_data.traj 解出的 positions/quaternions）。
 * - 只负责显示，不参与任何投影计算。
 */
export function createCameraMarker({ THREE, scene }) {
  const displayQuat = new THREE.Quaternion().setFromEuler(new THREE.Euler(Math.PI / 2, Math.PI, 0, "XYZ"));
  const tempPos = new THREE.Vector3();
  const tempQuat = new THREE.Quaternion();
  const MARKER_RADIUS = 0.035;
  const MARKER_LENGTH = 0.12;

  // 四棱锥默认沿 +Y；转到 +Z 后再后移半个长度，使“尖端”落在相机位置。
  // 这样尖端直接表示相机点，锥体向相机后方展开。
  const markerGeo = new THREE.ConeGeometry(MARKER_RADIUS, MARKER_LENGTH, 4, 1, false);
  markerGeo.rotateX(Math.PI / 2);
  markerGeo.translate(0, 0, -MARKER_LENGTH * 0.5);

  const markerMat = new THREE.MeshStandardMaterial({
    color: 0x22c55e,
    emissive: 0x0f3d1f,
    roughness: 0.35,
    metalness: 0.05,
    side: THREE.DoubleSide,
  });

  const marker = new THREE.Mesh(markerGeo, markerMat);
  marker.visible = false;
  scene.add(marker);

  function reset() {
    marker.visible = false;
  }

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

  function update(videoProjection, absFrame) {
    if (!videoProjection || absFrame < 0) {
      marker.visible = false;
      return;
    }

    const validMask = videoProjection.valid_mask;
    if (validMask instanceof Uint8Array && validMask[absFrame] <= 0) {
      marker.visible = false;
      return;
    }

    const p = readPosition(videoProjection.positions, absFrame);
    if (!p) {
      marker.visible = false;
      return;
    }

    const scale = Number(videoProjection.scale || 1);
    tempPos.set(p[0] * scale, p[1] * scale, p[2] * scale).applyQuaternion(displayQuat);
    marker.position.copy(tempPos);

    const q = readQuaternion(videoProjection.quaternions, absFrame);
    if (q) {
      tempQuat.set(q[0], q[1], q[2], q[3]).normalize();
      marker.quaternion.copy(displayQuat).multiply(tempQuat);
    } else {
      marker.quaternion.copy(displayQuat);
    }
    marker.visible = true;
  }

  function dispose() {
    scene.remove(marker);
    marker.geometry.dispose();
    marker.material.dispose();
  }

  return {
    update,
    reset,
    dispose,
  };
}
