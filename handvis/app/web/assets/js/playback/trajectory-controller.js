/**
 * 轨迹线控制器：
 * - 基于 chunk 顶点序列计算每帧几何中心，生成左右手移动曲线；
 * - 提供显隐开关；
 * - 支持在新的播放任务到来时中断旧任务，避免状态串线。
 */
export function createTrajectoryController({
  THREE,
  handRuntime,
  getLoadedChunkEntries,
}) {
  let leftLine = null;
  let rightLine = null;
  let leftVisible = true;
  let rightVisible = true;
  let buildToken = 0;

  function disposeObject3D(node) {
    if (!node) return;
    node.traverse((obj) => {
      if (obj.geometry?.dispose) obj.geometry.dispose();
      if (!obj.material) return;
      if (Array.isArray(obj.material)) obj.material.forEach((m) => m?.dispose?.());
      else obj.material.dispose?.();
    });
  }

  function removeLine(mesh, line) {
    if (!mesh || !line) return;
    mesh.remove(line);
    disposeObject3D(line);
  }

  function clearLines() {
    const { leftMesh, rightMesh } = handRuntime.getMeshes();
    removeLine(leftMesh, leftLine);
    removeLine(rightMesh, rightLine);
    leftLine = null;
    rightLine = null;
  }

  function setSideVisible(side, nextVisible) {
    const safeVisible = Boolean(nextVisible);
    if (side === "left") {
      leftVisible = safeVisible;
      if (leftLine) leftLine.visible = leftVisible;
      return;
    }
    if (side === "right") {
      rightVisible = safeVisible;
      if (rightLine) rightLine.visible = rightVisible;
      return;
    }
  }

  function computeFrameAnchorPoint(frame, frameOffset, vertexCount) {
    // 直接逻辑：轨迹点定义为“固定手部锚点顶点”的世界位置。
    // 当前使用 MANO 顶点 0 作为锚点，不做质心/兜底推断。
    const anchorIndex = 0;
    if (vertexCount <= anchorIndex) return null;
    const base = frameOffset + anchorIndex * 3;
    const x = frame[base];
    const y = frame[base + 1];
    const z = frame[base + 2];
    if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) return null;
    return new THREE.Vector3(x, y, z);
  }

  function isInvalidFrame(mask, absFrame) {
    return Array.isArray(mask) && absFrame >= 0 && absFrame < mask.length
      ? Number(mask[absFrame]) > 0
      : false;
  }

  function smoothPoints(points, alpha = 0.28) {
    if (!Array.isArray(points) || points.length <= 2) return points;
    const out = [points[0].clone()];
    for (let i = 1; i < points.length; i += 1) {
      const prev = out[i - 1];
      const cur = points[i];
      out.push(new THREE.Vector3(
        prev.x + (cur.x - prev.x) * alpha,
        prev.y + (cur.y - prev.y) * alpha,
        prev.z + (cur.z - prev.z) * alpha,
      ));
    }
    return out;
  }

  function createTrajectoryVisual(points, color) {
    if (!Array.isArray(points) || points.length < 2) return null;
    const smooth = smoothPoints(points);
    if (smooth.length < 2) return null;

    const curve = new THREE.CatmullRomCurve3(smooth, false, "centripetal", 0.5);
    const segments = Math.max(48, Math.min(1200, smooth.length * 3));
    const mainGeo = new THREE.TubeGeometry(curve, segments, 0.0024, 10, false);
    const mainMat = new THREE.MeshStandardMaterial({
      color,
      roughness: 0.35,
      metalness: 0.05,
      emissive: color,
      emissiveIntensity: 0.06,
      transparent: true,
      opacity: 0.95,
    });
    const mainMesh = new THREE.Mesh(mainGeo, mainMat);

    const shadowGeo = new THREE.TubeGeometry(curve, segments, 0.0029, 10, false);
    const shadowMat = new THREE.MeshBasicMaterial({
      color: 0x000000,
      transparent: true,
      opacity: 0.15,
      depthTest: false,
    });
    const shadowMesh = new THREE.Mesh(shadowGeo, shadowMat);
    shadowMesh.position.set(0.0, -0.0035, -0.0025);
    shadowMesh.renderOrder = 1;

    const group = new THREE.Group();
    group.add(shadowMesh);
    group.add(mainMesh);
    group.visible = true;
    group.renderOrder = 2;
    return group;
  }

  async function rebuild(playback) {
    buildToken += 1;
    const token = buildToken;
    clearLines();

    const { leftMesh, rightMesh } = handRuntime.getMeshes();
    if (!playback || !leftMesh || !rightMesh || !Array.isArray(playback.chunks) || playback.chunks.length === 0) return;
    const loadedEntries = Array.isArray(getLoadedChunkEntries?.())
      ? getLoadedChunkEntries().slice().sort((a, b) => Number(a[0]) - Number(b[0]))
      : [];
    if (!loadedEntries.length) return;

    const leftPoints = [];
    const rightPoints = [];
    const invalidMask = Array.isArray(playback.invalidMask) ? playback.invalidMask : null;
    const invalidLeftMask = Array.isArray(playback.invalidLeftMask) ? playback.invalidLeftMask : null;
    const invalidRightMask = Array.isArray(playback.invalidRightMask) ? playback.invalidRightMask : null;

    for (const [, chunk] of loadedEntries) {
      if (token !== buildToken) return;
      if (!chunk?.meta || !chunk.left || !chunk.right) continue;

      const frameCount = Number(chunk.meta.frame_count || 0);
      const vertexCount = Number(chunk.meta.vertex_count || 0);
      const frameStride = vertexCount * 3;
      if (frameCount <= 0 || vertexCount <= 0 || frameStride <= 0) continue;

      for (let f = 0; f < frameCount; f += 1) {
        const frameOffset = f * frameStride;
        const absFrame = Number(chunk.meta.frame_start || 0) + f;
        const frameInvalid = isInvalidFrame(invalidMask, absFrame);
        const leftInvalid = frameInvalid || isInvalidFrame(invalidLeftMask, absFrame);
        const rightInvalid = frameInvalid || isInvalidFrame(invalidRightMask, absFrame);
        const leftCenter = leftInvalid ? null : computeFrameAnchorPoint(chunk.left, frameOffset, vertexCount);
        const rightCenter = rightInvalid ? null : computeFrameAnchorPoint(chunk.right, frameOffset, vertexCount);
        if (leftCenter) {
          leftPoints.push(leftCenter);
        }
        if (rightCenter) {
          rightPoints.push(rightCenter);
        }
      }
    }

    if (token !== buildToken) return;
    if (leftPoints.length > 1) {
      leftLine = createTrajectoryVisual(leftPoints, 0xef4444);
      if (leftLine) {
        leftLine.visible = leftVisible;
        leftMesh.add(leftLine);
      }
    }
    if (rightPoints.length > 1) {
      rightLine = createTrajectoryVisual(rightPoints, 0x3b82f6);
      if (rightLine) {
        rightLine.visible = rightVisible;
        rightMesh.add(rightLine);
      }
    }
  }

  return {
    setSideVisible,
    rebuild,
    reset() {
      buildToken += 1;
      clearLines();
    },
  };
}
