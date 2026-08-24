// 轨迹小窗 3D 世界的纯工具函数，保持主控制器只负责场景生命周期。

export function disposeObject(node) {
  if (!node) return;
  if (node.geometry?.dispose) node.geometry.dispose();
  if (Array.isArray(node.material)) {
    node.material.forEach((m) => m?.dispose?.());
  } else if (node.material?.dispose) {
    node.material.dispose();
  }
}

export function clearGroup(group) {
  while (group.children.length > 0) {
    const child = group.children[0];
    group.remove(child);
    child.traverse?.((node) => disposeObject(node));
  }
}

export function finiteVector(THREE, x, y, z) {
  if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) return null;
  return new THREE.Vector3(x, y, z);
}

export function readAnchorPoint(THREE, frameData, frameOffset, vertexCount) {
  const anchorIndex = 0;
  if (!frameData || vertexCount <= anchorIndex) return null;
  const base = frameOffset + anchorIndex * 3;
  return finiteVector(THREE, frameData[base], frameData[base + 1], frameData[base + 2]);
}

export function isInvalidFrame(mask, absFrame) {
  return Array.isArray(mask) && absFrame >= 0 && absFrame < mask.length
    ? Number(mask[absFrame]) > 0
    : false;
}

export function pushSegmentPoint(segments, current, point, absFrame, prevFrame) {
  let nextCurrent = current;
  if (!nextCurrent || absFrame !== prevFrame + 1) {
    nextCurrent = [];
    segments.push(nextCurrent);
  }
  nextCurrent.push(point);
  return { current: nextCurrent, prevFrame: absFrame };
}

export function buildLine(THREE, points, color, opacity) {
  if (!Array.isArray(points) || points.length < 2) return null;
  const geometry = new THREE.BufferGeometry().setFromPoints(points);
  const material = new THREE.LineBasicMaterial({
    color,
    transparent: true,
    opacity,
    depthTest: true,
    depthWrite: false,
  });
  return new THREE.Line(geometry, material);
}

export function addSegmentedLines(THREE, group, pointsMap, color, opacity) {
  const segments = [];
  let current = null;
  let prevFrame = -Infinity;
  for (const frame of Array.from(pointsMap.keys()).sort((a, b) => a - b)) {
    const next = pushSegmentPoint(segments, current, pointsMap.get(frame), frame, prevFrame);
    current = next.current;
    prevFrame = next.prevFrame;
  }
  for (const segment of segments) {
    const line = buildLine(THREE, segment, color, opacity);
    if (line) group.add(line);
  }
}

export function createBoardGrid(THREE, boardConfig) {
  const positions = [];
  const xDivisions = 12;
  const yDivisions = 9;
  const halfWidth = boardConfig.width * 0.5;
  const halfDepth = boardConfig.depth * 0.5;

  for (let i = 0; i <= xDivisions; i += 1) {
    const x = -halfWidth + (boardConfig.width * i) / xDivisions;
    positions.push(x, -halfDepth, 0, x, halfDepth, 0);
  }
  for (let i = 0; i <= yDivisions; i += 1) {
    const y = -halfDepth + (boardConfig.depth * i) / yDivisions;
    positions.push(-halfWidth, y, 0, halfWidth, y, 0);
  }

  const geometry = new THREE.BufferGeometry();
  geometry.setAttribute("position", new THREE.BufferAttribute(new Float32Array(positions), 3));
  const material = new THREE.LineBasicMaterial({
    color: 0x9fb2cd,
    transparent: true,
    opacity: 0.45,
    depthWrite: false,
  });
  const grid = new THREE.LineSegments(geometry, material);
  grid.position.set(boardConfig.offsetX, boardConfig.belowPalmDistance, boardConfig.offsetZ);
  grid.rotation.set(
    (boardConfig.rotXDeg * Math.PI) / 180,
    (boardConfig.rotYDeg * Math.PI) / 180,
    (boardConfig.rotZDeg * Math.PI) / 180,
  );
  return grid;
}
