/**
 * 轨迹小窗 3D 世界：
 * 在独立 canvas 中创建 Three.js 场景，直接用 world 坐标绘制手部和相机轨迹。
 */
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";
import {
  addSegmentedLines,
  buildLine,
  clearGroup,
  createBoardGrid,
  disposeObject,
  finiteVector,
  isInvalidFrame,
  pushSegmentPoint,
  readAnchorPoint,
} from "./trajectory-mini-world-utils.js";

export function createTrajectoryMiniWorld({ THREE, canvas, state, chunkCache, viewConfig, boardConfig }) {
  if (!(canvas instanceof HTMLCanvasElement)) throw new Error("trajectory mini world requires canvas");

  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x0c1118);

  const camera = new THREE.PerspectiveCamera(
    viewConfig.cameraFov,
    1,
    viewConfig.cameraNear,
    viewConfig.cameraFar,
  );
  camera.position.set(...viewConfig.cameraPos);

  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, powerPreference: "high-performance" });
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.setPixelRatio(Math.max(1, Math.min(2, window.devicePixelRatio || 1)));

  const controls = new OrbitControls(camera, canvas);
  controls.enableDamping = true;
  controls.dampingFactor = 0.08;
  controls.screenSpacePanning = true;
  controls.target.set(...viewConfig.cameraTarget);
  controls.update();
  const displayQuat = new THREE.Quaternion().setFromEuler(new THREE.Euler(Math.PI / 2, Math.PI, 0, "XYZ"));

  const root = new THREE.Group();
  const helperGroup = new THREE.Group();
  const leftGroup = new THREE.Group();
  const rightGroup = new THREE.Group();
  const cameraGroup = new THREE.Group();
  const markerGroup = new THREE.Group();
  scene.add(root);
  root.add(helperGroup, leftGroup, rightGroup, cameraGroup, markerGroup);

  scene.add(new THREE.AmbientLight(0xffffff, 0.7));
  const keyLight = new THREE.DirectionalLight(0xffffff, 0.8);
  keyLight.position.set(1, 1.6, 0.8);
  scene.add(keyLight);

  const markerGeometry = new THREE.SphereGeometry(0.62, 14, 10);
  const leftMarker = createMarker(markerGeometry, 0xfbbf24);
  const rightMarker = createMarker(markerGeometry, 0x22c55e);
  const cameraMarker = createMarker(markerGeometry, 0x5ee0a0);
  markerGroup.add(leftMarker, rightMarker, cameraMarker);

  const leftPoints = new Map();
  const rightPoints = new Map();
  const cameraPoints = new Map();
  let panelVisible = true;
  let handSignature = "";
  let cameraDataRef = null;
  let sizeKey = "";
  let fitRadius = 0.2;

  function createMarker(geometry, color) {
    const marker = new THREE.Mesh(geometry, new THREE.MeshBasicMaterial({ color }));
    marker.visible = false;
    return marker;
  }

  function placeMarker(marker, point, visible, scale) {
    marker.visible = Boolean(visible && point);
    if (!marker.visible) return;
    marker.position.copy(point);
    marker.scale.setScalar(scale);
  }

  function toDisplayPoint(point) {
    point.applyQuaternion(displayQuat);
    return point;
  }

  function resizeIfNeeded() {
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(1, Math.floor(rect.width));
    const height = Math.max(1, Math.floor(rect.height));
    const nextKey = `${width}x${height}x${window.devicePixelRatio || 1}`;
    if (nextKey === sizeKey) return;
    sizeKey = nextKey;
    renderer.setPixelRatio(Math.max(1, Math.min(2, window.devicePixelRatio || 1)));
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
  }

  function fitCameraToTrajectories() {
    const box = new THREE.Box3();
    let hasPoint = false;
    for (const points of [leftPoints, rightPoints, cameraPoints]) {
      for (const point of points.values()) {
        box.expandByPoint(point);
        hasPoint = true;
      }
    }

    if (!hasPoint) {
      fitRadius = 0.2;
      clearGroup(helperGroup);
      helperGroup.add(createBoardGrid(THREE, boardConfig));
      return;
    }

    const size = box.getSize(new THREE.Vector3());
    fitRadius = Math.max(0.08, size.length() * 0.55);
    clearGroup(helperGroup);
    helperGroup.add(createBoardGrid(THREE, boardConfig));
    camera.updateProjectionMatrix();
  }

  function computeChunkSignature() {
    return Array.from(chunkCache.keys()).sort((a, b) => a - b).join(",");
  }

  function rebuildHandTrajectories() {
    clearGroup(leftGroup);
    clearGroup(rightGroup);
    leftPoints.clear();
    rightPoints.clear();

    const playback = state.get("playback");
    if (!playback) return;

    const invalidMask = Array.isArray(playback.invalidMask) ? playback.invalidMask : null;
    const invalidLeftMask = Array.isArray(playback.invalidLeftMask) ? playback.invalidLeftMask : null;
    const invalidRightMask = Array.isArray(playback.invalidRightMask) ? playback.invalidRightMask : null;
    const entries = Array.from(chunkCache.entries()).sort((a, b) => a[0] - b[0]);

    for (const [, chunk] of entries) {
      if (!chunk?.meta || !chunk.left || !chunk.right) continue;
      const frameCount = Number(chunk.meta.frame_count || 0);
      const vertexCount = Number(chunk.meta.vertex_count || 0);
      const frameStart = Number(chunk.meta.frame_start || 0);
      const frameStride = vertexCount * 3;
      if (frameCount <= 0 || vertexCount <= 0 || frameStride <= 0) continue;

      for (let f = 0; f < frameCount; f += 1) {
        const absFrame = frameStart + f;
        const offset = f * frameStride;
        const frameInvalid = isInvalidFrame(invalidMask, absFrame);
        if (!frameInvalid && !isInvalidFrame(invalidLeftMask, absFrame)) {
          const p = readAnchorPoint(THREE, chunk.left, offset, vertexCount);
          if (p) leftPoints.set(absFrame, toDisplayPoint(p));
        }
        if (!frameInvalid && !isInvalidFrame(invalidRightMask, absFrame)) {
          const p = readAnchorPoint(THREE, chunk.right, offset, vertexCount);
          if (p) rightPoints.set(absFrame, toDisplayPoint(p));
        }
      }
    }

    addSegmentedLines(THREE, leftGroup, leftPoints, 0xef4444, 0.92);
    addSegmentedLines(THREE, rightGroup, rightPoints, 0x3b82f6, 0.92);
  }

  function rebuildCameraTrajectory(videoProjection) {
    clearGroup(cameraGroup);
    cameraPoints.clear();
    cameraDataRef = videoProjection || null;
    if (!videoProjection) return;

    const positions = videoProjection.positions;
    const validMask = videoProjection.valid_mask;
    const scale = Number(videoProjection.scale || 1);
    const length = Math.floor(positions.length / 3);
    const segments = [];
    let current = null;
    let prevFrame = -Infinity;
    for (let i = 0; i < length; i += 1) {
      if (validMask instanceof Uint8Array && validMask[i] <= 0) continue;
      const base = i * 3;
      const point = finiteVector(
        THREE,
        positions[base] * scale,
        positions[base + 1] * scale,
        positions[base + 2] * scale,
      );
      if (!point) continue;
      const displayPoint = toDisplayPoint(point);
      cameraPoints.set(i, displayPoint);
      const next = pushSegmentPoint(segments, current, displayPoint, i, prevFrame);
      current = next.current;
      prevFrame = next.prevFrame;
    }
    for (const segment of segments) {
      const line = buildLine(THREE, segment, 0x5ee0a0, 0.95);
      if (line) cameraGroup.add(line);
    }
  }

  function updateMarkers(absFrame) {
    const frame = Number(absFrame);
    const scale = Math.max(0.006, fitRadius * 0.035);
    placeMarker(leftMarker, leftPoints.get(frame), panelVisible, scale);
    placeMarker(rightMarker, rightPoints.get(frame), panelVisible, scale);
    placeMarker(cameraMarker, cameraPoints.get(frame), panelVisible, scale);
  }

  fitCameraToTrajectories();

  return {
    setVisible(nextVisible) {
      panelVisible = Boolean(nextVisible);
    },
    setCameraData(videoProjection) {
      if (videoProjection === cameraDataRef) return;
      rebuildCameraTrajectory(videoProjection);
      fitCameraToTrajectories();
    },
    update(absFrame) {
      if (!panelVisible) return;
      const nextSignature = computeChunkSignature();
      if (nextSignature !== handSignature) {
        handSignature = nextSignature;
        rebuildHandTrajectories();
        fitCameraToTrajectories();
      }
      resizeIfNeeded();
      controls.update();
      updateMarkers(absFrame);
      renderer.render(scene, camera);
    },
    reset() {
      clearGroup(leftGroup);
      clearGroup(rightGroup);
      clearGroup(cameraGroup);
      leftPoints.clear();
      rightPoints.clear();
      cameraPoints.clear();
      handSignature = "";
      cameraDataRef = null;
      leftMarker.visible = false;
      rightMarker.visible = false;
      cameraMarker.visible = false;
      fitCameraToTrajectories();
      renderer.clear();
    },
    dispose() {
      this.reset();
      [leftMarker, rightMarker, cameraMarker].forEach((mesh) => disposeObject(mesh));
      markerGeometry.dispose();
      renderer.dispose();
    },
  };
}
