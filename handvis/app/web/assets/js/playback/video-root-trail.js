import { getCameraFrameTransform, transformWorldToCameraRaw } from "./camera-projection-utils.js";

/**
 * 视频投影层根节点轨迹。
 *
 * 每只手使用 MANO joints[0] 腕部根关节，只保留当前帧及其之前最多 29 帧；
 * 历史点使用各自帧的相机位姿投影到视频像素坐标，并保持统一显示强度。
 */

const TRAIL_FRAME_COUNT = 30;

function createPointMaterial(THREE, color) {
  return new THREE.ShaderMaterial({
    depthTest: false,
    depthWrite: false,
    uniforms: {
      trailColor: { value: new THREE.Color(color) },
    },
    vertexShader: `
      void main() {
        gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        gl_PointSize = 6.0;
      }
    `,
    fragmentShader: `
      uniform vec3 trailColor;
      void main() {
        vec2 center = gl_PointCoord - vec2(0.5);
        if (dot(center, center) > 0.25) discard;
        gl_FragColor = vec4(trailColor, 1.0);
      }
    `,
  });
}

function createSideVisual(THREE, color) {
  const pointGeometry = new THREE.BufferGeometry();
  pointGeometry.setAttribute("position", new THREE.BufferAttribute(new Float32Array(TRAIL_FRAME_COUNT * 3), 3));
  pointGeometry.setDrawRange(0, 0);

  const maxSegmentVertices = (TRAIL_FRAME_COUNT - 1) * 2;
  const lineGeometry = new THREE.BufferGeometry();
  lineGeometry.setAttribute("position", new THREE.BufferAttribute(new Float32Array(maxSegmentVertices * 3), 3));
  lineGeometry.setDrawRange(0, 0);

  const points = new THREE.Points(pointGeometry, createPointMaterial(THREE, color));
  const lines = new THREE.LineSegments(lineGeometry, new THREE.LineBasicMaterial({
    color,
    depthTest: false,
    depthWrite: false,
  }));
  points.frustumCulled = false;
  lines.frustumCulled = false;
  points.renderOrder = 20;
  lines.renderOrder = 19;

  const group = new THREE.Group();
  group.add(lines, points);
  return { group, points, lines };
}

function disposeVisual(scene, visual) {
  if (!visual) return;
  scene.remove(visual.group);
  for (const node of [visual.points, visual.lines]) {
    node.geometry.dispose();
    node.material.dispose();
  }
}

function updateVisual(visual, samples) {
  if (!visual) return;
  const pointPosition = visual.points.geometry.attributes.position;
  let pointCount = 0;

  for (const sample of samples) {
    if (!sample) continue;
    const base = pointCount * 3;
    pointPosition.array[base] = sample.x;
    pointPosition.array[base + 1] = sample.y;
    pointPosition.array[base + 2] = sample.z;
    pointCount += 1;
  }
  visual.points.geometry.setDrawRange(0, pointCount);
  pointPosition.needsUpdate = true;

  const linePosition = visual.lines.geometry.attributes.position;
  let lineVertexCount = 0;
  for (let i = 1; i < samples.length; i += 1) {
    const from = samples[i - 1];
    const to = samples[i];
    // 无效帧会留下断点，避免跨越缺失数据错误连线。
    if (!from || !to) continue;
    for (const sample of [from, to]) {
      const base = lineVertexCount * 3;
      linePosition.array[base] = sample.x;
      linePosition.array[base + 1] = sample.y;
      linePosition.array[base + 2] = sample.z;
      lineVertexCount += 1;
    }
  }
  visual.lines.geometry.setDrawRange(0, lineVertexCount);
  linePosition.needsUpdate = true;
}

function findChunkIndexForFrame(playback, absFrame, frameStepSec) {
  const chunks = Array.isArray(playback?.chunks) ? playback.chunks : [];
  const direct = chunks.findIndex((entry) => {
    const start = Number(entry?.frame_start);
    const count = Number(entry?.frame_count);
    return Number.isFinite(start) && Number.isFinite(count) && absFrame >= start && absFrame < start + count;
  });
  if (direct >= 0) return direct;

  const timeSec = absFrame * frameStepSec;
  return chunks.findIndex((entry) => timeSec >= Number(entry?.start_time || 0) && timeSec < Number(entry?.end_time || 0));
}

function findLoadedChunk(chunkCache, absFrame) {
  for (const chunk of chunkCache.values()) {
    const start = Number(chunk?.meta?.frame_start || 0);
    const count = Number(chunk?.meta?.frame_count || 0);
    if (absFrame >= start && absFrame < start + count) return chunk;
  }
  return null;
}

function isInvalid(mask, absFrame) {
  return Array.isArray(mask) && absFrame >= 0 && absFrame < mask.length
    ? Number(mask[absFrame]) > 0
    : false;
}

function projectRootPoint({ chunk, side, absFrame, videoProjection, cssW, cssH }) {
  const localFrame = absFrame - Number(chunk?.meta?.frame_start || 0);
  if (localFrame < 0) return null;

  const src = chunk.handRoots;
  // handRoots 布局：[frame][left/right][xyz]。
  const sourceBase = localFrame * 6 + (side === "left" ? 0 : 3);
  if (!src || sourceBase + 2 >= src.length) return null;
  const wx = src[sourceBase];
  const wy = src[sourceBase + 1];
  const wz = src[sourceBase + 2];
  if (!Number.isFinite(wx) || !Number.isFinite(wy) || !Number.isFinite(wz)) return null;

  const transform = getCameraFrameTransform(videoProjection, absFrame);
  if (!transform) return null;
  const cam = transformWorldToCameraRaw(wx, wy, wz, transform);
  if (!(cam.z > 1e-5)) return null;

  const focal = Number(videoProjection?.img_focal || 0);
  const center = videoProjection?.img_center || [];
  const baseW = Number(videoProjection?.base_width || 0);
  const baseH = Number(videoProjection?.base_height || 0);
  if (!(focal > 0) || !(baseW > 0) || !(baseH > 0)) return null;

  return {
    x: (focal * (cam.x / cam.z) + Number(center[0] || 0)) * (cssW / baseW),
    y: (focal * (cam.y / cam.z) + Number(center[1] || 0)) * (cssH / baseH),
    z: -0.01,
  };
}

export function createVideoRootTrail({ THREE, scene, state, chunkCache, ensureChunkLoaded, requestRedraw }) {
  let leftVisual = null;
  let rightVisual = null;
  const pendingChunks = new Set();

  function init() {
    disposeVisual(scene, leftVisual);
    disposeVisual(scene, rightVisual);
    leftVisual = createSideVisual(THREE, 0xef4444);
    rightVisual = createSideVisual(THREE, 0x3b82f6);
    scene.add(leftVisual.group, rightVisual.group);
  }

  function requestMissingChunk(chunkIdx) {
    if (chunkIdx < 0 || pendingChunks.has(chunkIdx) || typeof ensureChunkLoaded !== "function") return;
    pendingChunks.add(chunkIdx);
    ensureChunkLoaded(chunkIdx)
      .then((loaded) => {
        if (loaded) requestRedraw?.();
      })
      .catch(() => {})
      .finally(() => pendingChunks.delete(chunkIdx));
  }

  function update(absFrame, cssW, cssH) {
    const playback = state.get("playback");
    const videoProjection = playback?.videoProjection;
    if (!leftVisual || !rightVisual || !playback || !videoProjection || absFrame < 0) {
      updateVisual(leftVisual, []);
      updateVisual(rightVisual, []);
      return;
    }

    const startFrame = Math.max(0, absFrame - TRAIL_FRAME_COUNT + 1);
    const frameStepSec = Number(state.get("frameStepSec") || 0);
    const leftSamples = [];
    const rightSamples = [];
    for (let frame = startFrame; frame <= absFrame; frame += 1) {
      const chunk = findLoadedChunk(chunkCache, frame);
      if (!chunk) {
        requestMissingChunk(findChunkIndexForFrame(playback, frame, frameStepSec));
        leftSamples.push(null);
        rightSamples.push(null);
        continue;
      }

      const common = { chunk, absFrame: frame, videoProjection, cssW, cssH };
      const left = isInvalid(playback.invalidLeftMask || playback.invalidMask, frame)
        ? null
        : projectRootPoint({ ...common, side: "left" });
      const right = isInvalid(playback.invalidRightMask || playback.invalidMask, frame)
        ? null
        : projectRootPoint({ ...common, side: "right" });
      leftSamples.push(left);
      rightSamples.push(right);
    }

    updateVisual(leftVisual, leftSamples);
    updateVisual(rightVisual, rightSamples);
  }

  return {
    init,
    update,
    clear() {
      updateVisual(leftVisual, []);
      updateVisual(rightVisual, []);
    },
    dispose() {
      pendingChunks.clear();
      disposeVisual(scene, leftVisual);
      disposeVisual(scene, rightVisual);
      leftVisual = null;
      rightVisual = null;
    },
  };
}
