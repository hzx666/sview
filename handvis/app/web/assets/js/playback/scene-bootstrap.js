import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";

// 播放页 Three 场景引导模块：负责场景/相机/渲染器/灯光/白板基础对象创建。
export function bootstrapPlaybackScene({ mountEl, viewConfig, boardConfig, showWhiteboard }) {
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x0b0f14);

  const fov = Math.min(179.9, Math.max(1, Number(viewConfig.fov || 40)));
  const camera = new THREE.PerspectiveCamera(
    fov,
    window.innerWidth / window.innerHeight,
    0.01,
    100,
  );
  camera.position.set(
    viewConfig.cameraPos[0],
    viewConfig.cameraPos[1],
    viewConfig.cameraPos[2],
  );

  const renderer = new THREE.WebGLRenderer({ antialias: true, powerPreference: "high-performance" });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
  // 播放页使用 ACES + sRGB，保证肤色与高光更稳定。
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.1;
  renderer.setSize(window.innerWidth, window.innerHeight, false);
  mountEl.appendChild(renderer.domElement);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = false;
  controls.target.set(
    viewConfig.cameraTarget[0],
    viewConfig.cameraTarget[1],
    viewConfig.cameraTarget[2],
  );
  controls.minPolarAngle = 0.18;
  // 允许下压到理论上限；若出现操控抖动可回退到 Math.PI - 0.01。
  controls.maxPolarAngle = Math.PI;
  controls.update();

  const hemi = new THREE.HemisphereLight(0xffffff, 0x8ea2b5, 0.62);
  scene.add(hemi);

  const topKey = new THREE.DirectionalLight(0xffffff, 1.95);
  topKey.position.set(-0.016, -1.900, -1.012);
  scene.add(topKey);

  const frontFill = new THREE.DirectionalLight(0xf6fbff, 0.72);
  frontFill.position.set(1.0, 1.15, 1.25);
  scene.add(frontFill);

  const rimBack = new THREE.DirectionalLight(0xd7ecff, 0.36);
  rimBack.position.set(-1.25, 1.35, -1.1);
  scene.add(rimBack);

  const board = new THREE.Mesh(
    new THREE.PlaneGeometry(boardConfig.width, boardConfig.depth),
    new THREE.MeshBasicMaterial({ color: 0x9a9a9a, side: THREE.DoubleSide }),
  );
  board.visible = showWhiteboard;
  scene.add(board);

  const handRoot = new THREE.Group();
  scene.add(handRoot);

  return {
    scene,
    camera,
    renderer,
    controls,
    board,
    handRoot,
  };
}
