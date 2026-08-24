import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";

// 首页右侧 3D 预留区域的场景初始化器。
// 该函数只负责搭场景，不关心播放状态与帧更新。
export function bootstrapIndexScene({ mountEl, viewConfig, boardConfig, showWhiteboard }) {
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0xbababa);

  const camera = new THREE.PerspectiveCamera(36, 1, 0.01, 100);
  camera.position.set(
    viewConfig.cameraPos[0],
    viewConfig.cameraPos[1],
    viewConfig.cameraPos[2],
  );

  const renderer = new THREE.WebGLRenderer({ antialias: true, powerPreference: "high-performance" });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

  // 首页场景允许重复初始化，先清空挂载节点避免叠加多个 canvas。
  mountEl.innerHTML = "";
  mountEl.appendChild(renderer.domElement);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = false;
  controls.target.set(
    viewConfig.cameraTarget[0],
    viewConfig.cameraTarget[1],
    viewConfig.cameraTarget[2],
  );
  controls.minPolarAngle = 0.18;
  controls.maxPolarAngle = Math.PI - 0.18;
  controls.update();

  // handRoot 用于承载左右手 mesh，便于后续整体平移布局。
  const handRoot = new THREE.Group();
  scene.add(handRoot);

  scene.add(new THREE.AmbientLight(0xffffff, 1.05));

  const topKey = new THREE.DirectionalLight(0xffffff, 1.9);
  topKey.position.set(-0.03, -1.86, -1.03);
  scene.add(topKey);

  const frontFill = new THREE.DirectionalLight(0xf5f8ff, 0.7);
  frontFill.position.set(1, 1.1, 1.25);
  scene.add(frontFill);

  const boardMesh = new THREE.Mesh(
    new THREE.PlaneGeometry(boardConfig.width, boardConfig.depth),
    new THREE.MeshBasicMaterial({ color: 0x9a9a9a, side: THREE.DoubleSide }),
  );
  boardMesh.visible = showWhiteboard;
  scene.add(boardMesh);

  return {
    scene,
    camera,
    renderer,
    controls,
    handRoot,
    boardMesh,
  };
}
