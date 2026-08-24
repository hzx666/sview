// 双手 mesh 运行时：统一管理 dispose/init/layout/placeBoard 生命周期。
export function createHandRuntime({
  THREE,
  handRoot,
  showWhiteboard,
  materialPreset,
  sanitizeMeshPositions,
  applyBoardTransformFromAnchor,
  getBoardMesh,
  getBoardPlaced,
  setBoardPlaced,
  setBoardAnchor,
  getCameraTarget,
  getHandLayoutApplied,
  setHandLayoutApplied,
  getLeftMesh,
  setLeftMesh,
  getRightMesh,
  setRightMesh,
  setLeftPrevFrame,
  setRightPrevFrame,
}) {
  function dispose() {
    const leftMesh = getLeftMesh();
    const rightMesh = getRightMesh();

    if (leftMesh) {
      handRoot.remove(leftMesh);
      leftMesh.geometry.dispose();
      leftMesh.material.dispose();
      setLeftMesh(null);
    }

    if (rightMesh) {
      handRoot.remove(rightMesh);
      rightMesh.geometry.dispose();
      rightMesh.material.dispose();
      setRightMesh(null);
    }
  }

  function init(vertexCount, facesU32) {
    // 每次重建前先释放旧几何，避免 WebGL 内存累计。
    dispose();

    const leftGeo = new THREE.BufferGeometry();
    leftGeo.setAttribute("position", new THREE.BufferAttribute(new Float32Array(vertexCount * 3), 3));
    leftGeo.setIndex(new THREE.BufferAttribute(facesU32, 1));

    const rightGeo = new THREE.BufferGeometry();
    rightGeo.setAttribute("position", new THREE.BufferAttribute(new Float32Array(vertexCount * 3), 3));
    rightGeo.setIndex(new THREE.BufferAttribute(facesU32, 1));

    const leftMat = new THREE.MeshStandardMaterial({
      color: materialPreset.leftColor,
      roughness: materialPreset.roughness,
      metalness: materialPreset.metalness,
      side: THREE.DoubleSide,
    });
    const rightMat = new THREE.MeshStandardMaterial({
      color: materialPreset.rightColor,
      roughness: materialPreset.roughness,
      metalness: materialPreset.metalness,
      side: THREE.DoubleSide,
    });

    const leftMesh = new THREE.Mesh(leftGeo, leftMat);
    const rightMesh = new THREE.Mesh(rightGeo, rightMat);

    // 只做“观察朝向”调整：不改顶点数据本身，仅统一显示坐标系方向。
    // 目标：手根在外侧、手指朝屏幕内。
    leftMesh.position.set(0, 0, 0);
    rightMesh.position.set(0, 0, 0);
    leftMesh.rotation.set(Math.PI / 2, Math.PI, 0);
    rightMesh.rotation.set(Math.PI / 2, Math.PI, 0);

    handRoot.add(leftMesh);
    handRoot.add(rightMesh);

    setLeftMesh(leftMesh);
    setRightMesh(rightMesh);
    setLeftPrevFrame(new Float32Array(vertexCount * 3));
    setRightPrevFrame(new Float32Array(vertexCount * 3));
    setHandLayoutApplied(false);
    handRoot.position.set(0, 0, 0);
  }

  function applyBestHandLayout() {
    const leftMesh = getLeftMesh();
    const rightMesh = getRightMesh();
    if (!leftMesh || !rightMesh || getHandLayoutApplied()) return;
    // 数据可视化模式：关闭自动布局与重定位，handRoot 固定在原点。
    handRoot.position.set(0, 0, 0);
    setHandLayoutApplied(true);
    if (showWhiteboard && !getBoardPlaced()) {
      const target = getCameraTarget();
      setBoardAnchor(new THREE.Vector3(target.x, target.y, target.z));
      applyBoardTransformFromAnchor();
      setBoardPlaced(true);
    }
  }

  function placeBoardUnderHandsOnce() {
    const leftMesh = getLeftMesh();
    const rightMesh = getRightMesh();
    const boardMesh = getBoardMesh();
    if (!showWhiteboard || !leftMesh || !rightMesh || !boardMesh || getBoardPlaced()) return;

    sanitizeMeshPositions(leftMesh);
    sanitizeMeshPositions(rightMesh);
    const leftVisible = leftMesh.visible;
    const rightVisible = rightMesh.visible;
    // 计算包围盒时临时强制可见，确保隐藏手也参与定位。
    leftMesh.visible = true;
    rightMesh.visible = true;
    leftMesh.updateMatrixWorld(true);
    rightMesh.updateMatrixWorld(true);
    const box = new THREE.Box3().setFromObject(leftMesh).union(new THREE.Box3().setFromObject(rightMesh));
    leftMesh.visible = leftVisible;
    rightMesh.visible = rightVisible;

    if (!Number.isFinite(box.min.x) || !Number.isFinite(box.min.y) || !Number.isFinite(box.min.z)) {
      const target = getCameraTarget();
      setBoardAnchor(new THREE.Vector3(target.x, target.y, target.z));
    } else {
      const center = box.getCenter(new THREE.Vector3());
      setBoardAnchor(center.clone());
    }

    applyBoardTransformFromAnchor();
    setBoardPlaced(true);
  }

  function getMeshes() {
    return {
      leftMesh: getLeftMesh(),
      rightMesh: getRightMesh(),
    };
  }

  return {
    dispose,
    init,
    applyBestHandLayout,
    placeBoardUnderHandsOnce,
    getMeshes,
  };
}
