// 播放页白板控制器：
// 1) 同步调参 UI
// 2) 绑定滑杆与重置按钮
// 3) 将参数应用到白板 mesh
export function createBoardController({
  dom,
  boardConfig,
  resetValues,
  fmtNumber,
  degToRad,
  getBoardMesh,
  showWhiteboard,
}) {
  function updateBoardControlUI() {
    if (dom.boardYOffset) dom.boardYOffset.value = String(boardConfig.belowPalmDistance);
    if (dom.boardYOffsetVal) dom.boardYOffsetVal.textContent = fmtNumber(boardConfig.belowPalmDistance);
    if (dom.boardXOffset) dom.boardXOffset.value = String(boardConfig.offsetX);
    if (dom.boardXOffsetVal) dom.boardXOffsetVal.textContent = fmtNumber(boardConfig.offsetX);
    if (dom.boardZOffset) dom.boardZOffset.value = String(boardConfig.offsetZ);
    if (dom.boardZOffsetVal) dom.boardZOffsetVal.textContent = fmtNumber(boardConfig.offsetZ);
    if (dom.boardRotX) dom.boardRotX.value = String(boardConfig.rotXDeg);
    if (dom.boardRotXVal) dom.boardRotXVal.textContent = String(Math.round(boardConfig.rotXDeg));
    if (dom.boardRotY) dom.boardRotY.value = String(boardConfig.rotYDeg);
    if (dom.boardRotYVal) dom.boardRotYVal.textContent = String(Math.round(boardConfig.rotYDeg));
    if (dom.boardRotZ) dom.boardRotZ.value = String(boardConfig.rotZDeg);
    if (dom.boardRotZVal) dom.boardRotZVal.textContent = String(Math.round(boardConfig.rotZDeg));
  }

  // 历史命名保留：当前按绝对世界坐标/角度直接应用。
  function applyBoardTransformFromAnchor() {
    const board = getBoardMesh();
    if (!board) return;
    board.position.set(
      boardConfig.offsetX,
      boardConfig.belowPalmDistance,
      boardConfig.offsetZ,
    );
    board.rotation.set(
      degToRad(boardConfig.rotXDeg),
      degToRad(boardConfig.rotYDeg),
      degToRad(boardConfig.rotZDeg),
    );
    board.visible = Boolean(showWhiteboard);
  }

  function bindNumberInput(node, onInput) {
    if (!node) return;
    node.addEventListener("input", onInput);
  }

  function bindBoardControls() {
    updateBoardControlUI();

    bindNumberInput(dom.boardYOffset, () => {
      boardConfig.belowPalmDistance = Number(dom.boardYOffset.value || boardConfig.belowPalmDistance);
      updateBoardControlUI();
      applyBoardTransformFromAnchor();
    });

    bindNumberInput(dom.boardRotX, () => {
      boardConfig.rotXDeg = Number(dom.boardRotX.value || boardConfig.rotXDeg);
      updateBoardControlUI();
      applyBoardTransformFromAnchor();
    });

    bindNumberInput(dom.boardXOffset, () => {
      boardConfig.offsetX = Number(dom.boardXOffset.value || boardConfig.offsetX);
      updateBoardControlUI();
      applyBoardTransformFromAnchor();
    });

    bindNumberInput(dom.boardZOffset, () => {
      boardConfig.offsetZ = Number(dom.boardZOffset.value || boardConfig.offsetZ);
      updateBoardControlUI();
      applyBoardTransformFromAnchor();
    });

    bindNumberInput(dom.boardRotY, () => {
      boardConfig.rotYDeg = Number(dom.boardRotY.value || boardConfig.rotYDeg);
      updateBoardControlUI();
      applyBoardTransformFromAnchor();
    });

    bindNumberInput(dom.boardRotZ, () => {
      boardConfig.rotZDeg = Number(dom.boardRotZ.value || boardConfig.rotZDeg);
      updateBoardControlUI();
      applyBoardTransformFromAnchor();
    });

    if (dom.boardResetBtn) {
      dom.boardResetBtn.addEventListener("click", () => {
        boardConfig.belowPalmDistance = resetValues.belowPalmDistance;
        boardConfig.offsetX = resetValues.offsetX;
        boardConfig.offsetZ = resetValues.offsetZ;
        boardConfig.rotXDeg = resetValues.rotXDeg;
        boardConfig.rotYDeg = resetValues.rotYDeg;
        boardConfig.rotZDeg = resetValues.rotZDeg;
        updateBoardControlUI();
        applyBoardTransformFromAnchor();
      });
    }
  }

  return {
    updateBoardControlUI,
    applyBoardTransformFromAnchor,
    bindBoardControls,
  };
}
