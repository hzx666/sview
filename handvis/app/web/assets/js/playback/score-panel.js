// 播放页帧行评分区：不自动查询历史，提交时始终新增评分记录。

import { createBooleanChoice, createStarRating } from "../shared/score-controls.js";

async function parseResponse(response) {
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || "评分请求失败");
  return data;
}

export function createPlaybackScorePanel({ epDirInput, itemNameInput }) {
  const form = document.getElementById("playbackScoreForm");
  const commentInput = document.getElementById("playbackScoreComment");
  const saveButton = document.getElementById("playbackScoreSaveBtn");
  const message = document.getElementById("playbackScoreMessage");
  const egoChoice = createBooleanChoice(document.getElementById("playbackScoreEgoChoice"), null);
  const rating = createStarRating(
    document.getElementById("playbackScoreRating"),
    document.getElementById("playbackScoreValueLabel"),
    document.getElementById("playbackScoreZeroButton"),
  );

  function setMessage(text, ok = false) {
    message.textContent = text;
    message.className = `playback-score-message ${ok ? "ok" : ""}`;
  }

  function resetFormForCurrentItem() {
    egoChoice.reset(null);
    rating.reset(null);
    commentInput.value = "";
    const epDir = String(epDirInput?.value || "").trim();
    const itemName = String(itemNameInput?.value || "").trim();
    if (!epDir || !itemName) {
      setMessage("等待素材信息");
      return;
    }
    setMessage("每次提交会新增记录。");
  }

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const epDir = String(epDirInput?.value || "").trim();
    const itemName = String(itemNameInput?.value || "").trim();
    const isEgo = egoChoice.getValue();
    const score = rating.getValue();
    if (!epDir || !itemName) {
      setMessage("请先填写素材信息");
      return;
    }
    if (isEgo === null) {
      setMessage("请选择是否 ego");
      return;
    }
    if (score === null) {
      setMessage("请选择星级评分");
      return;
    }
    saveButton.disabled = true;
    setMessage("正在保存...");
    try {
      const data = await parseResponse(await fetch("/api/oss-file-info", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ep_dir: epDir,
          item_name: itemName,
          is_ego: isEgo,
          score,
          comment: commentInput.value,
        }),
      }));
      setMessage(`新增成功，ID ${data.id}`, true);
    } catch (error) {
      setMessage(error.message || String(error));
    } finally {
      saveButton.disabled = false;
    }
  });

  epDirInput?.addEventListener("input", resetFormForCurrentItem);
  itemNameInput?.addEventListener("input", resetFormForCurrentItem);
  resetFormForCurrentItem();
  return { resetFormForCurrentItem };
}
