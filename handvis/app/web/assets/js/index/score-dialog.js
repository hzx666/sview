// 首页评分弹窗：只加载规范化路径，提交时始终新增评分记录。

import { createBooleanChoice, createStarRating } from "../shared/score-controls.js";

async function parseResponse(response) {
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || "评分请求失败");
  return data;
}

export function createScoreDialog({ dom }) {
  let context = null;
  const egoChoice = createBooleanChoice(dom.scoreEgoChoice, null);
  const rating = createStarRating(dom.scoreRating, dom.scoreValueLabel, dom.scoreZeroButton);

  function setMessage(text, ok = false) {
    dom.scoreMessage.textContent = text;
    dom.scoreMessage.className = `score-message ${ok ? "ok" : "err"}`;
  }

  function close() {
    dom.scoreDialog?.close();
  }

  async function open({ epDir, itemName }) {
    context = { epDir, itemName };
    dom.scoreForm?.reset();
    egoChoice.reset(null);
    rating.reset();
    dom.scoreOssPath.value = "正在关联...";
    setMessage("");
    dom.scoreSaveBtn.disabled = true;
    dom.scoreDialog?.showModal();
    try {
      const params = new URLSearchParams({ ep_dir: epDir, item_name: itemName });
      const data = await parseResponse(await fetch(`/api/oss-file-info/context?${params}`));
      if (!context || context.epDir !== epDir || context.itemName !== itemName) return;
      dom.scoreOssPath.value = data.oss_path || "";
      setMessage("每次提交会新增记录。", true);
      dom.scoreSaveBtn.disabled = false;
    } catch (error) {
      setMessage(error.message || String(error));
    }
  }

  dom.scoreForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!context) return;
    const isEgo = egoChoice.getValue();
    const score = rating.getValue();
    if (isEgo === null) {
      setMessage("请选择是否为 ego 数据");
      return;
    }
    if (score === null) {
      setMessage("请选择星级评分");
      return;
    }
    dom.scoreSaveBtn.disabled = true;
    setMessage("正在保存...");
    try {
      const data = await parseResponse(await fetch("/api/oss-file-info", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ep_dir: context.epDir,
          item_name: context.itemName,
          is_ego: isEgo,
          score,
          comment: dom.scoreComment.value,
        }),
      }));
      setMessage(`新增成功，记录 ID：${data.id}`, true);
      setTimeout(close, 650);
    } catch (error) {
      setMessage(error.message || String(error));
      dom.scoreSaveBtn.disabled = false;
    }
  });

  dom.scoreCloseBtn?.addEventListener("click", close);
  dom.scoreCancelBtn?.addEventListener("click", close);
  return { open, close };
}
