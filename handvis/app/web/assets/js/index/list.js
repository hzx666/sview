/**
 * 首页列表模块：
 * - 渲染 ep 资产列表
 * - 保存可编辑名称
 * 通过依赖注入减少对全局变量的耦合，便于后续继续模块化。
 */

export function createListModule({
  dom,
  esc,
  epTail,
  formatError,
  setResult,
  pickEpDir,
  openPlaybackByEp,
}) {
  // 分页状态单点维护，避免 UI 与请求参数不同步。
  const pager = { page: 1, pageSize: 20, total: 0, totalPages: 1 };

  function updatePagerUI() {
    if (dom.listPageInfo) {
      dom.listPageInfo.textContent = `第 ${pager.page} / ${pager.totalPages} 页（共 ${pager.total} 条）`;
    }
    if (dom.prevListPageBtn) dom.prevListPageBtn.disabled = pager.page <= 1;
    if (dom.nextListPageBtn) dom.nextListPageBtn.disabled = pager.page >= pager.totalPages;
    if (dom.listPageSize && String(dom.listPageSize.value) !== String(pager.pageSize)) {
      dom.listPageSize.value = String(pager.pageSize);
    }
  }

  async function loadList({ page = pager.page, pageSize = pager.pageSize } = {}) {
    pager.page = Math.max(1, Number(page) || 1);
    pager.pageSize = Math.max(1, Math.min(200, Number(pageSize) || 20));
    updatePagerUI();
    dom.listBody.innerHTML = '<tr><td colspan="7">加载中...</td></tr>';
    try {
      const resp = await fetch(`/api/ep-assets/list?page=${pager.page}&page_size=${pager.pageSize}`);
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || "list 请求失败");
      // 以后端返回为准回写分页状态，避免边界页偏移。
      pager.total = Number(data.total || 0);
      pager.page = Number(data.page || pager.page) || pager.page;
      pager.pageSize = Number(data.page_size || pager.pageSize) || pager.pageSize;
      pager.totalPages = Math.max(1, Math.ceil(pager.total / pager.pageSize));
      updatePagerUI();
      const items = Array.isArray(data.items) ? data.items : [];
      if (items.length === 0) {
        dom.listBody.innerHTML = '<tr><td colspan="7">暂无数据</td></tr>';
        return;
      }

      dom.listBody.innerHTML = items.map((x) => `
        <tr>
          <td>${esc(x.id)}</td>
          <td class="name-cell">
            <input class="name-input" id="assetName_${esc(x.id)}" value="${esc(x.name || "")}" />
          </td>
          <td class="path" title="${esc(x.ep_dir)}">${esc(epTail(x.ep_dir))}</td>
          <td class="path" title="${esc(x.video_path || "")}">${esc(x.video_path || "")}</td>
          <td>${esc(x.items_count ?? 0)}</td>
          <td>${esc(x.updated_at || "")}</td>
          <td>
            <button class="pick-btn" onclick='saveAssetName(${Number(x.id)})'>编辑保存</button>
            <button class="pick-btn" onclick='pickEpDir(${JSON.stringify(x.ep_dir || "")})'>使用</button>
            <button class="pick-btn" onclick='copyEpDir(${JSON.stringify(x.ep_dir || "")})'>复制</button>
            <button class="pick-btn" onclick='openPlaybackByEp(${JSON.stringify(x.ep_dir || "")})'>播放页</button>
          </td>
        </tr>
      `).join("");

      if (!dom.epDir.value && items[0]?.ep_dir) {
        dom.epDir.value = items[0].ep_dir;
      }
    } catch (e) {
      updatePagerUI();
      dom.listBody.innerHTML = `<tr><td colspan=\"7\"><span class=\"err\">${esc(formatError(e))}</span></td></tr>`;
    }
  }

  async function prevPage() {
    if (pager.page <= 1) return;
    await loadList({ page: pager.page - 1, pageSize: pager.pageSize });
  }

  async function nextPage() {
    if (pager.page >= pager.totalPages) return;
    await loadList({ page: pager.page + 1, pageSize: pager.pageSize });
  }

  async function changePageSize(size) {
    const pageSize = Math.max(1, Math.min(200, Number(size) || pager.pageSize));
    await loadList({ page: 1, pageSize });
  }

  async function saveAssetName(assetId) {
    const id = Number(assetId);
    if (!Number.isFinite(id) || id <= 0) {
      setResult("无效的 asset id", false);
      return;
    }
    const input = document.getElementById(`assetName_${id}`);
    if (!input) {
      setResult("未找到名称输入框", false);
      return;
    }
    const name = String(input.value || "").trim();
    if (!name) {
      setResult("名称不能为空", false);
      return;
    }

    try {
      // 名称更新走后端 PATCH，前端只做最小字段校验。
      const resp = await fetch(`/api/ep-assets/id/${id}/name`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || "名称保存失败");
      input.value = data.name || name;
      setResult(`名称已保存：${input.value}`, true);
    } catch (e) {
      setResult(formatError(e), false);
    }
  }

  return {
    loadList,
    saveAssetName,
    prevPage,
    nextPage,
    changePageSize,
    pickEpDir,
    openPlaybackByEp,
  };
}
