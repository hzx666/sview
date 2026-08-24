// 首页查询控制器：管理查询生命周期、父目录解析、配对组展示、复制与跳转播放页。

export function createQueryController({
  dom,
  state,
  esc,
  epTail,
  setResult,
  formatError,
  copyText,
  setupThreePlayback,
  clearThreePlayback,
  refreshList,
  openPlaybackByEp,
  openScoreDialog,
}) {
  const PREVIEW_STATE_STORAGE_KEY = "handvis:index:preview_state:v1";
  const PAGE_SIZE_OPTIONS = [20, 50, 100];
  const previewPager = {
    page: 1,
    pageSize: 50,
    hasMore: false,
    cursor: "",
    nextCursor: "",
    pageCursors: { 1: "" },
  };

  function safeGetSessionStorage() {
    try {
      return window?.sessionStorage || null;
    } catch {
      return null;
    }
  }

  function readPersistedPreviewState() {
    const storage = safeGetSessionStorage();
    if (!storage) return null;
    try {
      const raw = storage.getItem(PREVIEW_STATE_STORAGE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return null;
      const epDir = String(parsed.ep_dir || "").trim();
      if (!epDir) return null;
      return {
        epDir,
        page: Math.max(1, Number(parsed.page) || 1),
        pageSize: normalizePageSize(parsed.page_size),
        cursor: String(parsed.cursor || ""),
        selectedItemName: String(parsed.selected_item || "").trim(),
      };
    } catch {
      return null;
    }
  }

  function persistPreviewState({
    epDir = dom.epDir?.value || "",
    selectedItemName = state.get("selectedItemName") || "",
  } = {}) {
    const storage = safeGetSessionStorage();
    if (!storage) return;
    const safeEpDir = String(epDir || "").trim();
    if (!safeEpDir) return;
    const payload = {
      ep_dir: safeEpDir,
      page: previewPager.page,
      page_size: previewPager.pageSize,
      cursor: previewPager.cursor,
      selected_item: String(selectedItemName || "").trim(),
    };
    try {
      storage.setItem(PREVIEW_STATE_STORAGE_KEY, JSON.stringify(payload));
    } catch {
      // 忽略存储失败：不影响主查询流程。
    }
  }

  function normalizePageSize(value) {
    const size = Number(value) || 50;
    return PAGE_SIZE_OPTIONS.includes(size) ? size : 50;
  }

  function setPreviewVisible(visible) {
    if (!dom.playerWrap) return;
    dom.playerWrap.hidden = !visible;
  }

  function getDirectPairInput() {
    return {
      posehandPath: String(dom.directPosehandPath?.value || "").trim(),
      videoPath: String(dom.directVideoPath?.value || "").trim(),
    };
  }

  function hasDirectPairInput() {
    const { posehandPath, videoPath } = getDirectPairInput();
    return Boolean(posehandPath || videoPath);
  }

  function hasCompleteDirectPairInput() {
    const { posehandPath, videoPath } = getDirectPairInput();
    return Boolean(posehandPath && videoPath);
  }

  function clearDirectPairState() {
    state.patch({ directPairPaths: null });
  }

  function directPairPathsForItem(epDir, itemName) {
    const rows = state.get("currentPairs") || [];
    const matched = rows.find((item) => String(item?.item_name || "") === String(itemName || ""));
    if (!matched) return state.get("directPairPaths") || null;
    return {
      video_path: String(matched.video_path || "").trim(),
      posehand_path: String(matched.posehand_path || "").trim(),
    };
  }

  function scrollToPreviewArea() {
    const previewWrap = dom.playerWrap;
    if (!previewWrap || previewWrap.hidden) return;
    window.requestAnimationFrame(() => {
      previewWrap.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    });
  }

  function shortPathTail(path, keepSegments = 4) {
    const raw = String(path || "").trim();
    if (!raw) return "";
    const parts = raw.split("/").filter(Boolean);
    if (parts.length <= keepSegments) return raw;
    return `.../${parts.slice(-keepSegments).join("/")}`;
  }

  // 父目录列表与配对列表互斥展示，防止用户误把上下文混在一起。
  function hideParentEpList() {
    if (dom.parentEpWrap) dom.parentEpWrap.hidden = true;
    if (dom.parentEpBody) dom.parentEpBody.innerHTML = '<tr><td colspan="3">暂无数据</td></tr>';
    if (dom.parentEpHint) dom.parentEpHint.textContent = "";
    if (dom.parentEpTitle) dom.parentEpTitle.textContent = "父目录子目录列表";
  }

  function hidePairList() {
    if (dom.pairWrap) dom.pairWrap.hidden = true;
    if (dom.pairBody) dom.pairBody.innerHTML = '<tr><td colspan="5">暂无数据</td></tr>';
    if (dom.pairHint) dom.pairHint.textContent = "";
    if (dom.pairTitle) dom.pairTitle.textContent = "当前 EP 配对列表";
    state.patch({ selectedItemName: "", currentPairs: [] });
    setPreviewVisible(false);
  }

  function syncPagerUI(type) {
    const isParent = type === "parent";
    const prevBtn = isParent ? dom.parentPrevPageBtn : dom.pairPrevPageBtn;
    const nextBtn = isParent ? dom.parentNextPageBtn : dom.pairNextPageBtn;
    const pageInfo = isParent ? dom.parentPageInfo : dom.pairPageInfo;
    const pageSizeSelect = isParent ? dom.parentPageSize : dom.pairPageSize;
    if (prevBtn) prevBtn.disabled = previewPager.page <= 1;
    if (nextBtn) nextBtn.disabled = !previewPager.hasMore;
    if (pageInfo) pageInfo.textContent = `第 ${previewPager.page} 页`;
    if (pageSizeSelect && String(pageSizeSelect.value) !== String(previewPager.pageSize)) {
      pageSizeSelect.value = String(previewPager.pageSize);
    }
  }

  function syncAllPagerUI() {
    syncPagerUI("parent");
    syncPagerUI("pair");
  }

  function renderParentEpList(parentDir, items, { hasMore = false } = {}) {
    if (!dom.parentEpWrap || !dom.parentEpBody) return;
    const rows = Array.isArray(items) ? items : [];

    dom.parentEpWrap.hidden = false;
    previewPager.hasMore = !!hasMore;
    if (dom.parentEpTitle) {
      dom.parentEpTitle.textContent = `父目录子目录列表：${epTail(parentDir)}`;
    }
    if (dom.parentEpHint) {
      dom.parentEpHint.textContent = `本页 ${rows.length} 个`;
    }
    syncPagerUI("parent");

    if (rows.length === 0) {
      dom.parentEpBody.innerHTML = '<tr><td colspan="3">未发现可用的 ep 子目录</td></tr>';
      return;
    }

    dom.parentEpBody.innerHTML = rows.map((item) => {
      const name = esc(item?.name || "");
      const epDir = String(item?.ep_dir || "");
      const epDirEscaped = esc(epDir);
      return `
        <tr>
          <td>${name}</td>
          <td class="path" title="${epDirEscaped}">${epDirEscaped}</td>
          <td>
            <button class="pick-btn" data-action="query" data-ep-dir="${epDirEscaped}">查询</button>
            <button class="pick-btn" data-action="playback" data-ep-dir="${epDirEscaped}">播放页</button>
          </td>
        </tr>
      `;
    }).join("");
  }

  function renderPairList(epDir, items, { hasMore = false, offset = 0 } = {}) {
    if (!dom.pairWrap || !dom.pairBody) return;
    const rows = Array.isArray(items) ? items : [];
    const selectedName = String(state.get("selectedItemName") || "").trim();

    dom.pairWrap.hidden = false;
    previewPager.hasMore = !!hasMore;
    if (dom.pairTitle) {
      dom.pairTitle.textContent = `当前 EP 配对列表：${epTail(epDir)}`;
    }
    if (dom.pairHint) {
      dom.pairHint.textContent = `本页 ${rows.length} 组`;
    }
    syncPagerUI("pair");

    if (rows.length === 0) {
      dom.pairBody.innerHTML = '<tr><td colspan="5">当前目录未发现可用的视频候选</td></tr>';
      state.patch({ selectedItemName: "", currentPairs: [] });
      setPreviewVisible(false);
      return;
    }

    // 选择策略：优先沿用上次选中项，否则默认首项，减少重复点击。
    const resolvedSelected = rows.some((x) => String(x?.item_name || "") === selectedName)
      ? selectedName
      : String(rows[0]?.item_name || "");
    state.patch({ selectedItemName: resolvedSelected, currentPairs: rows });

    const itemRows = rows.map((item, idx) => {
      const itemName = String(item?.item_name || "");
      const itemNameEsc = esc(itemName);
      const videoPath = String(item?.video_path || "");
      const posehandPath = String(item?.posehand_path || "");
      const videoTail = esc(shortPathTail(videoPath));
      const posehandTail = posehandPath ? esc(shortPathTail(posehandPath)) : "-";
      const posehandTitle = posehandPath ? esc(posehandPath) : "";
      const selected = itemName === resolvedSelected;
      return `
        <tr>
          <td>${offset + idx + 1}</td>
          <td>${selected ? `[*] ${itemNameEsc}` : itemNameEsc}</td>
          <td class="path" title="${esc(videoPath)}">${videoTail}</td>
          <td class="path" title="${posehandTitle}">${posehandTail}</td>
          <td>
            <button class="pick-btn" data-action="build" data-ep-dir="${esc(epDir)}" data-item-name="${itemNameEsc}">快速预览</button>
            <button class="pick-btn" data-action="playback" data-ep-dir="${esc(epDir)}" data-item-name="${itemNameEsc}">播放页</button>
            <button class="pick-btn score-pick-btn" data-action="score" data-ep-dir="${esc(epDir)}" data-item-name="${itemNameEsc}">评分</button>
          </td>
        </tr>
      `;
    });
    dom.pairBody.innerHTML = itemRows.join("");
  }

  let activeBuildStreamToken = 0;
  let activeBuildTaskSource = null;

  function stopBuildTaskStreamForIndex() {
    if (activeBuildTaskSource) {
      activeBuildTaskSource.close();
      activeBuildTaskSource = null;
    }
  }

  function waitBuildTask(taskId, { timeoutMs = 15 * 60 * 1000, acceptPartial = false } = {}) {
    return new Promise((resolve, reject) => {
      stopBuildTaskStreamForIndex();
      const token = ++activeBuildStreamToken;
      const source = new EventSource(`/api/playback/build-chunks/stream/${encodeURIComponent(taskId)}`);
      activeBuildTaskSource = source;

      const timeoutId = setTimeout(() => {
        if (token !== activeBuildStreamToken) return;
        stopBuildTaskStreamForIndex();
        reject(new Error("build task timeout"));
      }, timeoutMs);

      const closeCurrent = () => {
        clearTimeout(timeoutId);
        if (token !== activeBuildStreamToken) {
          source.close();
          return false;
        }
        stopBuildTaskStreamForIndex();
        return true;
      };

      source.addEventListener("status", (event) => {
        let taskData = null;
        try {
          taskData = JSON.parse(event.data || "{}");
        } catch {
          return;
        }
        if (token !== activeBuildStreamToken) return;

        const status = String(taskData.status || "").toLowerCase();
        if (status === "succeeded") {
          if (!closeCurrent()) return;
          resolve(taskData.result);
          return;
        }
        if (status === "failed") {
          if (!closeCurrent()) return;
          reject(new Error(taskData.error || "build task failed"));
          return;
        }
        if (acceptPartial && taskData.result && taskData.result.has_3d) {
          const partial = taskData.result;
          const partialStatus = String(partial.video_status || "").toLowerCase();
          const partialMediaId = Number(partial.media_id || 0);
          if (partialMediaId > 0 || partialStatus === "ready" || partialStatus === "failed") {
            if (!closeCurrent()) return;
            resolve(partial);
          }
        }
      });

      source.onerror = () => {
        if (token !== activeBuildStreamToken) {
          source.close();
          return;
        }
        if (!closeCurrent()) return;
        reject(new Error("build task stream disconnected"));
      };
    });
  }

  // SSE 视频状态监听：每次新构建递增 token，旧监听自动终止。
  let activeVideoPollToken = 0;
  let activeVideoStatusSource = null;

  function stopVideoStatusStreamForIndex() {
    if (activeVideoStatusSource) {
      activeVideoStatusSource.close();
      activeVideoStatusSource = null;
    }
  }

  function pollVideoStatusForIndex(mediaId, videoEl, { timeoutMs = 15 * 60 * 1000 } = {}) {
    if (!mediaId) return;

    stopVideoStatusStreamForIndex();
    const token = ++activeVideoPollToken;
    const source = new EventSource(`/api/playback/video-status/stream/${encodeURIComponent(mediaId)}`);
    activeVideoStatusSource = source;

    const timeoutId = setTimeout(() => {
      if (token !== activeVideoPollToken) return;
      stopVideoStatusStreamForIndex();
      setResult("视频转码等待超时，请稍后重试", false);
    }, timeoutMs);

    const closeCurrent = () => {
      clearTimeout(timeoutId);
      if (token !== activeVideoPollToken) {
        source.close();
        return false;
      }
      stopVideoStatusStreamForIndex();
      return true;
    };

    source.addEventListener("status", (event) => {
      let data = null;
      try {
        data = JSON.parse(event.data || "{}");
      } catch {
        return;
      }
      if (token !== activeVideoPollToken) return;

      const status = String(data.status || "").toLowerCase();
      if (status === "queued" || status === "running" || status === "pending") {
        setResult("正在视频转码，请稍等...", true);
        return;
      }
      if (status === "success" && data.video_url) {
        if (!closeCurrent()) return;
        if (videoEl) {
          videoEl.src = data.video_url;
          videoEl.load();
        }
        setResult("视频转码完成，已自动加载", true);
        return;
      }
      if (status === "failed") {
        if (!closeCurrent()) return;
        setResult(`视频转码失败：${data.error || "unknown"}`, false);
      }
    });

    source.onerror = () => {
      if (token !== activeVideoPollToken) {
        source.close();
      }
    };
  }

  async function previewEpDir(epDir, { page = 1, pageSize = 50, cursor = "" } = {}) {
    const refreshResp = await fetch("/api/ep-assets/preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ep_dir: epDir, page, page_size: pageSize, limit: pageSize, cursor }),
    });
    const refreshData = await refreshResp.json();
    if (!refreshResp.ok) throw new Error(refreshData.detail || "preview failed");
    return refreshData;
  }

  async function previewDirectPair({ videoPath, posehandPath }) {
    const resp = await fetch("/api/ep-assets/preview-pair", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ video_path: videoPath, posehand_path: posehandPath }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || "preview pair failed");
    return data;
  }

  async function buildSinglePair(epDir, itemName, { forceRefresh = false } = {}) {
    const normalizedItem = String(itemName || "").trim();
    if (!normalizedItem) {
      throw new Error("item_name 不能为空");
    }

    dom.player.removeAttribute("src");
    dom.player.load();
    // 仅在触发构建时显示预览区域。
    setPreviewVisible(true);
    scrollToPreviewArea();

    setResult(`处理中首次加载需要稍等片刻...\nitem: ${normalizedItem}`, true);
    activeBuildStreamToken += 1;
    stopBuildTaskStreamForIndex();
    activeVideoPollToken += 1;
    stopVideoStatusStreamForIndex();
    const submitResp = await fetch("/api/playback/build-chunks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ep_dir: epDir,
        item_name: normalizedItem,
        force_rebuild: !!forceRefresh,
        ...(state.get("directPairPaths") || {}),
      }),
    });
    const submitData = await submitResp.json();
    if (!submitResp.ok) throw new Error(submitData.detail || "build-chunks submit failed");
    if (!submitData.task_id) throw new Error("missing task_id");

    setResult("任务已提交，正在后台构建...", true);
    const data = await waitBuildTask(submitData.task_id, { acceptPartial: true });
    const isPartial = Boolean(data.still_building);
    setResult(isPartial ? "首段3D已就绪,后台继续构建..." : "构建上传完成，正在加载视频和3D...", true);

    state.set("lastResultData", data);
    state.set("selectedItemName", normalizedItem);
    persistPreviewState({ epDir, selectedItemName: normalizedItem });
    renderPairList(epDir, state.get("currentPairs") || [], {
      hasMore: previewPager.hasMore,
      offset: (previewPager.page - 1) * previewPager.pageSize,
    });

    // 仅在发生远程刷新/强刷后更新首页列表，降低无效请求频次。
    if (typeof refreshList === "function" && (data.source === "remote" || forceRefresh)) {
      await refreshList();
    }

    const videoStatus = String(data.video_status || "").toLowerCase();
    const videoReady = videoStatus === "ready" && data.video_url;
    if (videoReady) {
      dom.player.src = data.video_url;
      dom.player.load();
    }
    if (data.has_3d && data.index_url) {
      await setupThreePlayback(data.index_url);
      if (videoReady) {
        setResult(`查询成功，视频与3D已就绪，可播放\nitem: ${normalizedItem}\n来源: ${data.source || "cache"}`, true);
      } else if (videoStatus === "failed") {
        setResult(`3D 已就绪，但视频处理失败：${data.video_error || "unknown"}`, false);
      } else {
        setResult(`3D 已就绪，视频转码中（status=${videoStatus || "queued"}），稍后自动加载\nitem: ${normalizedItem}`, true);
        pollVideoStatusForIndex(data.media_id, dom.player);
      }
    } else if (videoReady) {
      if (typeof clearThreePlayback === "function") clearThreePlayback();
      setResult(`查询成功，当前素材无posehand，已切换为纯视频播放\nitem: ${normalizedItem}\n来源: ${data.source || "cache"}`, true);
    } else {
      if (typeof clearThreePlayback === "function") clearThreePlayback();
      setResult(`视频转码中（status=${videoStatus || "queued"}），稍后自动加载`, false);
      pollVideoStatusForIndex(data.media_id, dom.player);
    }
  }

  async function discoverParentChildren(parentDir) {
    const resp = await fetch("/api/ep-assets/discover-children", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ep_dir: parentDir }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || "discover children failed");
    return data;
  }

  async function queryAndRender({
    forceRefresh = false,
    overrideEpDir = "",
    page = 1,
    pageSize = previewPager.pageSize,
    cursor = "",
    resetCursor = false,
  } = {}) {
    if (state.get("queryInFlight")) return;
    const epDir = (overrideEpDir || dom.epDir.value || "").trim();
    const directPairModeRequested = hasDirectPairInput();
    if (!epDir && !directPairModeRequested) {
      setResult("请输入 ep_dir", false);
      return;
    }
    if (directPairModeRequested && !hasCompleteDirectPairInput()) {
      setResult("手动路径需要同时填写 posehand3D 文件路径和视频文件路径", false);
      return;
    }
    setPreviewVisible(false);

    try {
      state.set("queryInFlight", true);
      state.set("lastResultData", null);
      hidePairList();
      hideParentEpList();
      previewPager.page = Math.max(1, Number(page) || 1);
      previewPager.pageSize = normalizePageSize(pageSize);
      previewPager.hasMore = false;
      if (resetCursor) {
        previewPager.cursor = "";
        previewPager.nextCursor = "";
        previewPager.pageCursors = { 1: "" };
      }
      const pageCursor = String(cursor ?? previewPager.pageCursors[previewPager.page] ?? "");
      previewPager.cursor = pageCursor;
      syncAllPagerUI();
      let data = null;
      const directPairMode = hasCompleteDirectPairInput();
      if (directPairMode) {
        const { posehandPath, videoPath } = getDirectPairInput();
        setResult("正在匹配手动输入的视频与 posehand3D 文件...", true);
        data = await previewDirectPair({ videoPath, posehandPath });
        dom.epDir.value = data.ep_dir || epDir;
        state.set("directPairPaths", null);
      } else {
        clearDirectPairState();
        setResult("正在预览当前目录候选项...", true);
        data = await previewEpDir(epDir, {
          page: previewPager.page,
          pageSize: previewPager.pageSize,
          cursor: pageCursor,
        });
      }
      previewPager.page = Number(data.page || previewPager.page) || previewPager.page;
      previewPager.pageSize = normalizePageSize(data.page_size || previewPager.pageSize);
      previewPager.hasMore = !!data.has_more;
      previewPager.cursor = String(data.cursor || pageCursor || "");
      previewPager.nextCursor = String(data.next_cursor || "");
      previewPager.pageCursors[previewPager.page] = previewPager.cursor;
      if (!directPairMode && previewPager.nextCursor) {
        previewPager.pageCursors[previewPager.page + 1] = previewPager.nextCursor;
      }
      const pairs = Array.isArray(data.items) ? data.items : [];
      const childDirs = Array.isArray(data.child_dirs) ? data.child_dirs : [];
      if (pairs.length > 0) {
        renderPairList(data.ep_dir || epDir, pairs, {
          hasMore: !!data.has_more,
          offset: Number(data.offset || 0),
        });
      } else {
        hidePairList();
      }
      if (childDirs.length > 0) {
        renderParentEpList(data.ep_dir || epDir, childDirs, { hasMore: !!data.has_more });
      } else {
        hideParentEpList();
      }
      const total = pairs.length + childDirs.length;
      if (total > 0) {
        persistPreviewState({
          epDir: data.ep_dir || epDir,
          selectedItemName: state.get("selectedItemName"),
        });
        const suffix = data.has_more ? "，可继续下一页" : "";
        setResult(
          data.source === "manual-pair"
            ? (data.message || `手动路径匹配完成，已生成 ${pairs.length} 条可预览配对`)
            : `目录预览完成，第 ${previewPager.page} 页展示 ${total} 条候选${suffix}`,
          true,
        );
        return;
      }
      setResult(previewPager.page > 1 ? "当前页没有更多候选" : "当前目录未发现视频候选或子目录", false);
      if (data?.source === "manual-pair" && data?.message) {
        setResult(data.message, false);
      }
    } catch (e) {
      setResult(formatError(e), false);
    } finally {
      state.set("queryInFlight", false);
    }
  }

  async function copyEpDir(epDir) {
    const ok = await copyText(epDir);
    setResult(ok ? "已复制完整 ep_dir" : "复制失败，请手动复制", ok);
  }

  async function copyLastResult() {
    const lastResultData = state.get("lastResultData");
    if (!lastResultData) {
      setResult("暂无可复制结果，请先查询", false);
      return;
    }
    const withPosehand = String(lastResultData.posehand_path || "").trim();
    const text =
`ep_dir:
${lastResultData.ep_dir || ""}

item_name:
${lastResultData.item_name || ""}

source:
${lastResultData.source || ""}

video_url:
${lastResultData.video_url || ""}${withPosehand ? `\n\nposehand_path:\n${withPosehand}` : ""}`;

    const ok = await copyText(text);
    setResult(ok ? "已一键复制查询结果" : "复制失败，请手动复制", ok);
  }

  function openPlayback() {
    const params = new URLSearchParams();
    const epDir = (dom.epDir.value || "").trim();
    const lastResultData = state.get("lastResultData");
    const selectedItemName = String(state.get("selectedItemName") || "").trim();

    if (!lastResultData && !selectedItemName && epDir) {
      setResult("请先在配对列表选择 item_name，或先快速预览", false);
      return;
    }
    if (epDir) params.set("ep_dir", epDir);

    const itemName = String(lastResultData?.item_name || selectedItemName || "").trim();
    if (!itemName && epDir) {
      setResult("当前目录尚未选择 item_name，请先在配对列表点击“快速预览”或“播放页”", false);
      return;
    }
    if (itemName) params.set("item_name", itemName);
    const directPairPaths = state.get("directPairPaths") || directPairPathsForItem(epDir, itemName) || {};
    if (directPairPaths.video_path) params.set("video_path", directPairPaths.video_path);
    if (directPairPaths.posehand_path) params.set("posehand_path", directPairPaths.posehand_path);
    persistPreviewState({ epDir, selectedItemName: itemName });

    // 联合播放统一走“播放页重新构建并拿最新 URL”路径：
    // 1) 直连 OSS 时避免签名过期；
    // 2) 本地转码 URL 场景下也能拿到最新可播地址。
    params.set("auto_play", "1");
    const t = Number(dom.player?.currentTime || 0);
    if (Number.isFinite(t) && t > 0) params.set("t", String(t));
    const spd = Number(dom.player?.playbackRate || 1);
    if (Number.isFinite(spd) && spd > 0) params.set("speed", String(spd));

    const q = params.toString();
    window.location.href = q ? `/playback?${q}` : "/playback";
  }

  function bindParentEpActions() {
    dom.parentEpBody?.addEventListener("click", async (event) => {
      const btn = event.target?.closest?.("button[data-action][data-ep-dir]");
      if (!btn) return;
      const action = String(btn.getAttribute("data-action") || "");
      const epDir = String(btn.getAttribute("data-ep-dir") || "").trim();
      if (!epDir) return;

      if (action === "playback") {
        persistPreviewState({ epDir, selectedItemName: state.get("selectedItemName") });
        openPlaybackByEp(epDir);
        return;
      }
      if (action === "query") {
        dom.epDir.value = epDir;
        await queryAndRender({ forceRefresh: false, overrideEpDir: epDir, page: 1, cursor: "", resetCursor: true });
      }
    });
  }

  function bindPairActions() {
    dom.pairBody?.addEventListener("click", async (event) => {
      const btn = event.target?.closest?.("button[data-action][data-ep-dir][data-item-name]");
      if (!btn) return;
      const action = String(btn.getAttribute("data-action") || "");
      const epDir = String(btn.getAttribute("data-ep-dir") || "").trim();
      const itemName = String(btn.getAttribute("data-item-name") || "").trim();
      if (!epDir || !itemName) return;

      if (action === "score") {
        openScoreDialog({ epDir, itemName });
        return;
      }

      if (action === "playback") {
        persistPreviewState({ epDir, selectedItemName: itemName });
        openPlaybackByEp(epDir, itemName, directPairPathsForItem(epDir, itemName));
        return;
      }
      if (action === "build") {
        try {
          const currentPairs = state.get("currentPairs") || [];
          state.set("selectedItemName", itemName);
          state.set("directPairPaths", directPairPathsForItem(epDir, itemName));
          persistPreviewState({ epDir, selectedItemName: itemName });
          renderPairList(epDir, currentPairs, {
            hasMore: previewPager.hasMore,
            offset: (previewPager.page - 1) * previewPager.pageSize,
          });
          await buildSinglePair(epDir, itemName, { forceRefresh: false });
        } catch (e) {
          setResult(formatError(e), false);
        }
      }
    });
  }

  function submitQuery() {
    queryAndRender({ forceRefresh: false, page: 1, cursor: "", resetCursor: true });
  }

  function submitRefresh() {
    queryAndRender({ forceRefresh: true, page: 1, cursor: "", resetCursor: true });
  }

  async function changePreviewPageSize(value) {
    await queryAndRender({ page: 1, pageSize: normalizePageSize(value), cursor: "", resetCursor: true });
  }

  async function goPreviewPage(delta) {
    const step = Number(delta || 0);
    const nextPage = previewPager.page + step;
    if (nextPage < 1) return;
    if (step > 0 && (!previewPager.hasMore || !previewPager.nextCursor)) return;
    const nextCursor = step > 0 ? previewPager.nextCursor : (previewPager.pageCursors[nextPage] || "");
    await queryAndRender({ page: nextPage, pageSize: previewPager.pageSize, cursor: nextCursor });
  }

  function bindPreviewPager() {
    dom.parentPrevPageBtn?.addEventListener("click", () => goPreviewPage(-1));
    dom.pairPrevPageBtn?.addEventListener("click", () => goPreviewPage(-1));
    dom.parentNextPageBtn?.addEventListener("click", () => goPreviewPage(1));
    dom.pairNextPageBtn?.addEventListener("click", () => goPreviewPage(1));
    dom.parentPageSize?.addEventListener("change", (event) => changePreviewPageSize(event.target.value));
    dom.pairPageSize?.addEventListener("change", (event) => changePreviewPageSize(event.target.value));
    syncAllPagerUI();
  }

  bindPreviewPager();
  bindParentEpActions();
  bindPairActions();
  hideParentEpList();
  hidePairList();
  setPreviewVisible(false);

  async function restoreFromSession() {
    const saved = readPersistedPreviewState();
    if (!saved) return;
    dom.epDir.value = saved.epDir;
    if (saved.selectedItemName) {
      state.set("selectedItemName", saved.selectedItemName);
    }
    await queryAndRender({
      overrideEpDir: saved.epDir,
      page: saved.page,
      pageSize: saved.pageSize,
      cursor: saved.cursor,
      resetCursor: false,
    });
  }

  return {
    queryAndRender,
    copyEpDir,
    copyLastResult,
    openPlayback,
    submitQuery,
    submitRefresh,
    restoreFromSession,
  };
}
