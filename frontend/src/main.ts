import "./style.css";
import { CHANNELS, SviewApi, canvasDomId, playChannels, streamLabel } from "./api";
import { sharedClientId, SviewPlayer } from "./player";

const params = new URLSearchParams(location.search);
// Prefer Vite proxy so Master→OSS 302 is followed in Node (no browser CORS).
// Override: ?base=http://47.x.x.x:8080  (needs OSS Bucket CORS)
const defaultBase = params.get("base") ?? `${location.origin}/sview`;
const defaultSource =
  params.get("source") ??
  "oss://ss-oss-data-platform/original_data/ego_human_data/head_cam/SSIEGO_Cam_001V_20260805_132726_367_fixed.mcap";
const defaultJob = params.get("job") ?? "";
const defaultForce = ["1", "true", "yes"].includes(
  (params.get("force") ?? "").toLowerCase(),
);
const defaultStartSec = params.get("start_sec") ?? "";
const defaultSparse =
  params.get("publish_mode") === "sparse" ||
  ["1", "true", "yes"].includes((params.get("sparse") ?? "").toLowerCase());

const app = document.querySelector<HTMLDivElement>("#app")!;

app.innerHTML = `
  <header class="top">
    <div class="brand">
      <h1>SView</h1>
      <span class="tag">submit · cache · play</span>
    </div>
    <p class="hint">
      提交 mcap → Master 入队 → Worker 转码 → OSS player-bin。媒体默认
      <strong>签名直连公网 OSS</strong>（API 仍走 Base URL）。CORS 失败会回退 Master。
      调试：<code>?media=proxy</code> 强制走 Master；<code>?start_sec=3000</code> 从 50 分钟切开转。
    </p>
    <div class="controls">
      <label class="grow">
        Base URL
        <input id="base" spellcheck="false" value="${defaultBase}" />
      </label>
    </div>
    <div class="controls">
      <label class="grow">
        OSS source
        <input id="source" spellcheck="false" value="${defaultSource}" />
      </label>
      <label class="check" title="取消进行中的任务、清 play 缓存后重新入队">
        <input id="force" type="checkbox"${defaultForce ? " checked" : ""} />
        强制重跑
      </label>
      <label class="check" title="同一 Job 按 seek 位置独立转码并发布稀疏段">
        <input id="sparse" type="checkbox"${defaultSparse ? " checked" : ""} />
        稀疏按需 Seek
      </label>
      <label title="从袋子开头跳过的秒数，用于直接转后半段">
        起点(秒)
        <input id="start-sec" spellcheck="false" value="${defaultStartSec}" placeholder="0" />
      </label>
      <button class="btn" id="submit" type="button">提交并播放</button>
    </div>
    <div class="controls">
      <label>
        Job ID（可选，已缓存可直接加载）
        <input id="job" spellcheck="false" value="${defaultJob}" placeholder="SSIEGO_Cam_..." />
      </label>
      <button class="btn secondary" id="load" type="button">仅加载</button>
      <button class="btn danger" id="cancel" type="button">取消任务</button>
    </div>
    <div class="controls queue-row">
      <div class="queue" id="queue">队列：—</div>
      <button class="btn secondary" id="refresh-stats" type="button">刷新队列</button>
    </div>
    <div class="status" id="status">Ready</div>
    <div class="timing" id="timing">耗时：—</div>
  </header>
  <main class="stage">
    <div class="grid" id="grid" data-n="${CHANNELS.length}">
      ${CHANNELS.map(
        (ch) => `
        <div class="cam" data-ch="${ch}">
          <canvas id="${canvasDomId(ch)}"></canvas>
          <span class="label">${ch.toUpperCase()}</span>
        </div>`,
      ).join("")}
    </div>
    <div class="transport">
      <div class="btns">
        <button class="btn secondary" id="play" type="button" disabled>播放</button>
        <button class="btn secondary" id="pause" type="button" disabled>暂停</button>
      </div>
      <input id="seek" type="range" min="0" max="0" value="0" disabled />
      <div class="meta" id="meta">— / —</div>
    </div>
    <pre class="imu" id="imu">IMU —</pre>
  </main>
`;

const el = {
  base: app.querySelector<HTMLInputElement>("#base")!,
  source: app.querySelector<HTMLInputElement>("#source")!,
  force: app.querySelector<HTMLInputElement>("#force")!,
  sparse: app.querySelector<HTMLInputElement>("#sparse")!,
  startSec: app.querySelector<HTMLInputElement>("#start-sec")!,
  job: app.querySelector<HTMLInputElement>("#job")!,
  submit: app.querySelector<HTMLButtonElement>("#submit")!,
  load: app.querySelector<HTMLButtonElement>("#load")!,
  cancel: app.querySelector<HTMLButtonElement>("#cancel")!,
  refreshStats: app.querySelector<HTMLButtonElement>("#refresh-stats")!,
  queue: app.querySelector<HTMLDivElement>("#queue")!,
  status: app.querySelector<HTMLDivElement>("#status")!,
  timing: app.querySelector<HTMLDivElement>("#timing")!,
  play: app.querySelector<HTMLButtonElement>("#play")!,
  pause: app.querySelector<HTMLButtonElement>("#pause")!,
  seek: app.querySelector<HTMLInputElement>("#seek")!,
  meta: app.querySelector<HTMLDivElement>("#meta")!,
  imu: app.querySelector<HTMLPreElement>("#imu")!,
};

if (!("VideoDecoder" in window)) {
  el.status.classList.add("error");
  el.status.textContent =
    "当前浏览器不支持 WebCodecs VideoDecoder，请使用较新的 Chrome / Edge。";
}

function formatSec(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

/** Client-side timing for submit → playable / first frame / fully ready. */
let timingStart = 0;
let timingPlayableMs: number | null = null;
let timingFirstFrameMs: number | null = null;
let timingReadyMs: number | null = null;
let timingArmed = false;
let readyWatchToken = 0;

function resetTiming() {
  timingStart = 0;
  timingPlayableMs = null;
  timingFirstFrameMs = null;
  timingReadyMs = null;
  timingArmed = false;
  readyWatchToken += 1;
  el.timing.textContent = "耗时：—";
}

function renderTiming() {
  const parts: string[] = [];
  if (timingPlayableMs != null) {
    parts.push(`可起播 ${formatSec(timingPlayableMs)}`);
  }
  if (timingFirstFrameMs != null) {
    parts.push(`首帧上屏 ${formatSec(timingFirstFrameMs)}`);
  }
  if (timingReadyMs != null) {
    parts.push(`全解完 ${formatSec(timingReadyMs)}`);
  } else if (timingStart && timingPlayableMs != null) {
    parts.push("全解完 …");
  }
  el.timing.textContent = parts.length
    ? `耗时：${parts.join(" · ")}`
    : "耗时：计时中…";
}

function markPlayable() {
  if (!timingStart || timingPlayableMs != null) return;
  timingPlayableMs = performance.now() - timingStart;
  renderTiming();
}

function markFirstFrame() {
  if (!timingArmed || !timingStart || timingFirstFrameMs != null) return;
  timingFirstFrameMs = performance.now() - timingStart;
  timingArmed = false;
  renderTiming();
}

function markReady() {
  if (!timingStart || timingReadyMs != null) return;
  timingReadyMs = performance.now() - timingStart;
  renderTiming();
}

/** After progressive playable, keep watching until job is fully ready. */
function watchUntilFullyReady(api: SviewApi, jobId: string) {
  const token = ++readyWatchToken;
  void (async () => {
    try {
      const done = await api.waitUntilReady(jobId, {
        intervalMs: 1000,
        timeoutMs: 30 * 60 * 1000,
      });
      if (token !== readyWatchToken) return;
      if (done.status === "ready" || done.cached) {
        markReady();
      }
    } catch {
      /* ignore; timing line keeps "全解完 …" */
    }
  })();
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function canvasesFromGrid(channels: string[]): Record<string, HTMLCanvasElement> {
  return Object.fromEntries(
    channels.map((ch) => {
      const elCv = app.querySelector<HTMLCanvasElement>(`#${canvasDomId(ch)}`);
      if (!elCv) throw new Error(`missing canvas for ${ch}`);
      return [ch, elCv];
    }),
  );
}

function rebuildGrid(channels: string[]) {
  const grid = app.querySelector<HTMLDivElement>("#grid");
  if (!grid) return;
  const ids = channels.length ? channels : [...CHANNELS];
  grid.dataset.n = String(ids.length);
  grid.innerHTML = ids
    .map(
      (ch) => `
        <div class="cam" data-ch="${escapeHtml(ch)}">
          <canvas id="${canvasDomId(ch)}"></canvas>
          <span class="label">${escapeHtml(streamLabel(ch))}</span>
        </div>`,
    )
    .join("");
  player.setCanvases(canvasesFromGrid(ids));
}

let lastChannelSig = CHANNELS.join("\0");

const player = new SviewPlayer(canvasesFromGrid([...CHANNELS]), {
  onStatus: (t) => {
    el.status.classList.remove("error");
    el.status.textContent = t;
  },
  onError: (e) => {
    const msg = e.message || String(e);
    if (/加载|loading|404|425|pending|not ready/i.test(msg)) {
      el.status.classList.remove("error");
      el.status.textContent = msg.startsWith("加载") ? msg : `加载中… ${msg}`;
      return;
    }
    el.status.classList.add("error");
    el.status.textContent = msg;
    console.error(e);
  },
  onManifest: (m) => {
    const chs = playChannels(m);
    const sig = chs.join("\0");
    if (sig !== lastChannelSig) {
      lastChannelSig = sig;
      rebuildGrid(chs);
    }
    const total = m.bundle.frame_count ?? 1;
    el.seek.max = String(Math.max(0, total - 1));
  },
  onFrame: (index, tsNs, imu) => {
    if (seekHold != null) return;
    if (index === 0) markFirstFrame();
    const m = player.Manifest;
    const total = m?.bundle.frame_count ?? 0;
    const avail =
      m?.publish_mode === "sparse"
        ? total
        : m?.bundle.frames_available ?? total;
    const dur = m?.bundle.timeline?.duration_sec;
    el.seek.value = String(index);
    const durTxt =
      typeof dur === "number" && dur > 0 ? ` · ${dur.toFixed(1)}s` : "";
    el.meta.textContent =
      `${index} / ${Math.max(0, total - 1)}` +
      (avail < total ? ` (可用 ${avail})` : "") +
      durTxt +
      ` · t=${tsNs}`;
    if (imu && typeof imu === "object") {
      const samples = (imu as { samples?: unknown[] }).samples;
      const n = Array.isArray(samples) ? samples.length : 0;
      const head = Array.isArray(samples) ? samples.slice(0, 2) : [];
      el.imu.textContent =
        `IMU samples=${n}\n` + (head.length ? JSON.stringify(head, null, 0) : "—");
    } else {
      el.imu.textContent = "IMU —";
    }
  },
});

function setTransportEnabled(on: boolean) {
  el.play.disabled = !on;
  el.pause.disabled = !on;
  el.seek.disabled = !on;
}

function syncUrl(base: string, job: string, source: string) {
  const url = new URL(location.href);
  url.searchParams.set("base", base);
  if (job) url.searchParams.set("job", job);
  else url.searchParams.delete("job");
  if (source) url.searchParams.set("source", source);
  if (el.force.checked) url.searchParams.set("force", "1");
  else url.searchParams.delete("force");
  if (el.sparse.checked) url.searchParams.set("publish_mode", "sparse");
  else url.searchParams.delete("publish_mode");
  const startSec = readStartSec();
  if (!el.sparse.checked && startSec > 0) {
    url.searchParams.set("start_sec", String(startSec));
  }
  else url.searchParams.delete("start_sec");
  history.replaceState(null, "", url);
}

function readStartSec(): number {
  const n = Number(el.startSec.value.trim());
  if (!Number.isFinite(n) || n <= 0) return 0;
  return n;
}

function currentApi(): SviewApi {
  const base = el.base.value.trim().replace(/\/+$/, "");
  if (!base) throw new Error("请填写 Base URL");
  return new SviewApi(base);
}

async function refreshQueueStats() {
  try {
    const stats = await currentApi().getJobStats();
    const ids = (stats.jobs || [])
      .slice(0, 4)
      .map((j) => j.job_id)
      .join(", ");
    const more =
      (stats.jobs?.length || 0) > 4
        ? ` …+${stats.jobs.length - 4}`
        : ids
          ? ` · ${ids}`
          : "";
    const workers =
      typeof stats.workers_alive === "number" ? ` · worker ${stats.workers_alive}` : "";
    el.queue.textContent =
      `队列：排队 ${stats.queued} · 处理中 ${stats.processing} · 合计 ${stats.inflight}` +
      workers +
      more;
    el.queue.classList.remove("error");
  } catch (e) {
    el.queue.classList.add("error");
    el.queue.textContent =
      `队列：不可用（${e instanceof Error ? e.message : String(e)}）`;
  }
}

async function loadJob(base: string, jobId: string, mediaBase?: string) {
  await player.load(base, jobId, undefined, { mediaBase });
  await player.warmup();
  const m = player.Manifest;
  const total = m?.bundle.frame_count ?? 1;
  el.seek.max = String(Math.max(0, total - 1));
  el.seek.value = "0";
  el.job.value = jobId;
  setTransportEnabled(true);
  el.status.classList.remove("error");
  el.status.textContent = `${jobId} · 已缓冲，可播放`;
}

let touchTimer = 0;
let touchJobId = "";

function stopTouchLoop() {
  if (touchTimer) {
    window.clearInterval(touchTimer);
    touchTimer = 0;
  }
  touchJobId = "";
}

function startTouchLoop(api: SviewApi, jobId: string) {
  stopTouchLoop();
  touchJobId = jobId;
  touchTimer = window.setInterval(() => {
    if (!touchJobId) return;
    void api.touchJob(touchJobId).catch(() => {
      /* ignore */
    });
  }, 60_000);
  void api.touchJob(jobId).catch(() => {
    /* ignore */
  });
}

el.force.addEventListener("change", () => {
  try {
    syncUrl(
      el.base.value.trim().replace(/\/+$/, ""),
      el.job.value.trim(),
      el.source.value.trim(),
    );
  } catch {
    /* ignore */
  }
});

el.sparse.addEventListener("change", () => {
  el.startSec.disabled = el.sparse.checked;
  try {
    syncUrl(
      el.base.value.trim().replace(/\/+$/, ""),
      el.job.value.trim(),
      el.source.value.trim(),
    );
  } catch {
    /* ignore */
  }
});
el.startSec.disabled = el.sparse.checked;

el.refreshStats.addEventListener("click", () => {
  void refreshQueueStats();
});

el.cancel.addEventListener("click", async () => {
  el.cancel.disabled = true;
  try {
    const job = el.job.value.trim();
    if (!job) throw new Error("请填写要取消的 Job ID");
    const api = currentApi();
    el.status.classList.remove("error");
    el.status.textContent = `取消 ${job}…`;
    const result = await api.cancelJob(job);
    el.status.textContent = `已取消 ${result.job_id}（${result.status}）`;
    readyWatchToken += 1;
    void refreshQueueStats();
  } catch (e) {
    el.status.classList.add("error");
    el.status.textContent = e instanceof Error ? e.message : String(e);
  } finally {
    el.cancel.disabled = false;
  }
});

el.submit.addEventListener("click", async () => {
  el.submit.disabled = true;
  el.load.disabled = true;
  el.cancel.disabled = true;
  setTransportEnabled(false);
  el.status.classList.remove("error");
  resetTiming();
  timingStart = performance.now();
  timingArmed = true;
  el.timing.textContent = "耗时：计时中…";
  try {
    const base = el.base.value.trim().replace(/\/+$/, "");
    const source = el.source.value.trim();
    if (!base) throw new Error("请填写 Base URL");
    if (!source) throw new Error("请填写 OSS source");

    const api = new SviewApi(base);
    const force = el.force.checked;
    const startSec = readStartSec();
    const publishMode = el.sparse.checked ? "sparse" : "progressive";
    el.status.textContent = publishMode === "sparse"
      ? "提交稀疏按需任务…"
      : startSec > 0
      ? `从 ${startSec}s 切开提交…`
      : force
        ? "强制提交任务…"
        : "提交任务…";
    const created = await api.submitJob(source, force, {
      startSec: publishMode === "sparse" ? 0 : startSec,
      publishMode,
      clientId: publishMode === "sparse" ? sharedClientId() : undefined,
    });
    if (publishMode === "sparse") {
      await api.requestEncode(created.job_id, {
        frame_index: 0,
        prefetch_segments: 3,
        client_epoch: 0,
        client_id: sharedClientId(),
      });
    }
    el.job.value = created.job_id;
    syncUrl(base, created.job_id, source);
    void refreshQueueStats();

    if (created.status === "ready" || created.cached) {
      markPlayable();
      markReady();
      el.status.textContent = `已缓存 ${created.job_id}，加载中…`;
      // Media via Master (302 → signed OSS when private bucket). Avoid browser 403 on play_base.
      await loadJob(base, created.job_id);
      startTouchLoop(api, created.job_id);
      player.play();
      return;
    }

    el.status.textContent = `已入队 ${created.job_id}（${created.status}），SSE 等待可播放…`;
    const ready = await api.waitUntilPlayable(created.job_id, {
      intervalMs: 800,
      timeoutMs: 15 * 60 * 1000,
      // Start only after ~5s of published frames so playhead is not glued to the frontier.
      // Sparse segments are independently decodable; one published frame range
      // is enough to load, while progressive keeps its larger safety lead.
      minFrames: publishMode === "sparse" ? 1 : 150,
      preferSse: true,
      allowPollFallback: true,
      onTick: (s) => {
        const done = s.frames_done;
        const total = s.frame_count;
        let prog = "";
        if (done != null && total != null && total > 0) {
          const pct =
            typeof s.percent === "number"
              ? Math.round(s.percent)
              : Math.min(100, Math.round((done / total) * 100));
          prog = ` · ${done}/${total} 帧 (${pct}%)`;
        } else if (done != null) {
          prog = ` · frames_done=${done}`;
        }
        if (s.playable) prog += " · playable";
        if (s.play_base) prog += " · oss";
        if (s.encode_status) {
          prog += ` · ${s.encode_status}`;
          if (s.output_frames != null) prog += ` ${s.output_frames}`;
        }
        const elapsed = formatSec(performance.now() - timingStart);
        el.status.textContent = `job=${s.job_id} status=${s.status}${prog} · 已等待 ${elapsed}`;
      },
    });
    markPlayable();
    if (ready.status === "ready" || ready.cached) {
      markReady();
    } else {
      watchUntilFullyReady(api, ready.job_id);
    }
    const label =
      ready.status === "ready" || ready.cached
        ? `ready ${ready.job_id}`
        : `playable ${ready.job_id} (${ready.frames_done ?? "?"}/${ready.frame_count ?? "?"} frames)`;
    el.status.textContent = `${label}，加载播放器…`;
    await loadJob(base, ready.job_id);
    startTouchLoop(api, ready.job_id);
    player.play();
  } catch (e) {
    timingArmed = false;
    el.status.classList.add("error");
    el.status.textContent = e instanceof Error ? e.message : String(e);
  } finally {
    el.submit.disabled = false;
    el.load.disabled = false;
    el.cancel.disabled = false;
    void refreshQueueStats();
  }
});

el.load.addEventListener("click", async () => {
  el.load.disabled = true;
  el.submit.disabled = true;
  setTransportEnabled(false);
  try {
    const base = el.base.value.trim().replace(/\/+$/, "");
    const job = el.job.value.trim();
    if (!base) throw new Error("请填写 Base URL");
    if (!job) throw new Error("请填写 Job ID，或改用「提交并播放」");
    syncUrl(base, job, el.source.value.trim());
    const api = new SviewApi(base);
    await loadJob(base, job);
    startTouchLoop(api, job);
  } catch (e) {
    el.status.classList.add("error");
    el.status.textContent = e instanceof Error ? e.message : String(e);
  } finally {
    el.load.disabled = false;
    el.submit.disabled = false;
  }
});

el.play.addEventListener("click", () => {
  clearSeekHold();
  player.play();
});
el.pause.addEventListener("click", () => player.pause());

void refreshQueueStats();
window.setInterval(() => {
  void refreshQueueStats();
}, 5000);

let seekTimer = 0;
let seekRequestEpoch = 0;
/** Frame the user asked for while it is still being encoded. Slider stays here. */
let seekHold: number | null = null;
let seekHoldTimer = 0;

function clearSeekHold() {
  seekRequestEpoch += 1;
  seekHold = null;
  if (seekHoldTimer) {
    window.clearInterval(seekHoldTimer);
    seekHoldTimer = 0;
  }
}

function holdSeek(idx: number) {
  if (player.Sparse) return;
  seekHold = idx;
  el.seek.value = String(idx);
  const avail = Math.max(0, player.Available - 1);
  el.status.classList.remove("error");
  el.status.textContent = `第 ${idx} 帧还没转完，当前可播到 ${avail}。转到这一帧再跳过去`;
  if (seekHoldTimer) return;
  seekHoldTimer = window.setInterval(() => {
    const target = seekHold;
    if (target == null) {
      clearSeekHold();
      return;
    }
    void player
      .refreshManifest()
      .then(async () => {
        if (seekHold !== target) return;
        if (target > Math.max(0, player.Available - 1)) {
          el.seek.value = String(target);
          el.status.classList.remove("error");
          el.status.textContent =
            `第 ${target} 帧还没转完，当前可播到 ${Math.max(0, player.Available - 1)}。转到这一帧再跳过去`;
          return;
        }
        clearSeekHold();
        await player.seek(target);
      })
      .catch((e) => {
        el.status.classList.add("error");
        el.status.textContent = e instanceof Error ? e.message : String(e);
      });
  }, 1000);
}

el.seek.addEventListener("input", () => {
  const idx = Number(el.seek.value);
  const requestEpoch = ++seekRequestEpoch;
  seekHold = null;
  if (seekHoldTimer) {
    window.clearInterval(seekHoldTimer);
    seekHoldTimer = 0;
  }
  el.meta.textContent = `→ ${idx}`;
  window.clearTimeout(seekTimer);
  seekTimer = window.setTimeout(() => {
    if (requestEpoch !== seekRequestEpoch) return;
    void player.seek(idx).then((ok) => {
      if (requestEpoch !== seekRequestEpoch) return;
      if (!ok && !player.Sparse) holdSeek(idx);
    }).catch((e) => {
      if (requestEpoch !== seekRequestEpoch) return;
      el.status.classList.add("error");
      el.status.textContent = e instanceof Error ? e.message : String(e);
    });
  }, 80);
});
