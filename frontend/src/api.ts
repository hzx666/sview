export type StreamMeta = {
  position: string;
  width: number;
  height: number;
  fps: number;
  source_topic?: string;
  source_codec?: string;
  stored_codec?: string;
};

export type Manifest = {
  job_id: string;
  format: string;
  publish_mode?: "progressive" | "sparse";
  /** 发布端已接受的最新稀疏编码请求。 */
  segment_epoch?: number;
  /** 共享缓存发布版本，单调递增。 */
  manifest_revision?: number;
  /** 状态 partial = 还在写帧；done = 已完成 */
  status: string;
  /** 可选。边转边播缓存的 OSS/CDN 前缀。 */
  play_base?: string;
  /** 从包起点跳过的秒数（从 T 秒起转）。 */
  origin_sec?: number;
  bundle: {
    frame_count: number;
    /** 盘上已有、可以拉取的帧数（边转边播过程中会增长）。 */
    frames_available?: number;
    /** 已发布的稀疏帧区间，半开区间 [start, end)。 */
    available_ranges?: [number, number][];
    gop_size: number;
    key_frame_indices: number[];
    timeline: {
      start_ns: number;
      end_ns: number;
      duration_sec?: number;
    };
  };
  streams: Record<string, StreamMeta>;
};

export type BundleHeader = {
  packet_schema: string;
  frame_index: number;
  timestamp_ns: number;
  time_window_ns: [number, number];
  key_frame: boolean;
  streams: Record<
    string,
    {
      payload_offset: number;
      payload_length: number;
      is_null?: boolean;
      codec?: string;
    }
  >;
  imu: { payload_offset: number; payload_length: number };
  events: { payload_offset: number; payload_length: number };
};

/** 打好包的单帧 bundle 批次（约 gop_size / SEGMENT_FRAMES）。 */
export type SegmentHeader = {
  packet_schema: string;
  start_index: number;
  count: number;
  segment_frames: number;
};

export type JobStatus = {
  job_id: string;
  status: string;
  source?: string;
  error?: string | null;
  "error-en"?: string | null;
  frame_count?: number | null;
  frames_done?: number | null;
  playable?: boolean | null;
  duration_sec?: number | null;
  cached?: boolean;
  local_path?: string;
  updated_at?: string;
  /** 有值时，前端应从该 OSS 前缀拉 manifest 和 segment。 */
  play_base?: string;
  /** 已知 frames_total 时的进度，0–100。 */
  percent?: number | null;
  cache_bytes?: number | null;
  last_access_at?: string | number | null;
  origin_sec?: number | null;
  publish_mode?: "progressive" | "sparse";
  available_ranges?: [number, number][];
  segments_ready_count?: number;
  segment_epoch?: number;
  encode_window?: [number, number] | null;
  encode_status?: string | null;
  encode_frontier?: number | null;
  decode_start?: number | null;
  input_frames?: number | null;
  output_frames?: number | null;
  cache_id?: string;
  manifest_revision?: number;
};

export type JobStatsSession = {
  client_id: string;
  pod_ip?: string;
  worker_id?: string;
  segment?: number;
};

export type JobStatsItem = {
  job_id: string;
  status: string;
  owner_pod?: string;
  source?: string;
  playable?: boolean;
  frames_done?: number;
  /** 同一个文件上同时在编的窗口。两个人各看各的位置时这里有两条。 */
  clients?: string[];
  sessions?: JobStatsSession[];
};

export type JobStats = {
  queued: number;
  processing: number;
  inflight: number;
  workers_alive?: number;
  cache_bytes?: number;
  jobs: JobStatsItem[];
};

export type EncodeRequest = {
  request_id: string;
  job_id?: string;
  status: string;
  target_start?: number;
  starts?: number[];
  client_id?: string;
  client_epoch?: number;
  error?: string;
};

/** 头显四路；遥操 v2 改用 Object.keys(manifest.streams)。 */
export const CHANNELS = ["rr", "fr", "rl", "fl"] as const;
export type Channel = string;

const HEAD_CAM_IDS = new Set<string>(CHANNELS);

/** 只认 `rr` / `camera_rr`。`camera_f_l/color/image_raw` 是遥操 topic。 */
function isHeadcamStreamKey(key: string): boolean {
  const low = key.toLowerCase();
  if (HEAD_CAM_IDS.has(low)) return true;
  const prefixed = low.startsWith("camera_") ? low.slice("camera_".length) : "";
  return HEAD_CAM_IDS.has(prefixed);
}

export function playChannels(m: Manifest | null | undefined): string[] {
  if (!m?.streams) return [...CHANNELS];
  const keys = Object.keys(m.streams);
  if (!keys.length) return [...CHANNELS];
  const format = (m.format || "").toLowerCase();
  const quad =
    format === "head_cam" ||
    (format !== "teleop_nstream" && keys.every(isHeadcamStreamKey));
  if (!quad) return keys;
  const v1Keys = CHANNELS.filter(
    (ch) => `camera_${ch}` in m.streams || ch in m.streams,
  );
  return v1Keys.length ? [...v1Keys] : [...CHANNELS];
}

export function manifestFps(m: Manifest | null | undefined): number {
  if (!m?.streams) return 30;
  for (const ch of playChannels(m)) {
    const s = m.streams[`camera_${ch}`] || m.streams[ch];
    if (s && s.fps > 0) return s.fps;
  }
  for (const s of Object.values(m.streams)) {
    if (s && s.fps > 0) return s.fps;
  }
  return 30;
}

export function streamLabel(key: string): string {
  const low = key.toLowerCase();
  if ((CHANNELS as readonly string[]).includes(low) && key.length <= 2) {
    return key.toUpperCase();
  }
  return key;
}

export function canvasDomId(key: string): string {
  return `cv-${key.replace(/[^A-Za-z0-9_-]+/g, "_")}`;
}

function joinUrl(base: string, path: string): string {
  const b = base.replace(/\/+$/, "");
  const p = path.startsWith("/") ? path : `/${path}`;
  if (!b) return p;
  return `${b}${p}`;
}

function abortableDelay(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(new DOMException("Aborted", "AbortError"));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(new DOMException("Aborted", "AbortError"));
      },
      { once: true },
    );
  });
}

export function framesAvailable(m: Manifest | null | undefined): number {
  if (!m) return 0;
  const avail = m.bundle.frames_available;
  if (typeof avail === "number" && avail >= 0) return avail;
  return m.bundle.frame_count ?? 0;
}

export function isManifestComplete(m: Manifest | null | undefined): boolean {
  if (!m) return false;
  const s = (m.status || "done").toLowerCase();
  return s === "done" || s === "ready";
}

/** 时间轴上有这一帧，盘上还没有（边转边播）。 */
export class FramePendingError extends Error {
  readonly index: number;
  readonly httpStatus: number;
  constructor(index: number, httpStatus: number) {
    super(`frame ${index} loading`);
    this.name = "FramePendingError";
    this.index = index;
    this.httpStatus = httpStatus;
  }
}

export function isFramePendingError(e: unknown): e is FramePendingError {
  return e instanceof FramePendingError ||
    (!!e && typeof e === "object" && (e as Error).name === "FramePendingError");
}

class SignedMediaPendingError extends Error {
  readonly httpStatus: number;

  constructor(httpStatus: number) {
    super(`signed media loading (${httpStatus})`);
    this.name = "SignedMediaPendingError";
    this.httpStatus = httpStatus;
  }
}

export class SviewApi {
  /**
   * 优先走 Master ?proxy=1 拉流。签名 OSS 失败后的回退，
   * 或 preferSignedOss 为 false 时使用。
   */
  forceMasterProxy = false;
  /**
   * 浏览器直连公网 OSS，用 Master 签发的预签名 URL（RTT 短）。
   * 默认开；遇到 CORS/403 会自己关掉，回退到 Master 代理。
   */
  preferSignedOss = true;
  private signedUrlCache = new Map<string, { url: string; expMs: number }>();
  private signedInflight: Promise<void> | null = null;

  constructor(
    public baseUrl: string,
    /** 有值时为匿名 OSS 前缀（少见；私有桶需要签名）。 */
    public mediaBase: string = "",
  ) {
    // 关掉：?media=proxy → 只走 Master 流；?media=signed → 强制签名 OSS
    try {
      const m = new URLSearchParams(location.search).get("media")?.toLowerCase();
      if (m === "proxy" || m === "master") {
        this.preferSignedOss = false;
        this.forceMasterProxy = true;
      } else if (m === "signed" || m === "oss") {
        this.preferSignedOss = true;
        this.forceMasterProxy = false;
      }
    } catch {
      /* 非浏览器 */
    }
  }

  withMediaBase(mediaBase: string): SviewApi {
    const next = new SviewApi(this.baseUrl, mediaBase || "");
    next.forceMasterProxy = this.forceMasterProxy;
    next.preferSignedOss = this.preferSignedOss;
    return next;
  }

  private mediaUrl(path: string): string {
    const base = (this.mediaBase || this.baseUrl).replace(/\/+$/, "");
    const p = path.startsWith("/") ? path : `/${path}`;
    return `${base}${p}`;
  }

  private disableSignedOss(reason: string) {
    if (!this.preferSignedOss) return;
    console.warn(`[sview] signed OSS disabled (${reason}); falling back to Master`);
    this.preferSignedOss = false;
    // 同源 Vite/Nginx 代理可以在服务端跟随 Master 的 OSS 跳转，
    // 避开桶的 CORS，也不必走更慢的 ?proxy=1。
    try {
      const apiOrigin = new URL(this.baseUrl || location.origin, location.href).origin;
      this.forceMasterProxy = apiOrigin !== location.origin;
    } catch {
      this.forceMasterProxy = true;
    }
    this.signedUrlCache.clear();
  }

  private signedCacheKey(jobId: string, startIndex: number): string {
    return `${jobId}:${startIndex}`;
  }

  /** 给即将用到的 segment 起点预签名（批量）。可频繁调用，走缓存。 */
  async prefetchSignedSegmentUrls(
    jobId: string,
    starts: number[],
    signal?: AbortSignal,
  ): Promise<void> {
    if (!this.preferSignedOss || starts.length === 0) return;
    const now = Date.now();
    const need = [
      ...new Set(
        starts.filter((s) => {
          if (s < 0) return false;
          const hit = this.signedUrlCache.get(this.signedCacheKey(jobId, s));
          return !hit || hit.expMs < now + 60_000;
        }),
      ),
    ].sort((a, b) => a - b);
    if (need.length === 0) return;

    const run = async () => {
      // 按 API 上限 32 分批
      for (let i = 0; i < need.length; i += 32) {
        const batch = need.slice(i, i + 32);
        const q = new URLSearchParams({
          kind: "segments",
          starts: batch.join(","),
        });
        const res = await fetch(
          joinUrl(this.baseUrl, `/api/jobs/${jobId}/signed-media?${q}`),
          { cache: "no-store", signal },
        );
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          if (res.status === 404 || res.status === 425) {
            throw new SignedMediaPendingError(res.status);
          }
          throw new Error(data.error || `signed-media ${res.status}`);
        }
        const expiresIn = Number(data.expires_in) || 7200;
        const expMs = Date.now() + Math.max(60, expiresIn - 120) * 1000;
        for (const it of data.items || []) {
          const start = Number(it.start);
          const url = String(it.url || "");
          if (!Number.isFinite(start) || !url) continue;
          if (url.includes("-internal.")) {
            throw new Error("signed URL is internal endpoint");
          }
          this.signedUrlCache.set(this.signedCacheKey(jobId, start), {
            url,
            expMs,
          });
        }
      }
    };

    // 合并并发调用：先加入在途请求，再重试 need（可能已空）。
    while (this.signedInflight) {
      await this.signedInflight.catch(() => undefined);
      if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
      return this.prefetchSignedSegmentUrls(jobId, starts, signal);
    }
    this.signedInflight = run().finally(() => {
      this.signedInflight = null;
    });
    try {
      await this.signedInflight;
    } catch (e) {
      if (e instanceof DOMException && e.name === "AbortError") throw e;
      if (e instanceof SignedMediaPendingError) throw e;
      this.disableSignedOss(e instanceof Error ? e.message : "sign failed");
      throw e;
    }
  }

  private async fetchSegmentViaSignedOss(
    jobId: string,
    startIndex: number,
    signal?: AbortSignal,
  ): Promise<
    | { ok: true; buf: ArrayBuffer }
    | { ok: false; status: number; retryable: boolean }
    | { ok: false; fatal: true }
  > {
    try {
      await this.prefetchSignedSegmentUrls(jobId, [startIndex], signal);
    } catch (e) {
      if (e instanceof DOMException && e.name === "AbortError") throw e;
      if (e instanceof SignedMediaPendingError) {
        return { ok: false, status: e.httpStatus, retryable: true };
      }
      return { ok: false, fatal: true };
    }
    const hit = this.signedUrlCache.get(this.signedCacheKey(jobId, startIndex));
    if (!hit?.url) return { ok: false, fatal: true };
    try {
      let fetchUrl = hit.url;
      try {
        const apiOrigin = new URL(
          this.baseUrl || location.origin,
          location.href,
        ).origin;
        if (location.port === "5173" && apiOrigin === location.origin) {
          const signed = new URL(hit.url);
          fetchUrl = `${location.origin}/sview-oss${signed.pathname}${signed.search}`;
        }
      } catch {
        /* 直接用签名 URL */
      }
      const res = await fetch(fetchUrl, {
        cache: "no-store",
        mode: "cors",
        signal,
      });
      if (res.ok) return { ok: true, buf: await res.arrayBuffer() };
      if (res.status === 404 || res.status === 425) {
        return { ok: false, status: res.status, retryable: true };
      }
      if (res.status === 403 || res.status === 401) {
        this.disableSignedOss(`OSS ${res.status}`);
        return { ok: false, fatal: true };
      }
      this.disableSignedOss(`OSS HTTP ${res.status}`);
      return { ok: false, fatal: true };
    } catch (e) {
      if (e instanceof DOMException && e.name === "AbortError") throw e;
      // 常见原因：桶没配 CORS
      this.disableSignedOss(e instanceof Error ? e.message : "OSS fetch failed");
      return { ok: false, fatal: true };
    }
  }

  async healthz(): Promise<boolean> {
    try {
      const res = await fetch(joinUrl(this.baseUrl, "/healthz"));
      return res.ok;
    } catch {
      return false;
    }
  }

  async submitJob(
    source: string,
    force = false,
    opts: {
      startSec?: number;
      publishMode?: "progressive" | "sparse";
      clientId?: string;
    } = {},
  ): Promise<JobStatus> {
    const body: Record<string, unknown> = { source, force };
    if (opts.publishMode) body.publish_mode = opts.publishMode;
    if (opts.clientId) body.client_id = opts.clientId;
    if (opts.startSec != null && opts.startSec > 0) {
      body.start_sec = opts.startSec;
    }
    const res = await fetch(joinUrl(this.baseUrl, "/api/jobs"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok && res.status !== 202) {
      throw new Error(data.error || `submit ${res.status}`);
    }
    return data as JobStatus;
  }

  async getJob(jobId: string): Promise<JobStatus> {
    const res = await fetch(joinUrl(this.baseUrl, `/api/jobs/${jobId}`));
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.error || data.status || `job ${res.status}`);
    }
    return data as JobStatus;
  }

  async cancelJob(jobId: string): Promise<JobStatus> {
    const res = await fetch(joinUrl(this.baseUrl, `/api/jobs/${jobId}/cancel`), {
      method: "POST",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.error || `cancel ${res.status}`);
    }
    return data as JobStatus;
  }

  async getJobStats(): Promise<JobStats> {
    const res = await fetch(joinUrl(this.baseUrl, "/api/jobs/stats"), {
      cache: "no-store",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.error || `stats ${res.status}`);
    }
    return data as JobStats;
  }

  async touchJob(jobId: string): Promise<JobStatus> {
    const res = await fetch(joinUrl(this.baseUrl, `/api/jobs/${jobId}/touch`), {
      method: "POST",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.error || `touch ${res.status}`);
    }
    return data as JobStatus;
  }

  /**
   * 先用 SSE 等到可播，连不上再轮询（ACS Ingress
   * 经常把 EventSource 缓冲到流结束）。
   */
  async waitUntilPlayable(
    jobId: string,
    opts: {
      intervalMs?: number;
      timeoutMs?: number;
      minFrames?: number;
      onTick?: (s: JobStatus) => void;
      preferSse?: boolean;
      /** 默认 true：EventSource 失败后改轮询 GET /api/jobs/{id}。 */
      allowPollFallback?: boolean;
    } = {},
  ): Promise<JobStatus> {
    const timeoutMs = opts.timeoutMs ?? 15 * 60 * 1000;
    const minFrames = opts.minFrames ?? 30;
    const preferSse = opts.preferSse !== false;
    const allowPollFallback = opts.allowPollFallback !== false;

    const isDone = (s: JobStatus) => {
      if (s.status === "failed") throw new Error(s.error || "job failed");
      if (s.status === "cancelled") throw new Error(s.error || "job cancelled");
      if (s.status === "ready" || s.cached) return true;
      const done = s.frames_done ?? 0;
      // 起播前要有真实的帧领先量——单靠 API playable（约 30）太薄。
      return done >= minFrames;
    };

    if (preferSse && typeof EventSource !== "undefined") {
      try {
        return await this.waitUntilPlayableSse(jobId, {
          timeoutMs,
          minFrames,
          onTick: opts.onTick,
        });
      } catch (e) {
        if (!allowPollFallback) {
          const msg = e instanceof Error ? e.message : String(e);
          throw new Error(`SSE-only: ${msg}`);
        }
        console.warn("[sview] SSE failed, falling back to poll:", e);
      }
    } else if (!allowPollFallback) {
      throw new Error("SSE-only: EventSource unavailable");
    }

    const baseInterval = opts.intervalMs ?? 1200;
    const t0 = Date.now();
    let attempt = 0;
    while (Date.now() - t0 < timeoutMs) {
      const s = await this.getJob(jobId);
      opts.onTick?.(s);
      if (isDone(s)) return s;
      const delay = Math.min(8000, baseInterval * Math.pow(1.35, Math.min(attempt, 8)));
      attempt += 1;
      await new Promise((r) => setTimeout(r, delay));
    }
    throw new Error("timeout waiting for job playable");
  }

  private waitUntilPlayableSse(
    jobId: string,
    opts: {
      timeoutMs: number;
      minFrames: number;
      onTick?: (s: JobStatus) => void;
    },
  ): Promise<JobStatus> {
    const url = joinUrl(this.baseUrl, `/api/jobs/${jobId}/events`);
    return new Promise((resolve, reject) => {
      console.info("[sview] SSE connect", url);
      const es = new EventSource(url);
      let opened = false;
      const timer = window.setTimeout(() => {
        es.close();
        reject(new Error(`sse timeout (${opts.timeoutMs}ms) ${url}`));
      }, opts.timeoutMs);

      const finish = (fn: () => void) => {
        window.clearTimeout(timer);
        es.close();
        fn();
      };

      const onPayload = (raw: string) => {
        try {
          const s = JSON.parse(raw) as JobStatus;
          opts.onTick?.(s);
          if (s.status === "failed") {
            finish(() => reject(new Error(s.error || "job failed")));
            return;
          }
          if (s.status === "cancelled") {
            finish(() => reject(new Error(s.error || "job cancelled")));
            return;
          }
          const done = s.frames_done ?? 0;
          if (s.status === "ready" || s.cached || done >= opts.minFrames) {
            finish(() => resolve(s));
          }
        } catch (e) {
          finish(() =>
            reject(e instanceof Error ? e : new Error(String(e))),
          );
        }
      };

      es.onopen = () => {
        opened = true;
        console.info("[sview] SSE open", url);
      };
      es.addEventListener("progress", (ev) => {
        onPayload((ev as MessageEvent).data);
      });
      es.addEventListener("done", (ev) => {
        onPayload((ev as MessageEvent).data);
      });
      es.onerror = () => {
        // EventSource readyState：0 连接中，1 已打开，2 已关闭
        const state = es.readyState;
        finish(() =>
          reject(
            new Error(
              opened
                ? `sse connection lost (readyState=${state}) ${url}`
                : `sse connect failed (readyState=${state}) ${url}`,
            ),
          ),
        );
      };
    });
  }

  async waitUntilReady(
    jobId: string,
    opts: { intervalMs?: number; timeoutMs?: number; onTick?: (s: JobStatus) => void } = {},
  ): Promise<JobStatus> {
    const baseInterval = opts.intervalMs ?? 2000;
    const timeoutMs = opts.timeoutMs ?? 10 * 60 * 1000;
    const t0 = Date.now();
    let attempt = 0;
    while (Date.now() - t0 < timeoutMs) {
      const s = await this.getJob(jobId);
      opts.onTick?.(s);
      if (s.status === "ready") return s;
      if (s.status === "failed") {
        throw new Error(s.error || "job failed");
      }
      if (s.status === "cancelled") {
        throw new Error(s.error || "job cancelled");
      }
      const delay = Math.min(10000, baseInterval * Math.pow(1.25, Math.min(attempt, 8)));
      attempt += 1;
      await new Promise((r) => setTimeout(r, delay));
    }
    throw new Error("timeout waiting for job ready");
  }

  /** 403/CORS 后丢掉匿名 OSS mediaBase，之后的 segment/帧请求改打 Master。 */
  private disableOssMedia(reason: string) {
    if (!this.mediaBase) return;
    console.warn(`[sview] OSS media disabled (${reason}); falling back to Master`);
    this.mediaBase = "";
  }

  private async readJsonManifest(res: Response): Promise<Manifest> {
    const text = await res.text();
    try {
      return JSON.parse(text) as Manifest;
    } catch {
      const snippet = text.slice(0, 180).replace(/\s+/g, " ");
      throw new Error(
        `manifest JSON parse failed (HTTP ${res.status}): ${snippet || "(empty body)"}`,
      );
    }
  }

  async fetchManifest(
    jobId: string,
    opts: { retries?: number; signal?: AbortSignal } = {},
  ): Promise<Manifest> {
    if (this.mediaBase) {
      try {
        const res = await fetch(this.mediaUrl(`/manifest.json`), {
          cache: "no-store",
          signal: opts.signal,
        });
        if (res.ok) return await this.readJsonManifest(res);
        this.disableOssMedia(`manifest ${res.status}`);
      } catch (e) {
        if (e instanceof DOMException && e.name === "AbortError") throw e;
        this.disableOssMedia(e instanceof Error ? e.message : "manifest fetch failed");
      }
    }
    const retries = Math.max(0, opts.retries ?? 5);
    for (let attempt = 0; ; attempt++) {
      const res = await fetch(joinUrl(this.baseUrl, `/api/jobs/${jobId}/manifest.json`), {
        cache: "no-store",
        redirect: "follow",
        signal: opts.signal,
      });
      if (res.ok) {
        // 跨域 302 之后，有的浏览器会留下非 JSON 正文——这里显式校验。
        return await this.readJsonManifest(res);
      }
      const detail = await res.text();
      if (res.status !== 425 || attempt >= retries) {
        throw new Error(`manifest ${res.status}: ${detail}`);
      }
      const retryAfter = Number(res.headers.get("Retry-After"));
      const delayMs = Number.isFinite(retryAfter) && retryAfter > 0
        ? retryAfter * 1000
        : 1000;
      await abortableDelay(Math.min(5000, delayMs), opts.signal);
    }
  }

  async requestEncode(
    jobId: string,
    body: {
      frame_index: number;
      prefetch_segments: number;
      client_epoch: number;
      client_id: string;
    },
    signal?: AbortSignal,
  ): Promise<EncodeRequest & Record<string, unknown>> {
    const res = await fetch(joinUrl(this.baseUrl, `/api/jobs/${jobId}/encode`), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      cache: "no-store",
      signal,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok && res.status !== 202) {
      throw new Error(
        (data as { error?: string }).error || `encode ${res.status}`,
      );
    }
    return data as EncodeRequest & Record<string, unknown>;
  }

  async getEncodeRequest(
    jobId: string,
    requestId: string,
    signal?: AbortSignal,
  ): Promise<EncodeRequest> {
    const res = await fetch(
      joinUrl(this.baseUrl, `/api/jobs/${jobId}/encode/${requestId}`),
      { cache: "no-store", signal },
    );
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `encode request ${res.status}`);
    return data as EncodeRequest;
  }

  async cancelEncodeRequest(
    jobId: string,
    requestId: string,
  ): Promise<EncodeRequest> {
    const res = await fetch(
      joinUrl(
        this.baseUrl,
        `/api/jobs/${jobId}/encode/${requestId}/cancel`,
      ),
      { method: "POST", cache: "no-store" },
    );
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `cancel encode ${res.status}`);
    return data as EncodeRequest;
  }

  async fetchFrame(
    jobId: string,
    index: number,
    opts: {
      retries?: number;
      onWait?: (index: number, attempt: number, status: number) => void;
    } = {},
  ): Promise<ArrayBuffer> {
    const name = String(index).padStart(6, "0") + ".bin";
    const paths = this.mediaBase
      ? [this.mediaUrl(`/frames/${name}`)]
      : [
          joinUrl(this.baseUrl, `/api/jobs/${jobId}/frames/${name}`),
          joinUrl(this.baseUrl, `/${jobId}/frames/${name}`),
        ];
    // 边转边播：多等一会儿；界面显示加载中，不当 404。
    const retries = opts.retries ?? 120;
    let lastStatus = 0;
    for (let attempt = 0; attempt <= retries; attempt++) {
      let sawRetryable = false;
      for (const path of paths) {
        const res = await fetch(path, {
          cache: "no-store",
        });
        if (res.ok) return res.arrayBuffer();
        lastStatus = res.status;
        if (res.status === 425 || res.status === 404) {
          sawRetryable = true;
          continue;
        }
        throw new Error(`frame ${index} ${res.status}`);
      }
      if (sawRetryable) {
        opts.onWait?.(index, attempt, lastStatus);
        const delay = Math.min(2000, Math.max(250, 1000));
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      throw new Error(`frame ${index} ${lastStatus || 404}`);
    }
    throw new FramePendingError(index, lastStatus || 404);
  }

  /**
   * 经 Master 拉取。forceMasterProxy（Vite /sview）或 302 失败后优先 ?proxy=1；
   * 否则先试普通 URL，再回退代理。
   */
  private async fetchSegmentViaMaster(
    jobId: string,
    name: string,
    signal?: AbortSignal,
  ): Promise<{ ok: true; buf: ArrayBuffer } | { ok: false; status: number; retryable: boolean }> {
    const primary = joinUrl(this.baseUrl, `/api/jobs/${jobId}/segments/${name}`);
    const legacy = joinUrl(this.baseUrl, `/${jobId}/segments/${name}`);
    const withProxy = (u: string) => u + (u.includes("?") ? "&" : "?") + "proxy=1";

    const tryUrl = async (
      url: string,
    ): Promise<{ ok: true; buf: ArrayBuffer } | { ok: false; status: number; retryable: boolean } | "network"> => {
      try {
        const res = await fetch(url, { cache: "no-store", signal });
        if (res.ok) return { ok: true, buf: await res.arrayBuffer() };
        if (res.status === 425 || res.status === 404) {
          return { ok: false, status: res.status, retryable: true };
        }
        return { ok: false, status: res.status, retryable: false };
      } catch {
        if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
        return "network";
      }
    };

    for (const path of [primary, legacy]) {
      const order = this.forceMasterProxy
        ? [withProxy(path), path]
        : [path, withProxy(path)];

      let lastStatus = 0;
      for (let i = 0; i < order.length; i++) {
        const url = order[i]!;
        const got = await tryUrl(url);
        if (got === "network") {
          lastStatus = 0;
          continue;
        }
        if (got.ok) {
          if (url.includes("proxy=1")) this.forceMasterProxy = true;
          return got;
        }
        lastStatus = got.status;
        if (got.retryable) return got;
        // 普通 URL 上不可重试 → 再试代理；代理也试过了 → 失败。
      }
      if (lastStatus) return { ok: false, status: lastStatus, retryable: false };
    }
    return { ok: false, status: 0, retryable: false };
  }

  /**
   * 拉取打包好的一段，约 30 个连续帧。
   * `startIndex` 必须是段长的倍数（0、30、60…）。
   */
  async fetchSegment(
    jobId: string,
    startIndex: number,
    opts: {
      retries?: number;
      onWait?: (startIndex: number, attempt: number, status: number) => void;
      signal?: AbortSignal;
    } = {},
  ): Promise<ArrayBuffer> {
    const name = String(startIndex).padStart(6, "0") + ".bin";
    const retries = opts.retries ?? 120;
    let lastStatus = 0;
    for (let attempt = 0; attempt <= retries; attempt++) {
      // 1) 匿名 play_base（仅当已设置且可公开读）
      const ossBase = (this.mediaBase || "").replace(/\/+$/, "");
      if (ossBase) {
        const ossPath = this.mediaUrl(`/segments/${name}`);
        if (!ossPath.includes("-internal.")) {
          try {
            const res = await fetch(ossPath, {
              cache: "no-store",
              signal: opts.signal,
            });
            if (res.ok) return res.arrayBuffer();
            lastStatus = res.status;
            if (res.status === 403 || res.status === 401) {
              this.disableOssMedia(`segment ${res.status}`);
            } else if (res.status !== 425 && res.status !== 404) {
              this.disableOssMedia(`segment ${res.status}`);
            } else {
              opts.onWait?.(startIndex, attempt, lastStatus);
              await abortableDelay(1000, opts.signal);
              continue;
            }
          } catch (e) {
            if (e instanceof DOMException && e.name === "AbortError") throw e;
            this.disableOssMedia(e instanceof Error ? e.message : "segment fetch failed");
          }
        } else {
          this.disableOssMedia("internal OSS host");
        }
      }

      // 2) 预签名公网 OSS（浏览器直连，优先）
      if (this.preferSignedOss) {
        const signed = await this.fetchSegmentViaSignedOss(
          jobId,
          startIndex,
          opts.signal,
        );
        if ("fatal" in signed) {
          // 此时 preferSignedOss 已经清掉；落到 Master
        } else if (signed.ok) {
          return signed.buf;
        } else if (signed.retryable) {
          lastStatus = signed.status;
          opts.onWait?.(startIndex, attempt, lastStatus);
          await abortableDelay(1000, opts.signal);
          continue;
        }
      }

      // 3) Master 流 / 302 回退
      const got = await this.fetchSegmentViaMaster(jobId, name, opts.signal);
      if (got.ok) return got.buf;
      lastStatus = got.status;
      if (got.retryable) {
        opts.onWait?.(startIndex, attempt, lastStatus);
        await abortableDelay(1000, opts.signal);
        continue;
      }
      throw new Error(`segment ${startIndex} ${lastStatus || "network"}`);
    }
    throw new FramePendingError(startIndex, lastStatus || 404);
  }
}
