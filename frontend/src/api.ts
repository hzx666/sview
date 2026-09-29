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
  /** Latest sparse encode request accepted by the publisher. */
  segment_epoch?: number;
  /** Monotonic shared-cache publication revision. */
  manifest_revision?: number;
  /** partial = still writing frames; done = complete */
  status: string;
  /** Optional OSS/CDN prefix for progressive play cache. */
  play_base?: string;
  /** Seconds skipped from bag start (encode-from-T). */
  origin_sec?: number;
  bundle: {
    frame_count: number;
    /** How many frames are on disk and fetchable (grows during progressive publish). */
    frames_available?: number;
    /** Published sparse frame ranges using half-open [start, end) semantics. */
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

/** Packed batch of single-frame bundles (~gop_size / SEGMENT_FRAMES). */
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
  frame_count?: number | null;
  frames_done?: number | null;
  playable?: boolean | null;
  duration_sec?: number | null;
  cached?: boolean;
  local_path?: string;
  updated_at?: string;
  /** When set, frontend should fetch manifest/segments from this OSS prefix. */
  play_base?: string;
  /** 0–100 progress when frames_total known. */
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

export type JobStatsItem = {
  job_id: string;
  status: string;
  owner_pod?: string;
  source?: string;
  playable?: boolean;
  frames_done?: number;
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

/** Head-cam four-up; teleop v2 uses Object.keys(manifest.streams) instead. */
export const CHANNELS = ["rr", "fr", "rl", "fl"] as const;
export type Channel = string;

export function playChannels(m: Manifest | null | undefined): string[] {
  if (!m?.streams) return [...CHANNELS];
  const keys = Object.keys(m.streams);
  if (!keys.length) return [...CHANNELS];
  const format = (m.format || "").toLowerCase();
  const v1Keys = CHANNELS.filter(
    (ch) => `camera_${ch}` in m.streams || ch in m.streams,
  );
  if (format === "head_cam" || keys.every((k) => k.startsWith("camera_"))) {
    return v1Keys.length ? v1Keys : [...CHANNELS];
  }
  if (v1Keys.length === keys.length && v1Keys.length > 0) {
    return [...CHANNELS].filter((ch) => v1Keys.includes(ch));
  }
  return keys;
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

/** Frame exists in timeline but not on disk yet (progressive publish). */
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
   * Prefer Master ?proxy=1 streaming. Used as fallback after signed-OSS fails,
   * or when preferSignedOss is false.
   */
  forceMasterProxy = false;
  /**
   * Browser → public OSS with Master-issued presigned URLs (short RTT).
   * Default on; disables itself after CORS/403 and falls back to Master proxy.
   */
  preferSignedOss = true;
  private signedUrlCache = new Map<string, { url: string; expMs: number }>();
  private signedInflight: Promise<void> | null = null;

  constructor(
    public baseUrl: string,
    /** When set, anonymous OSS prefix (rare; private buckets need signed). */
    public mediaBase: string = "",
  ) {
    // Opt out: ?media=proxy  → Master stream only; ?media=signed → force signed OSS
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
      /* non-browser */
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
    // A same-origin Vite/Nginx proxy can follow Master's OSS redirect server-side,
    // avoiding bucket CORS without forcing the slower ?proxy=1 path.
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

  /** Presign upcoming segment starts (batched). Safe to call often — uses cache. */
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
      // Chunk to API max 32
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

    // Coalesce concurrent callers: join in-flight, then retry need (may be empty).
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
        /* use direct signed URL */
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
      // Typical: CORS missing on bucket
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
   * Wait until playable via SSE, then poll if SSE cannot connect (ACS Ingress
   * often buffers EventSource until the stream ends).
   */
  async waitUntilPlayable(
    jobId: string,
    opts: {
      intervalMs?: number;
      timeoutMs?: number;
      minFrames?: number;
      onTick?: (s: JobStatus) => void;
      preferSse?: boolean;
      /** Default true: EventSource 失败后改轮询 GET /api/jobs/{id}。 */
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
      // Require a real frame lead before start — API playable(~30) alone is too thin.
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
        // EventSource readyState: 0 CONNECTING, 1 OPEN, 2 CLOSED
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

  /** Drop anonymous OSS mediaBase after 403/CORS so later segment/frame calls hit Master. */
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
        // After a cross-origin 302, some browsers leave a non-JSON body — validate explicitly.
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
    // Progressive publish: keep waiting longer; UI shows loading, not 404.
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
   * Fetch via Master. Prefer ?proxy=1 when forceMasterProxy (Vite /sview) or after
   * a failed 302; otherwise try plain URL first then proxy fallback.
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
        // Non-retryable on plain URL → try proxy next; on proxy already tried → fail.
      }
      if (lastStatus) return { ok: false, status: lastStatus, retryable: false };
    }
    return { ok: false, status: 0, retryable: false };
  }

  /**
   * Fetch a packed segment of ~30 consecutive frames.
   * `startIndex` must be a multiple of segment size (0, 30, 60, …).
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
      // 1) Anonymous play_base (only if set and public-readable)
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

      // 2) Presigned public OSS (browser direct — preferred)
      if (this.preferSignedOss) {
        const signed = await this.fetchSegmentViaSignedOss(
          jobId,
          startIndex,
          opts.signal,
        );
        if ("fatal" in signed) {
          // preferSignedOss already cleared; fall through to Master
        } else if (signed.ok) {
          return signed.buf;
        } else if (signed.retryable) {
          lastStatus = signed.status;
          opts.onWait?.(startIndex, attempt, lastStatus);
          await abortableDelay(1000, opts.signal);
          continue;
        }
      }

      // 3) Master stream / 302 fallback
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
