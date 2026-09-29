import type { Channel, Manifest } from "./api";
import {
  FramePendingError,
  SviewApi,
  framesAvailable,
  isFramePendingError,
  isManifestComplete,
  manifestFps,
  playChannels,
  streamLabel,
} from "./api";
import {
  parseBundle,
  parseSegment,
  SEGMENT_FRAMES,
  segmentStartFor,
} from "./bundle";
import {
  annexBToAvcc,
  buildAvcC,
  codecFromSps,
  extractSpsPps,
  hasIdr,
} from "./h264";

export type PlayerCallbacks = {
  onFrame?: (index: number, tsNs: number, imu: unknown) => void;
  onStatus?: (text: string) => void;
  onError?: (err: Error) => void;
  /** Fired when manifest grows (progressive publish). */
  onManifest?: (m: Manifest) => void;
};

/** Decode preference. Windows HW (DXVA) often corrupts multi-stream H.264. */
export type DecodeAccel = "auto" | "prefer-hardware" | "prefer-software";

type CamState = {
  decoder: VideoDecoder | null;
  canvas: HTMLCanvasElement;
  ctx: CanvasRenderingContext2D | null;
  configured: boolean;
  codec: string;
  /** After decode error, skip deltas until next IDR. */
  needsKeyframe: boolean;
  decodeErrors: number;
  /** After HW glitches, force prefer-software on next configure. */
  forceSoftware: boolean;
  waiters: Array<() => void>;
  lastConfig: VideoDecoderConfig | null;
  description: Uint8Array | null;
  codedWidth: number;
  codedHeight: number;
  /** Timeline generation this decoder is allowed to present. */
  epoch: number;
};

/** How many segments ahead of the playhead to prefetch (Vite/Master proxy is slow). */
const PREFETCH_SEGMENTS = 8;
/** Sparse encode keeps a short forward window; the server continues it. */
const ENCODE_PREFETCH_SEGMENTS = 3;
/** Keep roughly this many segments of frame bundles in memory. */
const CACHE_SEGMENTS = 16;
/** Segments to fully cache before autoplay (reduces start-of-play underruns). */
const WARMUP_SEGMENTS = 5;
/** When this many frames remain in the current segment, hard-await the next. */
const BOUNDARY_AWAIT_FRAMES = 8;
/** Snap playback clock only if behind by more than this many frame periods. */
const MAX_FRAME_DEBT = 5;
/**
 * While progressive, never play into the last N published frames.
 * Stops micro-stalling every time the playhead kisses frames_available.
 */
const FRONTIER_PAD_FRAMES = 90; // ~3s @ 30fps
/** After hitting the pad, wait until lead recovers to this before resuming. */
const FRONTIER_RESUME_FRAMES = 150; // ~5s @ 30fps
const MANIFEST_POLL_MS = 1000;

function isAbortError(e: unknown): boolean {
  return e instanceof DOMException
    ? e.name === "AbortError"
    : e instanceof Error && e.name === "AbortError";
}

function isWindowsPlatform(): boolean {
  const nav = navigator as Navigator & { userAgentData?: { platform?: string } };
  const p = nav.userAgentData?.platform || navigator.platform || "";
  return /win/i.test(p);
}

export function sharedClientId(): string {
  const key = "sview.client_id";
  try {
    const current = sessionStorage.getItem(key);
    if (current) return current;
    const created = crypto.randomUUID();
    sessionStorage.setItem(key, created);
    return created;
  } catch {
    return crypto.randomUUID();
  }
}

/** Resolve accel: URL ?decode=software|hardware|auto, else Windows→software. */
export function resolveDecodeAccel(override?: DecodeAccel): DecodeAccel {
  if (override && override !== "auto") return override;
  try {
    const q = new URLSearchParams(location.search).get("decode")?.toLowerCase();
    if (q === "software" || q === "prefer-software") return "prefer-software";
    if (q === "hardware" || q === "prefer-hardware") return "prefer-hardware";
  } catch {
    /* ignore */
  }
  // Four concurrent HW decoders frequently tear / green-block on Windows.
  return isWindowsPlatform() ? "prefer-software" : "prefer-hardware";
}

export class SviewPlayer {
  private api: SviewApi;
  private jobId = "";
  private manifest: Manifest | null = null;
  private cams = new Map<Channel, CamState>();
  private cache = new Map<number, ArrayBuffer>();
  /** In-flight segment loads keyed by segment start index. */
  private segmentInflight = new Map<
    number,
    { epoch: number; promise: Promise<void> }
  >();
  private sparseReadyInflight = new Map<
    number,
    { epoch: number; promise: Promise<void> }
  >();
  private index = 0;
  private playing = false;
  private raf = 0;
  private lastTick = 0;
  private fps = 30;
  private cb: PlayerCallbacks;
  private hwMode = "unknown";
  private accel: DecodeAccel;
  private manifestTimer = 0;
  private waitingForFrames = false;
  /** True while fetch+decode+draw of one frame is in flight (backpressure). */
  private advancing = false;
  /** After frontier underrun: hold until resume lead is rebuilt. */
  private frontierHold = false;
  /** Monotonic generation shared with sparse encode requests. */
  private seekEpoch = 0;
  private seekAbort: AbortController | null = null;
  private seekSerial: Promise<unknown> = Promise.resolve();
  private resumeAfterSeek = false;
  private readonly clientId = sharedClientId();
  private activeRequestId = "";

  constructor(
    canvases: Record<string, HTMLCanvasElement>,
    cb: PlayerCallbacks = {},
    opts: { decodeAccel?: DecodeAccel } = {},
  ) {
    this.cb = cb;
    this.accel = resolveDecodeAccel(opts.decodeAccel);
    this.attachCanvases(canvases);
    this.api = new SviewApi("");
  }

  setCanvases(canvases: Record<string, HTMLCanvasElement>) {
    this.attachCanvases(canvases);
  }

  private attachCanvases(canvases: Record<string, HTMLCanvasElement>) {
    this.resetDecoders();
    this.cams.clear();
    const desync = !isWindowsPlatform();
    for (const ch of Object.keys(canvases)) {
      const canvas = canvases[ch];
      if (!canvas) continue;
      this.cams.set(ch, {
        decoder: null,
        canvas,
        ctx: canvas.getContext("2d", { alpha: false, desynchronized: desync }),
        configured: false,
        codec: "",
        needsKeyframe: true,
        decodeErrors: 0,
        forceSoftware: this.accel === "prefer-software",
        waiters: [],
        lastConfig: null,
        description: null,
        codedWidth: 0,
        codedHeight: 0,
        epoch: this.seekEpoch,
      });
    }
  }

  private channelList(): Channel[] {
    if (this.cams.size) return [...this.cams.keys()];
    return playChannels(this.manifest);
  }

  get Manifest() {
    return this.manifest;
  }

  get Index() {
    return this.index;
  }

  get Playing() {
    return this.playing;
  }

  /** Frames currently on disk / safe to request (encoder frontier). */
  get Available() {
    if (this.Sparse) return this.manifest?.bundle.frame_count ?? 0;
    return framesAvailable(this.manifest);
  }

  get Complete() {
    return isManifestComplete(this.manifest);
  }

  get Sparse() {
    return this.manifest?.publish_mode === "sparse";
  }

  /**
   * Highest index+1 the playhead may enter while progressive.
   * Leaves FRONTIER_PAD_FRAMES behind the encoder so we don't kiss the wall.
   */
  private playLimit(): number {
    const raw = this.Available;
    if (this.Sparse) return this.manifest?.bundle.frame_count ?? raw;
    if (this.Complete) return raw;
    return Math.max(0, raw - FRONTIER_PAD_FRAMES);
  }

  private frontierLead(): number {
    return Math.max(0, this.Available - this.index);
  }

  async load(
    baseUrl: string,
    jobId: string,
    manifest?: Manifest,
    opts: { mediaBase?: string } = {},
  ) {
    this.stop();
    this.cancelSeek();
    this.resetDecoders();
    this.cache.clear();
    this.segmentInflight.clear();
    this.sparseReadyInflight.clear();
    // Only use explicit mediaBase. Do NOT auto-switch to manifest.play_base:
    // private buckets block anonymous GET (403); Master 302 → signed OSS instead.
    const mediaBase = (opts.mediaBase || "").replace(/\/+$/, "") || "";
    this.api = new SviewApi(baseUrl, mediaBase);
    this.jobId = jobId;
    this.cb.onStatus?.(
      mediaBase ? `Loading manifest from OSS…` : "Loading manifest…",
    );
    this.manifest = manifest ?? (await this.api.fetchManifest(jobId));
    this.seekEpoch = Math.max(
      this.seekEpoch,
      this.manifest.segment_epoch ?? 0,
    );
    const st = (this.manifest.status || "done").toLowerCase();
    if (st !== "done" && st !== "partial" && st !== "ready") {
      throw new Error(`job status=${this.manifest.status}`);
    }
    const fr = manifestFps(this.manifest);
    this.fps = fr > 0 ? fr : 30;
    this.index = 0;
    const avail = this.Available;
    const total = this.manifest.bundle.frame_count ?? avail;
    const via = this.api.mediaBase ? " · oss-cache" : "";
    this.cb.onStatus?.(
      `${this.manifest.job_id} · ${avail}/${total} frames · ${this.fps} fps` +
        (st === "partial" ? " · progressive" : "") +
        via,
    );
    this.cb.onManifest?.(this.manifest);
    if (this.Sparse) {
      const epoch = this.nextSeekEpoch();
      await this.ensureSparseSegmentReady(0, epoch, this.seekAbort!.signal, true);
    }
    await this.ensureConfigured();
    if (avail < 1) {
      throw new Error("还没有可播放的帧，请稍后再试");
    }
    await this.showFrame(0);
    void this.prefetchAround(0);
    this.startManifestPolling();
    this.cb.onStatus?.(
      `${this.manifest.job_id} · ${st === "partial" ? "playing while encoding" : "ready"} · decode=${this.hwMode}` +
        via,
    );
  }

  /**
   * Block until several upcoming segments are in cache (or frontier runs out).
   * Call before play() when media rides a slow Master/Vite proxy.
   */
  async warmup(segments = WARMUP_SEGMENTS): Promise<void> {
    if (!this.manifest) return;
    const segN = this.segmentFrames();
    const n = Math.max(1, segments);
    this.cb.onStatus?.(
      `预缓冲 ${n} 段… · decode=${this.hwMode}` +
        (this.api.preferSignedOss
          ? " · signed-oss"
          : this.api.forceMasterProxy
            ? " · proxy=1"
            : ""),
    );
    const tasks: Promise<void>[] = [];
    const starts: number[] = [];
    for (let s = 0; s < n; s++) {
      const start = s * segN;
      const total = this.manifest.bundle.frame_count;
      if (start >= total) break;
      if (this.Sparse && !this.isSparseSegmentReady(start)) break;
      if (!this.Sparse && start >= this.Available) break;
      const fullReady = this.Sparse
        ? this.isSparseSegmentReady(start)
        : this.Available >= start + segN;
      const finalPartial = this.Sparse
        ? this.isSparseSegmentReady(start)
        : this.Complete && this.Available > start;
      if (!fullReady && !finalPartial) break;
      starts.push(start);
    }
    if (starts.length && this.api.preferSignedOss) {
      await this.api.prefetchSignedSegmentUrls(this.jobId, starts).catch(() => {
        /* fall back inside ensureSegment */
      });
    }
    for (const start of starts) {
      tasks.push(
        this.ensureSegment(start, {
          quiet: true,
          epoch: this.seekEpoch,
          signal: this.Sparse ? this.seekAbort?.signal : undefined,
        }),
      );
    }
    await Promise.all(tasks);
    void this.prefetchAround(0);
  }

  play() {
    if (!this.manifest || this.playing) return;
    this.playing = true;
    this.waitingForFrames = false;
    this.advancing = false;
    this.frontierHold = false;
    this.resumeAfterSeek = false;
    this.lastTick = performance.now();
    const loop = (now: number) => {
      if (!this.playing || !this.manifest) return;
      this.raf = requestAnimationFrame(loop);
      // Serialize: never overlap showFrame (was the main stutter source).
      if (this.advancing || this.waitingForFrames) return;

      const frameMs = 1000 / this.fps;
      if (now - this.lastTick < frameMs) return;

      this.advancing = true;
      void this.advanceOneFrame()
        .then((presented) => {
          if (!this.playing) return;
          if (!presented) {
            // Buffering / ended — freeze clock so we don't burst after wait.
            this.lastTick = performance.now();
            return;
          }
          this.lastTick += frameMs;
          // Mildly behind: present ASAP without snapping (avoids micro-hitches).
          // Badly behind: snap so we don't queue an ever-growing delay.
          const behind = performance.now() - this.lastTick;
          if (behind > frameMs * MAX_FRAME_DEBT) {
            this.lastTick = performance.now();
          }
        })
        .catch((e) => this.handlePlayError(e))
        .finally(() => {
          this.advancing = false;
        });
    };
    this.raf = requestAnimationFrame(loop);
  }

  private haltPlayback() {
    this.playing = false;
    this.waitingForFrames = false;
    this.advancing = false;
    this.frontierHold = false;
    if (this.raf) cancelAnimationFrame(this.raf);
    this.raf = 0;
  }

  pause() {
    this.resumeAfterSeek = false;
    this.haltPlayback();
  }

  stop() {
    this.pause();
    this.cancelSeek();
    this.stopManifestPolling();
  }

  private cancelSeek() {
    this.cancelActiveRequest();
    this.seekAbort?.abort();
    this.seekAbort = null;
    this.seekEpoch += 1;
    this.segmentInflight.clear();
    this.sparseReadyInflight.clear();
  }

  private nextSeekEpoch(): number {
    this.cancelActiveRequest();
    this.seekAbort?.abort();
    this.seekAbort = new AbortController();
    this.seekEpoch = Math.max(
      this.seekEpoch + 1,
      (this.manifest?.segment_epoch ?? 0) + 1,
    );
    this.segmentInflight.clear();
    this.sparseReadyInflight.clear();
    return this.seekEpoch;
  }

  private cancelActiveRequest() {
    const requestId = this.activeRequestId;
    this.activeRequestId = "";
    if (!requestId || !this.jobId) return;
    void this.api.cancelEncodeRequest(this.jobId, requestId).catch(() => {
      /* request may already be ready or expired */
    });
  }

  private assertCurrent(epoch: number, signal?: AbortSignal) {
    if (signal?.aborted || epoch !== this.seekEpoch) {
      throw new DOMException("Aborted", "AbortError");
    }
  }

  /** Latest seek wins; superseded seeks never cache or present a frame. */
  async seek(index: number): Promise<boolean> {
    if (!this.manifest) return false;
    const wasPlaying = this.playing || this.resumeAfterSeek;
    this.resumeAfterSeek = wasPlaying;
    this.haltPlayback();
    const epoch = this.nextSeekEpoch();
    const signal = this.seekAbort!.signal;
    const run = async (): Promise<boolean> => {
      const total = this.manifest?.bundle.frame_count ?? 0;
      const clamped = Math.max(0, Math.min(index, Math.max(0, total - 1)));
      if (!this.Sparse && clamped >= this.Available) {
        this.cb.onStatus?.(
          `第 ${clamped} 帧还没转完，当前可播到 ${Math.max(0, this.Available - 1)}。转到这一帧再跳过去`,
        );
        return false;
      }
      if (this.Sparse) {
        this.cb.onStatus?.(`请求稀疏编码第 ${clamped} 帧…`);
        await this.ensureSparseSegmentReady(clamped, epoch, signal, true);
      }
      this.assertCurrent(epoch, signal);
      const segStart = segmentStartFor(clamped, this.segmentFrames());
      // Sparse segments are independently decodable from their segment head.
      const keys = this.Sparse
        ? [segStart]
        : this.manifest!.bundle.key_frame_indices || [0];
      let k = keys[0] ?? 0;
      for (const ki of keys) {
        if (ki <= clamped) k = ki;
        else break;
      }
      this.cb.onStatus?.(`Seek ${clamped} via keyframe ${k}`);
      await this.resetAllDecoders(epoch, signal);
      this.assertCurrent(epoch, signal);
      for (const cam of this.cams.values()) cam.needsKeyframe = true;
      for (let i = k; i <= clamped; i++) {
        await this.decodeFrame(i, i === clamped, epoch, signal);
      }
      this.assertCurrent(epoch, signal);
      this.index = clamped;
      if (this.resumeAfterSeek) {
        this.resumeAfterSeek = false;
        this.play();
      }
      return true;
    };
    const result = this.seekSerial.then(run, run);
    this.seekSerial = result.catch(() => undefined);
    try {
      return await result;
    } catch (e) {
      if (isAbortError(e)) return false;
      throw e;
    }
  }

  /** Flush/close every decoder and rebind with last (or software) config. */
  private async resetAllDecoders(epoch = this.seekEpoch, signal?: AbortSignal) {
    for (const ch of this.channelList()) {
      this.assertCurrent(epoch, signal);
      const cam = this.cams.get(ch);
      if (!cam) continue;
      await this.resetDecoder(ch, cam, cam.forceSoftware);
    }
    this.assertCurrent(epoch, signal);
  }

  private async resetDecoder(
    ch: Channel,
    cam: CamState,
    preferSoftware = false,
  ) {
    const dec = cam.decoder;
    if (dec) {
      try {
        if (dec.state === "configured") await dec.flush();
      } catch {
        /* ignore flush errors on broken pipelines */
      }
      try {
        dec.close();
      } catch {
        /* */
      }
    }
    cam.decoder = null;
    cam.configured = false;
    cam.needsKeyframe = true;
    cam.waiters.splice(0).forEach((w) => w());

    if (preferSoftware) cam.forceSoftware = true;

    if (cam.codec && cam.description) {
      const cfg = await this.pickDecoderConfig(
        cam.codec,
        cam.description,
        preferSoftware || cam.forceSoftware,
      );
      if (cfg) {
        this.bindDecoder(ch, cam, cfg);
        return;
      }
    }
    // Fall back to full reconfigure from SPS/PPS on disk.
    cam.configured = false;
    await this.ensureConfigured();
  }

  /**
   * Advance exactly one display frame (awaited).
   * @returns true if a frame was presented.
   */
  private async advanceOneFrame(): Promise<boolean> {
    if (!this.manifest) return false;
    const next = this.index + 1;
    const raw = this.Available;
    const limit = this.playLimit();
    if (this.Sparse && next >= this.manifest.bundle.frame_count) {
      this.pause();
      this.cb.onStatus?.("Ended");
      return false;
    }

    // Progressive: hold playhead behind the encoder; hysteresis avoids chatter.
    if (!this.Sparse && !this.Complete) {
      const needLead = this.frontierHold
        ? FRONTIER_RESUME_FRAMES
        : FRONTIER_PAD_FRAMES;
      if (next >= limit || this.frontierLead() < needLead) {
        this.frontierHold = true;
        this.waitingForFrames = true;
        this.cb.onStatus?.(
          `缓冲中… 领先 ${this.frontierLead()} 帧（可用 ${raw}/${this.manifest.bundle.frame_count}，目标 ≥${FRONTIER_RESUME_FRAMES}）`,
        );
        try {
          await this.refreshManifest();
          if (!this.playing) return false;
          if (this.Complete) {
            this.frontierHold = false;
          } else if (this.frontierLead() >= FRONTIER_RESUME_FRAMES) {
            this.frontierHold = false;
          } else {
            await new Promise((r) => window.setTimeout(r, 200));
            return false;
          }
        } catch (e) {
          this.handlePlayError(e);
          return false;
        } finally {
          this.waitingForFrames = false;
        }
        // Fall through to present if resume condition met and next is still valid.
        if (!this.Complete && next >= this.playLimit()) {
          return false;
        }
      }
    }

    if (!this.Sparse && next >= raw) {
      if (this.Complete) {
        this.pause();
        this.cb.onStatus?.("Ended");
        return false;
      }
      this.frontierHold = true;
      this.waitingForFrames = true;
      this.cb.onStatus?.(
        `加载中… ${raw}/${this.manifest.bundle.frame_count} 帧`,
      );
      try {
        await this.refreshManifest();
        if (!this.playing) return false;
        const grown = this.Available;
        if (grown > raw && next < this.playLimit()) {
          this.frontierHold = false;
          await this.ensureSegmentBoundary(next);
          await this.showFrame(next);
          void this.prefetchAround(next);
          return true;
        }
        await new Promise((r) => window.setTimeout(r, 200));
        return false;
      } catch (e) {
        this.handlePlayError(e);
        return false;
      } finally {
        this.waitingForFrames = false;
      }
    }

    try {
      if (this.Sparse && !this.isSparseSegmentReady(next)) {
        this.waitingForFrames = true;
        this.cb.onStatus?.(`请求后续稀疏窗口… 第 ${next} 帧`);
        try {
          await this.ensureSparseSegmentReady(
            next,
            this.seekEpoch,
            this.seekAbort?.signal,
            true,
          );
        } finally {
          this.waitingForFrames = false;
        }
        if (!this.playing) return false;
      }
      await this.ensureSegmentBoundary(next);
      await this.showFrame(next);
      void this.prefetchAround(next);
      return true;
    } catch (e) {
      if (isAbortError(e)) return false;
      if (isFramePendingError(e)) {
        this.frontierHold = true;
        this.cb.onStatus?.(`加载中… 等待帧 ${e.index}`);
        await new Promise((r) => window.setTimeout(r, 200));
        return false;
      }
      throw e;
    }
  }

  /**
   * Near a segment boundary, finish loading the next segment before decode.
   * Avoids a hitch every ~1s when the playhead crosses into an unloaded pack.
   */
  private async ensureSegmentBoundary(index: number): Promise<void> {
    const segN = this.segmentFrames();
    const curStart = segmentStartFor(index, segN);
    const pos = index - curStart;
    const nextStart = curStart + segN;
    const epoch = this.seekEpoch;
    const signal = this.seekAbort?.signal;

    // Always keep current segment warm.
    if (!this.segmentCachedThrough(curStart, segN)) {
      await this.ensureSegment(curStart, { quiet: true, epoch, signal });
    }

    const total = this.manifest?.bundle.frame_count ?? this.Available;
    if (nextStart >= total) return;
    if (this.Sparse && !this.isSparseSegmentReady(nextStart)) {
      // Queue the next sparse window before playback reaches the boundary.
      if (pos === Math.floor(segN / 2)) {
        void this.ensureSparseSegmentReady(
          nextStart,
          epoch,
          signal,
          true,
        ).catch(() => {
          /* the boundary path retries and reports persistent failures */
        });
      }
      return;
    }
    if (!this.Sparse && nextStart >= this.Available) return;
    const fullReady = this.Sparse
      ? this.isSparseSegmentReady(nextStart)
      : this.Available >= nextStart + segN;
    const finalPartial = this.Sparse
      ? this.isSparseSegmentReady(nextStart)
      : this.Complete && this.Available > nextStart;
    if (!fullReady && !finalPartial) return;

    if (pos >= segN - BOUNDARY_AWAIT_FRAMES) {
      if (!this.segmentCachedThrough(nextStart, segN)) {
        await this.ensureSegment(nextStart, { quiet: true, epoch, signal });
      }
    } else if (
      pos >= Math.floor(segN / 2) &&
      !this.segmentCachedThrough(nextStart, segN) &&
      !this.segmentInflight.has(nextStart)
    ) {
      void this.ensureSegment(nextStart, {
        quiet: true,
        epoch,
        signal,
      }).catch(() => {});
    }
  }

  private handlePlayError(e: unknown) {
    if (isFramePendingError(e)) {
      this.cb.onStatus?.(`加载中… 等待帧 ${e.index}`);
      return;
    }
    const msg = e instanceof Error ? e.message : String(e);
    if (/frame \d+ (404|425)|loading|not ready|pending/i.test(msg)) {
      this.cb.onStatus?.(`加载中… ${msg}`);
      return;
    }
    this.cb.onError?.(e instanceof Error ? e : new Error(msg));
  }

  private startManifestPolling() {
    this.stopManifestPolling();
    if (this.Complete) return;
    this.manifestTimer = window.setInterval(() => {
      void this.refreshManifest().catch(() => {
        /* ignore transient */
      });
    }, MANIFEST_POLL_MS);
  }

  private stopManifestPolling() {
    if (this.manifestTimer) {
      window.clearInterval(this.manifestTimer);
      this.manifestTimer = 0;
    }
  }

  async refreshManifest(opts: { epoch?: number; signal?: AbortSignal } = {}) {
    if (!this.jobId) return;
    // Polling already retries every second; avoid overlapping per-request 425 loops.
    const next = await this.api.fetchManifest(this.jobId, {
      retries: 0,
      signal: opts.signal,
    });
    if (opts.epoch != null) this.assertCurrent(opts.epoch, opts.signal);
    if (
      (next.manifest_revision ?? 0) <
      (this.manifest?.manifest_revision ?? 0)
    ) {
      return;
    }
    this.manifest = next;
    this.cb.onManifest?.(next);
    if (isManifestComplete(next)) {
      this.stopManifestPolling();
      if (!this.playing) {
        this.cb.onStatus?.(
          `${next.job_id} · ready · ${next.bundle.frame_count} frames · decode=${this.hwMode}`,
        );
      }
    }
  }

  private resetDecoders() {
    for (const cam of this.cams.values()) {
      try {
        cam.decoder?.close();
      } catch {
        /* ignore */
      }
      cam.decoder = null;
      cam.configured = false;
      cam.codec = "";
      cam.needsKeyframe = true;
      cam.decodeErrors = 0;
      // Keep Windows / ?decode=software preference across reloads.
      cam.forceSoftware = this.accel === "prefer-software";
      cam.waiters = [];
      cam.lastConfig = null;
      cam.description = null;
      cam.codedWidth = 0;
      cam.codedHeight = 0;
      cam.epoch = this.seekEpoch;
    }
    this.hwMode = "unknown";
  }

  private segmentFrames(): number {
    const gop = this.manifest?.bundle.gop_size;
    if (typeof gop === "number" && gop > 0) return gop;
    return SEGMENT_FRAMES;
  }

  private isSparseSegmentReady(index: number): boolean {
    if (!this.Sparse || !this.manifest) return true;
    const segN = this.segmentFrames();
    const start = segmentStartFor(index, segN);
    const endExclusive = Math.min(
      this.manifest.bundle.frame_count,
      start + segN,
    );
    return (this.manifest.bundle.available_ranges ?? []).some(
      ([a, b]) => start >= a && endExclusive <= b,
    );
  }

  private waitAbortable(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    return new Promise((resolve, reject) => {
      const timer = window.setTimeout(resolve, ms);
      signal?.addEventListener(
        "abort",
        () => {
          window.clearTimeout(timer);
          reject(new DOMException("Aborted", "AbortError"));
        },
        { once: true },
      );
    });
  }

  private ensureSparseSegmentReady(
    frameIndex: number,
    epoch: number,
    signal?: AbortSignal,
    request = false,
  ): Promise<void> {
    if (!this.Sparse) return Promise.resolve();
    const start = segmentStartFor(frameIndex, this.segmentFrames());
    const existing = this.sparseReadyInflight.get(start);
    if (existing?.epoch === epoch) return existing.promise;
    const promise = this.waitForSparseSegment(
      frameIndex,
      epoch,
      signal,
      request,
    ).finally(() => {
      if (this.sparseReadyInflight.get(start)?.promise === promise) {
        this.sparseReadyInflight.delete(start);
      }
    });
    this.sparseReadyInflight.set(start, { epoch, promise });
    return promise;
  }

  private async waitForSparseSegment(
    frameIndex: number,
    epoch: number,
    signal?: AbortSignal,
    request = false,
  ): Promise<void> {
    this.assertCurrent(epoch, signal);
    if (request || !this.isSparseSegmentReady(frameIndex)) {
      const response = await this.api.requestEncode(
        this.jobId,
        {
          frame_index: frameIndex,
          prefetch_segments: ENCODE_PREFETCH_SEGMENTS,
          client_epoch: epoch,
          client_id: this.clientId,
        },
        signal,
      );
      this.assertCurrent(epoch, signal);
      if (response.request_id && response.status !== "ready") {
        this.activeRequestId = response.request_id;
      }
    }
    const started = performance.now();
    while (!this.isSparseSegmentReady(frameIndex)) {
      this.assertCurrent(epoch, signal);
      if (performance.now() - started > 2 * 60 * 1000) {
        throw new Error(`等待稀疏段 ${segmentStartFor(frameIndex, this.segmentFrames())} 超时`);
      }
      await this.waitAbortable(400, signal);
      await this.refreshManifest({ epoch, signal });
    }
    this.assertCurrent(epoch, signal);
    this.activeRequestId = "";
  }

  private trimFrameCache() {
    const segN = this.segmentFrames();
    const keepBehind = segN;
    const keepAhead = PREFETCH_SEGMENTS * segN;
    const maxFrames = CACHE_SEGMENTS * segN;
    if (this.cache.size <= maxFrames) return;
    const keys = [...this.cache.keys()].sort((a, b) => a - b);
    for (const k of keys) {
      if (k < this.index - keepBehind || k > this.index + keepAhead) {
        this.cache.delete(k);
      }
      if (this.cache.size <= Math.floor(maxFrames * 0.75)) break;
    }
  }

  private segmentCachedThrough(start: number, segN: number): boolean {
    const limit = this.Sparse
      ? this.manifest?.bundle.frame_count ?? 0
      : this.Available;
    const end = Math.min(limit, start + segN);
    if (end <= start) return true;
    for (let i = start; i < end; i++) {
      if (!this.cache.has(i)) return false;
    }
    return true;
  }

  /** Load one transport segment into the frame cache (deduped). */
  private ensureSegment(
    startIndex: number,
    opts: {
      retries?: number;
      quiet?: boolean;
      epoch?: number;
      signal?: AbortSignal;
    } = {},
  ): Promise<void> {
    const segN = this.segmentFrames();
    const start = segmentStartFor(startIndex, segN);
    if (this.segmentCachedThrough(start, segN)) return Promise.resolve();
    const epoch = opts.epoch ?? this.seekEpoch;
    this.assertCurrent(epoch, opts.signal);

    let entry = this.segmentInflight.get(start);
    if (!entry || entry.epoch !== epoch) {
      // Sign a window ahead so boundary crossings don't wait on Master RTT.
      const signAhead: number[] = [];
      for (let i = 0; i <= PREFETCH_SEGMENTS; i++) {
        const s = start + i * segN;
        const total = this.manifest?.bundle.frame_count ?? this.Available;
        if (s >= total) break;
        if (this.Sparse && !this.isSparseSegmentReady(s)) continue;
        if (!this.Sparse && s >= this.Available) break;
        signAhead.push(s);
      }
      const signP =
        this.api.preferSignedOss && signAhead.length
          ? this.api
              .prefetchSignedSegmentUrls(this.jobId, signAhead, opts.signal)
              .catch((e) => {
                if (isAbortError(e)) throw e;
              })
          : Promise.resolve();

      const promise = signP
        .then(() =>
          this.api.fetchSegment(this.jobId, start, {
            retries: opts.retries,
            signal: opts.signal,
            onWait: (segStart, attempt) => {
              if (opts.quiet) return;
              if (attempt === 0 || attempt % 4 === 0) {
                this.cb.onStatus?.(`加载中… 段 ${segStart}`);
              }
            },
          }),
        )
        .then((buf) => {
          this.assertCurrent(epoch, opts.signal);
          const parsed = parseSegment(buf);
          for (const [idx, frameBuf] of parsed.frames) {
            this.assertCurrent(epoch, opts.signal);
            this.cache.set(idx, frameBuf);
          }
          this.trimFrameCache();
        })
        .finally(() => {
          if (this.segmentInflight.get(start)?.promise === promise) {
            this.segmentInflight.delete(start);
          }
        });
      entry = { epoch, promise };
      this.segmentInflight.set(start, entry);
    }
    return entry.promise;
  }

  private async getFrameBuf(
    index: number,
    epoch = this.seekEpoch,
    signal?: AbortSignal,
  ): Promise<ArrayBuffer> {
    this.assertCurrent(epoch, signal);
    const hit = this.cache.get(index);
    if (hit) return hit;
    await this.ensureSegment(index, { epoch, signal });
    this.assertCurrent(epoch, signal);
    const after = this.cache.get(index);
    if (after) return after;
    throw new FramePendingError(index, 425);
  }

  private async prefetchAround(index: number) {
    if (!this.manifest) return;
    const segN = this.segmentFrames();
    const base = segmentStartFor(index, segN);
    const starts: number[] = [];
    const total = this.manifest.bundle.frame_count;
    for (let s = 0; s <= PREFETCH_SEGMENTS; s++) {
      const start = base + s * segN;
      if (start >= total) break;
      if (this.Sparse && !this.isSparseSegmentReady(start)) continue;
      if (!this.Sparse && start >= this.Available) break;
      const fullReady = this.Sparse
        ? this.isSparseSegmentReady(start)
        : this.Available >= start + segN;
      const finalPartial = this.Sparse
        ? this.isSparseSegmentReady(start)
        : this.Complete && this.Available > start;
      if (!fullReady && !finalPartial) break;
      starts.push(start);
    }
    if (starts.length && this.api.preferSignedOss) {
      void this.api
        .prefetchSignedSegmentUrls(this.jobId, starts, this.seekAbort?.signal)
        .catch(() => {
        /* signed disabled → Master fallback inside fetchSegment */
        });
    }
    for (const start of starts) {
      if (this.segmentCachedThrough(start, segN) || this.segmentInflight.has(start)) {
        continue;
      }
      void this.ensureSegment(start, {
        quiet: true,
        epoch: this.seekEpoch,
        signal: this.seekAbort?.signal,
      }).catch(() => {
        /* prefetch miss is fine */
      });
    }
  }

  /** Find first AU that contains SPS/PPS for each channel (usually a keyframe). */
  private async findParamSets(): Promise<
    Partial<Record<Channel, { sps: Uint8Array; pps: Uint8Array }>>
  > {
    const out: Partial<Record<Channel, { sps: Uint8Array; pps: Uint8Array }>> = {};
    const keys = [
      0,
      ...(this.manifest?.bundle.key_frame_indices ?? []),
    ].filter((v, i, a) => a.indexOf(v) === i);
    for (const idx of keys.slice(0, 8)) {
      if (idx >= this.Available) continue;
      if (this.Sparse && !this.isSparseSegmentReady(idx)) continue;
      const buf = await this.getFrameBuf(idx);
      const parsed = parseBundle(buf);
      for (const ch of this.channelList()) {
        if (out[ch]) continue;
        const sp = extractSpsPps(parsed.streams[ch]);
        if (sp) out[ch] = sp;
      }
      if (this.channelList().every((ch) => out[ch])) break;
    }
    return out;
  }

  private bindDecoder(ch: Channel, cam: CamState, cfg: VideoDecoderConfig) {
    try {
      cam.decoder?.close();
    } catch {
      /* */
    }
    const decoderEpoch = this.seekEpoch;
    const decoder = new VideoDecoder({
      output: (frame) => {
        try {
          if (decoderEpoch !== this.seekEpoch || cam.epoch !== decoderEpoch) {
            return;
          }
          if (
            cam.canvas.width !== frame.displayWidth ||
            cam.canvas.height !== frame.displayHeight
          ) {
            cam.canvas.width = frame.displayWidth;
            cam.canvas.height = frame.displayHeight;
          }
          cam.ctx!.drawImage(frame, 0, 0);
        } finally {
          frame.close();
          const waiters = cam.waiters.splice(0);
          for (const wfn of waiters) wfn();
        }
      },
      error: (e) => {
        const err = e instanceof Error ? e : new Error(String(e));
        cam.decodeErrors += 1;
        cam.configured = false;
        cam.needsKeyframe = true;
        try {
          cam.decoder?.close();
        } catch {
          /* */
        }
        cam.decoder = null;
        const waiters = cam.waiters.splice(0);
        for (const wfn of waiters) wfn();

        // After a few HW glitches, drop to software decode (runtime 降级).
        if (!cam.forceSoftware && cam.decodeErrors >= 2) {
          cam.forceSoftware = true;
          this.cb.onStatus?.(
            `${streamLabel(ch)} 硬解异常，降级软解… (${err.message})`,
          );
          void this.resetDecoder(ch, cam, true).catch(() => {
            /* ensureConfigured later */
          });
          return;
        }

        this.cb.onStatus?.(
          `${streamLabel(ch)} 解码异常，等待关键帧恢复… (${err.message})`,
        );
        if (cam.decodeErrors >= 8) {
          this.cb.onError?.(
            new Error(
              `${ch} decode: ${err.message}（连续失败，可能该路码流损坏或无 SPS/PPS）`,
            ),
          );
        }
      },
    });
    decoder.configure(cfg);
    cam.decoder = decoder;
    cam.configured = true;
    cam.lastConfig = cfg;
    cam.needsKeyframe = true;
    cam.epoch = decoderEpoch;
    const hw = cfg.hardwareAcceleration;
    if (hw === "prefer-software") cam.forceSoftware = true;
  }

  /** Build supported VideoDecoderConfig ladder; Windows / force → software first. */
  private async pickDecoderConfig(
    codec: string,
    description: Uint8Array,
    forceSoftware = false,
  ): Promise<VideoDecoderConfig | null> {
    // Omit codedWidth/Height — manifest size can disagree with SPS and break HW.
    const base = { codec, description };
    const softFirst =
      forceSoftware ||
      this.accel === "prefer-software" ||
      (this.accel === "auto" && isWindowsPlatform());

    const ladder: VideoDecoderConfig[] = softFirst
      ? [
          {
            ...base,
            optimizeForLatency: true,
            hardwareAcceleration: "prefer-software",
          },
          { ...base, hardwareAcceleration: "prefer-software" },
          { ...base, hardwareAcceleration: "no-preference" },
          // Last resort if software path is unavailable in this browser.
          { ...base, hardwareAcceleration: "prefer-hardware" },
        ]
      : [
          {
            ...base,
            optimizeForLatency: true,
            hardwareAcceleration: "prefer-hardware",
          },
          {
            ...base,
            optimizeForLatency: false,
            hardwareAcceleration: "prefer-hardware",
          },
          { ...base, hardwareAcceleration: "no-preference" },
          { ...base, hardwareAcceleration: "prefer-software" },
        ];

    for (const cfg of ladder) {
      try {
        const support = await VideoDecoder.isConfigSupported(cfg);
        if (support.supported) return support.config ?? cfg;
      } catch {
        /* try next */
      }
    }
    return null;
  }

  private async ensureConfigured() {
    if (!this.manifest) throw new Error("no manifest");
    const channels = this.channelList();
    const params = await this.findParamSets();
    const missing = channels.filter((ch) => !params[ch]);
    if (missing.length === channels.length) {
      throw new Error(
        "未在关键帧中找到 SPS/PPS，无法配置 VideoDecoder（检查 bin 是否含参数集）",
      );
    }
    if (missing.length) {
      this.cb.onStatus?.(
        `部分相机暂无参数集: ${missing.join(",").toUpperCase()}，将稍后恢复`,
      );
    }

    let anyHw = false;
    let anySw = false;

    for (const ch of channels) {
      const sp = params[ch];
      const cam = this.cams.get(ch);
      if (!sp || !cam || cam.configured) continue;

      const codec = codecFromSps(sp.sps);
      const description = buildAvcC(sp.sps, sp.pps);
      if (!cam.ctx) throw new Error(`no 2d context for ${ch}`);

      const chosen = await this.pickDecoderConfig(
        codec,
        description,
        cam.forceSoftware,
      );
      if (!chosen) {
        throw new Error(
          `${ch}: VideoDecoder 不支持 codec=${codec}` +
            "（可换 Chrome/Edge，或降低转码分辨率）",
        );
      }

      cam.codec = codec;
      cam.description = description;
      this.bindDecoder(ch, cam, chosen);
      const hw = chosen.hardwareAcceleration;
      if (hw === "prefer-hardware") anyHw = true;
      if (hw === "prefer-software") anySw = true;
    }

    if (anySw && !anyHw) this.hwMode = "prefer-software";
    else if (anyHw && !anySw) this.hwMode = "prefer-hardware";
    else if (anyHw && anySw) this.hwMode = "mixed";
    else this.hwMode = "auto";
    if (
      anyHw &&
      [...this.cams.values()].some((c) => c.forceSoftware)
    ) {
      this.hwMode = "prefer-software(fallback)";
    }
  }

  private async recoverChannel(ch: Channel) {
    const cam = this.cams.get(ch);
    if (!cam) return;
    if (cam.configured && cam.decoder) return;
    await this.resetDecoder(ch, cam, cam.forceSoftware || cam.decodeErrors >= 2);
  }

  private waitOutput(cam: CamState, timeoutMs = 2000): Promise<void> {
    return new Promise((resolve) => {
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        resolve();
      };
      cam.waiters.push(finish);
      window.setTimeout(finish, timeoutMs);
    });
  }

  private async decodeFrame(
    index: number,
    render: boolean,
    epoch = this.seekEpoch,
    signal?: AbortSignal,
  ) {
    this.assertCurrent(epoch, signal);
    if (![...this.cams.values()].some((c) => c.configured)) {
      await this.ensureConfigured();
    }
    this.assertCurrent(epoch, signal);
    const buf = await this.getFrameBuf(index, epoch, signal);
    this.assertCurrent(epoch, signal);
    const parsed = parseBundle(buf);
    // Relative PTS — absolute monotonic ns confuses some HW decoders
    const tsUs = Math.round((index * 1_000_000) / this.fps);

    const waits: Promise<void>[] = [];
    for (const ch of this.channelList()) {
      this.assertCurrent(epoch, signal);
      const cam = this.cams.get(ch);
      if (!cam) continue;
      const au = parsed.streams[ch];
      if (!au?.length) continue;
      if (!cam.decoder || !cam.configured) {
        await this.recoverChannel(ch);
      }
      this.assertCurrent(epoch, signal);
      if (!cam.decoder || cam.decoder.state !== "configured") continue;

      // Per-channel key detection — don't trust global header for chunk type.
      const key = hasIdr(au);
      if (cam.needsKeyframe && !key) continue;

      const data = annexBToAvcc(au, true);
      if (!data.length) continue;
      const chunk = new EncodedVideoChunk({
        type: key ? "key" : "delta",
        timestamp: tsUs,
        data,
      });
      if (render) waits.push(this.waitOutput(cam));
      try {
        cam.decoder.decode(chunk);
        if (key) {
          cam.needsKeyframe = false;
          cam.decodeErrors = 0;
        }
      } catch {
        cam.configured = false;
        cam.needsKeyframe = true;
        cam.decodeErrors += 1;
        try {
          cam.decoder.close();
        } catch {
          /* */
        }
        cam.decoder = null;
        if (!cam.forceSoftware && cam.decodeErrors >= 2) {
          cam.forceSoftware = true;
          this.cb.onStatus?.(
            `${streamLabel(ch)} decode 失败，降级软解…`,
          );
          void this.resetDecoder(ch, cam, true);
        } else {
          this.cb.onStatus?.(
            `${streamLabel(ch)} decode 调用失败，将从关键帧恢复`,
          );
        }
      }
    }
    if (render && waits.length) {
      await Promise.all(waits);
    }
    this.assertCurrent(epoch, signal);
    if (render) {
      this.index = index;
      this.cb.onFrame?.(index, parsed.header.timestamp_ns, parsed.imu);
    }
  }

  private async showFrame(index: number) {
    await this.decodeFrame(
      index,
      true,
      this.seekEpoch,
      this.seekAbort?.signal,
    );
  }
}
