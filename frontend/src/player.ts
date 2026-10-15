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
  /** 当 manifest 变长时触发（边转边播）。 */
  onManifest?: (m: Manifest) => void;
};

/** 解码偏好。Windows 硬解（DXVA）多路 H.264 经常花屏。 */
export type DecodeAccel = "auto" | "prefer-hardware" | "prefer-software";

type CamState = {
  decoder: VideoDecoder | null;
  canvas: HTMLCanvasElement;
  ctx: CanvasRenderingContext2D | null;
  configured: boolean;
  codec: string;
  /** 解码出错后，跳过差值帧直到下一个 IDR。 */
  needsKeyframe: boolean;
  decodeErrors: number;
  /** 硬解异常后，下次 configure 强制优先软解。 */
  forceSoftware: boolean;
  waiters: Array<() => void>;
  lastConfig: VideoDecoderConfig | null;
  description: Uint8Array | null;
  codedWidth: number;
  codedHeight: number;
  /** 这个解码器允许上屏的时间线代次。 */
  epoch: number;
};

/** 播放头前面预取多少段（Vite/Master 代理慢）。 */
const PREFETCH_SEGMENTS = 8;
/** 稀疏编码只留一小段前向窗口，服务端会接着编。 */
const ENCODE_PREFETCH_SEGMENTS = 3;
/** 内存里大约留这么多段的帧 bundle。 */
const CACHE_SEGMENTS = 16;
/** 自动播放前要整段缓存的段数（减少起播欠载）。 */
const WARMUP_SEGMENTS = 5;
/** 当前段还剩这么多帧时，硬等下一段。 */
const BOUNDARY_AWAIT_FRAMES = 8;
/** 落后超过这么多个帧周期才把播放时钟拉齐。 */
const MAX_FRAME_DEBT = 5;
/**
 * 边转边播时，不播进最后 N 个已发布帧。
 * 避免播放头一贴上 frames_available 就微卡一下。
 */
const FRONTIER_PAD_FRAMES = 90; // 约 3 秒 @ 30fps
/** 撞上垫层后，领先量恢复到这里再继续播。 */
const FRONTIER_RESUME_FRAMES = 150; // 约 5 秒 @ 30fps
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

/** 解析加速方式：URL ?decode=software|hardware|auto，否则 Windows 用软解。 */
export function resolveDecodeAccel(override?: DecodeAccel): DecodeAccel {
  if (override && override !== "auto") return override;
  try {
    const q = new URLSearchParams(location.search).get("decode")?.toLowerCase();
    if (q === "software" || q === "prefer-software") return "prefer-software";
    if (q === "hardware" || q === "prefer-hardware") return "prefer-hardware";
  } catch {
    /* 忽略 */
  }
  // 四路硬解同时跑，Windows 上经常撕裂或绿块。
  return isWindowsPlatform() ? "prefer-software" : "prefer-hardware";
}

export class SviewPlayer {
  private api: SviewApi;
  private jobId = "";
  private manifest: Manifest | null = null;
  private cams = new Map<Channel, CamState>();
  private cache = new Map<number, ArrayBuffer>();
  /** 在途的段加载，按段起点索引。 */
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
  /** 一帧的拉取、解码、绘制还在进行（背压）。 */
  private advancing = false;
  /** 前沿欠载后：先停住，等领先量补回来。 */
  private frontierHold = false;
  /** 与稀疏编码请求共用的单调代次。 */
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

  /**
   * 播放头前面已经编好的帧。
   * 稀疏区间随段发布增长；跟这个结尾走，跟非稀疏跟 frames_available 一样。
   * 不要把整段片子当成已经就绪。
   */
  private publishedEnd(): number {
    const m = this.manifest;
    if (!m) return 0;
    const idx = this.index;
    const prefix = m.bundle.frames_available ?? 0;
    if (idx < prefix) return prefix;
    let end = 0;
    for (const [a, b] of m.bundle.available_ranges ?? []) {
      if (idx >= a && idx < b) end = Math.max(end, b);
    }
    return end;
  }

  /** 当前盘上、可以请求的帧（编码器前沿）。 */
  get Available() {
    if (this.Sparse) return this.publishedEnd();
    return framesAvailable(this.manifest);
  }

  get Complete() {
    return isManifestComplete(this.manifest);
  }

  get Sparse() {
    return this.manifest?.publish_mode === "sparse";
  }

  private rangeStart(): number {
    const idx = this.index;
    for (const [a, b] of this.manifest?.bundle.available_ranges ?? []) {
      if (idx >= a && idx < b) return a;
    }
    const prefix = this.manifest?.bundle.frames_available ?? 0;
    if (idx < prefix) return 0;
    return idx;
  }

  /**
   * 一次 seek 会落在新区间的开头几帧。90 帧垫层会落在播放头后面，
   * 播放就一步都迈不出去。
   */
  private sparseAtRangeFront(): boolean {
    return this.Sparse && this.index - this.rangeStart() < FRONTIER_PAD_FRAMES;
  }

  /**
   * 边转边播时播放头最多能进到的 index+1。
   * 在编码器后面留出 FRONTIER_PAD_FRAMES，避免贴墙。
   */
  private playLimit(): number {
    const raw = this.Available;
    if (this.Complete) return raw;
    if (this.sparseAtRangeFront()) return raw;
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
    // 只用显式传入的 mediaBase。不要自动切到 manifest.play_base：
    // 私有桶会拦匿名 GET（403）；改由 Master 302 到签名 OSS。
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
   * 堵住，直到后面几段进缓存（或前沿用尽）。
   * 媒体走慢的 Master/Vite 代理时，play() 之前调用。
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
        /* 回退到 ensureSegment 里 */
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
      // 串行：showFrame 不能重叠（以前卡顿的主因）。
      if (this.advancing || this.waitingForFrames) return;

      const frameMs = 1000 / this.fps;
      if (now - this.lastTick < frameMs) return;

      this.advancing = true;
      void this.advanceOneFrame()
        .then((presented) => {
          if (!this.playing) return;
          if (!presented) {
            // 缓冲中 / 结束——冻住时钟，避免等完后突发。
            this.lastTick = performance.now();
            return;
          }
          this.lastTick += frameMs;
          // 稍微落后：尽快上屏，不拉齐（避免微顿）。
          // 落后很多：拉齐，避免延迟越积越长。
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
      /* 请求可能已经就绪或过期 */
    });
  }

  private assertCurrent(epoch: number, signal?: AbortSignal) {
    if (signal?.aborted || epoch !== this.seekEpoch) {
      throw new DOMException("Aborted", "AbortError");
    }
  }

  /** 以最后一次 seek 为准；被取代的 seek 不缓存、不上屏。 */
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
      // 稀疏段从段头就能独立解码。
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

  /** 每个解码器都 flush/关闭，再用上次（或软解）配置重新绑定。 */
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
        /* 坏管线上的 flush 错误忽略 */
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
    // 退回：从盘上的 SPS/PPS 完整重新配置。
    cam.configured = false;
    await this.ensureConfigured();
  }

  /**
   * 正好前进一帧显示（会等待）。
   * @returns 上屏了一帧则为 true。
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

    // 从头播要留 90 帧领先。刚 seek 还没有领先量：先播已经发布的帧，
    // 追上编码器之后再等。
    if (!this.Complete && !this.sparseAtRangeFront()) {
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
        // 恢复条件满足且下一帧仍有效，就继续上屏。
        if (!this.Complete && next >= this.playLimit()) {
          return false;
        }
      }
    }

    if (next >= raw) {
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
        this.frontierHold = true;
        this.cb.onStatus?.(
          `缓冲中… 领先 ${this.frontierLead()} 帧（可用 ${this.Available}/${this.manifest.bundle.frame_count}）`,
        );
        await this.refreshManifest();
        await new Promise((r) => window.setTimeout(r, 200));
        return false;
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
   * 靠近段边界时，解码前先把下一段加载完。
   * 避免播放头跨进未加载的包时，大约每 1 秒卡一下。
   */
  private async ensureSegmentBoundary(index: number): Promise<void> {
    const segN = this.segmentFrames();
    const curStart = segmentStartFor(index, segN);
    const pos = index - curStart;
    const nextStart = curStart + segN;
    const epoch = this.seekEpoch;
    const signal = this.seekAbort?.signal;

    // 当前段始终保持热着。
    if (!this.segmentCachedThrough(curStart, segN)) {
      await this.ensureSegment(curStart, { quiet: true, epoch, signal });
    }

    const total = this.manifest?.bundle.frame_count ?? this.Available;
    if (nextStart >= total) return;
    if (this.Sparse && !this.isSparseSegmentReady(nextStart)) {
      // 编码会话从 seek 点一直往前读。
      // 这里再要这一段会把前沿挪走，并重新打开 mcap。
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
        /* 忽略瞬时错误 */
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
    // 轮询已经每秒重试；不要再叠一层按请求的 425 循环。
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
        /* 忽略 */
      }
      cam.decoder = null;
      cam.configured = false;
      cam.codec = "";
      cam.needsKeyframe = true;
      cam.decodeErrors = 0;
      // 重新加载时保留 Windows / ?decode=software 偏好。
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

  /** 把一段传输包载入帧缓存（去重）。 */
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
      // 提前签一段窗口，跨边界时不用等 Master RTT。
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
        /* 签名关掉后，fetchSegment 里回退 Master */
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
        /* 预取没命中没关系 */
      });
    }
  }

  /** 找每个通道第一包含 SPS/PPS 的 AU（通常是关键帧）。 */
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

        // 硬解连续异常几次后，运行时降级软解。
        if (!cam.forceSoftware && cam.decodeErrors >= 2) {
          cam.forceSoftware = true;
          this.cb.onStatus?.(
            `${streamLabel(ch)} 硬解异常，降级软解… (${err.message})`,
          );
          void this.resetDecoder(ch, cam, true).catch(() => {
            /* 稍后 ensureConfigured */
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

  /** 搭一份浏览器支持的 VideoDecoderConfig 阶梯；Windows / 强制时软解优先。 */
  private async pickDecoderConfig(
    codec: string,
    description: Uint8Array,
    forceSoftware = false,
  ): Promise<VideoDecoderConfig | null> {
    // 不填 codedWidth/Height——manifest 尺寸可能和 SPS 不一致，硬解会挂。
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
          // 这个浏览器没有软解时的最后一档。
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
        /* 试下一档 */
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

  private async configureFromParams(
    ch: Channel,
    cam: CamState,
    sp: { sps: Uint8Array; pps: Uint8Array },
  ) {
    const codec = codecFromSps(sp.sps);
    const description = buildAvcC(sp.sps, sp.pps);
    const chosen = await this.pickDecoderConfig(codec, description, cam.forceSoftware);
    if (!chosen) return;
    cam.codec = codec;
    cam.description = description;
    this.bindDecoder(ch, cam, chosen);
  }

  private blankCanvas(cam: CamState) {
    if (!cam.ctx) return;
    cam.ctx.fillStyle = "#000";
    cam.ctx.fillRect(0, 0, cam.canvas.width, cam.canvas.height);
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
    // 相对 PTS——绝对单调纳秒会让有的硬解困惑
    const tsUs = Math.round((index * 1_000_000) / this.fps);

    const waits: Promise<void>[] = [];
    for (const ch of this.channelList()) {
      this.assertCurrent(epoch, signal);
      const cam = this.cams.get(ch);
      if (!cam) continue;
      const au = parsed.streams[ch];
      if (!au?.length) {
        // 这一路此帧还没有可解码画面（关键帧之前留空），别停在 seek 前的旧图上。
        if (render && cam.needsKeyframe) this.blankCanvas(cam);
        continue;
      }
      if (!cam.configured && !cam.codec) {
        // 开头几帧留空的那一路，启动时找不到参数集；第一张 IDR 自带 SPS/PPS。
        const sp = extractSpsPps(au);
        if (sp) await this.configureFromParams(ch, cam, sp);
      }
      if (!cam.decoder || !cam.configured) {
        await this.recoverChannel(ch);
      }
      this.assertCurrent(epoch, signal);
      if (!cam.decoder || cam.decoder.state !== "configured") continue;

      // 按通道判断关键帧，不拿全局头当 chunk 类型。
      const key = hasIdr(au);
      if (cam.needsKeyframe && !key) {
        if (render) this.blankCanvas(cam);
        continue;
      }

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
