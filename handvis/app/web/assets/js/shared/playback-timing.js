/**
 * 播放页时间轴映射工具。
 *
 * pose3d_hand 里的 fps 可能与浏览器实际视频时长不完全一致（转码、VFR、29.97 vs 30 等）。
 * 一旦 video.duration 可用，优先用 duration / (frame_count - 1) 作为帧步长，避免手模相对视频系统性滞后/超前。
 */

export function resolveFrameStepSec({ metaFrameStepSec, totalFrames, videoDuration }) {
  const meta = Number(metaFrameStepSec);
  const safeMeta = Number.isFinite(meta) && meta > 0 ? meta : 1 / 30;
  const frames = Number(totalFrames);
  const duration = Number(videoDuration);
  if (frames > 1 && Number.isFinite(duration) && duration > 0) {
    return duration / (frames - 1);
  }
  return safeMeta;
}

export function mapTimelineToLocalFrame({ tSec, chunkStartTime, frameStepSec, frameCount }) {
  const step = Number(frameStepSec);
  const count = Math.max(0, Number(frameCount) || 0);
  if (!(step > 0) || count <= 0) {
    return { localFrame: 0, alpha: 0, frameFloat: 0 };
  }
  const start = Number(chunkStartTime) || 0;
  const frameFloat = Math.max(0, (tSec - start) / step);
  const local = Math.floor(frameFloat + 1e-6);
  const localFrame = Math.max(0, Math.min(count - 1, local));
  const alpha = Math.max(0, Math.min(1, frameFloat - localFrame));
  return { localFrame, alpha, frameFloat };
}
