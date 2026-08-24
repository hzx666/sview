/**
 * 视频投影元数据加载器（v5 协议）。
 *
 * video_projection.bin 布局：
 *   magic(4 bytes) "VP01"
 *   version(uint32 LE)
 *   frame_count(uint32 LE)
 *   positions:    frame_count × 3 × float32 LE  (无效帧填 NaN)
 *   quaternions:  frame_count × 4 × float32 LE
 *   valid_mask:   frame_count × uint8
 */

const MAGIC = "VP01";
const HEADER_BYTES = 4 + 4 + 4;

/**
 * 解析 video_projection.bin → TypedArray 视图。
 */
export function parseVideoProjectionBinary(arrayBuffer) {
  if (!arrayBuffer || arrayBuffer.byteLength < HEADER_BYTES) {
    throw new Error("video_projection.bin 太短，无法解析头部");
  }

  const view = new DataView(arrayBuffer);
  const magic = String.fromCharCode(view.getUint8(0), view.getUint8(1), view.getUint8(2), view.getUint8(3));
  if (magic !== MAGIC) {
    throw new Error(`video_projection.bin magic 错误：${magic}`);
  }
  const version = view.getUint32(4, true);
  const frameCount = view.getUint32(8, true);
  if (frameCount <= 0) return { frameCount: 0, positions: null, quaternions: null, validMask: null };

  const expectBytes = HEADER_BYTES + frameCount * (3 + 4) * 4 + frameCount * 1;
  if (arrayBuffer.byteLength < expectBytes) {
    throw new Error(
      `video_projection.bin 长度不足：expect=${expectBytes} got=${arrayBuffer.byteLength}`,
    );
  }

  let offset = HEADER_BYTES;
  const positions = new Float32Array(arrayBuffer, offset, frameCount * 3);
  offset += frameCount * 3 * 4;
  const quaternions = new Float32Array(arrayBuffer, offset, frameCount * 4);
  offset += frameCount * 4 * 4;
  const validMask = new Uint8Array(arrayBuffer, offset, frameCount);

  return { frameCount, version, positions, quaternions, validMask };
}

/**
 * 从 index.video_projection 字段 + 二进制 fetch 加载投影元数据。
 */
export async function loadVideoProjection({ meta, chunkBase, fetchArrayBuffer, joinUrl }) {
  if (!meta || typeof meta !== "object") return null;
  const focal = Number(meta.img_focal || 0);
  const center = Array.isArray(meta.img_center) ? meta.img_center : [0, 0];
  const baseW = Number(meta.base_width || (Number(center[0]) || 0) * 2);
  const baseH = Number(meta.base_height || (Number(center[1]) || 0) * 2);
  if (!(focal > 0) || !(baseW > 0) || !(baseH > 0)) return null;

  const binaryUrl = String(meta.binary_url || "").trim();
  if (!binaryUrl) {
    throw new Error("video_projection 缺少 binary_url，请重新构建 chunk");
  }

  const buffer = await fetchArrayBuffer(joinUrl(chunkBase, binaryUrl));
  const parsed = parseVideoProjectionBinary(buffer);
  return {
    img_focal: focal,
    img_center: center,
    base_width: baseW,
    base_height: baseH,
    scale: Number(meta.scale || 1),
    stride: Number(meta.stride || 1),
    frame_count: parsed.frameCount,
    positions: parsed.positions,
    quaternions: parsed.quaternions,
    valid_mask: parsed.validMask,
  };
}
