/** 给 WebCodecs 用的 Annex-B H.264。 */

export type NalInfo = { offset: number; startCodeLen: number; type: number };

export function findNals(data: Uint8Array): NalInfo[] {
  const out: NalInfo[] = [];
  let pos = 0;
  while (pos < data.length - 3) {
    let sc = 0;
    if (
      data[pos] === 0 &&
      data[pos + 1] === 0 &&
      data[pos + 2] === 0 &&
      data[pos + 3] === 1
    ) {
      sc = 4;
    } else if (data[pos] === 0 && data[pos + 1] === 0 && data[pos + 2] === 1) {
      sc = 3;
    }
    if (sc) {
      const type = data[pos + sc] & 0x1f;
      out.push({ offset: pos, startCodeLen: sc, type });
      pos += sc + 1;
      continue;
    }
    pos += 1;
  }
  return out;
}

export function hasIdr(data: Uint8Array): boolean {
  return findNals(data).some((n) => n.type === 5);
}

export function extractSpsPps(
  data: Uint8Array | undefined | null,
): { sps: Uint8Array; pps: Uint8Array } | null {
  if (!data?.length) return null;
  const nals = findNals(data);
  let sps: Uint8Array | null = null;
  let pps: Uint8Array | null = null;
  for (let i = 0; i < nals.length; i++) {
    const n = nals[i];
    const end = i + 1 < nals.length ? nals[i + 1].offset : data.length;
    const body = data.subarray(n.offset + n.startCodeLen, end);
    if (n.type === 7) sps = body;
    if (n.type === 8) pps = body;
  }
  if (!sps || !pps) return null;
  return { sps, pps };
}

/** 从 SPS NAL 体（无起始码）拼 codec 字符串，如 avc1.640032。 */
export function codecFromSps(sps: Uint8Array): string {
  if (sps.length < 4) return "avc1.64001f";
  const profile = sps[1];
  const compat = sps[2];
  const level = sps[3];
  const hex = (n: number) => n.toString(16).padStart(2, "0");
  return `avc1.${hex(profile)}${hex(compat)}${hex(level)}`;
}

/** 给 VideoDecoder description 用的 AVCDecoderConfigurationRecord。 */
export function buildAvcC(sps: Uint8Array, pps: Uint8Array): Uint8Array {
  const out = new Uint8Array(11 + sps.length + pps.length);
  let o = 0;
  out[o++] = 1; // 配置版本
  out[o++] = sps[1];
  out[o++] = sps[2];
  out[o++] = sps[3];
  out[o++] = 0xff; // 长度占 4 字节（lengthSizeMinusOne = 3）
  out[o++] = 0xe1; // 一个 SPS
  out[o++] = (sps.length >> 8) & 0xff;
  out[o++] = sps.length & 0xff;
  out.set(sps, o);
  o += sps.length;
  out[o++] = 1; // 一个 PPS
  out[o++] = (pps.length >> 8) & 0xff;
  out[o++] = pps.length & 0xff;
  out.set(pps, o);
  return out;
}

/**
 * 把 Annex-B 转成 AVCC。
 * @param vclOnly 为 true 时（建议配合 avcC description）只保留
 *   切片 NAL（1/5）。硬解常会失败：SPS/PPS/SEI 若在
 *   调用 configure({ description }) 之后的 sample 里重复出现。
 */
export function annexBToAvcc(data: Uint8Array, vclOnly = true): Uint8Array {
  const nals = findNals(data);
  if (!nals.length) return data;
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (let i = 0; i < nals.length; i++) {
    const n = nals[i];
    const end = i + 1 < nals.length ? nals[i + 1].offset : data.length;
    const body = data.subarray(n.offset + n.startCodeLen, end);
    if (vclOnly) {
      if (n.type !== 1 && n.type !== 5) continue;
    } else if (n.type === 9) {
      continue;
    }
    const len = new Uint8Array(4);
    new DataView(len.buffer).setUint32(0, body.length, false);
    chunks.push(len, body);
    total += 4 + body.length;
  }
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.length;
  }
  return out;
}
