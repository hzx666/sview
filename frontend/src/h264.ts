/** Annex-B H.264 helpers for WebCodecs. */

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

/** Build codec string like avc1.640032 from SPS NAL body (no start code). */
export function codecFromSps(sps: Uint8Array): string {
  if (sps.length < 4) return "avc1.64001f";
  const profile = sps[1];
  const compat = sps[2];
  const level = sps[3];
  const hex = (n: number) => n.toString(16).padStart(2, "0");
  return `avc1.${hex(profile)}${hex(compat)}${hex(level)}`;
}

/** AVCDecoderConfigurationRecord for VideoDecoder description. */
export function buildAvcC(sps: Uint8Array, pps: Uint8Array): Uint8Array {
  const out = new Uint8Array(11 + sps.length + pps.length);
  let o = 0;
  out[o++] = 1; // configurationVersion
  out[o++] = sps[1];
  out[o++] = sps[2];
  out[o++] = sps[3];
  out[o++] = 0xff; // lengthSizeMinusOne = 3 → 4-byte lengths
  out[o++] = 0xe1; // one SPS
  out[o++] = (sps.length >> 8) & 0xff;
  out[o++] = sps.length & 0xff;
  out.set(sps, o);
  o += sps.length;
  out[o++] = 1; // one PPS
  out[o++] = (pps.length >> 8) & 0xff;
  out[o++] = pps.length & 0xff;
  out.set(pps, o);
  return out;
}

/**
 * Annex-B → AVCC.
 * @param vclOnly when true (recommended with avcC description), only keep
 *   slice NALs (1/5). Hardware decoders often fail if SPS/PPS/SEI are repeated
 *   inside samples after configure({ description }).
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
