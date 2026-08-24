import type { BundleHeader, SegmentHeader } from "./api";

export type ParsedBundle = {
  header: BundleHeader;
  streams: Record<string, Uint8Array>;
  imu: unknown;
  events: unknown;
};

export type ParsedSegment = {
  header: SegmentHeader;
  /** Frame index → original single-frame bundle bytes. */
  frames: Map<number, ArrayBuffer>;
};

/** Default transport batch size (~1s @ 30fps / 1 GOP). */
export const SEGMENT_FRAMES = 30;

export function segmentStartFor(index: number, segmentFrames = SEGMENT_FRAMES): number {
  const n = Math.max(1, segmentFrames);
  return Math.floor(Math.max(0, index) / n) * n;
}

function readLengthPrefixed(buf: ArrayBuffer): {
  headerJson: string;
  payload: Uint8Array;
} {
  const view = new DataView(buf);
  if (buf.byteLength < 8) throw new Error("bundle too small");

  const hlen = view.getUint32(0, true);
  const hdrStart = 4;
  const hdrEnd = hdrStart + hlen;
  if (hdrEnd + 4 > buf.byteLength) throw new Error("bad header length");

  const headerJson = new TextDecoder().decode(new Uint8Array(buf, hdrStart, hlen));
  const plen = view.getUint32(hdrEnd, true);
  const payloadStart = hdrEnd + 4;
  if (payloadStart + plen > buf.byteLength) throw new Error("bad payload length");
  return {
    headerJson,
    payload: new Uint8Array(buf, payloadStart, plen),
  };
}

export function parseBundle(buf: ArrayBuffer): ParsedBundle {
  const { headerJson, payload } = readLengthPrefixed(buf);
  const header = JSON.parse(headerJson) as BundleHeader;

  const streams: Record<string, Uint8Array> = {};
  for (const ch of Object.keys(header.streams)) {
    const s = header.streams[ch];
    if (!s || s.is_null || s.payload_length <= 0) {
      streams[ch] = new Uint8Array(0);
      continue;
    }
    streams[ch] = payload.subarray(s.payload_offset, s.payload_offset + s.payload_length);
  }

  let imu: unknown = null;
  let events: unknown = null;
  try {
    const imuSlice = payload.subarray(
      header.imu.payload_offset,
      header.imu.payload_offset + header.imu.payload_length,
    );
    imu = JSON.parse(new TextDecoder().decode(imuSlice));
  } catch {
    imu = null;
  }
  try {
    const evSlice = payload.subarray(
      header.events.payload_offset,
      header.events.payload_offset + header.events.payload_length,
    );
    events = JSON.parse(new TextDecoder().decode(evSlice));
  } catch {
    events = null;
  }

  return { header, streams, imu, events };
}

/** Unpack `playertrans.bundle_segment.v1` into per-frame bundle buffers. */
export function parseSegment(buf: ArrayBuffer): ParsedSegment {
  const { headerJson, payload } = readLengthPrefixed(buf);
  const header = JSON.parse(headerJson) as SegmentHeader;
  if (
    header.packet_schema &&
    header.packet_schema !== "playertrans.bundle_segment.v1"
  ) {
    throw new Error(`unexpected segment schema: ${header.packet_schema}`);
  }
  const start = header.start_index ?? 0;
  const count = header.count ?? 0;
  const frames = new Map<number, ArrayBuffer>();
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  let off = 0;
  for (let i = 0; i < count; i++) {
    if (off + 4 > payload.byteLength) throw new Error("segment truncated (len)");
    const flen = view.getUint32(off, true);
    off += 4;
    if (off + flen > payload.byteLength) throw new Error("segment truncated (frame)");
    const slice = payload.subarray(off, off + flen);
    const copy = slice.slice().buffer;
    frames.set(start + i, copy);
    off += flen;
  }
  return { header, frames };
}
