/**
 * 解析 handvis.mesh_segment.v1。
 * 信封与 SView playertrans.bundle_segment.v1 相同：
 *   header_len(uint32 LE) + header_json + payload_len(uint32 LE) + payload
 */

export const MESH_SEGMENT_SCHEMA = "handvis.mesh_segment.v1";

function readLengthPrefixed(buf) {
  const view = new DataView(buf);
  if (buf.byteLength < 8) throw new Error("mesh segment too small");
  const hlen = view.getUint32(0, true);
  const hdrStart = 4;
  const hdrEnd = hdrStart + hlen;
  if (hdrEnd + 4 > buf.byteLength) throw new Error("bad header length");
  const headerJson = new TextDecoder().decode(new Uint8Array(buf, hdrStart, hlen));
  const plen = view.getUint32(hdrEnd, true);
  const payloadStart = hdrEnd + 4;
  if (payloadStart + plen > buf.byteLength) throw new Error("bad payload length");
  return {
    header: JSON.parse(headerJson),
    payload: new Uint8Array(buf, payloadStart, plen),
  };
}

function copyF32(payload, stream) {
  if (!stream || stream.payload_length <= 0) return null;
  const off = Number(stream.payload_offset || 0);
  const len = Number(stream.payload_length || 0);
  if (off < 0 || len <= 0 || off + len > payload.byteLength) {
    throw new Error("mesh stream truncated");
  }
  const slice = payload.subarray(off, off + len);
  const copy = slice.slice();
  return new Float32Array(copy.buffer);
}

export function parseMeshSegment(buf) {
  const { header, payload } = readLengthPrefixed(buf);
  if (header.packet_schema && header.packet_schema !== MESH_SEGMENT_SCHEMA) {
    throw new Error(`unexpected segment schema: ${header.packet_schema}`);
  }
  const streams = header.streams || {};
  const left = copyF32(payload, streams.left);
  const right = copyF32(payload, streams.right);
  if (!left || !right) {
    throw new Error("mesh segment missing left/right streams");
  }
  return {
    header,
    left,
    right,
    handRoots: copyF32(payload, streams.hand_roots),
  };
}
