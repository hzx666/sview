#!/usr/bin/env python3
"""常驻 live ffmpeg：一个进程、多段 chunk、每幅图一个 AU。"""
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sview.worker.cpu import (  # noqa: E402
    FFMPEG,
    LiveX264Encoder,
    _h264_payload_has_idr,
    _split_h264_access_units,
)


def _run_ffmpeg(args: list[str], data: bytes | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [FFMPEG, "-hide_banner", "-loglevel", "error", *args],
        input=data,
        capture_output=True,
        check=True,
    )


def _split_source_frames(data: bytes, *, hevc: bool) -> list[bytes]:
    if not hevc:
        return _split_h264_access_units(data, None)
    vcl = {0, 1, 2, 3, 4, 5, 16, 17, 18, 19, 20, 21}
    starts = []
    pos = 0
    while pos < len(data) - 3:
        p4 = data.find(b"\x00\x00\x00\x01", pos)
        p3 = data.find(b"\x00\x00\x01", pos)
        if p4 < 0 and p3 < 0:
            break
        if p4 < 0 or (p3 >= 0 and p3 < p4):
            np, sc = p3, 3
        else:
            np, sc = p4, 4
        if np + sc < len(data):
            ntype = (data[np + sc] >> 1) & 0x3F
            starts.append((np, ntype))
        pos = np + sc + 1
    frames: list[bytes] = []
    au = 0
    seen = False
    for i, (np, ntype) in enumerate(starts):
        nxt = starts[i + 1][0] if i + 1 < len(starts) else len(data)
        if ntype in vcl and seen:
            frames.append(data[au:np])
            au = np
        seen = seen or ntype in vcl
    if seen:
        frames.append(data[au:])
    return frames


def _annexb_frames(codec: str, n: int) -> list[bytes]:
    enc = "libx264" if codec == "h264" else "libx265"
    extra = ["-x265-params", "log-level=error"] if codec == "hevc" else []
    proc = _run_ffmpeg(
        [
            "-f", "lavfi", "-i", "testsrc=size=160x120:rate=30",
            "-frames:v", str(n),
            "-pix_fmt", "yuv420p",
            "-c:v", enc, "-preset", "ultrafast", "-g", str(n), "-bf", "0",
            *extra,
            "-f", codec, "pipe:1",
        ]
    )
    frames = _split_source_frames(proc.stdout, hevc=codec == "hevc")
    if len(frames) != n:
        raise AssertionError(f"{codec} source split into {len(frames)} AUs, want {n}")
    return frames


def _decode_count(aus: list[bytes], codec: str = "h264") -> int:
    proc = subprocess.run(
        [FFMPEG, "-hide_banner", "-loglevel", "info", "-f", codec, "-i", "pipe:0", "-f", "null", "-"],
        input=b"".join(aus),
        capture_output=True,
        check=False,
    )
    text = proc.stderr.decode(errors="replace")
    for line in reversed(text.splitlines()):
        if "frame=" not in line:
            continue
        parts = line.replace("=", " ").split()
        if parts and parts[0] == "frame" and len(parts) > 1 and parts[1].isdigit():
            return int(parts[1])
    raise AssertionError(f"ffmpeg did not decode output:\n{text[-800:]}")


def _assert_chunk(aus: list[bytes], n: int, *, key0: bool) -> None:
    if len(aus) != n:
        raise AssertionError(f"got {len(aus)} pictures, want {n}")
    if any(not a for a in aus):
        raise AssertionError("empty picture in chunk")
    if _h264_payload_has_idr(aus[0]) != key0:
        raise AssertionError(f"first picture keyframe={not key0}, want {key0}")


def test_h264_persists_across_chunks():
    frames = _annexb_frames("h264", 40)
    enc = LiveX264Encoder.annexb(codec="h264", scale_w=96, scale_h=96, job_id="live-h264")
    try:
        first = enc.encode_frames(frames[:30])
        pid = enc._proc.pid
        _assert_chunk(first, 30, key0=True)
        if _h264_payload_has_idr(first[1]):
            raise AssertionError("frame 1 should be a P-frame")
        second = enc.encode_frames(frames[30:])
        if enc._proc is None or enc._proc.pid != pid:
            raise AssertionError("live session was restarted between chunks")
        # GOP 为 30，所以第 30 幅（本段第一幅）是 IDR。
        _assert_chunk(second, 10, key0=True)
        if _decode_count(first + second) != 40:
            raise AssertionError("concatenated pictures did not decode to 40 frames")
    finally:
        enc.close()
    if enc._proc is not None:
        raise AssertionError("close() left ffmpeg running")


def test_output_skip_drops_warmup():
    frames = _annexb_frames("h264", 8)
    enc = LiveX264Encoder.annexb(codec="h264", scale_w=96, scale_h=96, job_id="live-skip")
    try:
        aus = enc.encode_frames(frames, output_skip=3)
        _assert_chunk(aus, 5, key0=True)
        if _decode_count(aus) != 5:
            raise AssertionError("warmup pictures were encoded")
    finally:
        enc.close()


def test_jpeg_persists_across_chunks():
    proc = _run_ffmpeg(
        [
            "-f", "lavfi", "-i", "testsrc=size=160x120:rate=30",
            "-frames:v", "6",
            "-q:v", "4",
            "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1",
        ]
    )
    data = proc.stdout
    frames: list[bytes] = []
    pos = 0
    while True:
        start = data.find(b"\xff\xd8", pos)
        if start < 0:
            break
        end = data.find(b"\xff\xd9", start + 2)
        if end < 0:
            break
        frames.append(data[start : end + 2])
        pos = end + 2
    if len(frames) != 6:
        raise AssertionError(f"got {len(frames)} jpegs")
    enc = LiveX264Encoder.jpeg(scale_w=96, scale_h=96, job_id="live-jpeg", crf=28, direct=True)
    try:
        first = enc.encode_frames(frames[:3])
        pid = enc._proc.pid
        second = enc.encode_frames(frames[3:])
        _assert_chunk(first, 3, key0=True)
        # JPEG live 输出全是帧内，刷出的那幅不是参考帧。
        _assert_chunk(second, 3, key0=True)
        if enc._proc is None or enc._proc.pid != pid:
            raise AssertionError("jpeg session was restarted")
        if _decode_count(first + second) != 6:
            raise AssertionError("jpeg pictures did not decode to 6 frames")
    finally:
        enc.close()


def test_hevc_persists_across_chunks():
    try:
        frames = _annexb_frames("hevc", 8)
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or b"").decode(errors="replace")
        if "Unknown encoder" in err or "libx265" in err:
            print("skip hevc: libx265 not available")
            return
        raise
    enc = LiveX264Encoder.annexb(codec="hevc", scale_w=96, scale_h=96, job_id="live-hevc")
    try:
        first = enc.encode_frames(frames[:4])
        pid = enc._proc.pid
        second = enc.encode_frames(frames[4:])
        _assert_chunk(first, 4, key0=True)
        _assert_chunk(second, 4, key0=False)
        if enc._proc is None or enc._proc.pid != pid:
            raise AssertionError("hevc session was restarted")
        if _decode_count(first + second) != 8:
            raise AssertionError("hevc pictures did not decode to 8 frames")
    finally:
        enc.close()


if __name__ == "__main__":
    test_h264_persists_across_chunks()
    test_output_skip_drops_warmup()
    test_jpeg_persists_across_chunks()
    test_hevc_persists_across_chunks()
    print("ok")
