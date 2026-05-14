"""ffmpeg subprocess decoder.

Pipes the raw H.264 byte stream into ffmpeg on stdin and reads MJPEG
(concatenated JPEG frames) back on stdout. A JPEG is delimited by the
SOI/EOI markers `FF D8 FF ... FF D9`, so split on EOI to recover each
frame.

Known limitation: if the caller passes a bundle that does not contain a
self-contained GOP (IDR + subsequent frames), ffmpeg will log warnings
and may emit zero JPEGs — that is the documented case in the POC and is
handled by returning an empty list.
"""
from __future__ import annotations

import logging
import subprocess

from .base import Decoder


log = logging.getLogger(__name__)


class FfmpegDecoder(Decoder):
    def __init__(self, jpeg_quality: int = 85) -> None:
        # ffmpeg quality scale runs 2 (best) … 31 (worst); map 0–100 → 2–31
        q = max(2, min(31, int(31 - (jpeg_quality / 100) * 29)))
        # Input is MPEG-TS (daq-edge `mpegtsmux` → `udpsink`). We feed
        # `-f mpegts` explicitly to skip auto-probing and tolerate
        # segments that begin on a TS packet boundary but before the
        # first PAT/PMT repetition. edge-agent's GopAccumulator now
        # aligns its segment cuts to 188-byte TS boundaries.
        self._args = [
            "ffmpeg",
            "-hide_banner", "-loglevel", "error",
            "-err_detect", "ignore_err",
            "-fflags", "+discardcorrupt+genpts",
            "-probesize", "2M", "-analyzeduration", "2M",
            "-f", "mpegts", "-i", "pipe:0",
            "-f", "mjpeg", "-q:v", str(q),
            "pipe:1",
        ]

    def decode(self, h264: bytes) -> list[bytes]:
        if not h264:
            return []
        try:
            proc = subprocess.run(
                self._args,
                input=h264,
                capture_output=True,
                timeout=10,
                check=False,
            )
        except subprocess.TimeoutExpired:
            log.warning("ffmpeg timeout (%d bytes in)", len(h264))
            return []
        if proc.returncode != 0 and not proc.stdout:
            log.debug("ffmpeg rc=%d stderr=%s", proc.returncode, proc.stderr[:200])
            return []
        return _split_mjpeg(proc.stdout)


def _split_mjpeg(stream: bytes) -> list[bytes]:
    """Split concatenated JPEGs on EOI markers (`FF D9`)."""
    frames: list[bytes] = []
    start = 0
    i = 0
    n = len(stream)
    while i < n - 1:
        if stream[i] == 0xFF and stream[i + 1] == 0xD9:
            frames.append(stream[start : i + 2])
            start = i + 2
            i += 2
        else:
            i += 1
    return frames
