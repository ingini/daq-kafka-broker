"""Pick the "best" JPEG out of a decoded MJPEG stream.

Context: when an H.264 segment is cut mid-stream (at a GOP boundary but
with upstream references missing), ffmpeg often emits the first frame
as a solid grey placeholder because the decoder's reference state is
not yet populated. Visually: RGB (130,130,130), variance ~ 0.

Strategy:
  1. Scan in reverse order — later frames have warm decoder state.
  2. Accept the first frame whose brightness variance is above a small
     threshold (~10 on 64×64 downsample).
  3. Fallback: if every frame fails the check, return the last one
     anyway — it's still better than the corrupt first frame.
"""
from __future__ import annotations

from io import BytesIO
from typing import Sequence

from PIL import Image


def pick_best(jpegs: Sequence[bytes], variance_threshold: float = 10.0) -> bytes | None:
    if not jpegs:
        return None
    for j in reversed(jpegs):
        if _has_content(j, variance_threshold):
            return j
    return jpegs[-1]


def _has_content(jpeg_bytes: bytes, threshold: float) -> bool:
    try:
        img = Image.open(BytesIO(jpeg_bytes)).convert("L")
        # Downsample to 64×64 so variance computation is ~4096 ops
        # regardless of input resolution. Thumbnail preserves aspect.
        img.thumbnail((64, 64))
        pixels = list(img.getdata())
        mean = sum(pixels) / len(pixels)
        var = sum((p - mean) ** 2 for p in pixels) / len(pixels)
        return var > threshold
    except Exception:
        return False
