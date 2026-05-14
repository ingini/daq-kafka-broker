"""Unit tests for pick_best — run inline with `python -m transcoder.decoding.test_frame_picker`."""
from __future__ import annotations

from io import BytesIO

from PIL import Image

from .frame_picker import _has_content, pick_best


def _grey_jpeg() -> bytes:
    img = Image.new("RGB", (320, 240), (130, 130, 130))
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def _noisy_jpeg() -> bytes:
    import random
    img = Image.new("RGB", (320, 240))
    rng = random.Random(42)
    img.putdata([(rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255))
                 for _ in range(320 * 240)])
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def test_has_content_rejects_grey() -> None:
    assert _has_content(_grey_jpeg(), 10.0) is False, "solid grey must fail content check"


def test_has_content_accepts_noisy() -> None:
    assert _has_content(_noisy_jpeg(), 10.0) is True, "random pixels must pass content check"


def test_pick_best_skips_grey_prefix() -> None:
    grey = _grey_jpeg()
    good = _noisy_jpeg()
    # First 3 are grey (ffmpeg warm-up artifacts), then real frames.
    jpegs = [grey, grey, grey, good, good]
    selected = pick_best(jpegs)
    assert selected == good, "must pick the last good frame, not the first grey one"


def test_pick_best_fallback_when_all_grey() -> None:
    grey = _grey_jpeg()
    jpegs = [grey, grey]
    selected = pick_best(jpegs)
    assert selected == jpegs[-1], "fallback = last frame (still better than first)"


def test_pick_best_empty() -> None:
    assert pick_best([]) is None


if __name__ == "__main__":
    for name in list(globals()):
        if name.startswith("test_"):
            globals()[name]()
            print(f"  ✓ {name}")
    print("all tests passed")
