"""Passthrough decoder used for tests / dry-run mode."""
from __future__ import annotations

from .base import Decoder


class NoOpDecoder(Decoder):
    """Returns an empty list — useful for asserting wiring without invoking ffmpeg."""

    def decode(self, h264: bytes) -> list[bytes]:
        return []
