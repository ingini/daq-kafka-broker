"""Decoder Protocol — sole abstraction the orchestrator depends on."""
from __future__ import annotations

from typing import Protocol


class Decoder(Protocol):
    """Consumes an H.264 byte stream and returns one or more JPEG frames.

    Returning an empty list is a valid no-op (e.g. a partial stream that
    contains no complete frame). The orchestrator treats empty output as
    "skip this bundle" and does not write to MinIO.
    """

    def decode(self, h264: bytes) -> list[bytes]: ...
