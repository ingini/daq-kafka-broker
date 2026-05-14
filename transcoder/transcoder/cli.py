"""Composition root."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from . import config as config_mod
from .decoding.base import Decoder
from .decoding.ffmpeg import FfmpegDecoder
from .decoding.noop import NoOpDecoder
from .orchestration.runner import Runner


def run() -> int:
    p = argparse.ArgumentParser(description="H.264 → JPEG transcoder for RED POC")
    default_cfg = Path(__file__).resolve().parent.parent / "config.yml"
    p.add_argument("--config", default=str(default_cfg))
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = config_mod.load(args.config)
    decoder = _build_decoder(cfg)
    Runner(cfg, decoder).run()
    return 0


def _build_decoder(cfg: config_mod.AppConfig) -> Decoder:
    kind = cfg.decoder.kind
    if kind in ("", "ffmpeg"):
        return FfmpegDecoder(jpeg_quality=cfg.decoder.jpeg_quality)
    if kind == "noop":
        return NoOpDecoder()
    raise ValueError(f"unknown decoder kind: {kind!r}")
