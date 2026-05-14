"""Typed config for the transcoder service."""
from __future__ import annotations

from dataclasses import dataclass

import yaml


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    group_id: str
    input_topic: str
    output_topic: str


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool


@dataclass(frozen=True)
class DecoderConfig:
    kind: str
    jpeg_quality: int
    output_suffix: str


@dataclass(frozen=True)
class MetricsConfig:
    port: int


@dataclass(frozen=True)
class AppConfig:
    kafka: KafkaConfig
    minio: MinioConfig
    decoder: DecoderConfig
    metrics: MetricsConfig


def load(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return AppConfig(
        kafka=KafkaConfig(**raw["kafka"]),
        minio=MinioConfig(**raw["minio"]),
        decoder=DecoderConfig(**raw["decoder"]),
        metrics=MetricsConfig(**raw["metrics"]),
    )
