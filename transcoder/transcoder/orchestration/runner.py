"""Pipeline runner: Kafka consume → MinIO fetch → decode → MinIO put → Kafka produce."""
from __future__ import annotations

import json
import logging
import signal
import threading
from io import BytesIO

from confluent_kafka import Consumer, Producer
from minio import Minio
from prometheus_client import Counter, Histogram, start_http_server

from ..config import AppConfig
from ..decoding.base import Decoder
from ..decoding.frame_picker import pick_best


log = logging.getLogger("transcoder")


bundles_in = Counter("transcoder_bundles_in_total", "Bundles received from Kafka")
bundles_decoded = Counter("transcoder_bundles_decoded_total", "Bundles with ≥1 JPEG output")
sensor_decode_attempts = Counter(
    "transcoder_sensor_decode_attempts_total",
    "Per-sensor decode attempts (one per H.264 object in a bundle).",
)
sensor_decode_success = Counter(
    "transcoder_sensor_decode_success_total",
    "Per-sensor decode attempts that produced ≥1 JPEG.",
)
jpegs_out = Counter("transcoder_jpegs_out_total", "Total JPEG frames written to MinIO")
decode_duration = Histogram("transcoder_decode_duration_seconds", "Decode latency per sensor")
decode_errors = Counter(
    "transcoder_decode_errors_total",
    "Decode pipeline errors by stage.",
    labelnames=("stage",),   # "fetch" | "decode" | "upload" | "produce"
)
frames_skipped_grey = Counter(
    "transcoder_frames_skipped_grey_total",
    "Frames rejected by the variance filter (ffmpeg warm-up placeholders).",
)


class Runner:
    def __init__(self, cfg: AppConfig, decoder: Decoder) -> None:
        self._cfg = cfg
        self._decoder = decoder
        self._stop = threading.Event()

    def run(self) -> None:
        start_http_server(self._cfg.metrics.port)
        log.info("metrics on :%d", self._cfg.metrics.port)

        signal.signal(signal.SIGINT, lambda *_: self._stop.set())
        signal.signal(signal.SIGTERM, lambda *_: self._stop.set())

        minio = Minio(
            self._cfg.minio.endpoint,
            access_key=self._cfg.minio.access_key,
            secret_key=self._cfg.minio.secret_key,
            secure=self._cfg.minio.secure,
        )
        producer = Producer({"bootstrap.servers": self._cfg.kafka.bootstrap_servers})
        consumer = Consumer({
            "bootstrap.servers": self._cfg.kafka.bootstrap_servers,
            "group.id": self._cfg.kafka.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([self._cfg.kafka.input_topic])
        log.info("consuming %s -> %s", self._cfg.kafka.input_topic, self._cfg.kafka.output_topic)

        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                try:
                    self._handle(msg.value(), minio, producer)
                except Exception as e:  # noqa: BLE001
                    log.warning("handle failed: %s", e)
                consumer.commit(asynchronous=True)
            producer.flush(5)
        finally:
            consumer.close()

    def _handle(self, raw: bytes, minio: Minio, producer: Producer) -> None:
        bundles_in.inc()
        bundle = json.loads(raw)
        bundle_id = bundle.get("bundle_id", "?")
        new_uris: dict[str, str] = {}
        any_decoded = False
        for sensor, uri in bundle.get("images", {}).items():
            sensor_decode_attempts.inc()
            bucket, key = _split_s3(uri, self._cfg.minio.bucket)
            try:
                h264 = minio.get_object(bucket, key).read()
            except Exception as e:  # noqa: BLE001
                decode_errors.labels(stage="fetch").inc()
                log.warning("fetch failed %s/%s: %s", bucket, key, e)
                continue
            try:
                with decode_duration.time():
                    jpegs = self._decoder.decode(h264)
            except Exception as e:  # noqa: BLE001
                decode_errors.labels(stage="decode").inc()
                log.warning("decode failed %s/%s: %s", bucket, key, e)
                continue
            if not jpegs:
                continue
            # Pick a frame with real content — ffmpeg's first output after
            # a cold GOP boundary is often a grey placeholder.
            chosen = pick_best(jpegs)
            if chosen is None:
                continue
            # Metric: how many leading frames had to be rejected.
            leading_rejected = 0
            for j in jpegs:
                if j is chosen:
                    break
                leading_rejected += 1
            if leading_rejected > 0:
                frames_skipped_grey.inc(leading_rejected)
            new_key = key.rsplit(".", 1)[0] + "." + self._cfg.decoder.output_suffix
            try:
                minio.put_object(bucket, new_key, BytesIO(chosen), length=len(chosen),
                                 content_type="image/jpeg")
            except Exception as e:  # noqa: BLE001
                decode_errors.labels(stage="upload").inc()
                log.warning("upload failed %s/%s: %s", bucket, new_key, e)
                continue
            sensor_decode_success.inc()
            jpegs_out.inc(len(jpegs))
            new_uris[sensor] = f"s3://{bucket}/{new_key}"
            any_decoded = True
        if not any_decoded:
            log.debug("bundle %s produced no JPEGs; skipping output", bundle_id)
            return
        bundles_decoded.inc()
        out = dict(bundle)
        out["images_decoded"] = new_uris
        try:
            producer.produce(self._cfg.kafka.output_topic, value=json.dumps(out).encode("utf-8"))
            producer.poll(0)
        except Exception as e:  # noqa: BLE001
            decode_errors.labels(stage="produce").inc()
            log.warning("produce failed bundle=%s: %s", bundle_id, e)


def _split_s3(uri: str, fallback_bucket: str) -> tuple[str, str]:
    # s3://bucket/key → (bucket, key)
    if uri.startswith("s3://"):
        rest = uri[len("s3://"):]
        bucket, _, key = rest.partition("/")
        return bucket or fallback_bucket, key
    return fallback_bucket, uri
