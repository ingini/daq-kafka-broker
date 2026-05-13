"""
daq-consumer  -  Kafka topic consume -> transform -> MinIO 151번 저장

토픽 -> MinIO 경로:
  sensor.cam0.jpeg  ->  daq/year=YYYY/month=MM/day=dd/{vehicle_id}/cam0/{ts_ns}.jpg
  sensor.cam1.jpeg  ->  daq/year=YYYY/month=MM/day=dd/{vehicle_id}/cam1/{ts_ns}.jpg
  sensor.cam2.jpeg  ->  daq/year=YYYY/month=MM/day=dd/{vehicle_id}/cam2/{ts_ns}.jpg
  sensor.gnss       ->  daq/year=YYYY/month=MM/day=dd/{vehicle_id}/gnss/{ts_ns}.json

transform (bridge.py 규칙 동일):
  camera: [8B ts_ns BE][4B jpeg_len BE][JPEG] -> raw JPEG
  gnss:   latitude->lat, longitude->lon, height->alt, ts_ns->capture_ts_ns
"""
from __future__ import annotations

import base64
import io
import json
import logging
import os
import signal
import struct
import sys
import threading
import time
from datetime import datetime, timezone
from queue import Empty, Queue
from typing import Optional

from confluent_kafka import Consumer, KafkaError
from minio import Minio
from minio.error import S3Error
from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("daq-consumer")

# ── metrics ──────────────────────────────────────────────────
records_in = Counter(
    "daq_consumer_records_in_total",
    "Records consumed from Kafka.",
    ["topic"],
)
records_dropped = Counter(
    "daq_consumer_records_dropped_total",
    "Records dropped.",
    ["topic", "reason"],
)
minio_ok = Counter(
    "daq_consumer_minio_put_ok_total",
    "MinIO PUT 성공.",
    ["sensor"],
)
minio_err = Counter(
    "daq_consumer_minio_put_error_total",
    "MinIO PUT 실패.",
    ["sensor"],
)
queue_depth = Gauge(
    "daq_consumer_queue_depth",
    "Upload queue depth.",
)

# ── env ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP",  "localhost:9092")
KAFKA_GROUP_ID   = os.environ.get("KAFKA_GROUP_ID",   "daq-consumer-group")
KAFKA_TOPICS_STR = os.environ.get(
    "KAFKA_TOPICS",
    "sensor.cam0.jpeg,sensor.cam1.jpeg,sensor.cam2.jpeg,sensor.gnss",
)
KAFKA_TOPICS = [t.strip() for t in KAFKA_TOPICS_STR.split(",") if t.strip()]

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://192.168.1.151:9001")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "daq")
MAX_WORKERS      = int(os.environ.get("MAX_WORKERS",  "4"))
METRICS_PORT     = int(os.environ.get("METRICS_PORT", "2122"))

SENSOR_MAP = {
    "sensor.cam0.jpeg": "cam0",
    "sensor.cam1.jpeg": "cam1",
    "sensor.cam2.jpeg": "cam2",
    "sensor.gnss":      "gnss",
}


# ── MinIO 경로 ────────────────────────────────────────────────
def _minio_path(vehicle_id: str, sensor: str, ts_ns: int, ext: str) -> str:
    """
    daq/year=YYYY/month=MM/day=dd/{vehicle_id}/{sensor}/{ts_ns}.{ext}
    Hive-style 파티션 경로
    """
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    return (
        f"year={dt.strftime('%Y')}/"
        f"month={dt.strftime('%m')}/"
        f"day={dt.strftime('%d')}/"
        f"{vehicle_id}/{sensor}/"
        f"{ts_ns}.{ext}"
    )


# ── transform ────────────────────────────────────────────────
def transform_camera(
    raw_value: bytes,
) -> tuple[Optional[bytes], Optional[int], Optional[str]]:
    """
    daq-kafka-producer CameraWorker._send_kafka() 포맷:
      base64( struct.pack('>QI', ts_ns, len(jpeg)) + jpeg )
    REST Proxy 가 base64 decode 후 bytes 로 전달.
    """
    if len(raw_value) < 12:
        return None, None, "too_short"
    try:
        ts_ns, jpeg_len = struct.unpack(">QI", raw_value[:12])
    except struct.error:
        return None, None, "header_unpack_failed"
    if jpeg_len <= 0 or 12 + jpeg_len > len(raw_value):
        return None, None, "bad_length"
    return raw_value[12: 12 + jpeg_len], ts_ns, None


def transform_gnss(
    raw_value: bytes,
) -> tuple[Optional[dict], Optional[int], Optional[str]]:
    """
    daq-kafka-producer GnssWorker._send_kafka() 포맷: JSON 문자열
    bridge.py 필드명 규칙 동일:
      latitude->lat, longitude->lon, height->alt, ts_ns->capture_ts_ns
    """
    try:
        daq = json.loads(raw_value)
    except (ValueError, TypeError):
        return None, None, "json_parse_failed"
    if not isinstance(daq, dict):
        return None, None, "not_dict"

    ts_ns = daq.get("ts_ns")
    if not isinstance(ts_ns, int):
        return None, None, "missing_ts_ns"

    out: dict = {"capture_ts_ns": ts_ns}
    if "latitude"  in daq: out["lat"] = daq["latitude"]
    if "longitude" in daq: out["lon"] = daq["longitude"]
    if "height"    in daq: out["alt"] = daq["height"]

    skip = {"latitude", "longitude", "height", "ts_ns"}
    for k, v in daq.items():
        if k not in skip and k not in out:
            out[k] = v

    return out, ts_ns, None


# ── upload job ────────────────────────────────────────────────
class _Job:
    __slots__ = ("object_name", "data", "length", "content_type", "sensor")

    def __init__(self, object_name: str, data: bytes,
                 content_type: str, sensor: str):
        self.object_name  = object_name
        self.data         = data
        self.length       = len(data)
        self.content_type = content_type
        self.sensor       = sensor


# ── uploader worker ───────────────────────────────────────────
def _uploader_loop(
    idx: int, q: Queue, client: Minio,
    bucket: str, stop: threading.Event,
) -> None:
    while not stop.is_set():
        try:
            job: _Job = q.get(timeout=0.5)
        except Empty:
            continue
        try:
            client.put_object(
                bucket,
                job.object_name,
                io.BytesIO(job.data),
                length=job.length,
                content_type=job.content_type,
            )
            minio_ok.labels(sensor=job.sensor).inc()
            log.info("PUT ok  sensor=%-6s  %s  (%d B)",
                     job.sensor, job.object_name, job.length)
        except S3Error as e:
            minio_err.labels(sensor=job.sensor).inc()
            log.error("PUT S3Error  %s  %s", job.object_name, e)
        except Exception as e:
            minio_err.labels(sensor=job.sensor).inc()
            log.exception("PUT error  %s  %s", job.object_name, e)
        finally:
            queue_depth.set(q.qsize())


# ── vehicle_id 추출 ───────────────────────────────────────────
def _extract_vehicle_id(msg) -> str:
    """REST Proxy v2: plain bytes / v3: base64 둘 다 시도"""
    if not msg.headers():
        return "unknown"
    for k, v in msg.headers():
        if k == "vehicle_id" and v:
            try:
                return base64.b64decode(v).decode()
            except Exception:
                return v.decode() if isinstance(v, bytes) else str(v)
    return "unknown"


# ── main ──────────────────────────────────────────────────────
def main() -> int:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # MinIO 클라이언트
    secure = MINIO_ENDPOINT.startswith("https")
    host   = MINIO_ENDPOINT.replace("https://", "").replace("http://", "")
    minio_client = Minio(host,
                         access_key=MINIO_ACCESS_KEY,
                         secret_key=MINIO_SECRET_KEY,
                         secure=secure)

    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        log.info("bucket created: %s", MINIO_BUCKET)

    # Kafka consumer
    consumer = Consumer({
        "bootstrap.servers":         KAFKA_BOOTSTRAP,
        "group.id":                  KAFKA_GROUP_ID,
        "enable.auto.commit":        True,
        "auto.commit.interval.ms":   5000,
        "auto.offset.reset":         "latest",
        "session.timeout.ms":        30_000,
        "max.partition.fetch.bytes": 10_485_760,
        "fetch.max.bytes":           10_485_760,
    })
    consumer.subscribe(KAFKA_TOPICS)
    log.info("subscribed: %s", KAFKA_TOPICS)

    # upload queue + workers
    q: Queue   = Queue(maxsize=5_000)
    stop_event = threading.Event()
    workers = [
        threading.Thread(
            target=_uploader_loop,
            args=(i, q, minio_client, MINIO_BUCKET, stop_event),
            name=f"uploader-{i}", daemon=True,
        )
        for i in range(MAX_WORKERS)
    ]
    for t in workers:
        t.start()

    if METRICS_PORT > 0:
        start_http_server(METRICS_PORT)
        log.info("metrics on :%d", METRICS_PORT)

    def _shutdown(sig, _):
        log.info("signal %d received — stopping", sig)
        stop_event.set()
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("daq-consumer starting  kafka=%s  minio=%s  bucket=%s",
             KAFKA_BOOTSTRAP, MINIO_ENDPOINT, MINIO_BUCKET)

    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            topic     = msg.topic()
            sensor    = SENSOR_MAP.get(topic)
            raw_value = msg.value()

            if not sensor or not raw_value:
                records_dropped.labels(topic=topic, reason="unknown_or_empty").inc()
                continue

            vehicle_id = _extract_vehicle_id(msg)
            records_in.labels(topic=topic).inc()

            if sensor.startswith("cam"):
                jpeg, ts_ns, reason = transform_camera(raw_value)
                if jpeg is None:
                    records_dropped.labels(topic=topic, reason=reason).inc()
                    continue
                ts_ns = ts_ns or time.time_ns()
                job = _Job(
                    _minio_path(vehicle_id, sensor, ts_ns, "jpg"),
                    jpeg, "image/jpeg", sensor,
                )
            else:  # gnss
                payload, ts_ns, reason = transform_gnss(raw_value)
                if payload is None:
                    records_dropped.labels(topic=topic, reason=reason).inc()
                    continue
                ts_ns = ts_ns or time.time_ns()
                job = _Job(
                    _minio_path(vehicle_id, sensor, ts_ns, "json"),
                    json.dumps(payload, ensure_ascii=False).encode(),
                    "application/json", sensor,
                )

            try:
                q.put_nowait(job)
                queue_depth.set(q.qsize())
            except Exception:
                records_dropped.labels(topic=topic, reason="queue_full").inc()

    finally:
        log.info("draining workers...")
        stop_event.set()
        for t in workers:
            t.join(timeout=5)
        consumer.close()
        log.info("shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
