"""
daq-consumer  -  Kafka topic consume -> transform -> MinIO 저장

토픽 -> MinIO 경로:
  sensor.cam*.jpeg  -> daq/year=YYYY/month=MM/day=dd/{vehicle_id}/cam*/{ts_ns}.jpg
  sensor.gnss       -> daq/year=YYYY/month=MM/day=dd/{vehicle_id}/gnss/{ts_ns}.json

transform (daq-kafka-producer server.py 포맷 기준):
  camera: base64( struct.pack('>QI', ts_ns, jpeg_len) + jpeg ) -> raw JPEG
  gnss:   {latitude, longitude, height, ts_ns, ...}
          -> {lat, lon, alt, capture_ts_ns, ...}

MinIO 접근:
  MINIO_ENDPOINT=https://221.147.232.196:8443/poc/minio
  nginx /poc/minio/ -> MinIO 내부 서버로 프록시
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
from urllib.parse import urlparse

from confluent_kafka import Consumer, KafkaError
from minio import Minio
from minio.error import S3Error
from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("daq-consumer")

# ── metrics ──────────────────────────────────────────────────
records_in = Counter(
    "daq_consumer_records_in_total", "Records consumed.", ["topic"])
records_dropped = Counter(
    "daq_consumer_records_dropped_total", "Records dropped.", ["topic", "reason"])
minio_ok = Counter(
    "daq_consumer_minio_put_ok_total", "MinIO PUT 성공.", ["sensor"])
minio_err = Counter(
    "daq_consumer_minio_put_error_total", "MinIO PUT 실패.", ["sensor"])
queue_depth = Gauge(
    "daq_consumer_queue_depth", "Upload queue depth.")

# ── env ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP",  "localhost:9092")
KAFKA_GROUP_ID   = os.environ.get("KAFKA_GROUP_ID",   "daq-consumer-group")
KAFKA_TOPICS_STR = os.environ.get(
    "KAFKA_TOPICS",
    "sensor.cam0.jpeg,sensor.cam1.jpeg,sensor.cam2.jpeg,sensor.gnss",
)
KAFKA_TOPICS = [t.strip() for t in KAFKA_TOPICS_STR.split(",") if t.strip()]

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "swm")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "daq")
MAX_WORKERS      = int(os.environ.get("MAX_WORKERS",  "4"))
LOG_LEVEL        = os.environ.get("LOG_LEVEL",        "INFO").upper()

# ── topic → sensor 이름 매핑 ─────────────────────────────────
SENSOR_MAP = {
    "sensor.cam0.jpeg": "cam0",
    "sensor.cam1.jpeg": "cam1",
    "sensor.cam2.jpeg": "cam2",
    "sensor.gnss":      "gnss",
}


# ── MinIO 클라이언트 초기화 ───────────────────────────────────
def _build_minio_client() -> Minio:
    """
    MINIO_ENDPOINT 가 https://host:port/path 형태일 경우
    Minio SDK 는 host:port 만 받고 prefix_path 는 별도 처리
    nginx 가 /poc/minio/ → MinIO 로 프록시하는 구조
    """
    parsed = urlparse(MINIO_ENDPOINT)
    secure = parsed.scheme == "https"
    netloc = parsed.netloc  # 221.147.232.196:8443
    # path prefix: /poc/minio  (trailing slash 제거)
    prefix = parsed.path.rstrip("/")

    client = Minio(
        netloc,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
        http_client=None,
    )
    # nginx prefix 가 있으면 base_url 패치
    if prefix:
        # minio-py 7.x: _base_url 속성으로 접근
        from minio.helpers import BaseURL
        import urllib3
        base = f"{parsed.scheme}://{netloc}{prefix}/"
        client._base_url = BaseURL(base, None)

    return client


# ── MinIO 경로 ────────────────────────────────────────────────
def _minio_path(vehicle_id: str, sensor: str, ts_ns: int, ext: str) -> str:
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    return (
        f"year={dt.strftime('%Y')}/"
        f"month={dt.strftime('%m')}/"
        f"day={dt.strftime('%d')}/"
        f"{vehicle_id}/{sensor}/{ts_ns}.{ext}"
    )


# ── transform ────────────────────────────────────────────────
def transform_camera(raw: bytes) -> tuple[Optional[bytes], Optional[int], Optional[str]]:
    """
    daq-service CameraWorker._send_kafka() 포맷:
      base64( struct.pack('>QI', ts_ns, jpeg_len) + jpeg )
    REST Proxy 가 base64 decode 후 bytes 로 전달
    """
    if len(raw) < 12:
        return None, None, "too_short"
    try:
        ts_ns, jpeg_len = struct.unpack(">QI", raw[:12])
    except struct.error:
        return None, None, "header_unpack_failed"
    if jpeg_len <= 0 or 12 + jpeg_len > len(raw):
        return None, None, "bad_length"
    return raw[12: 12 + jpeg_len], ts_ns, None


def transform_gnss(raw: bytes) -> tuple[Optional[dict], Optional[int], Optional[str]]:
    """
    daq-service GnssWorker._send_kafka() 포맷: JSON 문자열
    필드 변환: latitude->lat, longitude->lon, height->alt, ts_ns->capture_ts_ns
    """
    try:
        daq = json.loads(raw)
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

    def __init__(self, object_name: str, data: bytes, content_type: str, sensor: str):
        self.object_name  = object_name
        self.data         = data
        self.length       = len(data)
        self.content_type = content_type
        self.sensor       = sensor


# ── uploader worker ───────────────────────────────────────────
def _uploader_loop(idx: int, q: Queue, client: Minio,
                   bucket: str, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            job: _Job = q.get(timeout=0.5)
        except Empty:
            continue
        try:
            client.put_object(
                bucket, job.object_name,
                io.BytesIO(job.data), length=job.length,
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
def _vehicle_id(msg) -> str:
    """
    daq-service 가 headers 에 vehicle_id 를 plain string 으로 실어서 보냄
    REST Proxy v2: plain bytes / v3: base64 둘 다 시도
    """
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
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # MinIO 클라이언트
    mc = _build_minio_client()
    try:
        if not mc.bucket_exists(MINIO_BUCKET):
            mc.make_bucket(MINIO_BUCKET)
            log.info("bucket created: %s", MINIO_BUCKET)
    except Exception as e:
        log.warning("MinIO 버킷 확인 실패 (나중에 재시도): %s", e)

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
    log.info("subscribed topics : %s", KAFKA_TOPICS)
    log.info("minio endpoint    : %s  bucket: %s", MINIO_ENDPOINT, MINIO_BUCKET)

    # upload queue + workers
    q          = Queue(maxsize=5_000)
    stop_event = threading.Event()
    workers = [
        threading.Thread(
            target=_uploader_loop,
            args=(i, q, mc, MINIO_BUCKET, stop_event),
            name=f"uploader-{i}", daemon=True,
        )
        for i in range(MAX_WORKERS)
    ]
    for t in workers:
        t.start()

    start_http_server(2122)
    log.info("prometheus metrics on :2122")

    def _shutdown(sig, _):
        log.info("signal %d — stopping", sig)
        stop_event.set()
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("daq-consumer ready — waiting for messages...")

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
            raw_value = msg.value()
            if not raw_value:
                records_dropped.labels(topic=topic, reason="empty_value").inc()
                continue

            sensor = SENSOR_MAP.get(topic, topic.replace("sensor.", ""))
            vid    = _vehicle_id(msg)
            records_in.labels(topic=topic).inc()

            # transform + enqueue
            if "cam" in topic:
                jpeg, ts_ns, reason = transform_camera(raw_value)
                if jpeg is None:
                    records_dropped.labels(topic=topic, reason=reason).inc()
                    continue
                ts_ns = ts_ns or time.time_ns()
                job = _Job(
                    _minio_path(vid, sensor, ts_ns, "jpg"),
                    jpeg, "image/jpeg", sensor,
                )
            elif "gnss" in topic:
                payload, ts_ns, reason = transform_gnss(raw_value)
                if payload is None:
                    records_dropped.labels(topic=topic, reason=reason).inc()
                    continue
                ts_ns = ts_ns or time.time_ns()
                job = _Job(
                    _minio_path(vid, sensor, ts_ns, "json"),
                    json.dumps(payload, ensure_ascii=False).encode(),
                    "application/json", sensor,
                )
            else:
                # 신규 topic: raw bytes 그대로 저장
                ts_ns = time.time_ns()
                job = _Job(
                    _minio_path(vid, sensor, ts_ns, "bin"),
                    raw_value, "application/octet-stream", sensor,
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
