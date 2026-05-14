"""Kafka → MinIO image consumer.

Drains the `raw.frame.images` topic (binary JPEG payloads) into the MinIO
bucket, using the daq-kafka external module's object-key layout:

    {YYYY}/{MM}/{DD}/{HH}/{vehicle_id}/{sensor}/{YYYYMMDDTHHMMSS}_{ts_ns}.jpg

That layout matches the external `reference/daq-kafka/consumers/minio/`
output exactly so analysis tools written for the daq-kafka stack can be
reused unchanged. kafka-frame-bridge MUST mirror this exact format in
its `_build_uri()` for processor S3 fetch to succeed (see Phase 2.6+).

Message contract — set by the producer (Confluent REST Proxy in Phase 1,
device-side kafka-rest-agent in Phase 2):

    key          = vehicle_id (utf-8 bytes; also used as partition key)
    value        = raw JPEG bytes (REST Proxy decodes base64 before producing)
    headers
        vehicle_id     = utf-8 (mirrors key; redundant but explicit)
        sensor         = utf-8 ("front_cam" | "side_left" | "side_right" ...)
        ts_ns          = utf-8 decimal string (epoch nanoseconds)
        content_type   = utf-8 (default "image/jpeg")

Delivery semantics: at-least-once. Offsets commit only after MinIO PUT
returns 2xx; transient MinIO errors raise back into the consumer loop and
cause a re-poll. Unrecoverable messages (missing headers, empty payload)
are routed to `raw.frame.images.dlq` with an `error` header so they can be
inspected without blocking the pipeline.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO

# Asia/Seoul (KST = UTC+9). fixed offset 으로 두어 zoneinfo / tzdata 의존성 회피
# (alpine 베이스 컨테이너에 별도 패키지 없이 동작). DST 없는 KR 환경 가정.
KST = timezone(timedelta(hours=9), name="KST")

from confluent_kafka import Consumer, KafkaError, Producer
from minio import Minio
from prometheus_client import Counter, Gauge, Histogram, start_http_server


log = logging.getLogger("kafka-image-consumer")

# ── metrics ─────────────────────────────────────────────────────────
messages_total = Counter(
    "kafka_image_consumer_messages_total",
    "Messages handled, labelled by terminal outcome.",
    ["result"],  # ok | dlq | kafka_error
)
put_seconds = Histogram(
    "kafka_image_consumer_minio_put_seconds",
    "MinIO PUT latency for one image.",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
dlq_total = Counter(
    "kafka_image_consumer_dlq_total",
    "Messages forwarded to the DLQ topic, labelled by reason.",
    ["reason"],
)
last_event_ts = Gauge(
    "kafka_image_consumer_last_event_ts_seconds",
    "Wall-clock seconds since the most recent successful PUT — staleness signal.",
)


def _hdr(headers, name, default=None):
    """Return the first header value matching `name`, decoded utf-8."""
    if not headers:
        return default
    for k, v in headers:
        if k == name:
            if isinstance(v, (bytes, bytearray)):
                try:
                    return v.decode("utf-8")
                except UnicodeDecodeError:
                    return default
            return v
    return default


def _build_object_key(vehicle_id: str, sensor: str, ts_ns: str) -> str:
    """daq-kafka layout: {YYYY}/{MM}/{DD}/{HH}/{vehicle}/{sensor}/{ts_str}_{ts_ns}.jpg

    HH 는 KST (Asia/Seoul) 기준. 운영자가 한국 시간으로 직관적 탐색 가능.
    UTC 였던 기존 키와는 9시간 차이 — 시점 cutoff 후의 신규 객체만 KST 폴더에 들어간다.

    Example:
        2026/05/06/11/VID_001/cam0/20260506T110550_1778033150502419910.jpg
                  ^^                          ^^
                  KST 11시 (= UTC 02시)       파일명 ts_str 도 KST

    kafka-frame-bridge._build_uri() MUST return identical key shape (modulo
    s3:// + bucket prefix) so processor S3 fetch can find the object.
    """
    dt = datetime.fromtimestamp(int(ts_ns) / 1e9, tz=KST)
    ts_str = dt.strftime("%Y%m%dT%H%M%S")
    return (
        f"{dt.strftime('%Y')}/"
        f"{dt.strftime('%m')}/"
        f"{dt.strftime('%d')}/"
        f"{dt.strftime('%H')}/"
        f"{vehicle_id}/"
        f"{sensor}/"
        f"{ts_str}_{ts_ns}.jpg"
    )


def _process_message(msg, minio_client: Minio, bucket: str) -> None:
    """Validate one Kafka record and PUT it to MinIO. Raises on error."""
    headers = msg.headers() or []
    key_bytes = msg.key()

    vehicle_id = _hdr(headers, "vehicle_id") or (
        key_bytes.decode("utf-8", errors="replace") if key_bytes else None
    )
    sensor = _hdr(headers, "sensor")
    ts_ns = _hdr(headers, "ts_ns")
    content_type = _hdr(headers, "content_type", "image/jpeg")

    if not (vehicle_id and sensor and ts_ns):
        raise ValueError(
            f"missing required headers — vehicle_id={vehicle_id!r} "
            f"sensor={sensor!r} ts_ns={ts_ns!r}"
        )

    payload = msg.value()
    if not payload:
        raise ValueError("empty payload")

    obj_key = _build_object_key(vehicle_id, sensor, ts_ns)

    t0 = time.time()
    minio_client.put_object(
        bucket,
        obj_key,
        BytesIO(payload),
        length=len(payload),
        content_type=content_type,
    )
    put_seconds.observe(time.time() - t0)
    last_event_ts.set(time.time())
    log.debug("put %s (%d bytes) ok", obj_key, len(payload))


def _to_dlq(producer: Producer, dlq_topic: str, src_topic: str, msg, reason: str) -> None:
    """Re-emit the failed record onto the DLQ with an `error` header."""
    base_headers = list(msg.headers() or [])
    base_headers.append(("error", reason.encode("utf-8")))
    base_headers.append(("source_topic", src_topic.encode("utf-8")))
    base_headers.append(("source_offset", str(msg.offset()).encode("utf-8")))
    base_headers.append(("source_partition", str(msg.partition()).encode("utf-8")))
    try:
        producer.produce(
            dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers=base_headers,
        )
        producer.poll(0)
        dlq_total.labels(reason="process_error").inc()
        log.warning("DLQ %s offset=%d reason=%s", src_topic, msg.offset(), reason)
    except BufferError:
        producer.flush(timeout=5)
        producer.produce(
            dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers=base_headers,
        )
        dlq_total.labels(reason="process_error").inc()


def main() -> int:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    topic = os.environ.get("KAFKA_TOPIC", "raw.frame.images")
    dlq_topic = os.environ.get("KAFKA_DLQ_TOPIC", "raw.frame.images.dlq")
    group_id = os.environ.get("KAFKA_GROUP_ID", "kafka-image-consumer")
    minio_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access = os.environ["MINIO_ACCESS_KEY"]
    minio_secret = os.environ["MINIO_SECRET_KEY"]
    minio_bucket = os.environ.get("MINIO_BUCKET", "red-poc")
    minio_secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    metrics_port = int(os.environ.get("METRICS_PORT", "2118"))

    if metrics_port > 0:
        start_http_server(metrics_port)
        log.info("metrics on :%d", metrics_port)

    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access,
        secret_key=minio_secret,
        secure=minio_secure,
    )
    if not minio_client.bucket_exists(minio_bucket):
        log.error("bucket %r does not exist on %s — run minio-init first",
                  minio_bucket, minio_endpoint)
        return 2
    log.info("minio ok: %s/%s (secure=%s)", minio_endpoint, minio_bucket, minio_secure)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,        # commit manually after MinIO PUT
        "auto.offset.reset": "earliest",
        "max.partition.fetch.bytes": 5_242_880,
        "fetch.max.bytes": 5_242_880,
        "session.timeout.ms": 30_000,
        "max.poll.interval.ms": 300_000,
    })
    consumer.subscribe([topic])

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "compression.type": "none",
        "message.max.bytes": 5_242_880,
        "enable.idempotence": True,
    })

    log.info("consuming topic=%s group=%s dlq=%s", topic, group_id, dlq_topic)

    stop = False

    def _shutdown(signum, _frame):
        nonlocal stop
        log.info("signal %d — draining and stopping", signum)
        stop = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Throughput summary every N seconds so INFO-level logs aren't silent.
    # Counters reset each window so the line reads as "rate over last window".
    LOG_WINDOW_SEC = 30
    win_start = time.time()
    win_ok = 0
    win_dlq = 0
    win_err = 0

    try:
        while not stop:
            now = time.time()
            if now - win_start >= LOG_WINDOW_SEC and (win_ok or win_dlq or win_err):
                rate = (win_ok + win_dlq) / (now - win_start)
                log.info("window %ds: ok=%d dlq=%d kafka_err=%d (%.1f msg/s)",
                         int(now - win_start), win_ok, win_dlq, win_err, rate)
                win_start = now
                win_ok = win_dlq = win_err = 0

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("kafka error: %s", err)
                messages_total.labels(result="kafka_error").inc()
                win_err += 1
                continue

            try:
                _process_message(msg, minio_client, minio_bucket)
                consumer.commit(msg, asynchronous=False)
                messages_total.labels(result="ok").inc()
                win_ok += 1
            except ValueError as exc:
                # Bad record — route to DLQ and skip. Do NOT block the
                # partition; one malformed message must not stall the lane.
                _to_dlq(producer, dlq_topic, topic, msg, str(exc))
                consumer.commit(msg, asynchronous=False)
                messages_total.labels(result="dlq").inc()
                win_dlq += 1
            except Exception as exc:  # noqa: BLE001 — broad on purpose
                # Transient failure (MinIO outage, network) — do NOT commit
                # so the message is redelivered after the next poll. Sleep
                # briefly to avoid hot-looping if the backend stays down.
                log.exception("transient process error: %s", exc)
                time.sleep(1.0)
    finally:
        log.info("closing consumer + flushing producer")
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            log.exception("consumer close failed")
        try:
            producer.flush(timeout=10)
        except Exception:  # noqa: BLE001
            log.exception("producer flush failed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
