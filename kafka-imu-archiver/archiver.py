"""Kafka raw.imu / raw.gps → MinIO Parquet batch archiver.

Drains BOTH `raw.imu` (RAWIMUX accel/gyro) and `raw.gps` (INSPVAXB / BESTGNSSPOS
lat/lon/alt) topics into MinIO as Parquet files (snappy-compressed),
batched per-vehicle by sample count. Object key matches the external
daq-kafka module's layout exactly:

    {YYYY}/{MM}/{DD}/{HH}/{vehicle_id}/{sensor}/{YYYYMMDDTHHMMSS}_{ts_ns}.parquet

Sensor folder is derived from the Kafka record's `sensor` header — typically
`imu` for raw.imu records and `gnss` for raw.gps records, but archiver does
not enforce; whatever publisher sets in the header is what ends up on disk.
This means daq-kafka-bridge can route sensor.imu → raw.imu (sensor=imu) and
sensor.gnss → raw.gps (sensor=gnss) and both land in distinct folders.

Message contract (both topics)
------------------------------
    key      = vehicle_id (utf-8)                — partition key
    value    = JSON payload bytes                 — daq schema (lat/lon/alt + 추가 필드)
    headers
        vehicle_id     = utf-8                    — mirrors key
        sensor         = utf-8 (e.g. "imu" | "gnss")
        ts_ns          = utf-8 decimal string     — first sample's epoch ns
        content_type   = utf-8 (default "application/json")

Bucketing
---------
- per (vehicle_id, sensor) bucket: { (vid, sensor) → list[parsed_dict] }
- on each record append; trigger flush when:
    1) BATCH_SIZE samples reached (default 1000) → trigger=batch_full
    2) IDLE_FLUSH_SEC seconds since last append   → trigger=idle
    3) shutdown                                   → trigger=shutdown
- the bucket's first sample ts_ns is used as the filename timestamp

Object name uses the timestamp of the FIRST sample in the batch (matching
external daq-kafka minio_consumer's `flush_parquet` logic — `rows[0]["ts_ns"]`).

Operational notes
-----------------
- 1 vehicle × 10 Hz × 1000 sample = ~100 s per file. parquet snappy ~18 KB.
- 30 일 보존 시 약 ~470 MB / 차량 (vs 4 GB for jsonl).
"""
from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pyarrow as pa

# Asia/Seoul (KST = UTC+9). consumer / frame-bridge / archiver 가 동일한 키
# layout 을 공유하므로 세 곳 모두 같은 시간대 기준을 사용해야 한다.
KST = timezone(timedelta(hours=9), name="KST")
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError
from minio import Minio
from prometheus_client import Counter, Gauge, Histogram, start_http_server


log = logging.getLogger("kafka-imu-archiver")

# ── metrics ─────────────────────────────────────────────────────────
records_in = Counter(
    "kafka_imu_archiver_records_in_total",
    "Sample records consumed (after parse).",
    ["vehicle_id", "sensor"],
)
records_dropped = Counter(
    "kafka_imu_archiver_records_dropped_total",
    "Records dropped before being added to a bucket.",
    ["reason"],   # missing_key | parse_error | missing_sensor | missing_ts_ns
)
objects_put = Counter(
    "kafka_imu_archiver_objects_put_total",
    "Parquet PUTs (one per batch flush).",
    ["vehicle_id", "sensor", "trigger"],   # batch_full | idle | shutdown
)
put_seconds = Histogram(
    "kafka_imu_archiver_minio_put_seconds",
    "MinIO PUT latency (Parquet).",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
buffer_samples = Gauge(
    "kafka_imu_archiver_buffer_samples",
    "Total samples buffered in memory across all (vehicle, sensor) pairs.",
)
state_buckets = Gauge(
    "kafka_imu_archiver_state_buckets",
    "Active (vehicle, sensor) bucket count.",
)
last_put_ts = Gauge(
    "kafka_imu_archiver_last_put_ts_seconds",
    "Wall-clock seconds at last successful PUT — staleness signal.",
)


@dataclass
class Bucket:
    """One (vehicle, sensor) accumulator."""
    samples: list[dict] = field(default_factory=list)   # parsed JSON dicts
    first_ts_ns: int = 0                                  # for object name
    last_append: float = 0.0                              # wall-clock


def _hdr(headers, name: str, default=None):
    """Decode-utf8 first header matching `name`."""
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


def _build_object_key(vehicle_id: str, sensor: str, ts_ns: int, ext: str) -> str:
    """daq-kafka layout — matches external module's minio_consumer.make_object_path().

    Example: 2026/05/06/11/VID_001/imu/20260506T110500_1778033100000000000.parquet
             (HH/ts_str 둘 다 KST. 기존 UTC 키와는 9시간 차이.)
    """
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=KST)
    ts_str = dt.strftime("%Y%m%dT%H%M%S")
    return (
        f"{dt.strftime('%Y')}/"
        f"{dt.strftime('%m')}/"
        f"{dt.strftime('%d')}/"
        f"{dt.strftime('%H')}/"
        f"{vehicle_id}/"
        f"{sensor}/"
        f"{ts_str}_{ts_ns}.parquet"
    )


def _flush(minio_client: Minio, bucket_name: str, vehicle_id: str,
           sensor: str, b: Bucket, trigger: str) -> None:
    """Serialize bucket samples as Parquet (snappy) and PUT to MinIO."""
    if not b.samples:
        return
    try:
        table = pa.Table.from_pylist(b.samples)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        body = buf.getvalue()
    except Exception as exc:  # noqa: BLE001 — pyarrow type inference can fail
        log.error("parquet serialize failed vehicle=%s sensor=%s rows=%d: %s",
                  vehicle_id, sensor, len(b.samples), exc)
        return

    obj_key = _build_object_key(vehicle_id, sensor, b.first_ts_ns, "parquet")
    t0 = time.time()
    try:
        minio_client.put_object(
            bucket_name,
            obj_key,
            io.BytesIO(body),
            length=len(body),
            content_type="application/octet-stream",
        )
    except Exception as exc:  # noqa: BLE001
        log.error("PUT failed key=%s vehicle=%s sensor=%s: %s",
                  obj_key, vehicle_id, sensor, exc)
        raise
    put_seconds.observe(time.time() - t0)
    objects_put.labels(vehicle_id=vehicle_id, sensor=sensor, trigger=trigger).inc()
    last_put_ts.set(time.time())
    log.info("PUT %s (%d samples, %d bytes, trigger=%s)",
             obj_key, len(b.samples), len(body), trigger)


def _parse_record(msg) -> tuple[str | None, str | None, dict | None, str | None]:
    """Return (vehicle_id, sensor, sample_dict, drop_reason)."""
    headers = msg.headers() or []
    key = msg.key()
    vehicle_id = _hdr(headers, "vehicle_id") or (
        key.decode("utf-8", errors="replace") if key else None
    )
    if not vehicle_id:
        return None, None, None, "missing_key"

    sensor = _hdr(headers, "sensor")
    if not sensor:
        return None, None, None, "missing_sensor"

    raw = msg.value()
    if not raw:
        return None, None, None, "parse_error"
    try:
        sample = json.loads(raw)
    except (ValueError, TypeError):
        return None, None, None, "parse_error"

    # ts_ns 우선순위: header > body
    ts_str = _hdr(headers, "ts_ns")
    ts_ns_int = None
    if ts_str:
        try:
            ts_ns_int = int(ts_str)
        except ValueError:
            pass
    if ts_ns_int is None:
        body_ts = sample.get("ts_ns") or sample.get("capture_ts_ns")
        if isinstance(body_ts, int):
            ts_ns_int = body_ts
    if ts_ns_int is None:
        return None, None, None, "missing_ts_ns"

    # ensure ts_ns 가 sample dict 에 들어가 parquet 에 보존되도록
    sample.setdefault("ts_ns", ts_ns_int)
    return vehicle_id, sensor, sample, None


def _flush_idle(state: dict, minio_client: Minio, bucket_name: str,
                idle_sec: float, now: float) -> None:
    """Flush + drop buckets with no recent activity."""
    stale = [k for k, b in state.items() if (now - b.last_append) >= idle_sec]
    for k in stale:
        b = state.pop(k)
        vid, sensor = k
        try:
            _flush(minio_client, bucket_name, vid, sensor, b, trigger="idle")
        except Exception:  # noqa: BLE001
            log.exception("idle flush failed vehicle=%s sensor=%s", vid, sensor)


def _recompute_metrics(state: dict) -> None:
    state_buckets.set(len(state))
    buffer_samples.set(sum(len(b.samples) for b in state.values()))


def main() -> int:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    # 두 토픽 listen — raw.imu (RAWIMUX) + raw.gps (INSPVAXB/BESTGNSSPOS).
    input_topics_str = os.environ.get("KAFKA_INPUT_TOPICS", "raw.imu,raw.gps")
    input_topics = [t.strip() for t in input_topics_str.split(",") if t.strip()]
    group_id = os.environ.get("KAFKA_GROUP_ID", "kafka-imu-archiver")
    minio_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access = os.environ["MINIO_ACCESS_KEY"]
    minio_secret = os.environ["MINIO_SECRET_KEY"]
    minio_bucket = os.environ.get("MINIO_BUCKET", "daq")
    minio_secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    batch_size = int(os.environ.get("BATCH_SIZE", "1000"))
    idle_flush_sec = float(os.environ.get("IDLE_FLUSH_SEC", "90"))
    metrics_port = int(os.environ.get("METRICS_PORT", "2121"))

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
        # auto-commit 5s. crash 시 in-flight bucket (수십~수백 KB) 손실 허용.
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "auto.offset.reset": "latest",
        "session.timeout.ms": 30_000,
    })
    consumer.subscribe(input_topics)

    log.info(
        "archiver starting: input=%s group=%s batch_size=%d idle_flush=%.1fs bucket=%s",
        input_topics, group_id, batch_size, idle_flush_sec, minio_bucket,
    )

    # state key = (vehicle_id, sensor) — sensor 별 폴더 분리 적재
    state: dict[tuple[str, str], Bucket] = {}
    stop = False

    def _shutdown(signum, _frame):
        nonlocal stop
        log.info("signal %d — flushing remaining buckets", signum)
        stop = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while not stop:
            now = time.time()

            # ── periodic idle flush ────────────────────────────────
            _flush_idle(state, minio_client, minio_bucket, idle_flush_sec, now)
            _recompute_metrics(state)

            # ── consume ────────────────────────────────────────────
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            vehicle_id, sensor, sample, drop_reason = _parse_record(msg)
            if drop_reason is not None:
                records_dropped.labels(reason=drop_reason).inc()
                continue
            assert vehicle_id and sensor and sample is not None

            key = (vehicle_id, sensor)
            bucket = state.get(key)
            if bucket is None:
                bucket = Bucket(first_ts_ns=int(sample["ts_ns"]))
                state[key] = bucket
            bucket.samples.append(sample)
            bucket.last_append = time.time()
            records_in.labels(vehicle_id=vehicle_id, sensor=sensor).inc()

            # ── batch_full trigger ─────────────────────────────────
            if len(bucket.samples) >= batch_size:
                try:
                    _flush(minio_client, minio_bucket, vehicle_id, sensor,
                           bucket, trigger="batch_full")
                except Exception:  # noqa: BLE001
                    log.exception("batch flush failed vehicle=%s sensor=%s",
                                  vehicle_id, sensor)
                state.pop(key, None)

    finally:
        log.info("draining: flushing %d remaining bucket(s)", len(state))
        for (vid, sensor), b in list(state.items()):
            if b.samples:
                try:
                    _flush(minio_client, minio_bucket, vid, sensor, b,
                           trigger="shutdown")
                except Exception:  # noqa: BLE001
                    log.exception("shutdown flush failed vehicle=%s sensor=%s",
                                  vid, sensor)
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            log.exception("consumer close failed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
