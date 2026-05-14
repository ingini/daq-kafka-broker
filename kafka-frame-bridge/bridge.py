"""kafka-frame-bridge — raw.frame.images → raw.frame.metadata.decoded.

Path B (Kafka REST Proxy) 의 missing link. kafka-image-consumer 가 MinIO 에
.jpg 만 적재하는 반면, processor 는 raw.frame.metadata.decoded 토픽을 listen
하므로 추론이 트리거되지 않는다. 본 bridge 는 raw.frame.images 의 sensor 별
record 들을 시간 window 단위로 묶어 FrameBundleMeta 형식으로
raw.frame.metadata.decoded 에 발행하여 path A 와 같은 처리 흐름으로 합류시킨다.

Architecture
------------
    [Path B device] ──▶ raw.frame.images ──┬─▶ kafka-image-consumer ──▶ MinIO PUT
                                            │
                                            └─▶ kafka-frame-bridge (this)
                                                    │ window_ms 단위로 sensor 별 latest
                                                    ▼
                                            raw.frame.metadata.decoded
                                                    │ (path A 와 동일 schema)
                                                    ▼
                                            processor (Triton 추론)
                                                    │
                                                    ▼
                                            events.detection
                                                    │
                                                    ▼
                                            notifier

Bundling 알고리즘
----------------
- vehicle_id 별 in-memory state: { sensor → (ts_ns, s3_uri) }
- 새 record 도착 시 sensor slot 덮어쓰기 (10 fps 일 때 같은 sensor 의 직전 frame 은
  대체됨 — sensor 당 latest 만 유지)
- 다음 중 먼저 도래하는 trigger 에 emit:
    1) BUNDLE_WINDOW_MS 만료 (default 1000ms — 1Hz 추론)
    2) SENSORS_EXPECTED 도달 (default 0 = off, race 회피)
- emit 후 state 비우고 다음 window 대기

MinIO PUT race
--------------
bridge 와 kafka-image-consumer 가 같은 토픽을 동시에 받지만 group.id 가 다르므로
독립 소비. consumer 의 MinIO PUT (~25ms LAN) 이 bridge 의 emit 보다 먼저 끝나야
processor 가 fetch 시 404 가 안 난다. 기본 BUNDLE_WINDOW_MS=1000 이 충분한 여유.
SENSORS_EXPECTED=3 즉시 emit 모드는 race 위험이라 default off.

At-least-once vs idempotency
----------------------------
auto-commit + reset=latest. crash 시 인-flight bundling 상태는 손실 (수 초의 추론
공백). bundle_id 는 매 emit 마다 새 UUID 라 downstream 멱등성은 events.detection
의 event_id (sha256(bundle_id)[..16]) 가 보장. 충분.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

# Asia/Seoul (KST = UTC+9). fixed offset 으로 두어 zoneinfo / tzdata 의존성 회피.
# kafka-image-consumer.consumer.KST 와 동일 — 두 모듈이 같은 키 layout 을 만든다.
KST = timezone(timedelta(hours=9), name="KST")

from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server


log = logging.getLogger("kafka-frame-bridge")

# ── metrics ─────────────────────────────────────────────────────────
records_in = Counter(
    "kafka_frame_bridge_records_in_total",
    "Records consumed from input topic.",
    ["sensor"],
)
records_dropped = Counter(
    "kafka_frame_bridge_records_dropped_total",
    "Records dropped (malformed headers).",
    ["reason"],
)
bundles_out = Counter(
    "kafka_frame_bridge_bundles_out_total",
    "Bundles emitted to output topic.",
    ["sensor_count", "trigger"],  # 1|2|3, trigger={full,window,shutdown}
)
bundle_window_seconds = Histogram(
    "kafka_frame_bridge_bundle_window_seconds",
    "Time from bundle window start to emit.",
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
)
state_vehicles = Gauge(
    "kafka_frame_bridge_state_vehicles",
    "Active vehicle states held in memory.",
)
last_emit_ts = Gauge(
    "kafka_frame_bridge_last_emit_ts_seconds",
    "Wall-clock seconds of last bundle emit (for staleness alerts).",
)


@dataclass
class VehicleState:
    """Per-vehicle accumulator. `sensors[sensor] = (ts_ns, s3_uri)`."""
    sensors: dict[str, tuple[int, str]] = field(default_factory=dict)
    window_start: float = 0.0  # wall-clock of first record in current window


def _hdr(headers, name: str, default: Optional[str] = None) -> Optional[str]:
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


def _build_uri(bucket: str, vehicle_id: str, sensor: str, ts_ns: str) -> str:
    """Reconstruct the s3 URI that kafka-image-consumer wrote.

    Must match `_build_object_key()` in `infra/kafka-image-consumer/consumer.py`
    exactly — bridge does NOT contact MinIO; it derives the URI from headers
    alone, trusting that the consumer's PUT will land at the same key.

    daq-kafka layout (matches external module's MinIO consumer):
        s3://{bucket}/{YYYY}/{MM}/{DD}/{HH}/{vehicle}/{sensor}/{ts_str}_{ts_ns}.jpg
    """
    # KST 기준 — kafka-image-consumer._build_object_key() 와 정확히 동일한 layout.
    # 두 모듈이 같은 ts_ns 입력으로 같은 키를 만들어야 processor S3 fetch 성공.
    dt = datetime.fromtimestamp(int(ts_ns) / 1e9, tz=KST)
    ts_str = dt.strftime("%Y%m%dT%H%M%S")
    return (
        f"s3://{bucket}/"
        f"{dt.strftime('%Y')}/"
        f"{dt.strftime('%m')}/"
        f"{dt.strftime('%d')}/"
        f"{dt.strftime('%H')}/"
        f"{vehicle_id}/"
        f"{sensor}/"
        f"{ts_str}_{ts_ns}.jpg"
    )


def _emit_bundle(producer: Producer, output_topic: str, vehicle_id: str,
                 vstate: VehicleState, trigger: str) -> None:
    """Build FrameBundleMeta JSON and publish to output topic."""
    if not vstate.sensors:
        return
    images = {sensor: uri for sensor, (_, uri) in vstate.sensors.items()}
    # bundle 의 capture_ts_ns 는 sensor 들의 latest — "이 시점까지 캡처된 프레임"
    # 의 의미. processor 의 GPS lookup 은 nearest(ts) 라 가까운 값이면 OK.
    capture_ts = max(ts for ts, _ in vstate.sensors.values())

    payload = {
        "bundle_id": str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "capture_ts_ns": capture_ts,
        # path B 는 .h264 가 없으므로 `images` 도 .jpg URI 로 채움.
        # downstream 은 `images_decoded` 만 fetch 하므로 무해.
        "images": images,
        "images_decoded": images,
    }
    try:
        producer.produce(
            output_topic,
            key=vehicle_id.encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
        producer.poll(0)
    except BufferError:
        producer.flush(timeout=5)
        producer.produce(
            output_topic,
            key=vehicle_id.encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )

    bundles_out.labels(sensor_count=str(len(vstate.sensors)), trigger=trigger).inc()
    if vstate.window_start > 0:
        bundle_window_seconds.observe(time.time() - vstate.window_start)
    last_emit_ts.set(time.time())
    log.debug("emit vehicle=%s sensors=%d capture_ts_ns=%d trigger=%s",
              vehicle_id, len(vstate.sensors), capture_ts, trigger)

    vstate.sensors.clear()
    vstate.window_start = 0.0


def main() -> int:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    input_topic = os.environ.get("KAFKA_INPUT_TOPIC", "raw.frame.images")
    output_topic = os.environ.get("KAFKA_OUTPUT_TOPIC", "raw.frame.metadata.decoded")
    group_id = os.environ.get("KAFKA_GROUP_ID", "kafka-frame-bridge")
    minio_bucket = os.environ.get("MINIO_BUCKET", "red-poc")
    bundle_window_ms = int(os.environ.get("BUNDLE_WINDOW_MS", "1000"))
    # 0 = window 만으로 emit (race 회피 default).
    # >0 = N 개 sensor 도달 시 즉시 emit (race 가능성 — MinIO PUT 안 끝났을 수 있음).
    sensors_expected = int(os.environ.get("SENSORS_EXPECTED", "0"))
    metrics_port = int(os.environ.get("METRICS_PORT", "2120"))

    if metrics_port > 0:
        start_http_server(metrics_port)
        log.info("metrics on :%d", metrics_port)

    log.info(
        "bridge starting: input=%s output=%s group=%s window_ms=%d expected=%d bucket=%s",
        input_topic, output_topic, group_id, bundle_window_ms, sensors_expected, minio_bucket,
    )

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        # bridge 는 본질적으로 stateless windowed aggregator. crash 시 in-flight
        # window 는 손실되지만 어차피 1초 단위라 운영 영향 작음. auto-commit 로 단순화.
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        # 신규 차량/재시작 시 backlog 처리 X — 라이브 stream 만.
        "auto.offset.reset": "latest",
        "session.timeout.ms": 30_000,
        "max.partition.fetch.bytes": 5_242_880,
        "fetch.max.bytes": 5_242_880,
    })
    consumer.subscribe([input_topic])

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 20,
        "compression.type": "lz4",
        "enable.idempotence": True,
    })

    state: dict[str, VehicleState] = {}
    window_s = bundle_window_ms / 1000.0

    stop = False

    def _shutdown(signum, _frame):
        nonlocal stop
        log.info("signal %d — flushing remaining state", signum)
        stop = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while not stop:
            # ── 1) periodic flush ──────────────────────────────────
            now = time.time()
            for vid, vs in list(state.items()):
                if vs.sensors and (now - vs.window_start) >= window_s:
                    _emit_bundle(producer, output_topic, vid, vs, "window")
            state_vehicles.set(len(state))

            # ── 2) ingest one record ───────────────────────────────
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            headers = msg.headers() or []
            vid = _hdr(headers, "vehicle_id") or (
                msg.key().decode("utf-8", errors="replace") if msg.key() else None
            )
            sensor = _hdr(headers, "sensor")
            ts_ns_str = _hdr(headers, "ts_ns")

            if not (vid and sensor and ts_ns_str):
                records_dropped.labels(reason="missing_headers").inc()
                log.warning(
                    "skip malformed record offset=%d (vid=%s sensor=%s ts=%s)",
                    msg.offset(), vid, sensor, ts_ns_str,
                )
                continue
            try:
                ts_ns = int(ts_ns_str)
            except ValueError:
                records_dropped.labels(reason="bad_ts_ns").inc()
                continue

            uri = _build_uri(minio_bucket, vid, sensor, ts_ns_str)
            vs = state.setdefault(vid, VehicleState())
            if not vs.sensors:
                vs.window_start = time.time()
            vs.sensors[sensor] = (ts_ns, uri)
            records_in.labels(sensor=sensor).inc()

            # ── 3) optional immediate emit on full bundle ─────────
            if sensors_expected > 0 and len(vs.sensors) >= sensors_expected:
                _emit_bundle(producer, output_topic, vid, vs, "full")

    finally:
        log.info("draining: flushing %d vehicle state(s)", len(state))
        for vid, vs in state.items():
            if vs.sensors:
                _emit_bundle(producer, output_topic, vid, vs, "shutdown")
        try:
            producer.flush(timeout=10)
        except Exception:  # noqa: BLE001
            log.exception("producer flush failed")
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            log.exception("consumer close failed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
