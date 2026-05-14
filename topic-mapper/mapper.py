"""topic-mapper — daq-kafka-producer (신규 device) 의 sensor.* 토픽을
기존 server 측 raw.* 토픽으로 변환·전달하는 server-side 어댑터.

배경
----
신규 device stack (daq_dashboard + daq-kafka-producer) 은 default config 로
다음 토픽에 publish:
    sensor.cam0.jpeg, sensor.cam1.jpeg, sensor.cam2.jpeg, sensor.gnss

기존 server 인프라는 다음 토픽을 소비:
    raw.frame.images, raw.gps
        ↓
    kafka-image-consumer / kafka-frame-bridge / kafka-imu-archiver
        ↓
    processor (Triton 추론) → events.detection → notifier → Slack/Postgres

본 모듈은 sensor.* → raw.* 의 단방향 forward + 다음 처리 수행:
  1. camera (3 토픽) → raw.frame.images
     - sensor 헤더를 topic 이름에서 추출하여 추가 (cam0/cam1/cam2)
     - binary value `[8B ts_ns | 4B len | JPEG]` 는 passthrough
     - key (vehicle_id) + 다른 헤더 모두 보존
  2. gnss → raw.gps
     - daq schema (latitude/longitude/height/ts_ns)
       → server schema (lat/lon/alt/capture_ts_ns)
     - 추가 필드 (accel_*, gyro_*, roll/pitch/azimuth 등) 보존
  3. vehicle_id 매핑 (선택)
     - env VEHICLE_ID_MAP 으로 device 의 vehicle_id 를 정규화
       예: {"AP500L-001": "VID_001"}
     - 미설정 시 device 가 보낸 값 그대로 forward

운영 메모
--------
- 기존 컴포넌트 (image-consumer / frame-bridge / imu-archiver / processor /
  notifier) 는 모두 무변경. mapper 가 produce 한 raw.* 메시지를 그대로 소비.
- mapper restart 시 5초 단절 동안 sensor.* 에 누적, 부팅 후 backlog drain.
- consumer group 'topic-mapper' 는 sensor.* 만 subscribe — raw.* 와 무관.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import signal
import sys
from typing import Optional

from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import Counter, Gauge, start_http_server


log = logging.getLogger("topic-mapper")

# ── config ──────────────────────────────────────────────────────────
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = os.environ.get("GROUP_ID", "topic-mapper")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "2123"))

SOURCE_CAM_TOPICS = [
    os.environ.get("SOURCE_CAM0", "sensor.cam0.jpeg"),
    os.environ.get("SOURCE_CAM1", "sensor.cam1.jpeg"),
    os.environ.get("SOURCE_CAM2", "sensor.cam2.jpeg"),
]
SOURCE_GNSS_TOPIC = os.environ.get("SOURCE_GNSS", "sensor.gnss")

TARGET_CAM_TOPIC = os.environ.get("TARGET_CAM_TOPIC", "raw.frame.images")
TARGET_GPS_TOPIC = os.environ.get("TARGET_GPS_TOPIC", "raw.gps")

# topic → sensor header 매핑. daq-kafka-producer 의 default topic 이름 기준.
TOPIC_SENSOR_MAP = {
    SOURCE_CAM_TOPICS[0]: "cam0",
    SOURCE_CAM_TOPICS[1]: "cam1",
    SOURCE_CAM_TOPICS[2]: "cam2",
    SOURCE_GNSS_TOPIC: "gnss",
}

# vehicle_id 정규화 (선택). env 로 JSON 받음.
#   VEHICLE_ID_MAP='{"AP500L-001": "VID_001"}'
try:
    VEHICLE_ID_MAP = json.loads(os.environ.get("VEHICLE_ID_MAP", "{}"))
except json.JSONDecodeError as exc:
    log.error("invalid VEHICLE_ID_MAP json: %s", exc)
    VEHICLE_ID_MAP = {}


# ── metrics ─────────────────────────────────────────────────────────
records_in = Counter(
    "topic_mapper_records_in_total",
    "Records consumed from source sensor.* topics.",
    ["source_topic"],
)
records_out = Counter(
    "topic_mapper_records_out_total",
    "Records produced to target raw.* topics.",
    ["target_topic", "result"],   # ok | produce_error
)
records_dropped = Counter(
    "topic_mapper_records_dropped_total",
    "Records dropped before forwarding.",
    ["reason"],
)
last_forward_ts = Gauge(
    "topic_mapper_last_forward_ts_seconds",
    "Wall-clock seconds at last successful produce.",
)


# ── transformation helpers ──────────────────────────────────────────
def _decode_b64_str(b: Optional[bytes]) -> str:
    """Header value 는 bytes 로 옴 — utf-8 decode (Kafka 표준)."""
    return b.decode("utf-8") if b else ""


def _maybe_remap_vehicle_id(vehicle_id: str) -> str:
    """Optional vehicle_id rewrite (옛 device 와 통일 시 사용)."""
    return VEHICLE_ID_MAP.get(vehicle_id, vehicle_id)


def _build_camera_headers(
    original_headers: list, sensor: str, vehicle_id: str
) -> list:
    """카메라 record 의 header 구성. sensor 헤더 보장, vehicle_id 매핑 적용."""
    out: list = []
    seen_sensor = False
    seen_vehicle = False
    for k, v in original_headers or []:
        if k == "sensor":
            # 명시적으로 mapper 가 설정한 값으로 오버라이드
            out.append(("sensor", sensor.encode("utf-8")))
            seen_sensor = True
        elif k == "vehicle_id":
            out.append(("vehicle_id", vehicle_id.encode("utf-8")))
            seen_vehicle = True
        else:
            out.append((k, v))
    if not seen_sensor:
        out.append(("sensor", sensor.encode("utf-8")))
    if not seen_vehicle:
        out.append(("vehicle_id", vehicle_id.encode("utf-8")))
    return out


def _convert_gnss_schema(daq_payload: dict) -> dict:
    """daq schema → server schema.

    daq 키:    latitude, longitude, height, ts_ns + 기타 (accel_*, gyro_*, ...)
    server 키: lat,      lon,       alt,    capture_ts_ns + 기타 보존
    """
    ts_ns = daq_payload.get("ts_ns")
    out: dict = {"capture_ts_ns": ts_ns} if ts_ns is not None else {}
    if "latitude" in daq_payload:
        out["lat"] = daq_payload["latitude"]
    if "longitude" in daq_payload:
        out["lon"] = daq_payload["longitude"]
    if "height" in daq_payload:
        out["alt"] = daq_payload["height"]
    # 변환되지 않은 필드 보존
    skip = {"latitude", "longitude", "height", "ts_ns"}
    for k, v in daq_payload.items():
        if k not in skip and k not in out:
            out[k] = v
    return out


# ── main ────────────────────────────────────────────────────────────
def main() -> int:
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    log.info(
        "starting: bootstrap=%s group=%s sources=%s targets=cam→%s gps→%s",
        BOOTSTRAP, GROUP_ID,
        SOURCE_CAM_TOPICS + [SOURCE_GNSS_TOPIC],
        TARGET_CAM_TOPIC, TARGET_GPS_TOPIC,
    )
    if VEHICLE_ID_MAP:
        log.info("vehicle_id remap: %s", VEHICLE_ID_MAP)

    start_http_server(METRICS_PORT)
    log.info("metrics on :%d", METRICS_PORT)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "auto.offset.reset": "latest",
        "session.timeout.ms": 30_000,
        "max.partition.fetch.bytes": 5_242_880,
        "fetch.max.bytes": 5_242_880,
    })
    consumer.subscribe(SOURCE_CAM_TOPICS + [SOURCE_GNSS_TOPIC])

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "message.max.bytes": 5_242_880,
        "compression.type": "none",
        "linger.ms": 5,
        "acks": "1",
        "retries": 3,
    })

    stop = False

    def _shutdown(signum, _frame):
        nonlocal stop
        log.info("signal %d — stopping", signum)
        stop = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    import time as _time
    try:
        while not stop:
            msg = consumer.poll(0.5)
            if msg is None:
                producer.poll(0)
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            source_topic = msg.topic()
            sensor = TOPIC_SENSOR_MAP.get(source_topic)
            if sensor is None:
                records_dropped.labels(reason="unknown_topic").inc()
                continue
            records_in.labels(source_topic=source_topic).inc()

            value = msg.value()
            if not value:
                records_dropped.labels(reason="empty_value").inc()
                continue

            # vehicle_id 추출 (header 우선, 없으면 key)
            headers = msg.headers() or []
            raw_vehicle = next(
                (_decode_b64_str(v) for k, v in headers if k == "vehicle_id"),
                "",
            )
            if not raw_vehicle and msg.key():
                raw_vehicle = _decode_b64_str(msg.key())
            if not raw_vehicle:
                records_dropped.labels(reason="missing_vehicle_id").inc()
                continue

            vehicle_id = _maybe_remap_vehicle_id(raw_vehicle)
            target_key = vehicle_id.encode("utf-8")

            if source_topic == SOURCE_GNSS_TOPIC:
                # GNSS — JSON schema 변환
                try:
                    daq_payload = json.loads(value)
                except (ValueError, TypeError):
                    records_dropped.labels(reason="gnss_json_parse_failed").inc()
                    continue
                if not isinstance(daq_payload, dict):
                    records_dropped.labels(reason="gnss_not_dict").inc()
                    continue
                server_payload = _convert_gnss_schema(daq_payload)
                new_value = json.dumps(
                    server_payload, ensure_ascii=False
                ).encode("utf-8")
                # GNSS 의 ts_ns 헤더도 갱신 (kafka-imu-archiver / processor 가 사용)
                ts_ns = server_payload.get("capture_ts_ns")
                target_headers = _build_camera_headers(
                    headers, sensor, vehicle_id
                )
                if ts_ns is not None:
                    target_headers = [
                        (k, v) for k, v in target_headers if k != "ts_ns"
                    ]
                    target_headers.append(("ts_ns", str(ts_ns).encode()))
                target_topic = TARGET_GPS_TOPIC
            else:
                # Camera - [8B ts_ns | 4B len | JPEG] -> JPEG + ts_ns header
                import struct
                try:
                    ts_ns_val, jpeg_len = struct.unpack(">QI", value[:12])
                    new_value = value[12: 12 + jpeg_len]
                except Exception:
                    new_value = value
                    ts_ns_val = None
                target_headers = _build_camera_headers(
                    headers, sensor, vehicle_id
                )
                if ts_ns_val:
                    target_headers = [
                        (k, v) for k, v in target_headers if k != "ts_ns"
                    ]
                    target_headers.append(("ts_ns", str(ts_ns_val).encode()))
                target_topic = TARGET_CAM_TOPIC

            try:
                producer.produce(
                    target_topic,
                    key=target_key,
                    value=new_value,
                    headers=target_headers,
                )
                producer.poll(0)
                records_out.labels(
                    target_topic=target_topic, result="ok"
                ).inc()
                last_forward_ts.set(_time.time())
            except BufferError:
                producer.flush(timeout=5)
                try:
                    producer.produce(
                        target_topic,
                        key=target_key,
                        value=new_value,
                        headers=target_headers,
                    )
                    records_out.labels(
                        target_topic=target_topic, result="ok"
                    ).inc()
                    last_forward_ts.set(_time.time())
                except Exception as exc:
                    log.warning(
                        "produce retry failed topic=%s err=%s", target_topic, exc
                    )
                    records_out.labels(
                        target_topic=target_topic, result="produce_error"
                    ).inc()
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "produce failed topic=%s err=%s", target_topic, exc
                )
                records_out.labels(
                    target_topic=target_topic, result="produce_error"
                ).inc()
    finally:
        log.info("draining producer...")
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
