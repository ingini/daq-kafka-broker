#!/usr/bin/env bash
# Kafka topic 초기화 — 기존 red-poc + daq-kafka-producer 차량 topic 통합
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
echo "[kafka-init] waiting for broker at $BOOTSTRAP ..."
for i in $(seq 1 60); do
    if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
        echo "[kafka-init] broker is up"
        break
    fi
    sleep 2
done

create_topic() {
    local name="$1"
    local partitions="$2"
    local rf="$3"
    local retention_ms="$4"
    local max_msg_bytes="${5:-}"

    if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
            --describe --topic "$name" >/dev/null 2>&1; then
        echo "[kafka-init] topic '$name' already exists — skipping"
        return
    fi

    local extra_config=()
    if [[ -n "$max_msg_bytes" ]]; then
        extra_config+=(--config "max.message.bytes=$max_msg_bytes")
    fi

    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create \
        --topic "$name" \
        --partitions "$partitions" \
        --replication-factor "$rf" \
        --config "retention.ms=$retention_ms" \
        --config "cleanup.policy=delete" \
        "${extra_config[@]}"
    echo "[kafka-init] created '$name' (partitions=$partitions, rf=$rf, retention=${retention_ms}ms${max_msg_bytes:+, max.bytes=$max_msg_bytes})"
}

# ── 기존 red-poc topic ────────────────────────────────────────
create_topic raw.frame.metadata          3 1 604800000
create_topic raw.frame.metadata.dlq      1 1 604800000
create_topic raw.frame.metadata.decoded  3 1 604800000
create_topic raw.gps                     3 1 172800000
create_topic events.detection            3 1 2592000000
create_topic events.detection.dlq        1 1 2592000000
create_topic raw.frame.images            3 1   3600000  10485760
create_topic raw.frame.images.dlq        1 1  86400000  10485760
create_topic raw.imu                     3 1 172800000

# ── daq-kafka-producer 차량 topic ─────────────────────────────
# sensor.cam*.jpeg: 카메라 JPEG (binary, 최대 10MB)
# 차량 → REST Proxy → 여기 적재 → daq-consumer → MinIO
create_topic sensor.cam0.jpeg  3 1  3600000  10485760
create_topic sensor.cam1.jpeg  3 1  3600000  10485760
create_topic sensor.cam2.jpeg  3 1  3600000  10485760

# sensor.gnss: NovAtel GNSS JSON (소형, 72시간 보존)
create_topic sensor.gnss       1 1 259200000

echo "[kafka-init] done"
