#!/bin/bash
# ============================================================
#  Kafka topic 초기화 (최초 1회)
#  실행: make init
# ============================================================
set -e

source ./config/config.env

echo "[topics] Waiting for Kafka broker..."
for i in $(seq 1 30); do
    if docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo "[topics] Broker ready."
        break
    fi
    echo "  waiting... (${i}/30)"
    sleep 3
done

KAFKA="docker exec kafka kafka-topics --bootstrap-server localhost:9092"

create_topic() {
    local topic=$1
    local partitions=${2:-$TOPIC_PARTITIONS}
    if $KAFKA --list | grep -q "^${topic}$"; then
        echo "[topics] Already exists: ${topic}"
    else
        $KAFKA --create \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor "${TOPIC_REPLICATION:-1}" \
            --config retention.ms=604800000 \
            --config retention.bytes=107374182400 \
            --config max.message.bytes=10485760
        echo "[topics] Created: ${topic}  (partitions=${partitions})"
    fi
}

create_topic "$TOPIC_CAM0" 3
create_topic "$TOPIC_CAM1" 3
create_topic "$TOPIC_CAM2" 3
create_topic "$TOPIC_GNSS" 1

echo ""
echo "[topics] Done. Topic list:"
$KAFKA --list | sed 's/^/  /'
