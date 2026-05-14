#!/bin/bash
# ============================================================
#  copy-sources.sh
#  red-poc 소스를 daq-kafka-broker 로 복사 (자급자족 구조)
#  실행: bash copy-sources.sh
# ============================================================
set -e

BROKER_DIR="$(cd "$(dirname "$0")" && pwd)"
KEPCO=/home/swmai/project/kepco

echo "================================================="
echo "  daq-kafka-broker 소스 복사"
echo "  from: $KEPCO"
echo "  to:   $BROKER_DIR"
echo "================================================="

# ── Python 서비스 (infra) ────────────────────────────────────
echo ""
echo "[1/7] kafka-image-consumer..."
mkdir -p "$BROKER_DIR/kafka-image-consumer"
cp "$KEPCO/infra/kafka-image-consumer/consumer.py"    "$BROKER_DIR/kafka-image-consumer/"
cp "$KEPCO/infra/kafka-image-consumer/Dockerfile"     "$BROKER_DIR/kafka-image-consumer/"
cp "$KEPCO/infra/kafka-image-consumer/requirements.txt" "$BROKER_DIR/kafka-image-consumer/"

echo "[2/7] kafka-frame-bridge..."
mkdir -p "$BROKER_DIR/kafka-frame-bridge"
cp "$KEPCO/infra/kafka-frame-bridge/bridge.py"        "$BROKER_DIR/kafka-frame-bridge/"
cp "$KEPCO/infra/kafka-frame-bridge/Dockerfile"       "$BROKER_DIR/kafka-frame-bridge/"
cp "$KEPCO/infra/kafka-frame-bridge/requirements.txt" "$BROKER_DIR/kafka-frame-bridge/"

echo "[3/7] kafka-imu-archiver..."
mkdir -p "$BROKER_DIR/kafka-imu-archiver"
cp "$KEPCO/infra/kafka-imu-archiver/archiver.py"      "$BROKER_DIR/kafka-imu-archiver/"
cp "$KEPCO/infra/kafka-imu-archiver/Dockerfile"       "$BROKER_DIR/kafka-imu-archiver/"
cp "$KEPCO/infra/kafka-imu-archiver/requirements.txt" "$BROKER_DIR/kafka-imu-archiver/"

echo "[4/7] topic-mapper (패치된 버전)..."
mkdir -p "$BROKER_DIR/topic-mapper"
# 패치된 mapper.py 를 컨테이너에서 복사
docker cp red-poc-topic-mapper:/app/mapper.py         "$BROKER_DIR/topic-mapper/mapper.py"
cp "$KEPCO/infra/topic-mapper/Dockerfile"             "$BROKER_DIR/topic-mapper/"
cp "$KEPCO/infra/topic-mapper/requirements.txt"       "$BROKER_DIR/topic-mapper/"

echo "[5/7] mqtt-kafka-bridge..."
mkdir -p "$BROKER_DIR/mqtt-kafka-bridge"
cp "$KEPCO/infra/mqtt-kafka-bridge/main.py"           "$BROKER_DIR/mqtt-kafka-bridge/"
cp "$KEPCO/infra/mqtt-kafka-bridge/Dockerfile"        "$BROKER_DIR/mqtt-kafka-bridge/"
cp "$KEPCO/infra/mqtt-kafka-bridge/requirements.txt"  "$BROKER_DIR/mqtt-kafka-bridge/"

# ── Java/Kotlin 서비스 (services) ────────────────────────────
echo "[6/7] processor (Kotlin)..."
mkdir -p "$BROKER_DIR/processor/src"
cp "$KEPCO/services/processor/Dockerfile"             "$BROKER_DIR/processor/"
cp "$KEPCO/services/processor/.dockerignore"          "$BROKER_DIR/processor/"
cp "$KEPCO/services/processor/build.gradle.kts"       "$BROKER_DIR/processor/"
cp "$KEPCO/services/processor/settings.gradle.kts"    "$BROKER_DIR/processor/"
cp -r "$KEPCO/services/processor/src"                 "$BROKER_DIR/processor/"

echo "[7/7] notifier (Kotlin)..."
mkdir -p "$BROKER_DIR/notifier/src"
cp "$KEPCO/services/notifier/Dockerfile"              "$BROKER_DIR/notifier/"
cp "$KEPCO/services/notifier/.dockerignore"           "$BROKER_DIR/notifier/"
cp "$KEPCO/services/notifier/build.gradle.kts"        "$BROKER_DIR/notifier/"
cp "$KEPCO/services/notifier/settings.gradle.kts"     "$BROKER_DIR/notifier/"
cp -r "$KEPCO/services/notifier/src"                  "$BROKER_DIR/notifier/"

# ── transcoder ───────────────────────────────────────────────
echo "[+] transcoder..."
mkdir -p "$BROKER_DIR/transcoder/transcoder"
cp "$KEPCO/services/transcoder/Dockerfile"            "$BROKER_DIR/transcoder/"
cp "$KEPCO/services/transcoder/.dockerignore"         "$BROKER_DIR/transcoder/"
cp "$KEPCO/services/transcoder/main.py"               "$BROKER_DIR/transcoder/"
cp "$KEPCO/services/transcoder/requirements.txt"      "$BROKER_DIR/transcoder/"
cp "$KEPCO/services/transcoder/config.yml"            "$BROKER_DIR/transcoder/"
cp -r "$KEPCO/services/transcoder/transcoder"         "$BROKER_DIR/transcoder/"

# ── Postgres init ─────────────────────────────────────────────
echo "[+] postgres init..."
mkdir -p "$BROKER_DIR/postgres/init"
cp -r "$KEPCO/infra/postgres/init/."                  "$BROKER_DIR/postgres/init/"

# ── Prometheus/Grafana ────────────────────────────────────────
echo "[+] prometheus..."
mkdir -p "$BROKER_DIR/prometheus"
cp "$KEPCO/infra/prometheus/prometheus.yml"           "$BROKER_DIR/prometheus/"
cp "$KEPCO/infra/prometheus/alerts.yml"               "$BROKER_DIR/prometheus/" 2>/dev/null || true

echo "[+] grafana..."
mkdir -p "$BROKER_DIR/grafana"
cp -r "$KEPCO/infra/grafana/provisioning"             "$BROKER_DIR/grafana/"

echo ""
echo "================================================="
echo "  ✅ 소스 복사 완료"
echo ""
echo "  복사된 서비스:"
echo "    kafka-image-consumer  kafka-frame-bridge"
echo "    kafka-imu-archiver    topic-mapper (패치본)"
echo "    mqtt-kafka-bridge     processor    notifier"
echo "    transcoder            postgres     prometheus  grafana"
echo ""
echo "  다음 단계:"
echo "    1. docker-compose.yml build context 상대경로 수정"
echo "    2. ./start.sh"
echo "================================================="
