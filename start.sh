#!/bin/bash
# ============================================================
#  daq-kafka-broker  start.sh
#  사용법: ./start.sh
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f .env ]; then
    echo "[ERROR] .env 파일이 없습니다."
    exit 1
fi
set -a; source .env; set +a

# SSL 인증서 확인
if [ ! -f nginx/certs/fullchain.pem ] || [ ! -f nginx/certs/privkey.pem ]; then
    echo "[ERROR] SSL 인증서가 없습니다."
    echo "  cp /home/swmai/project/kepco/infra/red-poc-edge/certs/fullchain.pem ./nginx/certs/"
    echo "  cp /home/swmai/project/kepco/infra/red-poc-edge/certs/privkey.pem ./nginx/certs/"
    exit 1
fi

echo ""
echo "================================================="
echo "  daq-kafka-broker 통합 스택 시작"
echo "================================================="

# ── 1. 기존 red-poc 중지 확인 ──────────────────────────────
if docker ps | grep -q "red-poc-"; then
    echo ""
    echo "[경고] 기존 red-poc 컨테이너가 실행 중입니다."
    echo "  중지하려면: cd /home/swmai/project/kepco && docker compose down"
    read -p "  계속 진행하시겠습니까? (y/N): " ans
    if [[ "$ans" != "y" && "$ans" != "Y" ]]; then
        echo "중단합니다."
        exit 1
    fi
fi

# ── 2. 빌드 ────────────────────────────────────────────────
echo ""
echo "[1/4] 이미지 빌드..."
docker compose build daq-consumer

# ── 3. 기동 ────────────────────────────────────────────────
echo ""
echo "[2/4] 컨테이너 기동..."
docker compose up -d

# ── 4. Kafka healthy 대기 ───────────────────────────────────
echo ""
echo "[3/4] Kafka 준비 대기..."
for i in $(seq 1 40); do
    if docker exec red-poc-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo "      ✅ Kafka ready."
        break
    fi
    printf "      waiting... (%d/40)\r" "$i"
    sleep 3
    if [ "$i" -eq 40 ]; then
        echo ""
        echo "[ERROR] Kafka 기동 실패. docker logs red-poc-kafka 확인"
        exit 1
    fi
done

# ── 5. 상태 출력 ────────────────────────────────────────────
echo ""
echo "[4/4] 상태 확인..."
docker compose ps

echo ""
echo "================================================="
echo ""
echo "  ✅ daq-kafka-broker 통합 스택 기동 완료"
echo ""
echo "  외부 접근:"
echo "    Kafka REST : https://221.147.232.196:8443/poc/kafka-rest"
echo "    MinIO      : https://221.147.232.196:8443/poc/minio"
echo "    Grafana    : https://221.147.232.196:8443/poc/grafana"
echo ""
echo "  차량 config/config.env:"
echo "    BROKER_REST_URL=https://221.147.232.196:8443/poc/kafka-rest"
echo ""
echo "  로그 확인:"
echo "    docker logs -f daq-consumer"
echo "    docker logs -f red-poc-kafka-image-consumer"
echo "    docker logs -f red-poc-processor"
echo ""
echo "  종료: ./stop.sh"
echo "================================================="

