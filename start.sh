#!/bin/bash
# ============================================================
#  daq-kafka-broker  start.sh
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f .env ]; then
    echo "[ERROR] .env 파일이 없습니다."
    exit 1
fi

# SSL 인증서 확인
if [ ! -f nginx/certs/fullchain.pem ] || [ ! -f nginx/certs/privkey.pem ]; then
    echo "[ERROR] SSL 인증서가 없습니다."
    echo "  79번 서버에서 복사:"
    echo "  docker cp red-poc-edge:/etc/nginx/certs/fullchain.pem ./nginx/certs/"
    echo "  docker cp red-poc-edge:/etc/nginx/certs/privkey.pem ./nginx/certs/"
    exit 1
fi

echo ""
echo "================================================="
echo "  daq-kafka-broker 시작"
echo "================================================="

# ── 1. 빌드 + 기동 ──────────────────────────────────────────
echo ""
echo "[1/3] 컨테이너 기동..."
if docker image inspect daq-consumer:latest > /dev/null 2>&1; then
    docker compose up -d
else
    echo "      최초 빌드 (시간 소요)..."
    docker compose up -d --build
fi

# ── 2. Kafka broker healthy 대기 ─────────────────────────────
echo ""
echo "[2/3] Kafka 준비 대기 중..."
for i in $(seq 1 40); do
    if docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo "      ✅ Kafka ready."
        break
    fi
    printf "      waiting... (%d/40)\r" "$i"
    sleep 3
    if [ "$i" -eq 40 ]; then
        echo ""
        echo "[ERROR] Kafka 기동 실패. docker logs kafka 확인 필요."
        exit 1
    fi
done

# ── 3. 상태 출력 ─────────────────────────────────────────────
echo ""
echo "[3/3] 상태 확인..."
docker compose ps

echo ""
echo "================================================="
echo ""
echo "  ✅ daq-kafka-broker 기동 완료"
echo ""
echo "  내부 접근:"
echo "    REST Proxy : http://192.168.1.66:8082  (컨테이너: kafka-rest-proxy)"
echo "    MinIO API  : http://192.168.1.66:9000"
echo "    MinIO UI   : http://192.168.1.66:9001"
echo ""
echo "  외부 접근 (nginx :8443):"
echo "    Kafka REST : https://221.147.232.196:8443/poc/kafka-rest"
echo "    MinIO      : https://221.147.232.196:8443/poc/minio"
echo ""
echo "  ※ 공유기 포트포워딩 확인:"
echo "    8443 → 192.168.1.66:8443"
echo ""
echo "  차량 config/config.env:"
echo "    BROKER_REST_URL=https://221.147.232.196:8443/poc/kafka-rest"
echo ""
echo "  유용한 명령:"
echo "    docker logs -f daq-consumer    # consumer 로그"
echo "    docker logs -f nginx-edge      # nginx 로그"
echo "    docker compose ps              # 상태 확인"
echo ""
echo "  종료: ./stop.sh"
echo "================================================="