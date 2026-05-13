#!/bin/bash
# ============================================================
#  daq-kafka-broker  start.sh
#  사용법: ./start.sh
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# .env 로드
if [ ! -f .env ]; then
    echo "[ERROR] .env 파일이 없습니다."
    exit 1
fi
set -a; source .env; set +a

echo ""
echo "================================================="
echo "  daq-kafka-broker 시작"
echo "================================================="

# ── 1. 빌드 + 기동 ──────────────────────────────────────────
echo ""
echo "[1/4] 컨테이너 빌드 및 기동..."
docker compose up -d --build

# ── 2. Kafka broker healthy 대기 ─────────────────────────────
echo ""
echo "[2/4] Kafka 준비 대기 중..."
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

# ── 3. Cloudflare URL 대기 ────────────────────────────────────
echo ""
echo "[3/4] Cloudflare 터널 URL 대기 중..."
TUNNEL_URL=""
for i in $(seq 1 30); do
    TUNNEL_URL=$(docker logs cloudflared 2>&1 \
        | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' \
        | tail -1)
    if [ -n "$TUNNEL_URL" ]; then
        echo "      ✅ 터널 URL 발급 완료."
        break
    fi
    printf "      waiting... (%d/30)\r" "$i"
    sleep 2
done

# ── 4. URL 메일 발송 ──────────────────────────────────────────
echo ""
echo "[4/4] Cloudflare URL 메일 발송..."

if [ -z "$TUNNEL_URL" ]; then
    echo "      ⚠️  URL 발급 실패. 수동 확인: docker logs cloudflared | grep trycloudflare"
else
    # python으로 메일 발송
    python3 - << PYEOF
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

smtp_host = "${SMTP_HOST}"
smtp_port = int("${SMTP_PORT}")
smtp_user = "${SMTP_USER}"
smtp_pass = "${SMTP_PASSWORD}"
smtp_to   = "${SMTP_TO}"
url       = "${TUNNEL_URL}"
hostname  = os.popen("hostname").read().strip()
now       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

msg = MIMEMultipart()
msg["From"]    = smtp_user
msg["To"]      = smtp_to
msg["Subject"] = f"[DAQ Broker] Cloudflare URL 갱신 - {now}"

body = f"""
DAQ Kafka Broker 가 시작되었습니다.

📡 Cloudflare Tunnel URL:
{url}

★ 차량 config/config.env 에 아래 값 입력:
BROKER_REST_URL={url}

서버: {hostname}
시간: {now}

MinIO 저장 경로:
  daq/year=YYYY/month=MM/day=dd/{{vehicle_id}}/{{sensor}}/{{ts_ns}}.jpg
"""

msg.attach(MIMEText(body, "plain", "utf-8"))

try:
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, smtp_to, msg.as_string())
    print(f"      ✅ 메일 발송 완료 → {smtp_to}")
except Exception as e:
    print(f"      ⚠️  메일 발송 실패: {e}")
    print(f"         수동으로 URL 확인: docker logs cloudflared | grep trycloudflare")
PYEOF
fi

# ── 완료 출력 ─────────────────────────────────────────────────
echo ""
echo "================================================="
echo ""
echo "  ✅ daq-kafka-broker 기동 완료"
echo ""
if [ -n "$TUNNEL_URL" ]; then
echo "  📡 Cloudflare Tunnel URL:"
echo "     $TUNNEL_URL"
echo ""
echo "  ★ 차량 config/config.env:"
echo "     BROKER_REST_URL=$TUNNEL_URL"
fi
echo ""
echo "  유용한 명령:"
echo "    docker logs -f daq-consumer    # consumer 로그"
echo "    docker logs -f cloudflared     # 터널 로그"
echo "    docker compose ps              # 컨테이너 상태"
echo ""
echo "  종료: ./stop.sh"
echo ""
echo "================================================="

