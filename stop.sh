#!/bin/bash
# ============================================================
#  daq-kafka-broker  stop.sh
# ============================================================
cd "$(dirname "$0")"
echo ""
echo "[daq-broker] 중지 중..."
docker compose down
echo "[daq-broker] ✅ 중지 완료."
echo ""
