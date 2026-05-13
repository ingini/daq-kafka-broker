#!/bin/bash
cd "$(dirname "$0")"
echo "[daq-broker] 중지 중..."
docker compose down
echo "[daq-broker] ✅ 중지 완료."