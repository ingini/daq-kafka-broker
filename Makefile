# ============================================================
#  daq-kafka-broker  Makefile
#
#  make up          전체 스택 시작
#  make init        topic 초기화 (최초 1회)
#  make tunnel      cloudflare URL 확인 → 차량 BROKER_REST_URL 에 입력
#  make down        중지
#  make restart     재시작
#  make status      전체 상태
#  make logs        전체 로그
#  make log-broker  Kafka 로그
#  make log-proxy   REST Proxy 로그
#  make log-consumer daq-consumer 로그
#  make log-tunnel  cloudflared 로그
#  make topics      topic 목록
#  make metrics     consumer 메트릭
#  make clean       컨테이너 + 볼륨 전체 삭제
# ============================================================

.PHONY: all up down restart init tunnel status logs \
        log-broker log-proxy log-consumer log-tunnel \
        topics metrics clean help

CONFIG := ./config/config.env
include $(CONFIG)
export

all: help

# ── 시작 / 중지 ───────────────────────────────────────────────
up:
	@echo "[broker] Starting all services..."
	docker compose --env-file $(CONFIG) up -d
	@echo "[broker] Started. Run 'make init' if first time."
	@echo "[broker] Run 'make tunnel' to get cloudflare URL."

down:
	docker compose --env-file $(CONFIG) down

restart: down up

# ── 초기화 (최초 1회) ─────────────────────────────────────────
init:
	@chmod +x scripts/init-topics.sh
	@bash scripts/init-topics.sh

# ── Cloudflare URL 확인 ───────────────────────────────────────
# 이 URL 을 차량 daq-kafka-producer/config/config.env 의
# BROKER_REST_URL 에 입력
tunnel:
	@echo "=== Cloudflare Tunnel URL ==="
	@docker logs cloudflared 2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1
	@echo ""
	@echo "차량 config.env 에 입력:"
	@echo "  BROKER_REST_URL=$$(docker logs cloudflared 2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1)"

# ── 상태 확인 ─────────────────────────────────────────────────
status:
	@echo "=== Containers ==="
	@docker compose --env-file $(CONFIG) ps
	@echo ""
	@echo "=== Topics ==="
	@docker exec kafka kafka-topics \
	    --bootstrap-server localhost:9092 --list 2>/dev/null \
	    | sed 's/^/  /' || echo "  broker not ready"
	@echo ""
	@echo "=== Cloudflare URL ==="
	@docker logs cloudflared 2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1 | sed 's/^/  /' || echo "  tunnel not ready"
	@echo ""
	@echo "=== REST Proxy ==="
	@curl -s http://localhost:8082/topics 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "  not ready"

topics:
	@docker exec kafka kafka-topics \
	    --bootstrap-server localhost:9092 \
	    --describe 2>/dev/null || echo "broker not ready"

metrics:
	@curl -s http://localhost:2122/metrics | grep daq_consumer

# ── 로그 ──────────────────────────────────────────────────────
logs:
	docker compose --env-file $(CONFIG) logs -f --tail=50

log-broker:
	docker logs -f kafka --tail=100

log-proxy:
	docker logs -f rest-proxy --tail=100

log-consumer:
	docker logs -f daq-consumer --tail=100

log-tunnel:
	docker logs -f cloudflared --tail=100

# ── 정리 ──────────────────────────────────────────────────────
clean:
	docker compose --env-file $(CONFIG) down -v --remove-orphans
	@echo "[broker] All containers and volumes removed."

# ── 도움말 ────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  daq-kafka-broker (KRaft + Cloudflare Tunnel)"
	@echo "  ============================================="
	@echo "  설정: config/config.env, config/minio.secret"
	@echo ""
	@echo "  make up           전체 스택 시작"
	@echo "  make init         topic 초기화 (최초 1회)"
	@echo "  make tunnel       cloudflare URL 확인"
	@echo "  make status       전체 상태"
	@echo "  make down         중지"
	@echo "  make restart      재시작"
	@echo "  make topics       topic 상세"
	@echo "  make metrics      consumer 메트릭"
	@echo "  make log-consumer consumer 로그"
	@echo "  make log-tunnel   cloudflared 로그"
	@echo "  make clean        전체 삭제 (볼륨 포함)"
	@echo ""
