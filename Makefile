# ============================================================
#  daq-kafka-broker  Makefile
#
#  make up          전체 스택 시작
#  make init        topic 초기화 (최초 1회)
#  make down        중지
#  make restart     재시작
#  make status      컨테이너 + topic + consumer 상태
#  make logs        전체 로그
#  make log-broker  Kafka 로그
#  make log-proxy   REST Proxy 로그
#  make log-consumer daq-consumer 로그
#  make log-nginx   nginx 로그
#  make topics      topic 목록
#  make metrics     consumer Prometheus 메트릭
#  make clean       컨테이너 + 볼륨 전체 삭제
# ============================================================

.PHONY: all up down restart init status logs \
        log-broker log-proxy log-consumer log-nginx \
        topics metrics clean help

CONFIG := ./config/config.env
include $(CONFIG)
export

all: help

# ── 시작 / 중지 ───────────────────────────────────────────────
up:
	@echo "[broker] Starting all services..."
	docker compose --env-file $(CONFIG) up -d
	@echo "[broker] Started."
	@echo "[broker] SSL 인증서 발급은 수분 소요 — docker logs acme-companion -f 로 확인"

down:
	docker compose --env-file $(CONFIG) down

restart: down up

# ── 초기화 (최초 1회) ─────────────────────────────────────────
init:
	@chmod +x scripts/init-topics.sh
	@bash scripts/init-topics.sh

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
	@echo "=== REST Proxy ==="
	@curl -s http://localhost:8082/topics 2>/dev/null \
	    | python3 -m json.tool 2>/dev/null || echo "  not ready (nginx SSL 발급 전이면 정상)"
	@echo ""
	@echo "=== SSL certs ==="
	@docker exec nginx ls /etc/nginx/certs/ 2>/dev/null | sed 's/^/  /' || true

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

log-nginx:
	docker logs -f nginx --tail=100

log-ssl:
	docker logs -f acme-companion --tail=100

# ── 정리 ──────────────────────────────────────────────────────
clean:
	docker compose --env-file $(CONFIG) down -v --remove-orphans
	@echo "[broker] All containers and volumes removed."

# ── 도움말 ────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  daq-kafka-broker (KRaft + SSL + Consumer)"
	@echo "  ==========================================="
	@echo "  설정 파일: config/config.env"
	@echo ""
	@echo "  make up           전체 스택 시작"
	@echo "  make init         topic 초기화 (최초 1회, up 후 실행)"
	@echo "  make status       상태 확인"
	@echo "  make down         중지"
	@echo "  make restart      재시작"
	@echo "  make topics       topic 상세"
	@echo "  make metrics      consumer 메트릭"
	@echo "  make log-consumer consumer 로그"
	@echo "  make log-ssl      SSL 발급 로그"
	@echo "  make clean        전체 삭제 (볼륨 포함)"
	@echo ""
