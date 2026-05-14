#!/bin/bash
# ============================================================
#  setup.sh  —  git clone 후 최초 1회 실행
#  red-poc 의존성 없음. 완전 자급자족.
#
#  사용법: bash setup.sh
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GRN='\033[0;32m'
YLW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GRN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YLW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo "================================================="
echo "  daq-kafka-broker 초기 설정"
echo "================================================="

# ── 1. Docker 확인 ──────────────────────────────────────────
echo ""
info "[1/5] Docker 확인..."
command -v docker &> /dev/null || error "Docker가 설치되어 있지 않습니다."
docker compose version &> /dev/null || error "Docker Compose v2가 필요합니다."
info "Docker $(docker --version | awk '{print $3}') 확인됨"

# ── 2. .env 설정 ────────────────────────────────────────────
echo ""
info "[2/5] .env 설정..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    warn ".env.example → .env 복사됨"
    echo ""
    echo "  ┌─────────────────────────────────────────┐"
    echo "  │  필수 입력 항목                          │"
    echo "  ├─────────────────────────────────────────┤"
    echo "  │  RED_EDGE_PUBLIC_URL                     │"
    echo "  │    외부 접근 URL                          │"
    echo "  │    예) https://1.2.3.4:8443              │"
    echo "  │                                          │"
    echo "  │  MINIO_ROOT_PASSWORD   MinIO 비밀번호    │"
    echo "  │  POSTGRES_PASSWORD     DB 비밀번호       │"
    echo "  │  GF_SECURITY_ADMIN_PASSWORD              │"
    echo "  │                        Grafana 비밀번호  │"
    echo "  │  GF_SERVER_ROOT_URL                      │"
    echo "  │    https://<IP>:<PORT>/poc/grafana/      │"
    echo "  └─────────────────────────────────────────┘"
    echo ""
    read -p "  지금 .env를 편집하시겠습니까? (y/N): " ans
    if [[ "$ans" == "y" || "$ans" == "Y" ]]; then
        ${EDITOR:-vi} .env
    else
        warn ".env 파일 편집 후 다시 setup.sh를 실행하세요."
        exit 0
    fi
else
    info ".env 확인됨"
fi

set -a; source .env; set +a

# ── 3. SSL 인증서 ───────────────────────────────────────────
echo ""
info "[3/5] SSL 인증서 확인..."
mkdir -p nginx/certs

if [ ! -f nginx/certs/fullchain.pem ] || [ ! -f nginx/certs/privkey.pem ]; then
    warn "인증서 없음 → self-signed 자동 생성"

    CN=$(echo "${RED_EDGE_PUBLIC_URL:-daq-broker}" | sed 's|https\?://||' | cut -d: -f1)

    openssl req -x509 -nodes -days 365 \
        -newkey rsa:2048 \
        -keyout nginx/certs/privkey.pem \
        -out nginx/certs/fullchain.pem \
        -subj "/C=KR/O=swm/CN=${CN}" \
        2>/dev/null

    info "self-signed 인증서 생성됨 (CN=${CN}, 365일)"
    warn "운영 환경에서는 정식 인증서로 교체하세요."
else
    info "SSL 인증서 확인됨"
fi

# ── 4. YOLO 모델 다운로드 ───────────────────────────────────
echo ""
info "[4/5] YOLO 모델 확인..."
mkdir -p models/yolo

if [ ! -f models/yolo/yolo26n.pt ]; then
    warn "yolo26n.pt 없음 → YOLOv8n pretrained 다운로드 (시간 소요)"

    docker run --rm \
        -v "$SCRIPT_DIR/models/yolo:/output" \
        ultralytics/ultralytics:latest \
        python3 -c "
from ultralytics import YOLO
import shutil
m = YOLO('yolov8n.pt')
shutil.copy(m.ckpt_path, '/output/yolo26n.pt')
print('saved to /output/yolo26n.pt')
"
    info "yolo26n.pt 다운로드 완료"
else
    info "yolo26n.pt 확인됨"
fi

# ── 5. ONNX export ──────────────────────────────────────────
echo ""
info "[5/5] Triton model.onnx 확인..."
mkdir -p triton/model_repository/yolo26n/1

if [ ! -f triton/model_repository/yolo26n/1/model.onnx ]; then
    warn "model.onnx 없음 → ONNX export 실행"

    EXPORTER_IMAGE=$(docker build -q ./triton/exporter)

    docker run --rm \
        -v "$SCRIPT_DIR/models/yolo:/source:ro" \
        -v "$SCRIPT_DIR/triton/model_repository:/models" \
        -e SRC=/source/yolo26n.pt \
        -e DEST_DIR=/models/yolo26n/1 \
        -e IMGSZ=640 \
        -e OPSET=12 \
        "$EXPORTER_IMAGE"

    info "model.onnx export 완료"
else
    info "model.onnx 확인됨"
fi

# ── 필수 디렉토리 ───────────────────────────────────────────
mkdir -p data/input logs

if [ ! -f data/input/imu_gps.jsonl ]; then
    echo '{"ts_ns":0,"lat":0,"lon":0,"alt":0}' > data/input/imu_gps.jsonl
    warn "data/input/imu_gps.jsonl 빈 파일 생성 (실제 GPS 데이터로 교체 권장)"
fi

# ── 완료 ────────────────────────────────────────────────────
echo ""
echo "================================================="
echo ""
echo -e "  ${GRN}✅ 초기 설정 완료${NC}"
echo ""
echo "  다음 단계:"
echo "    ./start.sh"
echo ""
echo "  외부 접근:"
echo "    Kafka REST : ${RED_EDGE_PUBLIC_URL}/poc/kafka-rest"
echo "    MinIO      : ${RED_EDGE_PUBLIC_URL}/poc/minio"
echo "    Grafana    : ${RED_EDGE_PUBLIC_URL}/poc/grafana"
echo ""
echo "  차량 config.env:"
echo "    BROKER_REST_URL=${RED_EDGE_PUBLIC_URL}/poc/kafka-rest"
echo "    BROKER_CLUSTER_ID=\"Some(red-poc-kraft-cluster)\""
echo "    BROKER_REST_TLS_VERIFY=false"
echo ""
echo "================================================="
