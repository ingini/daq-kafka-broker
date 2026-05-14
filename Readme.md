# daq-kafka-broker

차량(AP500L 등)에서 수집한 센서 데이터를 Kafka REST Proxy로 수신하고,  
MinIO에 저장하며 YOLO 기반 Triton 추론까지 처리하는 통합 브로커 서버.

---

## 전체 아키텍처

```
[차량 - 외부망]
  daq-kafka-producer / start_cameras.sh
  BROKER_REST_URL=https://<PUBLIC_IP>:<PORT>/poc/kafka-rest
  HTTP POST (REST Proxy v3, headers 포함)
         │
         │ HTTPS (self-signed TLS)
         ▼
[공인IP <PUBLIC_IP>:<PORT>]
  공유기 포트포워딩 → <SERVER_IP>:<PORT>
         │
         ▼
[서버 <SERVER_IP>]
  ┌──────────────────────────────────────────────────────┐
  │  nginx-edge (:<PORT>)                                │
  │    /poc/kafka-rest/ → kafka-rest-proxy:8082          │
  │    /poc/minio/      → minio:9001 (Console)           │
  │    /poc/grafana/    → grafana:3000                   │
  │    /               → minio:9000 (S3 API)             │
  └──────────────┬───────────────────────────────────────┘
                 │
        ┌────────▼──────────┐
        │  kafka-rest-proxy │ cp-kafka-rest:7.5.0
        └────────┬──────────┘
                 │ produce
        ┌────────▼──────────┐
        │  kafka (KRaft)    │ apache/kafka:3.7.0
        │  CLUSTER_ID:      │ KAFKA_AUTO_CREATE=false
        │  red-poc-kraft-   │
        │  cluster          │
        └────────┬──────────┘
                 │
         ┌───────▼────────┐
         │  topic-mapper  │ sensor.* → raw.*
         │  ① 12B 헤더    │ [8B ts_ns][4B len][JPEG]
         │    제거        │ → 순수 JPEG + ts_ns header
         │  ② schema 변환 │ gnss: lat/lon/alt 정규화
         └───────┬────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
    ▼            ▼            ▼
kafka-image-  kafka-frame-  kafka-imu-
consumer      bridge        archiver
(JPEG→MinIO)  (프레임 번들) (IMU/GPS→Parquet)
    │            │
    ▼            ▼
  MinIO       processor ──── Triton
  저장        (YOLO 추론)    (yolov8n ONNX)
                 │
              notifier
              (이벤트 알림)
              file_log / Slack
```

---

## 서비스 역할

| 서비스 | 역할 |
|--------|------|
| `kafka` | 메시지 브로커 (KRaft, ZooKeeper 없음) |
| `kafka-rest-proxy` | 차량 HTTP POST 수신 |
| `topic-mapper` | `sensor.*` → `raw.*` 변환 (12B 헤더 제거, schema 변환) |
| `kafka-image-consumer` | `raw.frame.images` → MinIO JPEG 저장 |
| `kafka-frame-bridge` | 프레임 번들링 → `raw.frame.metadata.decoded` |
| `kafka-imu-archiver` | IMU/GPS → MinIO Parquet 저장 |
| `triton` | YOLO ONNX 추론 서버 |
| `processor` | Triton 추론 결과 → `events.detection` |
| `notifier` | 이벤트 감지 → file_log / Slack 알림 |
| `minio` | 오브젝트 스토리지 |
| `postgres` | 이벤트 메타데이터 DB |
| `nginx-edge` | SSL termination + 라우팅 |

---

## Processor 상세

### 역할
```
raw.frame.metadata.decoded (Kafka)
  → MinIO에서 JPEG 가져오기
  → Triton YOLO 추론
  → bbox 시각화 (annotated JPEG → MinIO 저장)
  → events.detection (Kafka) 발행
  → notifier 트리거
```

### 설정 (`processor/config/application.yml`)

```yaml
inference:
  kind: triton                          # triton | mock
  triton:
    confidence_threshold: 0.4           # 검출 신뢰도 임계값 (0.0~1.0)
    timeout_ms: 5000                    # 추론 타임아웃
  allowed_classes: []                   # 빈 배열 = COCO 80클래스 전부
                                        # 예: ["car", "person", "bus", "truck"]

visualize:
  enabled: true                         # bbox 그린 결과 MinIO 저장
  jpeg_quality: 85
  subdir: result                        # 저장 경로: .../cam0/result/...
  key_suffix: _annotated.jpg

gps:
  kind: file                            # file | kafka | none
  file_path: /app/imu_gps.jsonl        # kind=file 일 때 사용
```

### .env에서 조정 가능한 항목

```env
PROCESSOR_CONFIDENCE_THRESHOLD=0.4     # 신뢰도 임계값
PROCESSOR_TRITON_TIMEOUT_MS=5000       # 추론 타임아웃
PROCESSOR_VISUALIZE_ENABLED=true       # 시각화 결과 저장
PROCESSOR_GPS_KIND=file                # GPS 소스 (file/kafka/none)
```

---

## Notifier 상세

### 역할
```
events.detection (Kafka)
  → Postgres 저장
  → file_log 또는 Slack 알림
  → presigned URL로 annotated JPEG 링크 포함
```

### Slack 설정 방법

**1. Slack App 생성**
```
https://api.slack.com/apps
→ Create New App → From scratch
→ App Name: daq-notifier
→ Workspace 선택
```

**2. Bot Token 권한 추가**
```
OAuth & Permissions → Bot Token Scopes
→ chat:write 추가
→ Install to Workspace
→ Bot User OAuth Token 복사 (xoxb-...)
```

**3. 채널에 Bot 초대**
```
Slack 채널에서: /invite @daq-notifier
```

**4. .env 설정**
```env
NOTIFIER_KIND=slack
NOTIFIER_SLACK_TOKEN=xoxb-xxxxxxxxxxxx-xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxxxx
NOTIFIER_SLACK_CHANNEL=#road-events
```

### file_log 모드 (기본)

Slack 없이 파일로 로그:
```env
NOTIFIER_KIND=file_log
```

로그 확인:
```bash
tail -f logs/messages.jsonl | python3 -m json.tool
```

---

## 디렉토리 구조

```
daq-kafka-broker/
├── .env                          ← 설정 전부 여기 (git 제외)
├── .env.example                  ← 템플릿 (git 포함)
├── .gitignore
├── setup.sh                      ← 최초 1회 실행 (자동 설정)
├── start.sh                      ← 기동
├── stop.sh                       ← 종료
│
├── nginx/
│   ├── edge.conf
│   └── certs/                    ← git 제외, setup.sh 자동 생성
│       ├── fullchain.pem
│       └── privkey.pem
│
├── kafka/
│   ├── init-topics.sh
│   └── init-bucket.sh
│
├── models/yolo/
│   └── yolo26n.pt                ← git 제외, setup.sh 자동 다운로드
│
├── triton/
│   ├── exporter/
│   └── model_repository/         ← git 제외, setup.sh 자동 생성
│
├── data/input/
│   └── imu_gps.jsonl             ← git 제외, setup.sh 자동 생성
│
├── processor/config/application.yml
├── notifier/config/application.yml
│
├── kafka-image-consumer/
├── kafka-frame-bridge/
├── kafka-imu-archiver/
├── topic-mapper/                 ← 패치본 (12B 헤더 제거)
├── mqtt-kafka-bridge/
├── processor/
├── notifier/
├── transcoder/
├── postgres/init/
├── prometheus/
├── grafana/
└── logs/                         ← git 제외
```

---

## 빠른 시작 (새 서버)

```bash
# 1. clone
git clone https://github.com/ingini/daq-kafka-broker.git
cd daq-kafka-broker

# 2. 자동 설정 (인증서 생성, 모델 다운로드 등)
bash setup.sh

# 3. 기동
./start.sh
```

---

## 사전 준비 (수동)

### 1. `.env` 설정

```bash
cp .env.example .env
vi .env
```

필수 입력:
```env
KAFKA_PUBLIC_URL=https://<PUBLIC_IP>:<PORT>
MINIO_ROOT_PASSWORD=<비밀번호>
POSTGRES_PASSWORD=<비밀번호>
GF_SECURITY_ADMIN_PASSWORD=<비밀번호>
GF_SERVER_ROOT_URL=https://<PUBLIC_IP>:<PORT>/poc/grafana/
```

### 2. SSL 인증서

신규 self-signed 생성:
```bash
openssl req -x509 -nodes -days 365 \
  -newkey rsa:2048 \
  -keyout nginx/certs/privkey.pem \
  -out nginx/certs/fullchain.pem \
  -subj "/C=KR/O=swm/CN=daq-broker"
```

### 3. YOLO 모델

자동 다운로드:
```bash
docker run --rm \
  -v "$PWD/models/yolo:/output" \
  ultralytics/ultralytics:latest \
  python3 -c "
from ultralytics import YOLO
import shutil
m = YOLO('yolov8n.pt')
shutil.copy(m.ckpt_path, '/output/yolo26n.pt')
"
```

### 4. 공유기 포트포워딩

```
외부포트 <PORT> → <SERVER_IP>:<PORT>
```

---

## 실행

```bash
./start.sh    # 기동
./stop.sh     # 종료
```

---

## 차량 설정

### `config/config.env` (daq-kafka-producer / start_cameras.sh)

```env
VEHICLE_ID=<차량 ID>
BROKER_REST_URL=https://<PUBLIC_IP>:<PORT>/poc/kafka-rest
BROKER_CLUSTER_ID="Some(red-poc-kraft-cluster)"
BROKER_REST_TLS_VERIFY=false
```

---

## MinIO 저장 경로

```
daq/
└── year=YYYY/month=MM/day=DD/HH/
    └── {vehicle_id}/
        ├── cam0/{YYYYMMDDTHHMMSS}_{ts_ns}.jpg
        ├── cam1/{YYYYMMDDTHHMMSS}_{ts_ns}.jpg
        ├── cam2/{YYYYMMDDTHHMMSS}_{ts_ns}.jpg
        ├── cam0/result/{ts}_annotated.jpg   ← processor 결과
        └── gnss/{ts_ns}.parquet
```

---

## 동작 확인

```bash
# REST Proxy 테스트
curl -k https://<PUBLIC_IP>:<PORT>/poc/kafka-rest/topics

# 로그
docker logs -f kafka-topic-mapper
docker logs -f kafka-kafka-image-consumer
docker logs -f kafka-processor
docker compose ps

# MinIO 저장 확인
docker run --rm --network daq-net \
  --entrypoint sh minio/mc:RELEASE.2024-06-12T14-34-03Z \
  -c 'mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && \
      mc ls --recursive local/daq | tail -10'
```

---

## 트러블슈팅

| 증상 | 원인 | 조치 |
|------|------|------|
| POST → XML 에러 (MinIO) | nginx S3 block에 IP 등록 | `edge.conf` `server_name s3.swm.ai`만 남기기 |
| POST → 422 headers 에러 | REST Proxy v2 사용 | v3 API 사용 (`/v3/clusters/...`) |
| JPEG 깨짐 | topic-mapper 12B 헤더 미제거 | topic-mapper 패치본 확인 |
| MinIO 로그인 불가 | IAM 초기화 필요 | `rm -rf <minio_data>/.minio.sys/config/iam/*` |
| Kafka 기동 실패 | 볼륨 충돌 | `docker compose down -v` 후 재시작 |
| nginx 502 | 백엔드 미기동 | `docker compose ps` 확인 |
| Triton 추론 실패 | model.onnx 없음 | `bash setup.sh` 재실행 |
| Slack 알림 안 옴 | Token/채널 설정 오류 | `.env` `NOTIFIER_SLACK_TOKEN` 확인, 채널에 Bot 초대 |

---

## .env 주요 항목

| 항목 | 필수 | 설명 |
|------|------|------|
| `KAFKA_BROKER_PUBLIC_URL` | ✅ | 외부 접근 URL |
| `MINIO_ROOT_USER` | ✅ | MinIO 계정 |
| `MINIO_ROOT_PASSWORD` | ✅ | MinIO 비밀번호 |
| `POSTGRES_PASSWORD` | ✅ | DB 비밀번호 |
| `GF_SECURITY_ADMIN_PASSWORD` | ✅ | Grafana 비밀번호 |
| `NOTIFIER_KIND` | - | `file_log` / `slack` (기본: file_log) |
| `NOTIFIER_SLACK_TOKEN` | - | Slack Bot Token (xoxb-...) |
| `NOTIFIER_SLACK_CHANNEL` | - | Slack 채널 (기본: #road-events) |
| `PROCESSOR_CONFIDENCE_THRESHOLD` | - | YOLO 신뢰도 (기본: 0.4) |
| `PROCESSOR_VISUALIZE_ENABLED` | - | bbox 시각화 저장 (기본: true) |
| `KAFKA_MESSAGE_MAX_BYTES` | - | 메시지 최대 크기 (기본: 10MB) |
| `KAFKA_LOG_RETENTION_HOURS` | - | Kafka 보존 시간 (기본: 72h) |
