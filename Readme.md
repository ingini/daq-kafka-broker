# daq-kafka-broker

차량(AP500L 등)에서 수집한 센서 데이터를 Kafka REST Proxy로 수신하고, MinIO에 저장하며 Triton 추론까지 처리하는 통합 브로커 서버.

---

## 전체 아키텍처

```
[차량 - 외부망]
  daq-kafka-producer / start_cameras.sh
  BROKER_REST_URL=https://221.147.232.196:8443/poc/kafka-rest
  HTTP POST (REST Proxy v3, headers 포함)
         │
         │ HTTPS (self-signed TLS)
         ▼
[공인IP 221.147.232.196:8443]
  공유기 포트포워딩 → 192.168.1.79:8443
         │
         ▼
[서버 192.168.1.79]
  ┌──────────────────────────────────────────────────────┐
  │  nginx-edge (:8443)                                  │
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
        │  CLUSTER_ID:      │
        │  red-poc-kraft-   │
        │  cluster          │
        └────────┬──────────┘
                 │
         ┌───────▼────────┐
         │  topic-mapper  │ sensor.* → raw.*
         │  - 12B 헤더    │ (ts_ns 추출 + header 추가)
         │    제거        │
         └───────┬────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
    ▼            ▼            ▼
kafka-image-  kafka-frame-  kafka-imu-
consumer      bridge        archiver
(JPEG→MinIO)  (번들링)      (IMU/GPS→Parquet)
    │            │
    ▼            ▼
  minio       processor
  (저장)      (Triton 추론)
                 │
              notifier
              (이벤트 알림)
```

---

## 디렉토리 구조

```
daq-kafka-broker/
├── .env                          ← 설정 전부 여기 (이 파일만 수정)
├── docker-compose.yml
├── start.sh                      ← 기동
├── stop.sh                       ← 종료
├── copy-sources.sh               ← 최초 1회 소스 복사
│
├── nginx/
│   ├── edge.conf                 ← nginx 설정
│   └── certs/                    ← SSL 인증서 (사전 준비 필요)
│       ├── fullchain.pem
│       └── privkey.pem
│
├── kafka/
│   ├── init-topics.sh            ← Kafka topic 초기화
│   └── init-bucket.sh            ← MinIO 버킷 초기화
│
├── models/yolo/
│   └── yolo26n.pt                ← YOLO 모델 (사전 준비 필요)
│
├── triton/
│   ├── exporter/                 ← .pt → .onnx 변환기
│   └── model_repository/         ← Triton 모델 저장소
│       └── yolo26n/
│           ├── config.pbtxt
│           └── 1/model.onnx
│
├── data/input/
│   └── imu_gps.jsonl             ← GPS 데이터 (processor용)
│
├── processor/config/
│   └── application.yml           ← Processor 설정
│
├── notifier/config/
│   └── application.yml           ← Notifier 설정
│
├── kafka-image-consumer/         ← copy-sources.sh 로 복사
├── kafka-frame-bridge/           ← copy-sources.sh 로 복사
├── kafka-imu-archiver/           ← copy-sources.sh 로 복사
├── topic-mapper/                 ← copy-sources.sh 로 복사 (패치본)
├── mqtt-kafka-bridge/            ← copy-sources.sh 로 복사
├── processor/                    ← copy-sources.sh 로 복사
├── notifier/                     ← copy-sources.sh 로 복사
├── transcoder/                   ← copy-sources.sh 로 복사
├── postgres/init/                ← copy-sources.sh 로 복사
├── prometheus/                   ← copy-sources.sh 로 복사
├── grafana/                      ← copy-sources.sh 로 복사
└── logs/                         ← notifier 로그 출력
```

---

## 사전 준비

### 1. SSL 인증서 준비

기존 `red-poc-edge` 컨테이너에서 복사:

```bash
mkdir -p nginx/certs

docker cp red-poc-edge:/etc/nginx/certs/fullchain.pem nginx/certs/
docker cp red-poc-edge:/etc/nginx/certs/privkey.pem nginx/certs/
```

또는 self-signed 인증서 신규 생성:

```bash
openssl req -x509 -nodes -days 365 \
  -newkey rsa:2048 \
  -keyout nginx/certs/privkey.pem \
  -out nginx/certs/fullchain.pem \
  -subj "/C=KR/O=swm/CN=daq-broker"
```

> ⚠️ `privkey.pem`은 절대 git에 올리지 마세요. `.gitignore`에 등록되어 있습니다.

---

### 2. 소스 복사 (최초 1회)

`red-poc` 프로젝트 소스를 `daq-kafka-broker`로 복사합니다.  
`red-poc` 프로젝트가 `/home/swmai/project/kepco`에 있어야 합니다.

```bash
bash copy-sources.sh
```

복사되는 서비스:
- `kafka-image-consumer` `kafka-frame-bridge` `kafka-imu-archiver`
- `topic-mapper` (패치본 — 12B 헤더 제거 + ts_ns header 추가)
- `mqtt-kafka-bridge` `processor` `notifier` `transcoder`
- `postgres/init` `prometheus` `grafana`

---

### 3. YOLO 모델 준비

모델이 없는 경우 복사:

```bash
mkdir -p models/yolo
cp /home/swmai/project/kepco/data/models/yolo/yolo26n.pt models/yolo/
```

`triton/model_repository/yolo26n/1/model.onnx`가 이미 있으면 자동으로 export 단계를 건너뜁니다.

---

### 4. GPS 데이터 준비 (processor용)

```bash
mkdir -p data/input
cp /home/swmai/project/kepco/data/input/imu_gps.jsonl data/input/
```

---

### 5. `.env` 설정 확인

```bash
vi .env
```

필수 확인 항목:

```env
# 외부 접근 URL
RED_EDGE_PUBLIC_URL=https://221.147.232.196:8443

# MinIO 크레덴셜
MINIO_ROOT_USER=swm
MINIO_ROOT_PASSWORD=your_password_here

# Postgres
POSTGRES_PASSWORD=your_password_here

# Grafana
GF_SECURITY_ADMIN_PASSWORD=your_password_here
```

---

### 6. 공유기 포트포워딩 확인

```
외부포트 8443 → 192.168.1.79:8443
```

---

## 실행

### 기동

```bash
./start.sh
```

정상 기동 시:
```
=================================================
  ✅ daq-kafka-broker 통합 스택 기동 완료

  Kafka REST : https://221.147.232.196:8443/poc/kafka-rest
  MinIO      : https://221.147.232.196:8443/poc/minio
  Grafana    : https://221.147.232.196:8443/poc/grafana
  Triton     : http://192.168.1.79:8100/v2/models
=================================================
```

### 종료

```bash
./stop.sh
```

---

## 차량 설정

### `config/config.env` (daq-kafka-producer)

```env
VEHICLE_ID=AP500L-001
BROKER_REST_URL=https://221.147.232.196:8443/poc/kafka-rest
BROKER_CLUSTER_ID="Some(red-poc-kraft-cluster)"
BROKER_REST_TLS_VERIFY=false
```

### `config.env` (start_cameras.sh)

```env
VEHICLE_ID=Grandeur-2288
BROKER_REST_URL=https://221.147.232.196:8443/poc/kafka-rest
BROKER_CLUSTER_ID="Some(red-poc-kraft-cluster)"
BROKER_REST_TLS_VERIFY=false
```

---

## MinIO 저장 경로

```
daq/
└── year=YYYY/
    └── month=MM/
        └── day=DD/
            └── HH/
                └── {vehicle_id}/
                    ├── cam0/{YYYYMMDDTHHMMSS}_{ts_ns}.jpg
                    ├── cam1/{YYYYMMDDTHHMMSS}_{ts_ns}.jpg
                    ├── cam2/{YYYYMMDDTHHMMSS}_{ts_ns}.jpg
                    └── gnss/{ts_ns}.parquet
```

---

## 동작 확인

### REST Proxy 연결 테스트

```bash
# topic 목록
curl -k https://221.147.232.196:8443/poc/kafka-rest/topics

# 단건 POST 테스트
curl -k -X POST \
  "https://221.147.232.196:8443/poc/kafka-rest/v3/clusters/Some(red-poc-kraft-cluster)/topics/sensor.cam0.jpeg/records" \
  -H "Content-Type: application/json" \
  -d '{
    "key":   {"type":"BINARY","data":"R3JhbmRldXItMjI4OA=="},
    "value": {"type":"BINARY","data":"dGVzdA=="},
    "headers": [
      {"name":"vehicle_id","value":"R3JhbmRldXItMjI4OA=="},
      {"name":"sensor",    "value":"Y2FtMA=="},
      {"name":"ts_ns",     "value":"MTIzNDU2Nzg5MA=="}
    ]
  }'
```

### 로그 확인

```bash
docker logs -f red-poc-kafka-image-consumer   # MinIO 저장 현황
docker logs -f red-poc-topic-mapper           # sensor.* → raw.* 변환
docker logs -f red-poc-processor              # Triton 추론
docker logs -f red-poc-edge                   # nginx 접근 로그
docker compose ps                             # 전체 상태
```

### MinIO 저장 확인

```bash
docker run --rm --network red-poc-net \
  --entrypoint sh \
  minio/mc:RELEASE.2024-06-12T14-34-03Z \
  -c 'mc alias set local http://minio:9000 swm your_password && \
      mc ls --recursive local/daq | tail -10'
```

---

## 운영 중 설정 변경

### topic 추가

```bash
docker exec red-poc-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic sensor.lidar \
  --partitions 3 \
  --replication-factor 1
```

### nginx 설정 변경 (무중단)

```bash
vi nginx/edge.conf
docker exec red-poc-edge nginx -t
docker exec red-poc-edge nginx -s reload
```

### MinIO lifecycle (자동 삭제)

```bash
docker run --rm --network red-poc-net \
  --entrypoint sh \
  minio/mc:RELEASE.2024-06-12T14-34-03Z \
  -c 'mc alias set local http://minio:9000 swm your_password && \
      mc ilm add --expiry-days 30 local/daq'
```

---

## 트러블슈팅

| 증상 | 원인 | 조치 |
|------|------|------|
| POST → XML 에러 (MinIO) | nginx S3 block에 IP 등록 | `edge.conf` `server_name s3.swm.ai`만 남기기 |
| POST → 422 Unrecognized headers | REST Proxy v2 사용 중 | v3 API 사용 (`/v3/clusters/...`) |
| JPEG 깨짐 (12B 헤더 포함) | topic-mapper passthrough | `copy-sources.sh` 재실행 (패치본 복사) |
| MinIO 로그인 불가 | IAM 초기화 필요 | `rm -rf /data14/poc-minio/.minio.sys/config/iam/*` 후 재시작 |
| Kafka 기동 실패 | 볼륨 데이터 충돌 | `docker compose down -v` 후 재시작 |
| nginx 502 | 백엔드 컨테이너 미기동 | `docker compose ps` 확인 |
| SSL 인증서 오류 | certs/ 파일 없음 | `nginx/certs/` 인증서 복사 |

---

## 데이터 보존 정책

| 항목 | 기본값 | 설명 |
|------|--------|------|
| Kafka 보존 시간 | 72시간 | `.env` `KAFKA_LOG_RETENTION_HOURS` |
| Kafka 보존 용량 | 100GB | `.env` `KAFKA_LOG_RETENTION_BYTES` |
| JPEG topic 보존 | 1시간 | `kafka/init-topics.sh` |
| MinIO | 무제한 | mc ilm 으로 설정 가능 |

---

## .env 주요 설정 항목

| 항목 | 기본값 | 설명 |
|------|--------|------|
| `KAFKA_MESSAGE_MAX_BYTES` | 10MB | 메시지 최대 크기 |
| `FRAME_BRIDGE_BUNDLE_WINDOW_MS` | 1000 | 프레임 번들 윈도우(ms) |
| `IMU_ARCHIVER_BATCH_SIZE` | 1000 | IMU parquet flush 배치 |
| `PROCESSOR_CONFIDENCE_THRESHOLD` | 0.4 | YOLO 신뢰도 임계값 |
| `PROCESSOR_VISUALIZE_ENABLED` | true | bbox 시각화 저장 여부 |
| `NOTIFIER_KIND` | file_log | 알림 방식 (file_log/slack) |
| `DAQ_CONSUMER_MAX_WORKERS` | 4 | MinIO 업로드 동시 worker |
