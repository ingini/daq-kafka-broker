# daq-kafka-broker

차량(AP500L)에서 수집한 센서 데이터를 Kafka로 수신하고 MinIO에 저장하는 브로커 서버.

## 전체 구조

```
[차량 AP500L - 외부망]
  daq-kafka-producer
    daq-service._send_kafka()
    BROKER_REST_URL=https://221.147.232.196:8443/poc/kafka-rest
    HTTP POST
      sensor.cam0.jpeg  ← JPEG + ts_ns 헤더
      sensor.cam1.jpeg
      sensor.cam2.jpeg
      sensor.gnss       ← NovAtel INSPVAXB/BESTGNSSPOS JSON
           │
           │ HTTPS (self-signed, TLS verify=false)
           ▼
[공인IP 221.147.232.196:8443]
  공유기 포트포워딩
  8443 → 192.168.1.66:8443
           │
           ▼
[서버 66번 - 192.168.1.66]
  ┌─────────────────────────────────────────┐
  │  nginx-edge (:8443)                     │
  │    /poc/kafka-rest/ → kafka-rest-proxy  │
  │    /poc/minio/      → minio             │
  └──────────────┬──────────────────────────┘
                 │
        ┌────────▼────────┐
        │ kafka-rest-proxy│  (:8082 내부)
        └────────┬────────┘
                 │ produce
        ┌────────▼────────┐
        │  kafka (KRaft)  │  (:9092 내부)
        │  topic 자동생성 │
        └────────┬────────┘
                 │ consume
        ┌────────▼────────┐
        │  daq-consumer   │
        │  transform:     │
        │  cam → JPEG     │
        │  gnss → JSON    │
        └────────┬────────┘
                 │ PUT
        ┌────────▼────────┐
        │     minio       │  (:9000 S3 API, :9001 Console)
        │  bucket: daq    │
        └─────────────────┘
```

## MinIO 저장 경로

```
daq/
└── year=YYYY/
    └── month=MM/
        └── day=DD/
            └── {vehicle_id}/
                ├── cam0/{ts_ns}.jpg
                ├── cam1/{ts_ns}.jpg
                ├── cam2/{ts_ns}.jpg
                └── gnss/{ts_ns}.json
```

Hive-style 파티션 경로로 Spark/Trino 파티션 pruning 가능.

## 디렉토리 구조

```
daq-kafka-broker/
├── .env                    ← 설정 전부 여기 (이 파일만 수정)
├── docker-compose.yml
├── start.sh                ← 기동 스크립트
├── stop.sh                 ← 종료 스크립트
├── README.md
├── nginx/
│   ├── default.conf        ← nginx 설정 (79번 설정 그대로)
│   └── certs/
│       ├── fullchain.pem   ← SSL 인증서 (79번에서 복사)
│       └── privkey.pem     ← SSL 개인키 (79번에서 복사)
└── daq-consumer/
    ├── Dockerfile
    ├── requirements.txt
    └── consumer.py
```

## 컨테이너 목록

| 컨테이너명 | 이미지 | 역할 | 포트 |
|-----------|--------|------|------|
| kafka | cp-kafka:7.6.1 | Kafka KRaft broker | 내부 9092 |
| kafka-rest-proxy | cp-kafka-rest:7.6.1 | REST Proxy | 내부 8082 |
| minio | minio:2024-06-13 | 오브젝트 스토리지 | 9000, 9001 |
| daq-consumer | daq-consumer:latest | topic consume → MinIO | - |
| nginx-edge | nginx:1.27-alpine | SSL termination + 리버스 프록시 | 8443, 8080 |

## 사전 준비

### 1. SSL 인증서 복사 (79번 서버에서)

```bash
# 79번 서버에서 실행
docker cp red-poc-edge:/etc/nginx/certs/fullchain.pem ./fullchain.pem
docker cp red-poc-edge:/etc/nginx/certs/privkey.pem ./privkey.pem

# 66번 서버로 전송
scp fullchain.pem privkey.pem swm@192.168.1.66:~/daq-kafka-broker/daq-kafka-broker/nginx/certs/
```

### 2. 공유기 포트포워딩 변경

공유기 관리페이지에서 8443 포트포워딩을 79번 → 66번으로 변경:

```
변경 전: 8443 → 192.168.1.79:8443
변경 후: 8443 → 192.168.1.66:8443
```

### 3. `.env` 수정

```bash
vi .env
```

```env
MINIO_ACCESS_KEY=swm
MINIO_SECRET_KEY=실제시크릿키
MINIO_BUCKET=daq
KAFKA_TOPICS=sensor.cam0.jpeg,sensor.cam1.jpeg,sensor.cam2.jpeg,sensor.gnss
```

### 4. daq-consumer 사전 빌드 (arm64 서버라 시간 소요)

```bash
docker compose build daq-consumer
```

> arm64 서버의 경우 `confluent-kafka` 패키지가 소스 컴파일되어 최초 빌드에
> 10~20분 소요됩니다. 이후 재기동 시에는 캐시를 사용하여 빠르게 됩니다.

## 실행

### 기동

```bash
./start.sh
```

정상 기동 시 출력:
```
=================================================
  ✅ daq-kafka-broker 기동 완료

  내부 접근:
    REST Proxy : http://192.168.1.66:8082
    MinIO API  : http://192.168.1.66:9000
    MinIO UI   : http://192.168.1.66:9001

  외부 접근 (nginx :8443):
    Kafka REST : https://221.147.232.196:8443/poc/kafka-rest
    MinIO      : https://221.147.232.196:8443/poc/minio
=================================================
```

### 종료

```bash
./stop.sh
```

### 재시작

```bash
./stop.sh && ./start.sh
```

## 동작 확인

### 서버에서 확인

```bash
# 컨테이너 상태
docker compose ps

# REST Proxy 응답 확인
curl http://localhost:8082/topics

# Kafka topic 목록
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list

# consumer 로그
docker logs -f daq-consumer

# nginx 로그
docker logs -f nginx-edge

# MinIO 헬스체크
curl http://localhost:9000/minio/health/live
```

### 외부에서 확인

```bash
# REST Proxy 외부 접근 (차량 환경 모의)
curl -k https://221.147.232.196:8443/poc/kafka-rest/topics

# MinIO 외부 접근
curl -k https://221.147.232.196:8443/poc/minio/health/live

# 브라우저에서 MinIO Console
https://221.147.232.196:8443/poc/minio
```

### 차량에서 수동 produce 테스트

```bash
# GNSS topic 테스트
curl -k -X POST https://221.147.232.196:8443/poc/kafka-rest/topics/sensor.gnss \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{
    "records": [{
      "key": "AP500L-001/1747000000000000000",
      "value": {
        "ts_ns": 1747000000000000000,
        "type": "gnss_pos",
        "latitude": 37.5665,
        "longitude": 126.9780,
        "height": 50.0
      },
      "headers": [
        {"key": "vehicle_id", "value": "AP500L-001"},
        {"key": "sensor", "value": "gnss"}
      ]
    }]
  }'
```

MinIO Console에서 저장 확인:
```
https://221.147.232.196:8443/poc/minio
bucket: daq
경로: year=YYYY/month=MM/day=DD/AP500L-001/gnss/
```

## 차량 설정 (daq-kafka-producer)

`config/config.env`:

```env
VEHICLE_ID=AP500L-001
BROKER_REST_URL=https://221.147.232.196:8443/poc/kafka-rest
BROKER_TOPIC_CAM0=sensor.cam0.jpeg
BROKER_TOPIC_CAM1=sensor.cam1.jpeg
BROKER_TOPIC_CAM2=sensor.cam2.jpeg
BROKER_TOPIC_GNSS=sensor.gnss
```

> `daq-service`는 USB SSD 연결 시 로컬 저장,
> USB 없을 시 자동으로 Kafka REST Proxy로 전송합니다.

## topic 추가

`.env`의 `KAFKA_TOPICS`에 추가 후 재시작:

```env
KAFKA_TOPICS=sensor.cam0.jpeg,sensor.cam1.jpeg,sensor.cam2.jpeg,sensor.gnss,sensor.lidar
```

```bash
./stop.sh && ./start.sh
```

`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` 설정으로 topic 자동 생성됩니다.

## 트러블슈팅

| 증상 | 원인 | 조치 |
|------|------|------|
| nginx 502 Bad Gateway | rest-proxy 또는 minio 미기동 | `docker compose ps` 확인 |
| SSL 인증서 오류 | certs/ 파일 없음 | 79번에서 인증서 복사 |
| Kafka 기동 실패 | 볼륨 데이터 충돌 | `docker compose down -v` 후 재시작 |
| consumer `too_short` drop | 카메라 포맷 불일치 | `daq-service/server.py` `_send_kafka()` 확인 |
| consumer `missing_ts_ns` | GNSS `ts_ns` 필드 누락 | NovAtel 파서 확인 |
| MinIO PUT 실패 | 버킷 없음 또는 인증 오류 | `.env` 키 확인, MinIO Console 접속 |
| 차량 연결 실패 | 포트포워딩 미변경 | 공유기 8443 → 192.168.1.66으로 변경 |

## 로그 확인

```bash
docker logs -f kafka          # Kafka broker
docker logs -f kafka-rest-proxy  # REST Proxy
docker logs -f daq-consumer   # Consumer (MinIO 저장 현황)
docker logs -f nginx-edge     # nginx 접근 로그
docker logs -f minio          # MinIO
```

## 데이터 보존 정책

| 설정 | 값 |
|------|-----|
| Kafka 보존 시간 | 72시간 |
| Kafka 보존 용량 | 100GB |
| 메시지 최대 크기 | 10MB |
| MinIO 보존 | 무제한 (수동 관리) |:
