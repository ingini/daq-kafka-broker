# daq-kafka-broker

서버 12번 (192.168.1.12) 에 띄우는 Kafka 풀스택.

## 구성

```
[차량 AP500L - 외부망]
  daq-kafka-producer
    HTTP POST https://swm-daq.ddns.net/topics/{topic}
         ↓
[서버 12번 - 192.168.1.12]
  nginx :443  (Let's Encrypt SSL 자동발급)
    ↓ 프록시
  rest-proxy :8082
    ↓ produce
  kafka (KRaft, Zookeeper 없음) :9092
    ↓ consume
  daq-consumer
    transform (camera 헤더 파싱, GNSS 필드 변환)
         ↓ PUT
[서버 151번 - MinIO]
  bucket: daq
  경로: year=YYYY/month=MM/day=dd/{vehicle_id}/{sensor}/{ts_ns}.jpg
```

## 디렉토리 구조

```
daq-kafka-broker/
├── Makefile
├── docker-compose.yml
├── config/
│   └── config.env          ← 이것만 수정
├── nginx/
│   └── custom.conf
├── scripts/
│   └── init-topics.sh
└── daq-consumer/
    ├── Dockerfile
    ├── requirements.txt
    └── consumer.py
```

## 사전 조건

공유기 포트포워딩 3개:
```
80  → 192.168.1.12:80    (Let's Encrypt HTTP-01 챌린지)
443 → 192.168.1.12:443   (차량 HTTPS POST)
```

## 실행

```bash
# 1. 설정 수정
vi config/config.env    # MINIO_ACCESS_KEY, MINIO_SECRET_KEY 입력

# 2. 전체 기동
make up

# 3. topic 초기화 (최초 1회, broker 기동 후)
make init

# 4. SSL 발급 확인 (수분 소요)
make log-ssl

# 5. 상태 확인
make status
```

## 차량 설정

`daq-kafka-producer/config/config.env`:
```env
BROKER_REST_URL=https://swm-daq.ddns.net
```

SSL 발급 전 테스트 시:
```env
BROKER_REST_URL=http://swm-daq.ddns.net:8082  # 임시, nginx 우회
```
단, 이 경우 공유기에서 8082 포트포워딩 추가 필요.

## MinIO 저장 경로

```
daq/
└── year=2026/
    └── month=05/
        └── day=13/
            └── AP500L-001/
                ├── cam0/
                │   └── 1747123456789000000.jpg
                ├── cam1/
                ├── cam2/
                └── gnss/
                    └── 1747123456789000000.json
```

Hive-style 파티션 경로로 Spark/Trino 파티션 pruning 가능.

## 모니터링

```bash
make log-consumer          # consumer 로그
make metrics               # Prometheus 메트릭
make topics                # topic 상세

# consumer 메트릭 주요 지표
daq_consumer_records_in_total          # Kafka에서 consume 수
daq_consumer_records_dropped_total     # 파싱 실패 수
daq_consumer_minio_put_ok_total        # MinIO PUT 성공
daq_consumer_minio_put_error_total     # MinIO PUT 실패
daq_consumer_queue_depth               # 현재 대기 중인 건수
```

## 트러블슈팅

| 증상 | 원인 / 조치 |
|------|-------------|
| SSL 발급 실패 | 80포트 포트포워딩 확인. `make log-ssl` |
| consumer `too_short` drop | 카메라 포맷 불일치. `daq-service/server.py` `_send_kafka()` 확인 |
| consumer `missing_ts_ns` drop | GNSS 파서 `ts_ns` 필드 누락 |
| MinIO PUT 실패 | 151번 접근 가능 여부. `MINIO_ACCESS_KEY/SECRET_KEY` 확인 |
| REST Proxy 400 | topic 미생성. `make init` 실행 |
