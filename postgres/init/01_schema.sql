-- Minimal schema for POC. Full schema per docs/05_data_schema.md §5 is
-- added in a later migration; this bootstrap only creates the event
-- table the notifier needs so the stack comes up healthy.

CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    vehicle_id      TEXT NOT NULL,
    bundle_id       TEXT NOT NULL,
    capture_ts_ns   BIGINT NOT NULL,
    detection_class TEXT,
    confidence      DOUBLE PRECISION,
    gps_lat         DOUBLE PRECISION,
    gps_lon         DOUBLE PRECISION,
    image_uris      JSONB,
    notify_status   TEXT NOT NULL DEFAULT 'PENDING',
    slack_message_ts TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_vehicle_time
    ON events (vehicle_id, capture_ts_ns DESC);

CREATE INDEX IF NOT EXISTS idx_events_notify_status
    ON events (notify_status)
    WHERE notify_status <> 'SENT';

-- ---------------------------------------------------------------------
-- sent_messages: 실제로 알림 채널로 내보낸 메시지의 원문 기록.
-- events 와 1:N (채널이 여러 개이거나 재발송될 수 있음).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sent_messages (
    id            BIGSERIAL PRIMARY KEY,
    event_id      TEXT NOT NULL,
    channel       TEXT NOT NULL,          -- 'slack' | 'file_log' | 'noop'
    sent_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    message_text  TEXT NOT NULL,          -- 사람이 읽는 본문
    message_json  JSONB,                  -- 구조화 페이로드 (Block Kit 등)
    provider_ref  TEXT,                   -- Slack ts / 파일 경로:행번호 등
    status        TEXT NOT NULL DEFAULT 'SENT'  -- SENT | FAILED
);

CREATE INDEX IF NOT EXISTS idx_sent_messages_event
    ON sent_messages (event_id);
CREATE INDEX IF NOT EXISTS idx_sent_messages_time
    ON sent_messages (sent_at DESC);
