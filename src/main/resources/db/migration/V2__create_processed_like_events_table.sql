-- Dedup ledger: lets the Kafka consumer apply each event's delta exactly once even
-- if Kafka redelivers it (at-least-once delivery + consumer retries).
CREATE TABLE IF NOT EXISTS processed_like_events (
    event_id     UUID PRIMARY KEY,
    video_id     BIGINT NOT NULL,
    amount       BIGINT NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_processed_like_events_video_id ON processed_like_events (video_id);
