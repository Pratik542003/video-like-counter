CREATE TABLE IF NOT EXISTS video_like_counter (
    video_id    BIGINT PRIMARY KEY,
    like_count  BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_video_like_counter_like_count ON video_like_counter (like_count);
CREATE INDEX IF NOT EXISTS idx_video_like_counter_updated_at ON video_like_counter (updated_at);
