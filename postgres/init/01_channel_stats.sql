-- Applied automatically on first PostgreSQL container initialization only.
-- See postgres/schema.sql for the full documented schema.

CREATE TABLE IF NOT EXISTS channel_stats (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_title TEXT,
    channel_description TEXT,
    total_views BIGINT,
    subscriber_count BIGINT,
    video_count BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_channel_stats_processed_at
    ON channel_stats (processed_at DESC);

CREATE OR REPLACE VIEW channel_stats_freshness AS
SELECT
    channel_id,
    channel_title,
    processed_at,
    CURRENT_TIMESTAMP - processed_at AS data_age,
    CASE
        WHEN processed_at >= CURRENT_TIMESTAMP - INTERVAL '6 hours' THEN 'fresh'
        ELSE 'stale'
    END AS freshness_status
FROM channel_stats;
