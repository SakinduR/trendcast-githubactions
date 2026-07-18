-- =============================================================================
-- YouTube Pipeline - channel_stats DDL
-- Location: postgres/schema.sql
-- =============================================================================
-- Run manually:
--   docker compose exec -T postgres psql -U airflow -d youtube_data -f /path/in/container
-- Or from host (after pipeline stack is up):
--   docker compose exec -T postgres psql -U airflow -d youtube_data < postgres/schema.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS channel_stats (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_title TEXT,
    channel_description TEXT,
    total_views BIGINT,
    subscriber_count BIGINT,
    video_count BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Supports freshness checks and temporal analysis (e.g. ORDER BY processed_at DESC)
CREATE INDEX IF NOT EXISTS idx_channel_stats_processed_at
    ON channel_stats (processed_at DESC);

-- Optional: identify stale records (channels not refreshed within 6 hours)
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
