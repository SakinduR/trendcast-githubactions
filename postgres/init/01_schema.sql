-- =============================================================================
-- PostgreSQL Schema Initialisation
-- File: postgres/init/01_schema.sql
-- Runs automatically on first container start (docker-entrypoint-initdb.d)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Extension: ensure we have UUID generation support (optional)
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- TABLE: channel_stats
-- Stores YouTube channel metadata and performance metrics.
-- Primary Key: channel_id (YouTube's globally unique channel identifier)
-- =============================================================================

CREATE TABLE IF NOT EXISTS channel_stats (
    -- Unique YouTube channel identifier (e.g. "UCxxxxxxxxxxxxxxxxxxxxxx")
    channel_id              VARCHAR(64)         PRIMARY KEY,

    -- Channel display name as shown on YouTube
    channel_title           VARCHAR(255)        NOT NULL,

    -- Channel "About" description (can be up to 5000 chars on YouTube)
    channel_description     TEXT,

    -- ISO 8601 UTC timestamp when the channel was created on YouTube
    published_at            TIMESTAMPTZ,

    -- ISO 3166-1 alpha-2 country code (e.g. "US", "IN", "GB")
    -- NULL if the channel owner has not set a country
    country                 VARCHAR(10),

    -- Cumulative lifetime view count across all videos
    -- BIGINT required: top channels exceed 2^31 views
    total_views             BIGINT              NOT NULL DEFAULT 0,

    -- Current subscriber count
    -- YouTube may hide exact counts for channels with fewer than 1000 subscribers
    -- BIGINT accommodates channels with 100M+ subscribers
    subscriber_count        BIGINT              NOT NULL DEFAULT 0,

    -- Total number of public videos uploaded to the channel
    video_count             INTEGER             NOT NULL DEFAULT 0,

    -- Timestamp when this record was last extracted from the YouTube API
    processed_at            TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    -- Timestamp when this row was first inserted into the database
    created_at              TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT chk_total_views_positive        CHECK (total_views >= 0),
    CONSTRAINT chk_subscriber_count_positive   CHECK (subscriber_count >= 0),
    CONSTRAINT chk_video_count_positive        CHECK (video_count >= 0)
);

-- =============================================================================
-- INDEXES — optimised for the most common query patterns
-- =============================================================================

-- Leaderboard queries: ORDER BY subscriber_count DESC
CREATE INDEX IF NOT EXISTS idx_channel_stats_subscribers
    ON channel_stats (subscriber_count DESC);

-- Time-series / freshness queries: ORDER BY processed_at DESC
CREATE INDEX IF NOT EXISTS idx_channel_stats_processed
    ON channel_stats (processed_at DESC);

-- Views leaderboard
CREATE INDEX IF NOT EXISTS idx_channel_stats_views
    ON channel_stats (total_views DESC);

-- Geo-filtering: WHERE country = 'US'
CREATE INDEX IF NOT EXISTS idx_channel_stats_country
    ON channel_stats (country);

-- =============================================================================
-- COMMENTS — document the table for pg_catalog introspection
-- =============================================================================
COMMENT ON TABLE channel_stats IS
    'Stores YouTube channel metadata and performance metrics, refreshed every 6 hours by the Airflow ETL pipeline.';

COMMENT ON COLUMN channel_stats.channel_id IS
    'YouTube globally unique channel identifier (UCxxxxxxxxxx format).';

COMMENT ON COLUMN channel_stats.total_views IS
    'Cumulative lifetime view count. BIGINT to handle channels exceeding 2^31 views.';

COMMENT ON COLUMN channel_stats.subscriber_count IS
    'Current subscriber count. May be rounded by YouTube for large channels.';

COMMENT ON COLUMN channel_stats.processed_at IS
    'UTC timestamp of the most recent successful API extraction. Used to detect stale records.';

-- =============================================================================
-- VIEW: channel_stats_enriched
-- Pre-computes engagement KPIs for use in Jupyter and BI tools
-- =============================================================================
CREATE OR REPLACE VIEW channel_stats_enriched AS
SELECT
    cs.channel_id,
    cs.channel_title,
    cs.channel_description,
    cs.published_at,
    cs.country,
    cs.total_views,
    cs.subscriber_count,
    cs.video_count,
    cs.processed_at,
    cs.created_at,

    -- Avg views per uploaded video (guarded against divide-by-zero)
    CASE
        WHEN cs.video_count > 0
        THEN ROUND(cs.total_views::NUMERIC / cs.video_count, 2)
        ELSE 0
    END AS avg_views_per_video,

    -- Views per subscriber (engagement depth metric)
    CASE
        WHEN cs.subscriber_count > 0
        THEN ROUND(cs.total_views::NUMERIC / cs.subscriber_count, 4)
        ELSE 0
    END AS views_per_subscriber,

    -- Engagement ratio: what proportion of viewers subscribed (%)
    CASE
        WHEN cs.total_views > 0
        THEN ROUND((cs.subscriber_count::NUMERIC / cs.total_views) * 100, 6)
        ELSE 0
    END AS engagement_ratio,

    -- Categorical channel tier based on subscriber count
    CASE
        WHEN cs.subscriber_count >= 1000000  THEN 'Mega (1M+)'
        WHEN cs.subscriber_count >= 100000   THEN 'Large (100K–1M)'
        WHEN cs.subscriber_count >= 10000    THEN 'Mid (10K–100K)'
        WHEN cs.subscriber_count >= 1000     THEN 'Small (1K–10K)'
        ELSE                                      'Micro (<1K)'
    END AS size_tier,

    -- Channel age in days since creation
    EXTRACT(DAY FROM NOW() - cs.published_at)::INTEGER AS channel_age_days

FROM channel_stats cs;

COMMENT ON VIEW channel_stats_enriched IS
    'Derived view exposing pre-computed engagement KPIs on top of channel_stats. Use in Jupyter notebooks and BI dashboards.';
