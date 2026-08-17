-- =============================================================================
-- Migration: Add video metadata columns + derived tier_category view field
-- File: postgres/migrations/002_add_video_metadata.sql
-- Run manually against the live Supabase database (idempotent — safe to
-- re-run). Mirrors the equivalent block already added to
-- postgres/init/01_schema.sql, which remains the source of truth for a
-- fresh DB init.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. New video-level metadata columns, populated by Job 1 (videos.list) and
--    backfilled for existing rows by backfill_video_metadata.py.
-- ---------------------------------------------------------------------------
ALTER TABLE videos
    ADD COLUMN IF NOT EXISTS title           VARCHAR(255),
    ADD COLUMN IF NOT EXISTS description     TEXT,
    ADD COLUMN IF NOT EXISTS thumbnail_url   TEXT,
    ADD COLUMN IF NOT EXISTS tags            TEXT[],
    ADD COLUMN IF NOT EXISTS category_id     VARCHAR(16),
    ADD COLUMN IF NOT EXISTS duration        VARCHAR(32);

-- ---------------------------------------------------------------------------
-- 2. channel_stats_enriched — add tier_category as a derived field (mode of
--    category_id across the channel's videos). View logic only, so it
--    recomputes automatically as new videos arrive; no ETL writes this.
-- ---------------------------------------------------------------------------
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
    EXTRACT(DAY FROM NOW() - cs.published_at)::INTEGER AS channel_age_days,

    -- Most common video category_id for this channel (mode), derived from
    -- the videos table so it recomputes automatically as new videos arrive.
    -- NULL for channels with no categorized videos.
    (
        SELECT v.category_id
        FROM videos v
        WHERE v.channel_id = cs.channel_id
          AND v.category_id IS NOT NULL
        GROUP BY v.category_id
        ORDER BY COUNT(*) DESC, v.category_id ASC
        LIMIT 1
    ) AS tier_category

FROM channel_stats cs;

COMMENT ON VIEW channel_stats_enriched IS
    'Derived view exposing pre-computed engagement KPIs on top of channel_stats. Use in Jupyter notebooks and BI dashboards.';
