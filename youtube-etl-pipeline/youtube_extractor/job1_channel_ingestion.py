"""
================================================================================
job1_channel_ingestion.py — Standalone Script (GitHub Actions)
================================================================================
Schedule  : Every 12 hours (0 */12 * * *)
Purpose   : Read active YouTube channel seeds from Supabase PostgreSQL,
            simulate recent upload discovery, and upsert the latest videos
            into the polling queue.

Environment Variables:
    SUPABASE_DB_URL   — PostgreSQL connection string
    YOUTUBE_API_KEY   — YouTube Data API v3 key (reserved for future real API)
================================================================================
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import psycopg2

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL Templates
# ---------------------------------------------------------------------------
UPSERT_VIDEOS_SQL = """
INSERT INTO videos (
    video_id,
    channel_id,
    published_at,
    status,
    last_polled_at,
    next_poll_at,
    current_interval_hours
) VALUES (
    %(video_id)s,
    %(channel_id)s,
    %(published_at)s,
    'active',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    1
)
ON CONFLICT (video_id) DO UPDATE SET
    channel_id = EXCLUDED.channel_id,
    published_at = EXCLUDED.published_at,
    status = 'active',
    last_polled_at = CURRENT_TIMESTAMP,
    next_poll_at = CURRENT_TIMESTAMP,
    current_interval_hours = videos.current_interval_hours;
"""


# ---------------------------------------------------------------------------
# Step 1: Load active channel seeds from PostgreSQL
# ---------------------------------------------------------------------------
def get_active_channels(conn) -> List[Dict[str, str]]:
    """Load active channel playlist seeds from PostgreSQL."""

    query = """
        SELECT channel_id, uploads_playlist_id
        FROM channel_stats
        WHERE uploads_playlist_id IS NOT NULL
          AND uploads_playlist_id <> ''
        ORDER BY channel_id
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    channels = [
        {"channel_id": row[0], "uploads_playlist_id": row[1]}
        for row in rows
    ]
    log.info("Loaded %d active channel seeds", len(channels))
    return channels


# ---------------------------------------------------------------------------
# Step 2: Simulate latest uploads for a channel
# ---------------------------------------------------------------------------
def fetch_latest_uploads(channel_info: Dict[str, str]) -> List[Dict[str, Any]]:
    """Simulate the playlistItems.list response for the five latest uploads."""

    channel_id = channel_info["channel_id"]
    playlist_id = channel_info["uploads_playlist_id"]
    base_published_at = datetime.now(timezone.utc)

    dummy_videos: List[Dict[str, Any]] = []
    for index in range(5):
        dummy_videos.append(
            {
                "video_id": f"{playlist_id}_video_{index + 1}",
                "channel_id": channel_id,
                "published_at": (base_published_at - timedelta(hours=index)).isoformat(),
            }
        )

    log.info(
        "Simulated %d uploads for channel %s",
        len(dummy_videos),
        channel_id,
    )
    return dummy_videos


# ---------------------------------------------------------------------------
# Step 3: Upsert all videos into the polling queue
# ---------------------------------------------------------------------------
def merge_new_videos_to_db(conn, videos: List[Dict[str, Any]]) -> int:
    """Upsert video records into the polling queue."""

    if not videos:
        log.info("No videos to upsert")
        return 0

    affected_rows = 0
    with conn.cursor() as cur:
        for video in videos:
            cur.execute(UPSERT_VIDEOS_SQL, video)
            affected_rows += 1
    conn.commit()

    log.info("Upserted %d video rows", affected_rows)
    return affected_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        log.error("SUPABASE_DB_URL environment variable is not set")
        sys.exit(1)

    log.info("Job 1 — Channel Ingestion starting")

    conn = psycopg2.connect(db_url)
    try:
        # Step 1: Get active channels
        channels = get_active_channels(conn)

        # Step 2: Fetch latest uploads for each channel
        all_videos: List[Dict[str, Any]] = []
        for channel in channels:
            uploads = fetch_latest_uploads(channel)
            all_videos.extend(uploads)

        # Step 3: Upsert into DB
        total = merge_new_videos_to_db(conn, all_videos)
        log.info("Job 1 complete — %d total video rows processed", total)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
