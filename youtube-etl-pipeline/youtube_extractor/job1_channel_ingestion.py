"""
================================================================================
job1_channel_ingestion.py — Standalone Script (GitHub Actions)
================================================================================
Schedule  : Every 15 minutes (*/15 * * * *)
Purpose   : Read active YouTube channel seeds from Supabase PostgreSQL,
            discover REAL recent uploads via YouTube Data API v3, and upsert
            the latest videos into the polling queue.

Environment Variables:
    SUPABASE_DB_URL    — PostgreSQL connection string
    YOUTUBE_API_KEYS   — Comma-separated YouTube Data API v3 keys (preferred)
    YOUTUBE_API_KEY    — Single YouTube API key (fallback)
================================================================================
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import psycopg2
from googleapiclient.errors import HttpError

# Ensure sibling modules (key_pool.py) are importable when run as a standalone script
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from key_pool import APIKeyPool, AllKeysExhaustedError

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
    if not channels:
        log.warning(
            "No channels with uploads_playlist_id found in channel_stats. "
            "Seed new channels using: python youtube_extractor/switch_channels.py --csv <file>"
        )
    return channels


# ---------------------------------------------------------------------------
# Step 2: Fetch REAL latest uploads for a channel via YouTube Data API v3
# ---------------------------------------------------------------------------
def fetch_latest_uploads(
    channel_info: Dict[str, str],
    key_pool: APIKeyPool,
) -> List[Dict[str, Any]]:
    """Fetch real recent uploads using playlistItems.list on the channel's
    uploads playlist. Returns up to 50 videos per channel.
    """

    channel_id = str(channel_info["channel_id"]).strip()[:64]
    raw_playlist = str(channel_info["uploads_playlist_id"]).strip()
    playlist_id = raw_playlist.split("=")[-1] if "=" in raw_playlist else raw_playlist

    videos: List[Dict[str, Any]] = []
    page_token: str | None = None

    # Fetch up to 50 items (one page) from the uploads playlist
    try:
        response = key_pool.execute_with_rotation(
            lambda svc, pid=playlist_id: svc.playlistItems().list(
                part="snippet",
                playlistId=pid,
                maxResults=50,
            )
        )
    except AllKeysExhaustedError:
        log.error("All YouTube API keys exhausted while fetching uploads for %s", channel_id)
        return []
    except HttpError as exc:
        log.error("YouTube API error for channel %s: %s", channel_id, exc)
        return []

    for item in response.get("items", []):
        snippet = item.get("snippet", {})
        vid = snippet.get("resourceId", {}).get("videoId")
        if not vid:
            continue

        published_at = snippet.get("publishedAt", datetime.now(timezone.utc).isoformat())

        videos.append(
            {
                "video_id": vid,
                "channel_id": channel_id,
                "published_at": published_at,
            }
        )

    log.info("Fetched %d real uploads for channel %s", len(videos), channel_id)
    return videos


from psycopg2.extras import execute_batch


# ---------------------------------------------------------------------------
# Step 3: Upsert all videos into the polling queue
# ---------------------------------------------------------------------------
def merge_new_videos_to_db(conn, videos: List[Dict[str, Any]]) -> int:
    """Upsert video records into the polling queue."""

    if not videos:
        log.info("No videos to upsert")
        return 0

    # Sort deterministically by video_id to prevent PostgreSQL deadlocks with Job 2
    videos.sort(key=lambda x: x["video_id"])

    with conn.cursor() as cur:
        execute_batch(cur, UPSERT_VIDEOS_SQL, videos, page_size=200)
    conn.commit()

    log.info("Upserted %d video rows", len(videos))
    return len(videos)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        log.error("SUPABASE_DB_URL environment variable is not set")
        sys.exit(1)

    log.info("Job 1 — Channel Ingestion starting")

    # Initialise the YouTube API key pool from environment
    key_pool = APIKeyPool.from_env()

    conn = psycopg2.connect(db_url)
    try:
        # Step 1: Get active channels
        channels = get_active_channels(conn)

        # Step 2: Fetch REAL latest uploads for each channel
        all_videos: List[Dict[str, Any]] = []
        for channel in channels:
            uploads = fetch_latest_uploads(channel, key_pool)
            all_videos.extend(uploads)

        # Step 3: Upsert into DB
        total = merge_new_videos_to_db(conn, all_videos)
        log.info("Job 1 complete — %d total video rows processed", total)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
