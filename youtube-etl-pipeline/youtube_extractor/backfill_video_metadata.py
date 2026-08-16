"""
================================================================================
backfill_video_metadata.py — One-off Backfill Script (run manually, NOT in CI)
================================================================================
Purpose   : Find rows in `videos` that were ingested before video-metadata
            columns existed (title IS NULL), fetch their true metadata via
            the YouTube Data API v3 videos.list endpoint in batches of 50,
            and update those rows in place.

Usage:
    python youtube_extractor/backfill_video_metadata.py
    python youtube_extractor/backfill_video_metadata.py --limit 50   # small test run

Environment Variables:
    SUPABASE_DB_URL    — PostgreSQL connection string
    YOUTUBE_API_KEYS   — Comma-separated YouTube Data API v3 keys (preferred)
    YOUTUBE_API_KEY    — Single YouTube API key (fallback)

NOTE: This script is intentionally not wired into any GitHub Actions
workflow — run it manually / on-demand.
================================================================================
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_batch

# Ensure sibling modules are importable when run as a standalone script
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from key_pool import APIKeyPool
from job1_channel_ingestion import fetch_video_metadata

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
UPDATE_VIDEO_METADATA_SQL = """
UPDATE videos
SET title = %(title)s,
    description = %(description)s,
    thumbnail_url = %(thumbnail_url)s,
    tags = %(tags)s,
    category_id = %(category_id)s,
    duration = %(duration)s
WHERE video_id = %(video_id)s;
"""


# ---------------------------------------------------------------------------
# Step 1: Find rows with missing metadata
# ---------------------------------------------------------------------------
def get_videos_missing_metadata(conn, limit: Optional[int]) -> List[str]:
    """Find video_ids that have never had real metadata fetched.

    `title` is used as the sole indicator (rather than requiring every
    metadata column to be NULL) because columns like `tags` are legitimately
    NULL for videos that have no tags but full metadata otherwise — using
    them as a detection signal would re-queue those rows forever.
    """

    query = "SELECT video_id FROM videos WHERE title IS NULL ORDER BY video_id"
    params: Tuple = ()
    if limit is not None:
        query += " LIMIT %s"
        params = (limit,)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    video_ids = [row[0] for row in rows]
    log.info("Found %d videos with missing metadata", len(video_ids))
    return video_ids


# ---------------------------------------------------------------------------
# Step 2: Apply fetched metadata back to the videos table
# ---------------------------------------------------------------------------
def apply_metadata_updates(conn, video_ids: List[str], metadata: dict) -> Tuple[int, List[str]]:
    """Update rows for which metadata was successfully fetched.

    Returns (updated_count, failed_video_ids) — video_ids for which
    videos.list did not return a result (deleted/private in the meantime,
    or dropped due to key exhaustion) are reported as failed, not raised.
    """

    rows = []
    failed: List[str] = []

    for vid in video_ids:
        meta = metadata.get(vid)
        if meta is None:
            failed.append(vid)
            continue
        rows.append({"video_id": vid, **meta})

    if rows:
        rows.sort(key=lambda x: x["video_id"])
        with conn.cursor() as cur:
            execute_batch(cur, UPDATE_VIDEO_METADATA_SQL, rows, page_size=200)
        conn.commit()

    return len(rows), failed


# ---------------------------------------------------------------------------
# CLI / Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing video metadata (title/description/thumbnail/tags/category/duration) via videos.list."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process up to this many videos — useful for small test runs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        log.error("SUPABASE_DB_URL environment variable is not set")
        sys.exit(1)

    log.info("Video Metadata Backfill starting (limit=%s)", args.limit)

    # Initialise the YouTube API key pool from environment — reuses the same
    # rotation logic as Job 1 / Job 2.
    key_pool = APIKeyPool.from_env()

    conn = psycopg2.connect(db_url)
    try:
        video_ids = get_videos_missing_metadata(conn, args.limit)
        if not video_ids:
            log.info("No videos need backfilling — exiting")
            return

        # Batched at up to 50 IDs per videos.list call, reusing key_pool
        # rotation — same quota-safety pattern as Job 2.
        metadata = fetch_video_metadata(video_ids, key_pool)

        updated, failed = apply_metadata_updates(conn, video_ids, metadata)

        log.info("Backfill complete — %d rows updated, %d rows failed", updated, len(failed))
        if failed:
            log.warning(
                "No metadata returned for %d video(s) — left unchanged: %s",
                len(failed),
                ", ".join(sorted(failed)),
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
