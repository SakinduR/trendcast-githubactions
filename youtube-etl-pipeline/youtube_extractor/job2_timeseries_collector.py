"""
================================================================================
job2_timeseries_collector.py — Standalone Script (GitHub Actions)
================================================================================
Schedule  : Every 15 minutes (*/15 * * * *)
Purpose   : Poll due videos, collect time-series metrics, and adjust polling
            cadence based on video age.

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
INSERT_TIMESERIES_SQL = """
INSERT INTO view_timeseries (
    video_id,
    scraped_at,
    view_count,
    like_count,
    comment_count
) VALUES (
    %(video_id)s,
    %(scraped_at)s,
    %(view_count)s,
    %(like_count)s,
    %(comment_count)s
);
"""

UPDATE_VIDEO_POLLS_SQL = """
UPDATE videos
SET last_polled_at = %(last_polled_at)s,
    next_poll_at = %(next_poll_at)s,
    current_interval_hours = %(current_interval_hours)s
WHERE video_id = %(video_id)s;
"""


# ---------------------------------------------------------------------------
# Decay Logic
# ---------------------------------------------------------------------------
def select_interval_hours(age_hours: float) -> int:
    """Choose the next polling interval based on how old the video is."""
    if age_hours < 24:
        return 1
    if age_hours < 7 * 24:
        return 6
    if age_hours < 30 * 24:
        return 24
    return 168


# ---------------------------------------------------------------------------
# Step 1: Query due videos from the database
# ---------------------------------------------------------------------------
def query_due_videos(conn) -> List[Dict[str, Any]]:
    """Fetch videos whose next_poll_at has passed."""

    query = """
        SELECT video_id, published_at
        FROM videos
        WHERE next_poll_at <= CURRENT_TIMESTAMP
        ORDER BY next_poll_at ASC, video_id ASC
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    due_videos = [
        {
            "video_id": row[0],
            "published_at": row[1].isoformat() if hasattr(row[1], "isoformat") else row[1],
        }
        for row in rows
    ]

    log.info("Found %d due videos", len(due_videos))
    return due_videos


# ---------------------------------------------------------------------------
# Step 2: Fetch YouTube stats for each video (currently simulated)
# ---------------------------------------------------------------------------
def fetch_youtube_stats(due_videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Mock a YouTube videos.list response for due videos."""

    fetched_at = datetime.now(timezone.utc).isoformat()
    metrics: List[Dict[str, Any]] = []

    for index, video in enumerate(due_videos, start=1):
        metrics.append(
            {
                "video_id": video["video_id"],
                "published_at": video["published_at"],
                "scraped_at": fetched_at,
                "view_count": 1000 + index * 25,
                "like_count": 100 + index * 3,
                "comment_count": 10 + index,
            }
        )

    log.info("Fetched metrics for %d videos", len(metrics))
    return metrics


# ---------------------------------------------------------------------------
# Step 3: Persist metrics and apply decay-polling updates
# ---------------------------------------------------------------------------
def apply_decay_and_update(conn, metrics: List[Dict[str, Any]]) -> int:
    """Insert timeseries rows and recalculate polling cadence for each video."""

    if not metrics:
        log.info("No metrics to persist")
        return 0

    # Sort deterministically by video_id to prevent PostgreSQL deadlocks with Job 1
    metrics.sort(key=lambda x: x["video_id"])

    now = datetime.now(timezone.utc)
    written_rows = 0

    with conn.cursor() as cur:
        for metric in metrics:
            # Parse published_at
            published_at_raw = metric["published_at"]
            if isinstance(published_at_raw, str):
                published_at = datetime.fromisoformat(
                    published_at_raw.replace("Z", "+00:00")
                )
            else:
                published_at = published_at_raw

            age_hours = (now - published_at).total_seconds() / 3600.0
            current_interval_hours = select_interval_hours(age_hours)
            next_poll_at = now + timedelta(hours=current_interval_hours)

            # Insert timeseries row
            cur.execute(
                INSERT_TIMESERIES_SQL,
                {
                    "video_id": metric["video_id"],
                    "scraped_at": metric["scraped_at"],
                    "view_count": metric["view_count"],
                    "like_count": metric["like_count"],
                    "comment_count": metric["comment_count"],
                },
            )

            # Update polling cadence
            cur.execute(
                UPDATE_VIDEO_POLLS_SQL,
                {
                    "video_id": metric["video_id"],
                    "last_polled_at": now.isoformat(),
                    "next_poll_at": next_poll_at.isoformat(),
                    "current_interval_hours": current_interval_hours,
                },
            )
            written_rows += 1

    conn.commit()
    log.info("Persisted %d time-series rows and updated polling cadence", written_rows)
    return written_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        log.error("SUPABASE_DB_URL environment variable is not set")
        sys.exit(1)

    log.info("Job 2 — Timeseries Collector starting")

    conn = psycopg2.connect(db_url)
    try:
        # Step 1: Get due videos
        due_videos = query_due_videos(conn)

        if not due_videos:
            log.info("No videos are due for polling — exiting")
            return

        # Step 2: Fetch stats
        metrics = fetch_youtube_stats(due_videos)

        # Step 3: Persist and apply decay
        total = apply_decay_and_update(conn, metrics)
        log.info("Job 2 complete — %d total rows processed", total)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
