"""
================================================================================
job1_channel_ingestion.py — Standalone Script (GitHub Actions)
================================================================================
Schedule  : Every 12 hours (0 */12 * * *)
Purpose   : Read active YouTube channel seeds from Supabase PostgreSQL,
            refresh channel-level stats via channels.list, discover REAL
            recent uploads via playlistItems.list, enrich them with true
            metadata via videos.list, and upsert everything into Postgres.

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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_batch
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

# YouTube Data API v3 accepts up to 50 IDs per channels.list / videos.list call
API_BATCH_SIZE = 50

# Thumbnail quality preference, highest first — maxres is frequently absent
THUMBNAIL_QUALITY_PREFERENCE = ("maxres", "standard", "high", "medium", "default")

# ---------------------------------------------------------------------------
# SQL Templates
# ---------------------------------------------------------------------------
UPSERT_CHANNEL_STATS_SQL = """
INSERT INTO channel_stats (
    channel_id,
    channel_title,
    channel_description,
    published_at,
    country,
    total_views,
    subscriber_count,
    video_count,
    processed_at
) VALUES (
    %(channel_id)s,
    %(channel_title)s,
    %(channel_description)s,
    %(published_at)s,
    %(country)s,
    %(total_views)s,
    %(subscriber_count)s,
    %(video_count)s,
    CURRENT_TIMESTAMP
)
ON CONFLICT (channel_id) DO UPDATE SET
    channel_title        = EXCLUDED.channel_title,
    channel_description   = EXCLUDED.channel_description,
    published_at          = EXCLUDED.published_at,
    country                = EXCLUDED.country,
    total_views            = EXCLUDED.total_views,
    subscriber_count       = EXCLUDED.subscriber_count,
    video_count            = EXCLUDED.video_count,
    processed_at           = CURRENT_TIMESTAMP;
"""

UPSERT_VIDEOS_SQL = """
INSERT INTO videos (
    video_id,
    channel_id,
    published_at,
    status,
    last_polled_at,
    next_poll_at,
    current_interval_hours,
    title,
    description,
    thumbnail_url,
    tags,
    category_id,
    duration
) VALUES (
    %(video_id)s,
    %(channel_id)s,
    %(published_at)s,
    'active',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    1,
    %(title)s,
    %(description)s,
    %(thumbnail_url)s,
    %(tags)s,
    %(category_id)s,
    %(duration)s
)
ON CONFLICT (video_id) DO UPDATE SET
    channel_id = EXCLUDED.channel_id,
    published_at = EXCLUDED.published_at,
    status = 'active',
    last_polled_at = CURRENT_TIMESTAMP,
    next_poll_at = CURRENT_TIMESTAMP,
    current_interval_hours = videos.current_interval_hours,
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    thumbnail_url = EXCLUDED.thumbnail_url,
    tags = EXCLUDED.tags,
    category_id = EXCLUDED.category_id,
    duration = EXCLUDED.duration;
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
# Step 2: Refresh channel-level stats via YouTube Data API v3 (channels.list)
# ---------------------------------------------------------------------------
def fetch_channel_metadata(
    channel_ids: List[str],
    key_pool: APIKeyPool,
) -> Dict[str, Dict[str, Any]]:
    """Fetch snippet + statistics for channels, batched at up to 50 IDs per
    channels.list call. Requesting extra `part`s does not change the quota
    cost (still 1 unit per call), so `snippet` is always included alongside
    `statistics` to populate description/published_at/video_count in one
    round trip.
    """

    metadata: Dict[str, Dict[str, Any]] = {}

    for i in range(0, len(channel_ids), API_BATCH_SIZE):
        chunk = channel_ids[i : i + API_BATCH_SIZE]
        ids_csv = ",".join(chunk)

        try:
            response = key_pool.execute_with_rotation(
                lambda svc, ids=ids_csv: svc.channels().list(
                    part="snippet,statistics",
                    id=ids,
                    maxResults=50,
                )
            )
        except AllKeysExhaustedError:
            log.error("All YouTube API keys exhausted while fetching channel metadata — aborting remaining batches")
            break
        except HttpError as exc:
            log.error("YouTube API error fetching channel metadata for batch: %s", exc)
            continue

        for item in response.get("items", []):
            channel_id = item.get("id")
            if not channel_id:
                continue

            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})

            metadata[channel_id] = {
                "channel_title": snippet.get("title") or "",
                "channel_description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
                "country": snippet.get("country"),
                # statistics.* are returned as strings by the API — cast explicitly
                "total_views": int(stats.get("viewCount", 0) or 0),
                "subscriber_count": int(stats.get("subscriberCount", 0) or 0),
                "video_count": int(stats.get("videoCount", 0) or 0),
            }

    log.info("Fetched channel metadata for %d / %d channels", len(metadata), len(channel_ids))
    return metadata


def update_channel_stats(conn, channel_metadata: Dict[str, Dict[str, Any]]) -> int:
    """Upsert refreshed channel-level stats into channel_stats."""

    if not channel_metadata:
        log.info("No channel metadata to update")
        return 0

    rows = [
        {"channel_id": channel_id, **fields}
        for channel_id, fields in channel_metadata.items()
    ]
    # Sort deterministically by channel_id to avoid lock-ordering surprises
    rows.sort(key=lambda x: x["channel_id"])

    with conn.cursor() as cur:
        execute_batch(cur, UPSERT_CHANNEL_STATS_SQL, rows, page_size=200)
    conn.commit()

    log.info("Updated channel_stats for %d channels", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Step 3: Discover recent upload IDs via playlistItems.list
# ---------------------------------------------------------------------------
def fetch_latest_uploads(
    channel_info: Dict[str, str],
    key_pool: APIKeyPool,
) -> List[Dict[str, Any]]:
    """Discover recent uploads using playlistItems.list on the channel's
    uploads playlist. Returns up to 50 videos per channel.

    NOTE: playlistItems.snippet.publishedAt is the time the item was added
    to the uploads playlist, not the true video publish time (it can lag or
    lead the real publish moment, e.g. for premieres/scheduled uploads). It
    is used here only as a last-resort fallback if videos.list can't be
    reached for a given ID later — see fetch_video_metadata().
    """

    channel_id = str(channel_info["channel_id"]).strip()[:64]
    raw_playlist = str(channel_info["uploads_playlist_id"]).strip()
    playlist_id = raw_playlist.split("=")[-1] if "=" in raw_playlist else raw_playlist

    videos: List[Dict[str, Any]] = []

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

        playlist_added_at = snippet.get("publishedAt", datetime.now(timezone.utc).isoformat())

        videos.append(
            {
                "video_id": vid,
                "channel_id": channel_id,
                "playlist_added_at": playlist_added_at,
            }
        )

    log.info("Fetched %d real uploads for channel %s", len(videos), channel_id)
    return videos


# ---------------------------------------------------------------------------
# Step 4: Enrich discovered videos with true metadata via videos.list
# ---------------------------------------------------------------------------
def _select_thumbnail_url(thumbnails: Dict[str, Any]) -> Optional[str]:
    """Pick the best available thumbnail URL: maxres > standard > high >
    medium > default. Returns None if no thumbnail is present at all —
    never raises.
    """
    for quality in THUMBNAIL_QUALITY_PREFERENCE:
        entry = thumbnails.get(quality)
        if entry and entry.get("url"):
            return entry["url"]
    return None


def fetch_video_metadata(
    video_ids: List[str],
    key_pool: APIKeyPool,
) -> Dict[str, Dict[str, Any]]:
    """Fetch true snippet + contentDetails for the given video IDs via
    videos.list, batched at up to 50 IDs per call (reuses the shared
    key-pool rotation — never called once per video).

    Also resolves Bug B: snippet.publishedAt from videos.list is the video's
    real publish time, unlike playlistItems.snippet.publishedAt.
    """

    metadata: Dict[str, Dict[str, Any]] = {}

    for i in range(0, len(video_ids), API_BATCH_SIZE):
        chunk = video_ids[i : i + API_BATCH_SIZE]
        ids_csv = ",".join(chunk)

        log.info(
            "Requesting video metadata batch %d–%d of %d videos",
            i + 1,
            min(i + API_BATCH_SIZE, len(video_ids)),
            len(video_ids),
        )

        try:
            response = key_pool.execute_with_rotation(
                lambda svc, ids=ids_csv: svc.videos().list(
                    part="snippet,contentDetails",
                    id=ids,
                    maxResults=50,
                )
            )
        except AllKeysExhaustedError:
            log.error("All YouTube API keys exhausted while fetching video metadata — aborting remaining batches")
            break
        except HttpError as exc:
            log.error("YouTube API error fetching video metadata for batch: %s", exc)
            continue

        for item in response.get("items", []):
            vid = item.get("id")
            if not vid:
                continue

            snippet = item.get("snippet", {})
            content_details = item.get("contentDetails", {})

            metadata[vid] = {
                "published_at": snippet.get("publishedAt"),
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "thumbnail_url": _select_thumbnail_url(snippet.get("thumbnails", {}) or {}),
                "tags": snippet.get("tags"),
                "category_id": snippet.get("categoryId"),
                "duration": content_details.get("duration"),
            }

    log.info("Fetched video metadata for %d / %d videos", len(metadata), len(video_ids))
    return metadata


def merge_upload_and_metadata(
    uploads: List[Dict[str, Any]],
    video_metadata: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Combine playlist-discovered uploads with their true videos.list
    metadata. `published_at` is NOT NULL in the schema, so if videos.list
    didn't return a given ID (e.g. deleted between discovery and lookup)
    we fall back to the playlist-add timestamp rather than drop the video
    or abort the batch.
    """

    merged: List[Dict[str, Any]] = []

    for upload in uploads:
        vid = upload["video_id"]
        meta = video_metadata.get(vid)

        if meta is None:
            log.warning(
                "No videos.list metadata for %s — falling back to playlist-add timestamp for published_at",
                vid,
            )
            merged.append(
                {
                    "video_id": vid,
                    "channel_id": upload["channel_id"],
                    "published_at": upload["playlist_added_at"],
                    "title": None,
                    "description": None,
                    "thumbnail_url": None,
                    "tags": None,
                    "category_id": None,
                    "duration": None,
                }
            )
        else:
            merged.append(
                {
                    "video_id": vid,
                    "channel_id": upload["channel_id"],
                    "published_at": meta["published_at"] or upload["playlist_added_at"],
                    "title": meta["title"],
                    "description": meta["description"],
                    "thumbnail_url": meta["thumbnail_url"],
                    "tags": meta["tags"],
                    "category_id": meta["category_id"],
                    "duration": meta["duration"],
                }
            )

    return merged


# ---------------------------------------------------------------------------
# Step 5: Upsert all videos into the polling queue
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

        # Step 2: Refresh channel-level stats (fixes channel_stats.video_count,
        # and populates channel_description/published_at)
        channel_ids = [c["channel_id"] for c in channels]
        channel_metadata = fetch_channel_metadata(channel_ids, key_pool)
        update_channel_stats(conn, channel_metadata)

        # Step 3: Discover recent upload IDs for each channel
        all_uploads: List[Dict[str, Any]] = []
        for channel in channels:
            uploads = fetch_latest_uploads(channel, key_pool)
            all_uploads.extend(uploads)

        # Step 4: Enrich uploads with true metadata (fixes published_at, adds
        # title/description/thumbnail_url/tags/category_id/duration)
        video_ids = [u["video_id"] for u in all_uploads]
        video_metadata = fetch_video_metadata(video_ids, key_pool)
        enriched_videos = merge_upload_and_metadata(all_uploads, video_metadata)

        # Step 5: Upsert into DB
        total = merge_new_videos_to_db(conn, enriched_videos)
        log.info("Job 1 complete — %d total video rows processed", total)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
