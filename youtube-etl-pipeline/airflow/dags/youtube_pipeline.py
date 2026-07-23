"""
================================================================================
youtube_video_trends_pipeline.py — Apache Airflow DAG
================================================================================
DAG Name  : youtube_video_trends_pipeline
Schedule  : Every 6 hours (0 */6 * * *)
Purpose   : Fetches metadata for the latest 50 videos from specified channels
            and inserts new rows to track viewership trends growth over time.
================================================================================
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Import the key pool manager for multi-key quota rotation
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
try:
    from youtube_extractor.key_pool import APIKeyPool, AllKeysExhaustedError
except ImportError:
    # Fallback: define minimal stubs if the module isn't on the path
    # (e.g. in unit-test environments with a mocked DAG)
    APIKeyPool = None  # type: ignore[assignment, misc]
    AllKeysExhaustedError = Exception  # type: ignore[assignment, misc]

log = logging.getLogger(__name__)

# DAG Default Arguments
DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": 300,  # 5 minutes
}

# ===========================================================================
# SQL Statements (DDL & DML)
# ===========================================================================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS video_trends (
    id                      SERIAL PRIMARY KEY,
    video_id                VARCHAR(64)     NOT NULL,
    channel_title           VARCHAR(255)    NOT NULL,
    project_category        VARCHAR(100),
    title                   TEXT            NOT NULL,
    description             TEXT,
    published_at            TIMESTAMPTZ     NOT NULL,
    thumbnail_url           TEXT,
    tags                    TEXT[],         -- PostgreSQL Array for tags
    youtube_category_id     VARCHAR(10),
    live_broadcast_content  VARCHAR(50),
    view_count              BIGINT          DEFAULT 0,
    like_count              BIGINT          DEFAULT 0,
    comment_count           BIGINT          DEFAULT 0,
    duration                VARCHAR(50),    -- ISO 8601 duration (e.g., PT15M33S)
    dimension               VARCHAR(10),
    definition              VARCHAR(10),
    caption                 VARCHAR(10),
    licensed_content        BOOLEAN,
    projection              VARCHAR(20),
    topic_categories        TEXT[],         -- PostgreSQL Array for Wikipedia topic URLs
    upload_status           VARCHAR(50),
    privacy_status          VARCHAR(50),
    embeddable              BOOLEAN,
    public_stats_viewable   BOOLEAN,
    captured_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Configured Indexes for trend analysis (For time series querying)
CREATE INDEX IF NOT EXISTS idx_video_trends_id_time ON video_trends (video_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_video_trends_captured ON video_trends (captured_at DESC);
"""

# Configured INSERT statement to insert new rows only
INSERT_SQL = """
INSERT INTO video_trends (
    video_id, channel_title, project_category, title, description, published_at,
    thumbnail_url, tags, youtube_category_id, live_broadcast_content, view_count,
    like_count, comment_count, duration, dimension, definition, caption,
    licensed_content, projection, topic_categories, upload_status, privacy_status,
    embeddable, public_stats_viewable, captured_at
) VALUES (
    %(video_id)s, %(channel_title)s, %(project_category)s, %(title)s, %(description)s, %(published_at)s,
    %(thumbnail_url)s, %(tags)s, %(youtube_category_id)s, %(live_broadcast_content)s, %(view_count)s,
    %(like_count)s, %(comment_count)s, %(duration)s, %(dimension)s, %(definition)s, %(caption)s,
    %(licensed_content)s, %(projection)s, %(topic_categories)s, %(upload_status)s, %(privacy_status)s,
    %(embeddable)s, %(public_stats_viewable)s, %(captured_at)s
);
"""

# ===========================================================================
# Task Functions
# ===========================================================================

def create_tables(postgres_conn_id: str = "youtube_postgres", **kwargs) -> None:
    """Idempotent table creation for video trends tracking."""
    log.info("Running DDL: creating video_trends table if not exists")
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
    log.info("video_trends table ready")


def extract_and_load_video_trends(postgres_conn_id: str = "youtube_postgres", **kwargs) -> Dict[str, Any]:
    """
    Main logic: Collects the latest 50 videos from each channel 
    and inserts them into the database as new rows for trending analysis.
    Uses APIKeyPool for automatic key rotation on quota exhaustion.
    """
    channel_ids_raw = os.environ.get("YOUTUBE_CHANNEL_IDS", "")
    # optional: project_category can be fetched from the environment or an Airflow variable
    proj_cat = os.environ.get("PROJECT_CATEGORY", "Data Science & AI") 

    if not channel_ids_raw:
        raise ValueError("YOUTUBE_CHANNEL_IDS environment variable must be set.")

    # Initialise the API key pool (reads YOUTUBE_API_KEYS or YOUTUBE_API_KEY)
    if APIKeyPool is not None:
        pool = APIKeyPool.from_env()
    else:
        # Fallback to single-key mode if key_pool module isn't available
        api_key = os.environ.get("YOUTUBE_API_KEY", "")
        if not api_key:
            raise ValueError("YOUTUBE_API_KEY environment variable must be set.")
        pool = None

    channel_ids = [cid.strip() for cid in channel_ids_raw.split(",") if cid.strip()]
    
    captured_time = datetime.now(timezone.utc).isoformat()
    all_video_records: List[Dict[str, Any]] = []

    for channel_id in channel_ids:
        log.info(r"Processing channel ID: %s", channel_id)
        try:
            if pool is not None:
                # Use key pool with automatic rotation on quota errors
                # 1. Fetch the 'Uploads' Playlist ID of the channel
                ch_response = pool.execute_with_rotation(
                    lambda svc, cid=channel_id: svc.channels().list(
                        part="contentDetails", id=cid
                    )
                )
            else:
                youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
                ch_response = youtube.channels().list(part="contentDetails", id=channel_id).execute()

            items = ch_response.get("items", [])
            if not items:
                log.warning(r"Channel not found or no content details for: %s", channel_id)
                continue
            
            uploads_playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

            # 2. Get the IDs of the latest 50 videos from that Playlist
            if pool is not None:
                playlist_response = pool.execute_with_rotation(
                    lambda svc, pid=uploads_playlist_id: svc.playlistItems().list(
                        part="contentDetails",
                        playlistId=pid,
                        maxResults=50
                    )
                )
            else:
                playlist_response = youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=50
                ).execute()

            video_ids = [item["contentDetails"]["videoId"] for item in playlist_response.get("items", [])]
            if not video_ids:
                log.info(r"No videos found for channel: %s", channel_id)
                continue

            # 3. Get detailed metadata for the 50 video IDs (Batch Request)
            # YouTube API allows up to 50 IDs in a single video list call
            if pool is not None:
                video_response = pool.execute_with_rotation(
                    lambda svc, vids=video_ids: svc.videos().list(
                        part="snippet,statistics,contentDetails,status,topicDetails",
                        id=",".join(vids)
                    )
                )
            else:
                video_response = youtube.videos().list(
                    part="snippet,statistics,contentDetails,status,topicDetails",
                    id=",".join(video_ids)
                ).execute()

            # 4. Data Preprocessing (Structuring data to fit the database)
            for v_item in video_response.get("items", []):
                snippet = v_item.get("snippet", {})
                stats = v_item.get("statistics", {})
                content_details = v_item.get("contentDetails", {})
                status = v_item.get("status", {})
                topic_details = v_item.get("topicDetails", {})

                # Selecting the highest quality URL from thumbnails
                thumbnails = snippet.get("thumbnails", {})
                best_thumb = thumbnails.get("maxres", thumbnails.get("high", thumbnails.get("default", {})))
                thumbnail_url = best_thumb.get("url", "")

                # Data Type Formatting & Fallbacks (Cleaning)
                record = {
                    "video_id": v_item.get("id"),
                    "channel_title": snippet.get("channelTitle", ""),
                    "project_category": proj_cat,
                    "title": snippet.get("title", ""),
                    "description": snippet.get("description", ""),
                    "published_at": snippet.get("publishedAt"),
                    "thumbnail_url": thumbnail_url,
                    "tags": snippet.get("tags", []),  # Python list maps directly to PG Text[]
                    "youtube_category_id": snippet.get("categoryId"),
                    "live_broadcast_content": snippet.get("liveBroadcastContent", "none"),
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                    "duration": content_details.get("duration", ""),
                    "dimension": content_details.get("dimension", ""),
                    "definition": content_details.get("definition", ""),
                    "caption": content_details.get("caption", ""),
                    "licensed_content": content_details.get("licensedContent", False),
                    "projection": content_details.get("projection", ""),
                    "topic_categories": topic_details.get("topicCategories", []), # List format
                    "upload_status": status.get("uploadStatus", ""),
                    "privacy_status": status.get("privacyStatus", ""),
                    "embeddable": status.get("embeddable", True),
                    "public_stats_viewable": status.get("publicStatsViewable", True),
                    "captured_at": captured_time # Assigning the same timestamp to all videos (Run Timestamp)
                }
                all_video_records.append(record)

        except AllKeysExhaustedError:
            log.error("All API keys exhausted — aborting extraction for remaining channels.")
            break  # Stop processing further channels
        except HttpError as exc:
            log.error(r"YouTube API error for channel %s: %s", channel_id, exc)
            continue

    # 5. Insert all data into PostgreSQL (Insert New Rows)
    if not all_video_records:
        log.warning("No video records collected.")
        return {"inserted_videos": 0}

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    inserted_count = 0
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for record in all_video_records:
                cur.execute(INSERT_SQL, record)
                inserted_count += 1
        conn.commit()

    log.info(r"Successfully inserted %d new trend rows.", inserted_count)
    return {"inserted_videos": inserted_count, "captured_at": captured_time}


# ===========================================================================
# DAG Definition
# ===========================================================================
with DAG(
    dag_id="youtube_video_trends_pipeline",
    description="Collects latest 50 videos per channel to track viewership trends.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "ml-data", "trends"],
) as dag:

    t_create_tables = PythonOperator(
        task_id="create_video_trends_table",
        python_callable=create_tables,
        op_kwargs={"postgres_conn_id": "youtube_postgres"},
    )

    t_extract_and_load = PythonOperator(
        task_id="extract_load_video_trends",
        python_callable=extract_and_load_video_trends,
        op_kwargs={"postgres_conn_id": "youtube_postgres"},
    )

    t_create_tables >> t_extract_and_load