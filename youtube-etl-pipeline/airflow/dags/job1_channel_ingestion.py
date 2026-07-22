"""
================================================================================
job1_channel_ingestion.py — Apache Airflow DAG
================================================================================
DAG Name  : job1_channel_ingestion
Schedule  : Every 12 hours (0 */12 * * *)
Purpose   : Read active YouTube channel seeds, simulate recent upload discovery,
            and upsert the latest videos into PostgreSQL for polling.
================================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

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


@dag(
    dag_id="job1_channel_ingestion",
    description="Fetch active channel playlists and seed the videos queue.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 */12 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "ingestion", "postgres"],
)
def job1_channel_ingestion() -> None:
    """Build the channel ingestion DAG using taskflow and dynamic mapping."""

    @task
    def get_active_channels(postgres_conn_id: str = "youtube_postgres") -> List[Dict[str, str]]:
        """Load active channel playlist seeds from PostgreSQL."""

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        query = """
            SELECT channel_id, uploads_playlist_id
            FROM channel_stats
            WHERE uploads_playlist_id IS NOT NULL
              AND uploads_playlist_id <> ''
            ORDER BY channel_id
        """

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

        channels = [
            {"channel_id": row[0], "uploads_playlist_id": row[1]}
            for row in rows
        ]
        log.info("Loaded %d active channel seeds", len(channels))
        return channels

    @task
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

    @task
    def merge_new_videos_to_db(
        mapped_video_batches: List[List[Dict[str, Any]]],
        postgres_conn_id: str = "youtube_postgres",
    ) -> int:
        """Upsert mapped video batches into the polling queue."""

        videos_to_upsert: List[Dict[str, Any]] = [
            video
            for batch in mapped_video_batches
            for video in batch
        ]

        if not videos_to_upsert:
            log.info("No videos returned from mapped fetch task")
            return 0

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        affected_rows = 0
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for video in videos_to_upsert:
                    cur.execute(UPSERT_VIDEOS_SQL, video)
                    affected_rows += 1
            conn.commit()

        log.info("Upserted %d video rows", affected_rows)
        return affected_rows

    active_channels = get_active_channels()
    latest_uploads = fetch_latest_uploads.expand(channel_info=active_channels)
    merge_new_videos_to_db(latest_uploads)


job1_channel_ingestion()