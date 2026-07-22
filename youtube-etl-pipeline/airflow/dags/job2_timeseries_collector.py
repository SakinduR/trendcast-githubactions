"""
================================================================================
job2_timeseries_collector.py — Apache Airflow DAG
================================================================================
DAG Name  : job2_timeseries_collector
Schedule  : Every 15 minutes (*/15 * * * *)
Purpose   : Poll due videos, collect time-series metrics, and adjust polling
            cadence based on video age.
================================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, DefaultDict, Dict, List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

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


@dag(
    dag_id="job2_timeseries_collector",
    description="Poll due videos and collect viewership time-series data.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "timeseries", "postgres"],
)
def job2_timeseries_collector() -> None:
    """Build the dynamic polling DAG."""

    @task
    def query_due_videos(postgres_conn_id: str = "youtube_postgres") -> List[List[Dict[str, Any]]]:
        """Fetch due videos and batch them into chunks of up to 50 entries."""

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        query = """
            SELECT video_id, published_at
            FROM videos
            WHERE next_poll_at <= CURRENT_TIMESTAMP
            ORDER BY next_poll_at ASC, video_id ASC
        """

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

        ordered_videos = [
            {"video_id": row[0], "published_at": row[1].isoformat() if hasattr(row[1], "isoformat") else row[1]}
            for row in rows
        ]

        batches: List[List[Dict[str, Any]]] = [
            ordered_videos[index:index + 50]
            for index in range(0, len(ordered_videos), 50)
        ]

        log.info("Queued %d due videos across %d batches", len(ordered_videos), len(batches))
        return batches

    @task
    def fetch_youtube_stats(video_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Mock a YouTube videos.list response for a batch of due videos."""

        fetched_at = datetime.now(timezone.utc).isoformat()
        metrics: List[Dict[str, Any]] = []

        for index, video in enumerate(video_batch, start=1):
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

        log.info("Mocked %d metric rows for a batch", len(metrics))
        return metrics

    @task
    def apply_decay_and_update(
        mapped_metrics: List[List[Dict[str, Any]]],
        postgres_conn_id: str = "youtube_postgres",
    ) -> int:
        """Persist raw metrics, then recalculate polling cadence for each video."""

        collected_metrics: List[Dict[str, Any]] = [
            metric
            for batch in mapped_metrics
            for metric in batch
        ]

        if not collected_metrics:
            log.info("No metrics returned from mapped polling task")
            return 0

        now = datetime.now(timezone.utc)
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        written_rows = 0

        def select_interval_hours(age_hours: float) -> int:
            if age_hours < 24:
                return 1
            if age_hours < 7 * 24:
                return 6
            if age_hours < 30 * 24:
                return 24
            return 168

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for metric in collected_metrics:
                    published_at_raw = metric["published_at"]
                    if isinstance(published_at_raw, str):
                        published_at = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
                    else:
                        published_at = published_at_raw

                    age_hours = (now - published_at).total_seconds() / 3600.0
                    current_interval_hours = select_interval_hours(age_hours)
                    next_poll_at = now + timedelta(hours=current_interval_hours)

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

    due_video_batches = query_due_videos()
    polled_batches = fetch_youtube_stats.expand(video_batch=due_video_batches)
    apply_decay_and_update(polled_batches)


job2_timeseries_collector()