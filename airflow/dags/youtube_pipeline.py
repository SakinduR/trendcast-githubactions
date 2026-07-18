"""
YouTube Data Pipeline DAG

Orchestrates DDL setup and idempotent loading of YouTube channel statistics
into PostgreSQL on a 6-hour schedule.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "youtube_postgres"
YOUTUBE_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"
MAX_CHANNELS_PER_REQUEST = 50
REQUEST_DELAY_SECONDS = 0.5

CREATE_CHANNEL_STATS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS channel_stats (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_title TEXT,
    channel_description TEXT,
    total_views BIGINT,
    subscriber_count BIGINT,
    video_count BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_channel_stats_processed_at
    ON channel_stats (processed_at DESC);
"""

UPSERT_CHANNEL_STATS_SQL = """
INSERT INTO channel_stats (
    channel_id,
    channel_title,
    channel_description,
    total_views,
    subscriber_count,
    video_count,
    processed_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (channel_id) DO UPDATE SET
    channel_title = EXCLUDED.channel_title,
    channel_description = EXCLUDED.channel_description,
    total_views = EXCLUDED.total_views,
    subscriber_count = EXCLUDED.subscriber_count,
    video_count = EXCLUDED.video_count,
    processed_at = EXCLUDED.processed_at;
"""

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _parse_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _chunked(items: Sequence[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), size):
        yield list(items[index : index + size])


def _build_youtube_session(max_retries: int = 5) -> requests.Session:
    retry_strategy = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _fetch_channel_batch(
    session: requests.Session,
    api_key: str,
    channel_ids: list[str],
) -> list[dict[str, Any]]:
    response = session.get(
        YOUTUBE_CHANNELS_URL,
        params={
            "part": "snippet,statistics",
            "id": ",".join(channel_ids),
            "key": api_key,
        },
        timeout=30,
    )

    if not response.ok:
        try:
            error_payload = response.json().get("error", {})
            message = error_payload.get("message", response.text)
            reasons = [
                err.get("reason", "")
                for err in error_payload.get("errors", [])
                if isinstance(err, dict)
            ]
        except ValueError:
            message = response.text
            reasons = []

        if response.status_code == 403 and (
            "quotaExceeded" in reasons or "dailyLimitExceeded" in reasons
        ):
            raise RuntimeError(f"YouTube API quota exceeded: {message}")

        if response.status_code == 429 or "rateLimitExceeded" in reasons:
            raise RuntimeError(f"YouTube API rate limit exceeded: {message}")

        raise RuntimeError(
            f"YouTube API request failed ({response.status_code}): {message}"
        )

    items = response.json().get("items", [])
    records: list[dict[str, Any]] = []

    for item in items:
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        channel_id = item.get("id")

        if not channel_id:
            logger.warning("Skipping malformed API response item without channel_id")
            continue

        records.append(
            {
                "channel_id": channel_id,
                "channel_title": snippet.get("title", ""),
                "channel_description": snippet.get("description", ""),
                "total_views": _parse_int(statistics.get("viewCount")),
                "subscriber_count": _parse_int(statistics.get("subscriberCount")),
                "video_count": _parse_int(statistics.get("videoCount")),
            }
        )

    return records


def fetch_youtube_channel_stats(
    api_key: str,
    channel_ids: Sequence[str],
) -> list[dict[str, Any]]:
    """Fetch channel metadata from YouTube Data API v3 (mirrors Step 4 extractor logic)."""
    normalized_ids = []
    seen: set[str] = set()
    for channel_id in channel_ids:
        cleaned = channel_id.strip()
        if cleaned and cleaned not in seen:
            normalized_ids.append(cleaned)
            seen.add(cleaned)

    if not normalized_ids:
        raise ValueError("At least one YouTube channel ID is required.")

    session = _build_youtube_session()
    all_records: list[dict[str, Any]] = []

    for batch_index, batch in enumerate(
        _chunked(normalized_ids, MAX_CHANNELS_PER_REQUEST), start=1
    ):
        logger.info(
            "Fetching YouTube metadata for batch %d (%d channel ID(s))",
            batch_index,
            len(batch),
        )
        all_records.extend(_fetch_channel_batch(session, api_key, batch))

        if batch_index * MAX_CHANNELS_PER_REQUEST < len(normalized_ids):
            time.sleep(REQUEST_DELAY_SECONDS)

    found_ids = {record["channel_id"] for record in all_records}
    missing_ids = set(normalized_ids) - found_ids
    if missing_ids:
        logger.warning(
            "No API data returned for channel ID(s): %s",
            ", ".join(sorted(missing_ids)),
        )

    return all_records


def extract_youtube_data(**context: Any) -> None:
    """Extract channel stats from YouTube and upsert into PostgreSQL."""
    api_key = Variable.get("YOUTUBE_API_KEY")
    channel_ids_raw = Variable.get("YOUTUBE_CHANNEL_IDS")
    channel_ids = [
        channel_id.strip()
        for channel_id in channel_ids_raw.split(",")
        if channel_id.strip()
    ]

    if not channel_ids:
        raise ValueError(
            "Airflow Variable YOUTUBE_CHANNEL_IDS must contain at least one channel ID."
        )

    records = fetch_youtube_channel_stats(api_key, channel_ids)
    processed_at = datetime.now(timezone.utc)

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()

    try:
        with conn.cursor() as cursor:
            for record in records:
                cursor.execute(
                    UPSERT_CHANNEL_STATS_SQL,
                    (
                        record["channel_id"],
                        record["channel_title"],
                        record["channel_description"],
                        record["total_views"],
                        record["subscriber_count"],
                        record["video_count"],
                        processed_at,
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to upsert YouTube channel stats")
        raise
    finally:
        conn.close()

    logger.info("Successfully upserted %d channel record(s)", len(records))


with DAG(
    dag_id="youtube_data_pipeline",
    default_args=default_args,
    description="Extract YouTube channel statistics and load into PostgreSQL",
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["youtube", "etl", "postgres"],
) as dag:
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_CHANNEL_STATS_TABLE_SQL,
    )

    extract_youtube_data_task = PythonOperator(
        task_id="extract_youtube_data",
        python_callable=extract_youtube_data,
    )

    create_tables >> extract_youtube_data_task
