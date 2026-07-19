"""
================================================================================
extractor.py — YouTube Channel Statistics Extractor
================================================================================
Responsibilities:
  1. Fetch channel statistics from the YouTube Data API v3.
  2. Publish raw JSON records to a Kafka topic (streaming mode).
  3. Optionally upsert records directly to PostgreSQL (batch mode).

Environment Variables Required:
  YOUTUBE_API_KEY         — YouTube Data API v3 key
  YOUTUBE_CHANNEL_IDS     — Comma-separated channel IDs
  KAFKA_BOOTSTRAP_SERVERS — e.g. kafka:9092
  KAFKA_TOPIC             — Target Kafka topic name
  POSTGRES_HOST / PORT / DB / USER / PASSWORD — DB connection params

Usage:
  python extractor.py                   # Kafka streaming mode (default)
  python extractor.py --mode=postgres   # Direct PostgreSQL upsert mode
================================================================================
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pythonjsonlogger import jsonlogger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Load environment variables from .env (only for local dev; Docker sets them)
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Structured JSON logging — integrates cleanly with log aggregators
# ---------------------------------------------------------------------------
logger = logging.getLogger("youtube_extractor")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(
    jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
)
logger.addHandler(_handler)


# ===========================================================================
# Configuration — read from environment, fail fast if required keys missing
# ===========================================================================
class Config:
    """Centralises all configuration and validates required values on startup."""

    def __init__(self) -> None:
        self.api_key: str = self._require("YOUTUBE_API_KEY")
        self.channel_ids: List[str] = [
            cid.strip()
            for cid in self._require("YOUTUBE_CHANNEL_IDS").split(",")
            if cid.strip()
        ]
        self.kafka_servers: str = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
        )
        self.kafka_topic: str = os.getenv(
            "KAFKA_TOPIC", "youtube_raw_data"
        )
        self.pg_host: str = os.getenv("POSTGRES_HOST", "postgres")
        self.pg_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
        self.pg_db: str = os.getenv("POSTGRES_DB", "youtube_pipeline")
        self.pg_user: str = os.getenv("POSTGRES_USER", "airflow")
        self.pg_password: str = os.getenv("POSTGRES_PASSWORD", "")
        # YouTube API max channel IDs per request
        self.api_batch_size: int = 50
        # Quota cost per channels.list call = 1 unit
        # Daily quota: 10,000 units → can make 10,000 calls/day
        self.quota_delay_seconds: float = float(
            os.getenv("QUOTA_DELAY_SECONDS", "0.1")
        )

    @staticmethod
    def _require(key: str) -> str:
        value = os.getenv(key)
        if not value:
            logger.error("Required environment variable missing", extra={"key": key})
            sys.exit(1)
        return value


# ===========================================================================
# YouTubeAPIClient — wraps google-api-python-client with retry logic
# ===========================================================================
class YouTubeAPIClient:
    """
    Thin wrapper around the YouTube Data API v3.

    Quota Management:
    - channels.list costs 1 quota unit.
    - Default daily quota: 10,000 units.
    - We batch up to 50 IDs per request to minimise quota usage.
    - Exponential back-off on transient errors (5xx, 429).
    """

    # Parts to request — avoid requesting unnecessary parts to save quota
    CHANNEL_PARTS = "snippet,statistics"

    def __init__(self, api_key: str) -> None:
        self._service = build(
            "youtube", "v3", developerKey=api_key, cache_discovery=False
        )
        logger.info("YouTube API client initialised")

    @retry(
        retry=retry_if_exception_type(HttpError),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def fetch_channel_statistics(
        self, channel_ids: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch statistics for up to 50 channel IDs in a single API call.

        Args:
            channel_ids: List of YouTube channel ID strings.

        Returns:
            List of normalised channel stat dictionaries.

        Raises:
            HttpError: On non-retryable API errors (e.g. 403 quota exceeded).
        """
        if not channel_ids:
            return []

        logger.info(
            "Fetching channel stats",
            extra={"channel_count": len(channel_ids)},
        )

        try:
            response = (
                self._service.channels()
                .list(
                    part=self.CHANNEL_PARTS,
                    id=",".join(channel_ids),
                    maxResults=50,
                )
                .execute()
            )
        except HttpError as exc:
            status_code = exc.resp.status
            if status_code == 403:
                logger.error(
                    "YouTube API quota exceeded or access forbidden",
                    extra={"status_code": status_code, "details": str(exc)},
                )
                raise  # Do not retry quota exhaustion — re-raise immediately
            elif status_code in (500, 502, 503, 504):
                logger.warning(
                    "Transient API error — will retry",
                    extra={"status_code": status_code},
                )
                raise  # Retryable — tenacity will back off
            else:
                logger.error(
                    "Non-retryable API error",
                    extra={"status_code": status_code, "details": str(exc)},
                )
                raise

        items = response.get("items", [])
        return [self._normalise(item) for item in items]

    @staticmethod
    def _normalise(item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten a raw YouTube API channel item into a clean dict.

        Args:
            item: Raw API response item dict.

        Returns:
            Normalised dict with typed fields.
        """
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})

        return {
            "channel_id": item.get("id", ""),
            "channel_title": snippet.get("title", ""),
            "channel_description": snippet.get("description", ""),
            "published_at": snippet.get("publishedAt", ""),
            "country": snippet.get("country", ""),
            "total_views": int(stats.get("viewCount", 0)),
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "video_count": int(stats.get("videoCount", 0)),
            # ISO 8601 UTC timestamp of extraction
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }


# ===========================================================================
# KafkaPublisher — publishes JSON records to a Kafka topic
# ===========================================================================
class KafkaPublisher:
    """
    Publishes channel stat records to a Kafka topic.

    Key design decisions:
    - channel_id used as the Kafka message key → ensures ordered delivery
      per channel and consistent partition assignment.
    - acks='all' ensures at-least-once delivery semantics.
    - Serialisation: JSON encoded as UTF-8 bytes.
    """

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = self._build_producer(bootstrap_servers)
        logger.info(
            "Kafka producer initialised",
            extra={"topic": topic, "servers": bootstrap_servers},
        )

    @staticmethod
    def _build_producer(servers: str) -> KafkaProducer:
        """Build and return a KafkaProducer with retry on connection failure."""
        max_attempts = 10
        for attempt in range(1, max_attempts + 1):
            try:
                return KafkaProducer(
                    bootstrap_servers=servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    compression_type="gzip",
                    batch_size=16384,
                    linger_ms=10,
                )
            except KafkaError as exc:
                logger.warning(
                    "Kafka not ready, retrying…",
                    extra={"attempt": attempt, "error": str(exc)},
                )
                time.sleep(5)
        logger.error("Could not connect to Kafka after max attempts")
        raise RuntimeError("Kafka connection failed")

    def publish(self, records: List[Dict[str, Any]]) -> int:
        """
        Publish a list of records to Kafka.

        Args:
            records: List of normalised channel stat dicts.

        Returns:
            Number of successfully published records.
        """
        published = 0
        for record in records:
            try:
                future = self._producer.send(
                    topic=self._topic,
                    key=record.get("channel_id"),
                    value=record,
                )
                future.get(timeout=10)  # Block to confirm delivery
                published += 1
                logger.info(
                    "Record published to Kafka",
                    extra={
                        "channel_id": record.get("channel_id"),
                        "channel_title": record.get("channel_title"),
                    },
                )
            except KafkaError as exc:
                logger.error(
                    "Failed to publish record",
                    extra={"channel_id": record.get("channel_id"), "error": str(exc)},
                )

        self._producer.flush()
        return published

    def close(self) -> None:
        """Gracefully close the Kafka producer."""
        self._producer.close()
        logger.info("Kafka producer closed")


# ===========================================================================
# PostgresWriter — direct upsert to the channel_stats table
# ===========================================================================
class PostgresWriter:
    """
    Writes channel stats directly to PostgreSQL using an UPSERT pattern
    to ensure idempotency — safe to run multiple times for the same channels.
    """

    UPSERT_SQL = """
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
        )
        VALUES (
            %(channel_id)s,
            %(channel_title)s,
            %(channel_description)s,
            %(published_at)s,
            %(country)s,
            %(total_views)s,
            %(subscriber_count)s,
            %(video_count)s,
            %(processed_at)s
        )
        ON CONFLICT (channel_id)
        DO UPDATE SET
            channel_title       = EXCLUDED.channel_title,
            channel_description = EXCLUDED.channel_description,
            total_views         = EXCLUDED.total_views,
            subscriber_count    = EXCLUDED.subscriber_count,
            video_count         = EXCLUDED.video_count,
            processed_at        = EXCLUDED.processed_at;
    """

    CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS channel_stats (
            channel_id          VARCHAR(64)     PRIMARY KEY,
            channel_title       VARCHAR(255)    NOT NULL,
            channel_description TEXT,
            published_at        TIMESTAMPTZ,
            country             VARCHAR(10),
            total_views         BIGINT          DEFAULT 0,
            subscriber_count    BIGINT          DEFAULT 0,
            video_count         INTEGER         DEFAULT 0,
            processed_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
            created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
        );

        -- Index on subscriber_count for sorted queries
        CREATE INDEX IF NOT EXISTS idx_channel_stats_subscribers
            ON channel_stats (subscriber_count DESC);

        -- Index on processed_at for time-series queries
        CREATE INDEX IF NOT EXISTS idx_channel_stats_processed
            ON channel_stats (processed_at DESC);
    """

    def __init__(self, host: str, port: int, db: str, user: str, password: str) -> None:
        self._conn_params = {
            "host": host,
            "port": port,
            "dbname": db,
            "user": user,
            "password": password,
            "connect_timeout": 10,
        }

    def _get_connection(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(**self._conn_params)

    def ensure_table(self) -> None:
        """Create the channel_stats table and indexes if they do not exist."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(self.CREATE_TABLE_SQL)
            conn.commit()
        logger.info("channel_stats table verified / created")

    def upsert(self, records: List[Dict[str, Any]]) -> int:
        """
        Upsert a list of channel stat records.

        Args:
            records: List of normalised channel stat dicts.

        Returns:
            Number of rows affected.
        """
        if not records:
            return 0

        affected = 0
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for record in records:
                    cur.execute(self.UPSERT_SQL, record)
                    affected += cur.rowcount
            conn.commit()

        logger.info("Upsert complete", extra={"rows_affected": affected})
        return affected


# ===========================================================================
# Orchestrator — ties together API, Kafka/Postgres, and batching logic
# ===========================================================================
class YouTubeExtractorOrchestrator:
    """
    Main orchestrator:
    - Batches channel IDs to respect API limits.
    - Applies a small delay between batches for quota management.
    - Routes output to Kafka (streaming) or PostgreSQL (batch).
    """

    def __init__(
        self,
        config: Config,
        mode: str = "kafka",
    ) -> None:
        self._config = config
        self._mode = mode
        self._api_client = YouTubeAPIClient(config.api_key)

        if mode == "kafka":
            self._publisher: Optional[KafkaPublisher] = KafkaPublisher(
                config.kafka_servers, config.kafka_topic
            )
            self._db_writer: Optional[PostgresWriter] = None
        else:
            self._publisher = None
            self._db_writer = PostgresWriter(
                config.pg_host,
                config.pg_port,
                config.pg_db,
                config.pg_user,
                config.pg_password,
            )
            self._db_writer.ensure_table()

    def run(self) -> None:
        """
        Entry-point: iterate over channel ID batches, extract stats,
        and dispatch to the configured output.
        """
        channel_ids = self._config.channel_ids
        batch_size = self._config.api_batch_size
        total_processed = 0

        logger.info(
            "Extraction started",
            extra={"total_channels": len(channel_ids), "mode": self._mode},
        )

        # Split channel_ids into batches of `batch_size`
        for batch_start in range(0, len(channel_ids), batch_size):
            batch = channel_ids[batch_start : batch_start + batch_size]

            try:
                records = self._api_client.fetch_channel_statistics(batch)
            except HttpError as exc:
                logger.error(
                    "Batch fetch failed — skipping batch",
                    extra={"batch": batch, "error": str(exc)},
                )
                continue

            if not records:
                logger.warning(
                    "No records returned for batch",
                    extra={"batch": batch},
                )
                continue

            if self._mode == "kafka" and self._publisher:
                count = self._publisher.publish(records)
            else:
                count = self._db_writer.upsert(records)  # type: ignore[union-attr]

            total_processed += count

            # Polite delay between batches to stay within quota budget
            time.sleep(self._config.quota_delay_seconds)

        logger.info(
            "Extraction complete",
            extra={"total_processed": total_processed, "mode": self._mode},
        )

        # Clean up resources
        if self._publisher:
            self._publisher.close()


# ===========================================================================
# CLI Entry Point
# ===========================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="YouTube Channel Statistics Extractor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["kafka", "postgres"],
        default=os.getenv("EXTRACTOR_MODE", "kafka"),
        help=(
            "Output mode: 'kafka' publishes to a Kafka topic (default), "
            "'postgres' upserts directly into PostgreSQL."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = Config()

    logger.info(
        "YouTube Extractor starting",
        extra={
            "mode": args.mode,
            "channel_count": len(config.channel_ids),
        },
    )

    orchestrator = YouTubeExtractorOrchestrator(config=config, mode=args.mode)
    orchestrator.run()


if __name__ == "__main__":
    main()
