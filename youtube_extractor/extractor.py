#!/usr/bin/env python3
"""
YouTube Data Extraction Module

Fetches channel metadata from the YouTube Data API v3 and optionally persists
results to PostgreSQL. Runs standalone via CLI or can be imported by Airflow/Spark.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

YOUTUBE_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"
MAX_CHANNELS_PER_REQUEST = 50
DEFAULT_REQUEST_DELAY_SECONDS = 0.5
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 1.0


@dataclass(frozen=True)
class ChannelData:
    """Normalized channel record from the YouTube Data API."""

    channel_id: str
    title: str
    description: str
    view_count: int | None
    subscriber_count: int | None
    video_count: int | None
    extracted_at: str


class YouTubeExtractorError(Exception):
    """Base exception for extractor failures."""


class QuotaExceededError(YouTubeExtractorError):
    """Raised when the YouTube API daily quota is exhausted."""


class RateLimitError(YouTubeExtractorError):
    """Raised when the YouTube API rate limit is hit."""


class YouTubeAPIError(YouTubeExtractorError):
    """Raised for non-recoverable YouTube API errors."""


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


class YouTubeExtractor:
    """Client for retrieving YouTube channel metadata."""

    def __init__(
        self,
        api_key: str,
        *,
        request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        timeout_seconds: int = 30,
    ) -> None:
        if not api_key or api_key == "your_youtube_api_key_here":
            raise ValueError("A valid YOUTUBE_API_KEY must be provided.")

        self.api_key = api_key
        self.request_delay_seconds = request_delay_seconds
        self.timeout_seconds = timeout_seconds
        self.request_count = 0
        self.estimated_quota_units = 0

        retry_strategy = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def fetch_channels(self, channel_ids: Sequence[str]) -> list[ChannelData]:
        """Fetch metadata for one or more channel IDs."""
        normalized_ids = _normalize_channel_ids(channel_ids)
        if not normalized_ids:
            raise ValueError("At least one YouTube channel ID is required.")

        logger.info("Fetching metadata for %d channel(s)", len(normalized_ids))
        results: list[ChannelData] = []

        for batch_index, batch in enumerate(
            _chunked(normalized_ids, MAX_CHANNELS_PER_REQUEST), start=1
        ):
            logger.debug(
                "Processing batch %d with %d channel ID(s)", batch_index, len(batch)
            )
            batch_results = self._fetch_channel_batch(batch)
            results.extend(batch_results)

            if batch_index * MAX_CHANNELS_PER_REQUEST < len(normalized_ids):
                logger.debug(
                    "Sleeping %.2fs before next API request to reduce burst load",
                    self.request_delay_seconds,
                )
                time.sleep(self.request_delay_seconds)

        found_ids = {record.channel_id for record in results}
        missing_ids = set(normalized_ids) - found_ids
        if missing_ids:
            logger.warning(
                "No data returned for channel ID(s): %s",
                ", ".join(sorted(missing_ids)),
            )

        logger.info(
            "Completed extraction: %d record(s), %d API request(s), ~%d quota unit(s)",
            len(results),
            self.request_count,
            self.estimated_quota_units,
        )
        return results

    def _fetch_channel_batch(self, channel_ids: list[str]) -> list[ChannelData]:
        params = {
            "part": "snippet,statistics",
            "id": ",".join(channel_ids),
            "key": self.api_key,
        }

        response = self._request_channels(params)
        payload = response.json()
        items = payload.get("items", [])

        extracted_at = datetime.now(timezone.utc).isoformat()
        records: list[ChannelData] = []

        for item in items:
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            channel_id = item.get("id", "")

            if not channel_id:
                logger.warning("Skipping malformed API item without channel ID")
                continue

            records.append(
                ChannelData(
                    channel_id=channel_id,
                    title=snippet.get("title", ""),
                    description=snippet.get("description", ""),
                    view_count=_parse_int(statistics.get("viewCount")),
                    subscriber_count=_parse_int(statistics.get("subscriberCount")),
                    video_count=_parse_int(statistics.get("videoCount")),
                    extracted_at=extracted_at,
                )
            )

        return records

    def _request_channels(self, params: dict[str, str]) -> requests.Response:
        self.request_count += 1
        self.estimated_quota_units += 1  # channels.list costs 1 quota unit per call

        try:
            response = self.session.get(
                YOUTUBE_CHANNELS_URL,
                params=params,
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            logger.exception("Network error while calling YouTube Data API")
            raise YouTubeAPIError("Failed to reach YouTube Data API") from exc

        if response.ok:
            return response

        self._handle_api_error(response)
        raise YouTubeAPIError("Unexpected YouTube API failure")

    def _handle_api_error(self, response: requests.Response) -> None:
        try:
            error_payload = response.json()
            error_info = error_payload.get("error", {})
            message = error_info.get("message", response.text)
            reasons = [
                err.get("reason", "")
                for err in error_info.get("errors", [])
                if isinstance(err, dict)
            ]
        except ValueError:
            message = response.text
            reasons = []

        logger.error(
            "YouTube API error (%s): %s | reasons=%s",
            response.status_code,
            message,
            reasons,
        )

        if response.status_code == 403 and (
            "quotaExceeded" in reasons or "dailyLimitExceeded" in reasons
        ):
            raise QuotaExceededError(
                "YouTube API quota exceeded. Reduce request volume or retry after quota reset."
            )

        if response.status_code == 429 or "rateLimitExceeded" in reasons:
            raise RateLimitError(
                "YouTube API rate limit exceeded. Increase request delay or retry later."
            )

        raise YouTubeAPIError(
            f"YouTube API request failed with status {response.status_code}: {message}"
        )


class PostgreSQLWriter:
    """Persists extracted channel records to PostgreSQL."""

    CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS youtube_channels (
            channel_id VARCHAR(64) PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            view_count BIGINT,
            subscriber_count BIGINT,
            video_count BIGINT,
            extracted_at TIMESTAMPTZ NOT NULL
        );
    """

    UPSERT_SQL = """
        INSERT INTO youtube_channels (
            channel_id,
            title,
            description,
            view_count,
            subscriber_count,
            video_count,
            extracted_at
        )
        VALUES (
            %(channel_id)s,
            %(title)s,
            %(description)s,
            %(view_count)s,
            %(subscriber_count)s,
            %(video_count)s,
            %(extracted_at)s
        )
        ON CONFLICT (channel_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            view_count = EXCLUDED.view_count,
            subscriber_count = EXCLUDED.subscriber_count,
            video_count = EXCLUDED.video_count,
            extracted_at = EXCLUDED.extracted_at;
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ) -> None:
        self.connection_kwargs = {
            "host": host,
            "port": port,
            "dbname": database,
            "user": user,
            "password": password,
        }

    def save_channels(self, channels: Sequence[ChannelData]) -> int:
        if not channels:
            logger.info("No channel records to persist")
            return 0

        rows = [asdict(channel) for channel in channels]

        try:
            with psycopg2.connect(**self.connection_kwargs) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(self.CREATE_TABLE_SQL)
                    psycopg2.extras.execute_batch(cursor, self.UPSERT_SQL, rows)
                connection.commit()
        except psycopg2.Error as exc:
            logger.exception("Failed to persist channel data to PostgreSQL")
            raise YouTubeExtractorError("PostgreSQL write failed") from exc

        logger.info("Persisted %d channel record(s) to PostgreSQL", len(rows))
        return len(rows)


def _normalize_channel_ids(channel_ids: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for channel_id in channel_ids:
        cleaned = channel_id.strip()
        if not cleaned or cleaned in seen:
            continue
        normalized.append(cleaned)
        seen.add(cleaned)

    return normalized


def _parse_channel_ids_from_env() -> list[str]:
    channel_ids: list[str] = []

    env_ids = os.getenv("YOUTUBE_CHANNEL_IDS", "")
    if env_ids:
        channel_ids.extend(env_ids.split(","))

    single_id = os.getenv("YOUTUBE_CHANNEL_ID", "").strip()
    if single_id and single_id != "your_channel_id_here":
        channel_ids.append(single_id)

    return _normalize_channel_ids(channel_ids)


def extract_channels(
    channel_ids: Sequence[str],
    *,
    api_key: str | None = None,
    request_delay_seconds: float | None = None,
) -> list[dict[str, Any]]:
    """
    Programmatic entry point for other pipeline components.

    Returns a list of dictionaries suitable for JSON serialization or downstream ETL.
    """
    resolved_api_key = api_key or os.getenv("YOUTUBE_API_KEY", "")
    delay = request_delay_seconds
    if delay is None:
        delay = float(os.getenv("YOUTUBE_REQUEST_DELAY", DEFAULT_REQUEST_DELAY_SECONDS))

    extractor = YouTubeExtractor(resolved_api_key, request_delay_seconds=delay)
    records = extractor.fetch_channels(channel_ids)
    return [asdict(record) for record in records]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract YouTube channel metadata using the YouTube Data API v3."
    )
    parser.add_argument(
        "--channel-id",
        action="append",
        dest="channel_ids",
        default=[],
        help="YouTube channel ID. Can be passed multiple times.",
    )
    parser.add_argument(
        "--channel-ids",
        help="Comma-separated list of YouTube channel IDs.",
    )
    parser.add_argument(
        "--output",
        choices=("stdout", "json", "postgres"),
        default=os.getenv("EXTRACTOR_OUTPUT", "stdout"),
        help="Output destination for extracted records.",
    )
    parser.add_argument(
        "--output-file",
        default=os.getenv("EXTRACTOR_OUTPUT_FILE", "youtube_channels.json"),
        help="File path used when --output json is selected.",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=float(
            os.getenv("YOUTUBE_REQUEST_DELAY", DEFAULT_REQUEST_DELAY_SECONDS)
        ),
        help="Delay in seconds between batched API requests.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging verbosity.",
    )
    return parser


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)

    channel_ids = list(args.channel_ids)
    if args.channel_ids_csv := args.channel_ids:
        channel_ids.extend(args.channel_ids_csv.split(","))
    channel_ids.extend(_parse_channel_ids_from_env())
    channel_ids = _normalize_channel_ids(channel_ids)

    if not channel_ids:
        logger.error(
            "No channel IDs provided. Use --channel-id, --channel-ids, or YOUTUBE_CHANNEL_ID."
        )
        return 1

    try:
        extractor = YouTubeExtractor(
            os.getenv("YOUTUBE_API_KEY", ""),
            request_delay_seconds=args.request_delay,
        )
        records = extractor.fetch_channels(channel_ids)
        payload = [asdict(record) for record in records]

        if args.output == "stdout":
            print(json.dumps(payload, indent=2))
        elif args.output == "json":
            with open(args.output_file, "w", encoding="utf-8") as output_file:
                json.dump(payload, output_file, indent=2)
            logger.info("Wrote %d record(s) to %s", len(payload), args.output_file)
        elif args.output == "postgres":
            writer = PostgreSQLWriter(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "youtube_data"),
                user=os.getenv("POSTGRES_USER", "airflow"),
                password=os.getenv("POSTGRES_PASSWORD", ""),
            )
            writer.save_channels(records)

        return 0

    except QuotaExceededError as exc:
        logger.error("Quota exceeded: %s", exc)
        return 2
    except RateLimitError as exc:
        logger.error("Rate limit exceeded: %s", exc)
        return 3
    except (YouTubeExtractorError, ValueError) as exc:
        logger.error("Extraction failed: %s", exc)
        return 1
    except KeyboardInterrupt:
        logger.warning("Extraction interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())
