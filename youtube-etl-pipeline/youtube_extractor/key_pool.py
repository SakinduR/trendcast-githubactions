"""
================================================================================
key_pool.py — YouTube API Key Pool Manager
================================================================================
Manages a pool of YouTube Data API v3 keys with automatic rotation when a key's
daily quota is exhausted (HTTP 403 quotaExceeded).

Environment Variables:
  YOUTUBE_API_KEYS  — Comma-separated list of API keys (preferred)
  YOUTUBE_API_KEY   — Single API key (backward-compatible fallback)

Usage:
  pool = APIKeyPool.from_env()
  service = pool.get_service()          # returns a googleapiclient Resource
  # ... on quota error:
  pool.mark_exhausted()                 # marks current key as used-up
  service = pool.get_service()          # returns a new Resource with next key
================================================================================
"""

from __future__ import annotations

import logging
import os
import sys
from typing import List, Optional

from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError

logger = logging.getLogger("youtube_extractor")


class AllKeysExhaustedError(Exception):
    """Raised when every API key in the pool has hit its quota limit."""


class APIKeyPool:
    """
    Round-robin YouTube API key pool with automatic rotation on 403 errors.

    Design decisions:
    - Keys are tried in order; once a key is marked exhausted it is never
      retried during the same process lifetime (quota resets daily at
      midnight PST — no point retrying within the same run).
    - A new ``googleapiclient.discovery.Resource`` is built lazily each time
      the active key changes, because the developer key is baked into the
      Resource at construction time.
    - Thread safety is *not* a goal; each worker / task should own its own
      pool instance.
    """

    def __init__(self, api_keys: List[str]) -> None:
        if not api_keys:
            logger.error("No YouTube API keys provided")
            raise ValueError("At least one YouTube API key is required")

        self._keys: List[str] = api_keys
        self._exhausted: set[int] = set()  # indices of exhausted keys
        self._current_index: int = 0
        self._service: Optional[Resource] = None

        logger.info(
            "API key pool initialised",
            extra={"pool_size": len(self._keys)},
        )

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    @classmethod
    def from_env(cls) -> "APIKeyPool":
        """
        Build an APIKeyPool from environment variables.

        Priority:
          1. YOUTUBE_API_KEYS  (comma-separated, e.g. "key1,key2,key3")
          2. YOUTUBE_API_KEY   (single key — backward compatibility)
        """
        raw_keys = os.getenv("YOUTUBE_API_KEYS", "")
        if raw_keys:
            keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
        else:
            single = os.getenv("YOUTUBE_API_KEY", "").strip()
            keys = [single] if single else []

        if not keys:
            logger.error(
                "Required environment variable missing: "
                "set YOUTUBE_API_KEYS or YOUTUBE_API_KEY"
            )
            sys.exit(1)

        return cls(keys)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @property
    def active_key(self) -> str:
        """Return the currently active API key."""
        return self._keys[self._current_index]

    @property
    def pool_size(self) -> int:
        """Total number of keys in the pool."""
        return len(self._keys)

    @property
    def remaining_keys(self) -> int:
        """Number of keys that have not been marked exhausted."""
        return len(self._keys) - len(self._exhausted)

    def get_service(self) -> Resource:
        """
        Return a ``googleapiclient.discovery.Resource`` for the active key.

        A new Resource is built only when the active key has changed since
        the last call (or on first invocation).
        """
        if self._service is None:
            self._service = self._build_service(self.active_key)
        return self._service

    def mark_exhausted(self) -> None:
        """
        Mark the current key as quota-exhausted and rotate to the next
        available key.

        Raises:
            AllKeysExhaustedError: If no more keys are available.
        """
        idx = self._current_index
        masked_key = self._mask_key(self._keys[idx])

        self._exhausted.add(idx)
        logger.warning(
            "API key marked as quota-exhausted",
            extra={
                "key": masked_key,
                "exhausted_count": len(self._exhausted),
                "pool_size": len(self._keys),
            },
        )

        # Attempt to find the next non-exhausted key
        if not self._rotate():
            raise AllKeysExhaustedError(
                f"All {len(self._keys)} API key(s) have been exhausted. "
                "Quota resets at midnight PST."
            )

    def execute_with_rotation(self, request_builder_fn):
        """
        Execute a YouTube API request, automatically rotating to the next
        key on HTTP 403 (quota exhausted) errors.

        Args:
            request_builder_fn: A callable that accepts a
                ``googleapiclient.discovery.Resource`` and returns an
                ``HttpRequest`` (i.e. the result of
                ``service.channels().list(...)``).

        Returns:
            The parsed API response dict.

        Raises:
            AllKeysExhaustedError: If every key has been exhausted.
            HttpError: On non-quota API errors (e.g. 400, 404).
        """
        while True:
            service = self.get_service()
            try:
                request = request_builder_fn(service)
                return request.execute()
            except HttpError as exc:
                if exc.resp.status == 403 and self._is_quota_error(exc):
                    logger.warning(
                        "Quota exceeded — rotating to next API key",
                        extra={"error": str(exc)},
                    )
                    self.mark_exhausted()  # raises AllKeysExhaustedError if none left
                    continue
                raise  # Non-quota errors propagate immediately

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _rotate(self) -> bool:
        """
        Advance ``_current_index`` to the next non-exhausted key.

        Returns True if a usable key was found, False if all are exhausted.
        """
        n = len(self._keys)
        for offset in range(1, n + 1):
            candidate = (self._current_index + offset) % n
            if candidate not in self._exhausted:
                self._current_index = candidate
                self._service = None  # force rebuild on next get_service()
                masked = self._mask_key(self._keys[candidate])
                logger.info(
                    "Rotated to next API key",
                    extra={
                        "key": masked,
                        "remaining_keys": self.remaining_keys,
                    },
                )
                return True
        return False

    def _build_service(self, api_key: str) -> Resource:
        """Build a YouTube Data API v3 service resource."""
        service = build(
            "youtube", "v3", developerKey=api_key, cache_discovery=False
        )
        masked = self._mask_key(api_key)
        logger.info(
            "YouTube API service built",
            extra={"key": masked},
        )
        return service

    @staticmethod
    def _mask_key(key: str) -> str:
        """Mask an API key for safe logging (show first 4 + last 4 chars)."""
        if len(key) <= 8:
            return "****"
        return f"{key[:4]}…{key[-4:]}"

    @staticmethod
    def _is_quota_error(exc: HttpError) -> bool:
        """Check whether an HttpError is specifically a quota exhaustion error."""
        error_str = str(exc).lower()
        return any(
            reason in error_str
            for reason in ("quotaexceeded", "dailylimitexceeded", "rateLimitExceeded".lower())
        )
