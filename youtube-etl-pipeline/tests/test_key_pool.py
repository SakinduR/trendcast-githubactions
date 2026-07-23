"""
================================================================================
test_key_pool.py — Unit tests for the YouTube API Key Pool Manager
================================================================================
Tests cover:
  - Pool initialisation with single and multiple keys
  - Key rotation on quota exhaustion (HTTP 403)
  - AllKeysExhaustedError when every key is used up
  - execute_with_rotation() automatic retry with next key
  - Backward compatibility with YOUTUBE_API_KEY fallback
================================================================================
"""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError
import httplib2

# Ensure the project root is importable
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from youtube_extractor.key_pool import APIKeyPool, AllKeysExhaustedError


def _make_http_error(status: int, reason: str = "quotaExceeded") -> HttpError:
    """Helper to construct a mock HttpError."""
    resp = httplib2.Response({"status": status})
    content = f'{{"error": {{"errors": [{{"reason": "{reason}"}}], "code": {status}, "message": "{reason}"}}}}'
    return HttpError(resp, content.encode("utf-8"))


class TestAPIKeyPoolInit(unittest.TestCase):
    """Tests for pool construction and from_env()."""

    def test_init_single_key(self):
        pool = APIKeyPool(["key_abc"])
        self.assertEqual(pool.pool_size, 1)
        self.assertEqual(pool.active_key, "key_abc")
        self.assertEqual(pool.remaining_keys, 1)

    def test_init_multiple_keys(self):
        pool = APIKeyPool(["k1", "k2", "k3"])
        self.assertEqual(pool.pool_size, 3)
        self.assertEqual(pool.active_key, "k1")
        self.assertEqual(pool.remaining_keys, 3)

    def test_init_empty_raises(self):
        with self.assertRaises(ValueError):
            APIKeyPool([])

    @patch.dict(os.environ, {"YOUTUBE_API_KEYS": "a,b,c"}, clear=False)
    def test_from_env_multi_keys(self):
        pool = APIKeyPool.from_env()
        self.assertEqual(pool.pool_size, 3)
        self.assertEqual(pool.active_key, "a")

    @patch.dict(os.environ, {"YOUTUBE_API_KEYS": "", "YOUTUBE_API_KEY": "fallback_key"}, clear=False)
    def test_from_env_fallback_single_key(self):
        pool = APIKeyPool.from_env()
        self.assertEqual(pool.pool_size, 1)
        self.assertEqual(pool.active_key, "fallback_key")

    @patch.dict(os.environ, {"YOUTUBE_API_KEYS": "  key1 , key2 ,  key3  "}, clear=False)
    def test_from_env_trims_whitespace(self):
        pool = APIKeyPool.from_env()
        self.assertEqual(pool.pool_size, 3)
        self.assertEqual(pool.active_key, "key1")


class TestKeyRotation(unittest.TestCase):
    """Tests for mark_exhausted() and rotation behaviour."""

    def test_rotate_to_next_key(self):
        pool = APIKeyPool(["k1", "k2", "k3"])
        self.assertEqual(pool.active_key, "k1")

        pool.mark_exhausted()
        self.assertEqual(pool.active_key, "k2")
        self.assertEqual(pool.remaining_keys, 2)

    def test_rotate_wraps_around(self):
        pool = APIKeyPool(["k1", "k2", "k3"])
        pool.mark_exhausted()  # k1 → k2
        pool.mark_exhausted()  # k2 → k3
        self.assertEqual(pool.active_key, "k3")
        self.assertEqual(pool.remaining_keys, 1)

    def test_all_keys_exhausted_raises(self):
        pool = APIKeyPool(["k1", "k2"])
        pool.mark_exhausted()  # k1 → k2
        with self.assertRaises(AllKeysExhaustedError):
            pool.mark_exhausted()  # k2 → none left

    def test_single_key_exhausted_raises(self):
        pool = APIKeyPool(["only_key"])
        with self.assertRaises(AllKeysExhaustedError):
            pool.mark_exhausted()

    def test_remaining_keys_count(self):
        pool = APIKeyPool(["a", "b", "c", "d"])
        self.assertEqual(pool.remaining_keys, 4)
        pool.mark_exhausted()
        self.assertEqual(pool.remaining_keys, 3)
        pool.mark_exhausted()
        self.assertEqual(pool.remaining_keys, 2)


class TestGetService(unittest.TestCase):
    """Tests for get_service() lazy construction."""

    @patch("youtube_extractor.key_pool.build")
    def test_get_service_builds_once(self, mock_build):
        mock_build.return_value = MagicMock()
        pool = APIKeyPool(["k1"])

        svc1 = pool.get_service()
        svc2 = pool.get_service()

        self.assertIs(svc1, svc2)
        mock_build.assert_called_once()

    @patch("youtube_extractor.key_pool.build")
    def test_get_service_rebuilds_after_rotation(self, mock_build):
        svc_a = MagicMock(name="service_a")
        svc_b = MagicMock(name="service_b")
        mock_build.side_effect = [svc_a, svc_b]

        pool = APIKeyPool(["k1", "k2"])
        first = pool.get_service()
        self.assertIs(first, svc_a)

        pool.mark_exhausted()
        second = pool.get_service()
        self.assertIs(second, svc_b)
        self.assertEqual(mock_build.call_count, 2)


class TestExecuteWithRotation(unittest.TestCase):
    """Tests for execute_with_rotation() automatic retry on quota errors."""

    @patch("youtube_extractor.key_pool.build")
    def test_success_on_first_key(self, mock_build):
        mock_service = MagicMock()
        mock_request = MagicMock()
        mock_request.execute.return_value = {"items": []}
        mock_service.channels.return_value.list.return_value = mock_request
        mock_build.return_value = mock_service

        pool = APIKeyPool(["k1", "k2"])
        result = pool.execute_with_rotation(
            lambda svc: svc.channels().list(part="snippet", id="UC123")
        )
        self.assertEqual(result, {"items": []})

    @patch("youtube_extractor.key_pool.build")
    def test_rotates_on_quota_error(self, mock_build):
        # First service raises 403 quota error
        mock_svc1 = MagicMock()
        mock_req1 = MagicMock()
        mock_req1.execute.side_effect = _make_http_error(403, "quotaExceeded")
        mock_svc1.channels.return_value.list.return_value = mock_req1

        # Second service succeeds
        mock_svc2 = MagicMock()
        mock_req2 = MagicMock()
        mock_req2.execute.return_value = {"items": [{"id": "UC123"}]}
        mock_svc2.channels.return_value.list.return_value = mock_req2

        mock_build.side_effect = [mock_svc1, mock_svc2]

        pool = APIKeyPool(["k1", "k2"])
        result = pool.execute_with_rotation(
            lambda svc: svc.channels().list(part="snippet", id="UC123")
        )
        self.assertEqual(result, {"items": [{"id": "UC123"}]})
        self.assertEqual(pool.active_key, "k2")

    @patch("youtube_extractor.key_pool.build")
    def test_all_keys_exhausted_during_execute(self, mock_build):
        mock_svc = MagicMock()
        mock_req = MagicMock()
        mock_req.execute.side_effect = _make_http_error(403, "quotaExceeded")
        mock_svc.channels.return_value.list.return_value = mock_req
        mock_build.return_value = mock_svc

        pool = APIKeyPool(["k1"])
        with self.assertRaises(AllKeysExhaustedError):
            pool.execute_with_rotation(
                lambda svc: svc.channels().list(part="snippet", id="UC123")
            )

    @patch("youtube_extractor.key_pool.build")
    def test_non_quota_error_propagates(self, mock_build):
        mock_svc = MagicMock()
        mock_req = MagicMock()
        mock_req.execute.side_effect = _make_http_error(400, "badRequest")
        mock_svc.channels.return_value.list.return_value = mock_req
        mock_build.return_value = mock_svc

        pool = APIKeyPool(["k1", "k2"])
        with self.assertRaises(HttpError):
            pool.execute_with_rotation(
                lambda svc: svc.channels().list(part="snippet", id="UC123")
            )
        # Key should NOT have rotated on a non-quota error
        self.assertEqual(pool.active_key, "k1")


class TestMaskKey(unittest.TestCase):
    """Tests for the _mask_key() helper."""

    def test_normal_key(self):
        self.assertEqual(APIKeyPool._mask_key("AIzaSyD12345XYZ"), "AIza…5XYZ")

    def test_short_key(self):
        self.assertEqual(APIKeyPool._mask_key("abc"), "****")


if __name__ == "__main__":
    unittest.main()
