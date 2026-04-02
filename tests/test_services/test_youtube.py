"""Tests for YouTubeClient class and YouTube helper functions."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.services.youtube import (
    YouTubeClient,
    parse_date_range,
    parse_iso_duration,
    uploads_playlist_id,
)


class TestYouTubeClientInit:
    @pytest.mark.unit
    def test_stores_config(self):
        config = {"page_size": 25, "enrichment_batch_size": 10, "max_retries": 2, "retry_backoff_base": 3}
        client = YouTubeClient("fake-key", config)
        assert client.api_key == "fake-key"
        assert client.page_size == 25
        assert client.batch_size == 10
        assert client.max_retries == 2
        assert client.backoff_base == 3
        assert client.quota_used == 0

    @pytest.mark.unit
    def test_default_config_values(self):
        client = YouTubeClient("key", {})
        assert client.page_size == 50
        assert client.batch_size == 50
        assert client.max_retries == 3


class TestYouTubeClientApiGet:
    @pytest.mark.integration
    def test_retries_on_429(self, mocker):
        client = YouTubeClient("key", {"max_retries": 2, "retry_backoff_base": 1})
        mock_429 = MagicMock(status_code=429)
        mock_200 = MagicMock(status_code=200)
        mock_200.json.return_value = {"ok": True}

        mocker.patch("src.services.youtube.requests.get", side_effect=[mock_429, mock_200])
        mocker.patch("src.services.youtube.time.sleep")

        result = client.api_get("https://example.com", {})
        assert result == {"ok": True}

    @pytest.mark.integration
    def test_no_retry_on_403(self, mocker):
        client = YouTubeClient("key", {"max_retries": 3})
        mock_403 = MagicMock(status_code=403)
        mock_403.raise_for_status.side_effect = Exception("403")
        mocker.patch("src.services.youtube.requests.get", return_value=mock_403)

        with pytest.raises(Exception, match="403"):
            client.api_get("https://example.com", {})


class TestYouTubeClientEnrich:
    @pytest.mark.integration
    def test_tracks_quota(self, mocker):
        client = YouTubeClient("key", {"enrichment_batch_size": 50})
        mocker.patch.object(
            client,
            "api_get",
            return_value={
                "items": [
                    {
                        "id": "v1",
                        "contentDetails": {"duration": "PT5M"},
                        "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "5"},
                    }
                ]
            },
        )

        details, calls = client.enrich_videos(["v1"])
        assert calls == 1
        assert client.quota_used == 1
        assert details["v1"]["duration_seconds"] == 300

    @pytest.mark.integration
    def test_batches_correctly(self, mocker):
        client = YouTubeClient("key", {"enrichment_batch_size": 2})
        mocker.patch.object(client, "api_get", return_value={"items": []})

        details, calls = client.enrich_videos(["v1", "v2", "v3", "v4", "v5"])
        assert calls == 3  # 5 / 2 = 3 batches


class TestYouTubeClientSearchVideos:
    @pytest.mark.integration
    def test_quota_100_for_search(self, mocker):
        client = YouTubeClient("key", {"enrichment_batch_size": 50})
        mocker.patch.object(
            client,
            "api_get",
            side_effect=[
                {"items": []},  # search returns empty
            ],
        )

        videos, quota = client.fetch_search_videos("UC123", "viewCount")
        assert videos == []
        assert quota == 100


# ─── Pure helper functions ───────────────────────────────────────────────────


class TestUploadsPlaylistId:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            ("UCxxxxxxxx", "UUxxxxxxxx"),
            ("UCddiYMoUpa0oeICKWQyGkn2Q", "UUddiYMoUpa0oeICKWQyGkn2Q"),
        ],
        ids=["standard", "real-id"],
    )
    def test_converts(self, give, want):
        assert uploads_playlist_id(give) == want


class TestParseIsoDuration:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            ("PT3M45S", 225),
            ("PT1H2M3S", 3723),
            ("PT10M", 600),
            ("", 0),
            (None, 0),
        ],
        ids=["minutes-seconds", "full", "minutes-only", "empty", "none"],
    )
    def test_parses(self, give, want):
        assert parse_iso_duration(give) == want


class TestParseDateRange:
    @pytest.mark.unit
    def test_all_returns_epoch(self):
        result = parse_date_range("all")
        assert result == datetime(1970, 1, 1, tzinfo=timezone.utc)

    @pytest.mark.unit
    def test_absolute_date(self):
        result = parse_date_range("20230615")
        assert result == datetime(2023, 6, 15, tzinfo=timezone.utc)

    @pytest.mark.unit
    def test_relative_months(self):
        result = parse_date_range("today-6months")
        now = datetime.now(timezone.utc)
        diff = (now - result).days
        assert 170 < diff < 200
