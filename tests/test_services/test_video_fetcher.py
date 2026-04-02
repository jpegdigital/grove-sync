"""Tests for VideoFetcher — cache-aware video candidate fetching."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.services.video_fetcher import VideoFetcher

# ─── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def mock_yt():
    return MagicMock()


@pytest.fixture
def fetcher(mock_yt, tmp_path):
    return VideoFetcher(mock_yt, tmp_path)


# ═════════════════════════════════════════════════════════════════════════════
# fetch_playlist
# ═════════════════════════════════════════════════════════════════════════════


class TestFetchPlaylist:
    """fetch_playlist: playlist API call with cache support."""

    @pytest.mark.unit
    def test_api_call_returns_candidates_and_quota(self, fetcher, mock_yt):
        """from_cache=False calls YouTube API, returns candidates + quota."""
        candidates = [{"video_id": "v1", "title": "Video 1"}]
        mock_yt.fetch_playlist_videos.return_value = (candidates, 50, 3)

        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        result, quota = fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        assert result == candidates
        assert quota == 3
        mock_yt.fetch_playlist_videos.assert_called_once_with("UU123", 50, cutoff, 3)

    @pytest.mark.unit
    def test_api_call_saves_to_cache(self, fetcher, mock_yt, tmp_path):
        """Successful API fetch writes cache file."""
        candidates = [{"video_id": "v1", "title": "Video 1"}]
        mock_yt.fetch_playlist_videos.return_value = (candidates, 50, 2)

        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        cache_file = tmp_path / "UC123_recent.json"
        assert cache_file.exists()

    @pytest.mark.unit
    def test_empty_api_result_does_not_save_cache(self, fetcher, mock_yt, tmp_path):
        """Empty API result should not write a cache file."""
        mock_yt.fetch_playlist_videos.return_value = ([], 0, 1)

        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        cache_file = tmp_path / "UC123_recent.json"
        assert not cache_file.exists()

    @pytest.mark.unit
    def test_cache_hit_returns_data_with_zero_quota(self, fetcher, mock_yt, tmp_path):
        """from_cache=True with existing cache returns data and quota=0."""
        # Seed cache via API call
        candidates = [{"video_id": "v1", "title": "Video 1"}]
        mock_yt.fetch_playlist_videos.return_value = (candidates, 50, 2)
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        mock_yt.reset_mock()

        # Now fetch from cache
        result, quota = fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3, from_cache=True)

        assert result == candidates
        assert quota == 0
        mock_yt.fetch_playlist_videos.assert_not_called()

    @pytest.mark.unit
    def test_cache_miss_returns_empty_with_zero_quota(self, fetcher, mock_yt):
        """from_cache=True with no cache file returns empty list + quota=0."""
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        result, quota = fetcher.fetch_playlist("UC_MISSING", "UU_MISSING", 50, cutoff, 3, from_cache=True)

        assert result == []
        assert quota == 0
        mock_yt.fetch_playlist_videos.assert_not_called()

    @pytest.mark.edge_cases
    def test_corrupt_cache_returns_empty(self, fetcher, mock_yt, tmp_path):
        """from_cache=True with corrupt JSON returns empty + quota=0."""
        corrupt_file = tmp_path / "UC123_recent.json"
        corrupt_file.write_text("{invalid json", encoding="utf-8")

        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        result, quota = fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3, from_cache=True)

        assert result == []
        assert quota == 0

    @pytest.mark.unit
    def test_api_call_overwrites_existing_cache(self, fetcher, mock_yt, tmp_path):
        """Second API fetch overwrites the cache file."""
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)

        mock_yt.fetch_playlist_videos.return_value = ([{"video_id": "v1"}], 1, 1)
        fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        mock_yt.fetch_playlist_videos.return_value = ([{"video_id": "v2"}], 1, 1)
        fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        # Load from cache — should have v2, not v1
        result, _ = fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3, from_cache=True)
        assert result == [{"video_id": "v2"}]

    @pytest.mark.unit
    def test_from_cache_false_hits_api_even_if_cache_exists(self, fetcher, mock_yt):
        """from_cache=False always calls API, ignoring existing cache."""
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)

        # Seed cache
        mock_yt.fetch_playlist_videos.return_value = ([{"video_id": "v1"}], 1, 1)
        fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        # Fetch again with from_cache=False — should call API, not read cache
        mock_yt.fetch_playlist_videos.return_value = ([{"video_id": "v2"}], 1, 2)
        result, quota = fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3, from_cache=False)

        assert result == [{"video_id": "v2"}]
        assert quota == 2
        assert mock_yt.fetch_playlist_videos.call_count == 2

    @pytest.mark.safety
    def test_api_exception_propagates(self, fetcher, mock_yt):
        """YouTube API errors propagate to caller — fetcher does not catch."""
        mock_yt.fetch_playlist_videos.side_effect = RuntimeError("API down")

        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(RuntimeError, match="API down"):
            fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)


# ═════════════════════════════════════════════════════════════════════════════
# fetch_search_pair
# ═════════════════════════════════════════════════════════════════════════════


class TestFetchSearchPair:
    """fetch_search_pair: popular + rated search calls with combined cache."""

    @pytest.mark.unit
    def test_api_call_returns_popular_rated_and_quota(self, fetcher, mock_yt):
        """from_cache=False calls search twice and returns combined quota."""
        popular = [{"video_id": "p1"}]
        rated = [{"video_id": "r1"}]
        mock_yt.fetch_search_videos.side_effect = [(popular, 101), (rated, 101)]

        pop, rat, quota = fetcher.fetch_search_pair("UC123")

        assert pop == popular
        assert rat == rated
        assert quota == 202
        assert mock_yt.fetch_search_videos.call_count == 2

    @pytest.mark.unit
    def test_api_call_saves_both_to_cache(self, fetcher, mock_yt, tmp_path):
        """Successful search saves popular + rated together."""
        mock_yt.fetch_search_videos.side_effect = [
            ([{"video_id": "p1"}], 101),
            ([{"video_id": "r1"}], 101),
        ]

        fetcher.fetch_search_pair("UC123")

        cache_file = tmp_path / "UC123_full.json"
        assert cache_file.exists()

    @pytest.mark.unit
    def test_cache_hit_returns_both_with_zero_quota(self, fetcher, mock_yt):
        """from_cache=True returns cached popular + rated, quota=0."""
        mock_yt.fetch_search_videos.side_effect = [
            ([{"video_id": "p1"}], 101),
            ([{"video_id": "r1"}], 101),
        ]
        fetcher.fetch_search_pair("UC123")
        mock_yt.reset_mock()

        pop, rat, quota = fetcher.fetch_search_pair("UC123", from_cache=True)

        assert pop == [{"video_id": "p1"}]
        assert rat == [{"video_id": "r1"}]
        assert quota == 0
        mock_yt.fetch_search_videos.assert_not_called()

    @pytest.mark.unit
    def test_cache_miss_returns_empty(self, fetcher, mock_yt):
        """from_cache=True with no cache returns empty lists + quota=0."""
        pop, rat, quota = fetcher.fetch_search_pair("UC_MISSING", from_cache=True)

        assert pop == []
        assert rat == []
        assert quota == 0

    @pytest.mark.edge_cases
    def test_corrupt_cache_returns_empty(self, fetcher, mock_yt, tmp_path):
        """from_cache=True with corrupt cache returns empty."""
        corrupt_file = tmp_path / "UC123_full.json"
        corrupt_file.write_text("not json", encoding="utf-8")

        pop, rat, quota = fetcher.fetch_search_pair("UC123", from_cache=True)

        assert pop == []
        assert rat == []
        assert quota == 0

    @pytest.mark.safety
    def test_first_search_exception_propagates_no_cache(self, fetcher, mock_yt, tmp_path):
        """If popular search fails, exception propagates and nothing is cached."""
        mock_yt.fetch_search_videos.side_effect = RuntimeError("quota exceeded")

        with pytest.raises(RuntimeError, match="quota exceeded"):
            fetcher.fetch_search_pair("UC123")

        cache_file = tmp_path / "UC123_full.json"
        assert not cache_file.exists()

    @pytest.mark.safety
    def test_second_search_exception_propagates_no_cache(self, fetcher, mock_yt, tmp_path):
        """If rated search fails after popular succeeds, exception propagates, no partial cache."""
        mock_yt.fetch_search_videos.side_effect = [
            ([{"video_id": "p1"}], 101),
            RuntimeError("rating search failed"),
        ]

        with pytest.raises(RuntimeError, match="rating search failed"):
            fetcher.fetch_search_pair("UC123")

        cache_file = tmp_path / "UC123_full.json"
        assert not cache_file.exists()

    @pytest.mark.unit
    def test_both_empty_does_not_save_cache(self, fetcher, mock_yt, tmp_path):
        """If both searches return empty, don't write cache."""
        mock_yt.fetch_search_videos.side_effect = [([], 101), ([], 101)]

        fetcher.fetch_search_pair("UC123")

        cache_file = tmp_path / "UC123_full.json"
        assert not cache_file.exists()


# ═════════════════════════════════════════════════════════════════════════════
# Cache internals
# ═════════════════════════════════════════════════════════════════════════════


class TestCacheInternals:
    """Verify cache file format and metadata."""

    @pytest.mark.unit
    def test_cache_file_contains_metadata(self, fetcher, mock_yt, tmp_path):
        """Cache file includes channel_id, mode, and cached_at timestamp."""
        import json

        mock_yt.fetch_playlist_videos.return_value = ([{"video_id": "v1"}], 1, 1)
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        data = json.loads((tmp_path / "UC123_recent.json").read_text(encoding="utf-8"))
        assert data["channel_id"] == "UC123"
        assert data["mode"] == "recent"
        assert "cached_at" in data

    @pytest.mark.unit
    def test_cache_dir_created_if_missing(self, mock_yt, tmp_path):
        """Cache dir is created on first save if it doesn't exist."""
        nested = tmp_path / "deep" / "cache"
        fetcher = VideoFetcher(mock_yt, nested)

        mock_yt.fetch_playlist_videos.return_value = ([{"video_id": "v1"}], 1, 1)
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        fetcher.fetch_playlist("UC123", "UU123", 50, cutoff, 3)

        assert nested.exists()
        assert (nested / "UC123_recent.json").exists()
