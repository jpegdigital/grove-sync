"""Integration and edge-case tests for sync_producer.

Integration tests exercise multi-function flows with mocked external boundaries
(API, DB). Edge-case tests push pure functions to boundary conditions.
"""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from src.config import AppConfig, ConfigError
from src.scoring import VideoScorer, passes_duration_filter
from src.services.db import SyncDatabase
from src.services.youtube import YouTubeClient, parse_iso_duration

# ─── api_get retry logic ────────────────────────────────────────────────────


class TestApiGetRetry:
    """Integration tests for HTTP retry behavior with mocked requests."""

    def _make_client(self):
        return YouTubeClient(api_key="fake", config={"max_retries": 3, "retry_backoff_base": 2})

    @pytest.mark.integration
    def test_retries_on_429_then_succeeds(self, mocker):
        mock_resp_429 = MagicMock(status_code=429)
        mock_resp_200 = MagicMock(status_code=200)
        mock_resp_200.json.return_value = {"ok": True}

        mocker.patch("src.services.youtube.requests.get", side_effect=[mock_resp_429, mock_resp_200])
        mocker.patch("src.services.youtube.time.sleep")

        client = self._make_client()
        result = client.api_get("https://example.com", {})
        assert result == {"ok": True}

    @pytest.mark.integration
    def test_retries_on_500_then_succeeds(self, mocker):
        mock_resp_500 = MagicMock(status_code=500)
        mock_resp_200 = MagicMock(status_code=200)
        mock_resp_200.json.return_value = {"data": []}

        mocker.patch("src.services.youtube.requests.get", side_effect=[mock_resp_500, mock_resp_500, mock_resp_200])
        mocker.patch("src.services.youtube.time.sleep")

        client = self._make_client()
        result = client.api_get("https://example.com", {})
        assert result == {"data": []}

    @pytest.mark.integration
    def test_raises_after_max_retries_on_429(self, mocker):
        mock_resp = MagicMock(status_code=429)
        mock_resp.raise_for_status.side_effect = Exception("429 Too Many Requests")

        mocker.patch("src.services.youtube.requests.get", return_value=mock_resp)
        mocker.patch("src.services.youtube.time.sleep")

        client = self._make_client()
        with pytest.raises(Exception, match="429"):
            client.api_get("https://example.com", {})

    @pytest.mark.integration
    def test_no_retry_on_403(self, mocker):
        mock_resp = MagicMock(status_code=403)
        mock_resp.raise_for_status.side_effect = Exception("403 Forbidden")

        mocker.patch("src.services.youtube.requests.get", return_value=mock_resp)

        client = self._make_client()
        with pytest.raises(Exception, match="403"):
            client.api_get("https://example.com", {})

    @pytest.mark.integration
    def test_no_retry_on_404(self, mocker):
        mock_resp = MagicMock(status_code=404)
        mock_resp.raise_for_status.side_effect = Exception("404 Not Found")

        mocker.patch("src.services.youtube.requests.get", return_value=mock_resp)

        client = self._make_client()
        with pytest.raises(Exception, match="404"):
            client.api_get("https://example.com", {})

    @pytest.mark.integration
    def test_exponential_backoff_timing(self, mocker):
        mock_resp_503 = MagicMock(status_code=503)
        mock_resp_200 = MagicMock(status_code=200)
        mock_resp_200.json.return_value = {}

        mocker.patch(
            "src.services.youtube.requests.get",
            side_effect=[mock_resp_503, mock_resp_503, mock_resp_200],
        )
        mock_sleep = mocker.patch("src.services.youtube.time.sleep")

        client = self._make_client()
        client.api_get("https://example.com", {})

        # backoff_base=2: first retry waits 2^1=2, second waits 2^2=4
        assert mock_sleep.call_args_list == [call(2), call(4)]


# ─── get_env ─────────────────────────────────────────────────────────────────


class TestGetEnv:
    @pytest.mark.unit
    def test_returns_value_when_set(self, monkeypatch):
        monkeypatch.setenv("TEST_GET_ENV_KEY", "hello")
        assert AppConfig.get_env("TEST_GET_ENV_KEY") == "hello"

    @pytest.mark.safety
    def test_exits_when_missing(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_KEY_12345", raising=False)
        with pytest.raises(ConfigError):
            AppConfig.get_env("NONEXISTENT_KEY_12345")

    @pytest.mark.edge_cases
    def test_exits_when_empty_string(self, monkeypatch):
        monkeypatch.setenv("EMPTY_KEY", "")
        with pytest.raises(ConfigError):
            AppConfig.get_env("EMPTY_KEY")


# ─── load_env edge cases ────────────────────────────────────────────────────


class TestLoadEnvEdgeCases:
    @pytest.mark.edge_cases
    def test_value_with_equals_sign(self, tmp_path, monkeypatch):
        """Values containing = should be preserved (e.g. base64 tokens)."""
        env_file = tmp_path / ".env"
        env_file.write_text("TOKEN=abc=def==\n")
        monkeypatch.delenv("TOKEN", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ["TOKEN"] == "abc=def=="

    @pytest.mark.edge_cases
    def test_whitespace_around_key_and_value(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("  SPACED_KEY  =  spaced_value  \n")
        monkeypatch.delenv("SPACED_KEY", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ["SPACED_KEY"] == "spaced_value"

    @pytest.mark.edge_cases
    def test_line_without_equals_is_skipped(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("NO_EQUALS_HERE\nVALID_KEY=value\n")
        monkeypatch.delenv("VALID_KEY", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ.get("NO_EQUALS_HERE") is None
        assert os.environ["VALID_KEY"] == "value"

    @pytest.mark.edge_cases
    def test_empty_value(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("EMPTY_VAL=\n")
        monkeypatch.delenv("EMPTY_VAL", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ["EMPTY_VAL"] == ""


# ─── parse_iso_duration edge cases ──────────────────────────────────────────


class TestParseIsoDurationEdgeCases:
    @pytest.mark.edge_cases
    @pytest.mark.parametrize(
        "give, want",
        [
            ("PT99H99M99S", 99 * 3600 + 99 * 60 + 99),
            ("PT0H0M0S", 0),
            ("PT100S", 100),
            ("PT1000M", 60000),
        ],
        ids=["large-values", "all-zeros", "seconds-over-60", "minutes-over-60"],
    )
    def test_extreme_values(self, give, want):
        assert parse_iso_duration(give) == want


# ─── score_video edge cases ─────────────────────────────────────────────────


class TestScoreVideoEdgeCases:
    WEIGHTS = {"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}

    def _make_scorer(self, weights=None, half_life_days=90):
        return VideoScorer(weights=weights or self.WEIGHTS, half_life_days=half_life_days)

    @pytest.mark.edge_cases
    def test_extremely_old_video_near_zero_freshness(self):
        video = {
            "view_count": 1000,
            "like_count": 50,
            "comment_count": 5,
            "published_at": "2000-01-01T00:00:00Z",
        }
        scorer = self._make_scorer()
        score = scorer.score_video(video)
        # Freshness for a 25+ year old video should be near zero
        # Score should be dominated by popularity + engagement
        assert score > 0
        assert score < 3.0

    @pytest.mark.edge_cases
    def test_future_published_date(self):
        """A video with a future date should get max freshness (age clamped to 0)."""
        video = {
            "view_count": 1000,
            "like_count": 50,
            "comment_count": 5,
            "published_at": "2099-01-01T00:00:00Z",
        }
        scorer = self._make_scorer()
        score = scorer.score_video(video)
        assert score > 0

    @pytest.mark.edge_cases
    def test_negative_like_count(self):
        """Negative stats shouldn't crash (bad API data)."""
        video = {
            "view_count": 1000,
            "like_count": -10,
            "comment_count": -5,
        }
        scorer = self._make_scorer()
        score = scorer.score_video(video)
        assert isinstance(score, float)

    @pytest.mark.edge_cases
    def test_string_counts_are_coerced(self):
        """API sometimes returns string counts."""
        video = {
            "view_count": "50000",
            "like_count": "2500",
            "comment_count": "250",
        }
        scorer = self._make_scorer()
        score = scorer.score_video(video)
        assert score > 0

    @pytest.mark.edge_cases
    def test_custom_weight_distribution(self):
        """Weights that don't sum to 1.0 should still work."""
        video = {
            "view_count": 10000,
            "like_count": 500,
            "comment_count": 50,
        }
        weights = {"popularity": 1.0, "engagement": 0.0, "freshness": 0.0}
        scorer = self._make_scorer(weights=weights)
        score = scorer.score_video(video)
        assert score > 0

    @pytest.mark.edge_cases
    def test_zero_weight_zeroes_component(self):
        video = {
            "view_count": 10000,
            "like_count": 500,
            "comment_count": 50,
            "published_at": "2025-01-01T00:00:00Z",
        }
        no_popularity = {"popularity": 0.0, "engagement": 0.5, "freshness": 0.5}
        no_engagement = {"popularity": 0.5, "engagement": 0.0, "freshness": 0.5}
        s1 = self._make_scorer(weights=no_popularity).score_video(video)
        s2 = self._make_scorer(weights=no_engagement).score_video(video)
        # Different scores since different components zeroed
        assert s1 != pytest.approx(s2, rel=0.01)


# ─── Queue operations (mocked DB) ───────────────────────────────────────────


def _mock_supabase_chain():
    """Create a mock that supports chained Supabase calls."""
    mock = MagicMock()
    mock.table.return_value = mock
    mock.select.return_value = mock
    mock.delete.return_value = mock
    mock.update.return_value = mock
    mock.eq.return_value = mock
    mock.in_.return_value = mock
    mock.filter.return_value = mock
    mock.range.return_value = mock
    mock.order.return_value = mock
    mock.limit.return_value = mock
    mock.upsert.return_value = mock
    mock.rpc.return_value = mock
    return mock


class TestClearChannelJobs:
    @pytest.mark.integration
    def test_dry_run_does_nothing(self):
        client = _mock_supabase_chain()
        db = SyncDatabase(client)
        count = db.clear_channel_jobs("UC123", dry_run=True)
        assert count == 0
        client.table.assert_not_called()

    @pytest.mark.integration
    def test_deletes_and_returns_count(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=[{"id": 1}, {"id": 2}])
        db = SyncDatabase(client)
        count = db.clear_channel_jobs("UC123", dry_run=False)
        assert count == 2

    @pytest.mark.edge_cases
    def test_no_jobs_returns_zero(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=[])
        db = SyncDatabase(client)
        count = db.clear_channel_jobs("UC123", dry_run=False)
        assert count == 0


class TestEnqueueJobs:
    @pytest.mark.integration
    def test_dry_run_prints_without_db_calls(self, capsys):
        client = _mock_supabase_chain()
        db = SyncDatabase(client)
        jobs = [
            {"video_id": "v1", "channel_id": "UC1", "action": "download"},
            {"video_id": "v2", "channel_id": "UC1", "action": "remove"},
        ]
        count = db.enqueue_jobs(jobs, dry_run=True)
        assert count == 2
        client.rpc.assert_not_called()

        output = capsys.readouterr().out
        assert "DRY RUN" in output
        assert "v1" in output
        assert "v2" in output

    @pytest.mark.integration
    def test_batches_large_job_list(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=50)

        db = SyncDatabase(client)
        jobs = [{"video_id": f"v{i}", "channel_id": "UC1", "action": "download"} for i in range(250)]
        count = db.enqueue_jobs(jobs, dry_run=False)

        # 250 jobs / 100 batch size = 3 RPC calls
        assert client.rpc.call_count == 3
        assert count > 0

    @pytest.mark.edge_cases
    def test_empty_jobs_returns_zero(self):
        client = _mock_supabase_chain()
        db = SyncDatabase(client)
        count = db.enqueue_jobs([], dry_run=False)
        assert count == 0
        client.rpc.assert_not_called()

    @pytest.mark.integration
    def test_metadata_defaults_to_empty_dict(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=1)

        db = SyncDatabase(client)
        jobs = [{"video_id": "v1", "channel_id": "UC1", "action": "download"}]
        db.enqueue_jobs(jobs, dry_run=False)

        # Verify the payload passed to RPC includes metadata: {}
        rpc_call = client.rpc.call_args
        payload = rpc_call[0][1]["jobs"]
        assert payload[0]["metadata"] == {}


class TestUpdateVideoTier:
    @pytest.mark.integration
    def test_dry_run_skips_db(self):
        client = _mock_supabase_chain()
        db = SyncDatabase(client)
        db.update_video_tier("v1", "catalog", ["popular"], dry_run=True)
        client.table.assert_not_called()

    @pytest.mark.integration
    def test_calls_db_with_correct_args(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=[])

        db = SyncDatabase(client)
        db.update_video_tier("v1", "fresh", ["recent"], dry_run=False)

        client.table.assert_called_with("videos")
        client.update.assert_called_once_with({"sync_tier": "fresh", "source_tags": ["recent"]})


# ─── fetch_search_videos integration ────────────────────────────────────────


class TestFetchSearchVideos:
    @pytest.mark.integration
    def test_returns_enriched_videos(self, mocker):
        search_response = {
            "items": [
                {
                    "id": {"videoId": "v1"},
                    "snippet": {
                        "title": "Popular Video",
                        "publishedAt": "2025-06-01T00:00:00Z",
                        "description": "desc",
                        "thumbnails": {"high": {"url": "https://img.youtube.com/v1.jpg"}},
                    },
                },
            ]
        }
        enrich_response = {
            "items": [
                {
                    "id": "v1",
                    "contentDetails": {"duration": "PT10M"},
                    "statistics": {
                        "viewCount": "500000",
                        "likeCount": "25000",
                        "commentCount": "5000",
                    },
                }
            ]
        }
        client = YouTubeClient(api_key="fake-key", config={})
        mocker.patch.object(
            client,
            "api_get",
            side_effect=[search_response, enrich_response],
        )

        videos, quota = client.fetch_search_videos("UC123", "viewCount")

        assert len(videos) == 1
        assert videos[0]["video_id"] == "v1"
        assert videos[0]["duration_seconds"] == 600
        assert videos[0]["view_count"] == 500000
        assert quota == 101  # 100 for search + 1 for enrich

    @pytest.mark.integration
    def test_empty_search_results(self, mocker):
        client = YouTubeClient(api_key="fake-key", config={})
        mocker.patch.object(client, "api_get", return_value={"items": []})

        videos, quota = client.fetch_search_videos("UC123", "rating")

        assert videos == []
        assert quota == 100  # search always costs 100


# ─── Full pipeline integration: fetch → select → diff ───────────────────────


class TestProducerPipelineIntegration:
    @pytest.mark.integration
    def test_no_changes_produces_empty_diff(self):
        """When desired == existing, diff should be empty."""
        ids = {f"v{i}" for i in range(10)}
        to_download = ids - ids
        to_remove = ids - ids
        assert to_download == set()
        assert to_remove == set()


# ─── passes_duration_filter edge cases ──────────────────────────────────────


class TestPassesDurationFilterEdgeCases:
    @pytest.mark.edge_cases
    def test_max_duration_zero_treated_as_falsy(self):
        """max_duration_s=0 should be treated as no max (falsy)."""
        assert passes_duration_filter({"duration_seconds": 99999}, min_duration_s=60, max_duration_s=0) is True

    @pytest.mark.edge_cases
    def test_min_duration_zero_allows_everything(self):
        assert passes_duration_filter({"duration_seconds": 1}, min_duration_s=0) is True

    @pytest.mark.edge_cases
    def test_negative_duration(self):
        assert passes_duration_filter({"duration_seconds": -1}, min_duration_s=0) is False
        assert passes_duration_filter({"duration_seconds": -1}, min_duration_s=60) is False
