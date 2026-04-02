"""Tests for producer-domain functions — scoring, selection, parsing, config."""

import os
from datetime import datetime, timezone

import pytest

from src.config import AppConfig
from src.scoring import VideoScorer, estimate_gb, passes_duration_filter
from src.services.youtube import parse_date_range, parse_iso_duration, uploads_playlist_id

# ─── uploads_playlist_id ────────────────────────────────────────────────────


class TestUploadsPlaylistId:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            ("UCxxxxxxxxxxxxxxxxxxxxxxxx", "UUxxxxxxxxxxxxxxxxxxxxxxxx"),
            ("UCddiYMoUpa0oeICKWQyGkn2Q", "UUddiYMoUpa0oeICKWQyGkn2Q"),
            ("UC12", "UU12"),
        ],
        ids=["standard-channel", "real-channel-id", "short-id"],
    )
    def test_converts_uc_to_uu(self, give, want):
        assert uploads_playlist_id(give) == want


# ─── parse_iso_duration ──────────────────────────────────────────────────────


class TestParseIsoDuration:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            ("PT3M45S", 225),
            ("PT1H2M3S", 3723),
            ("PT10M", 600),
            ("PT30S", 30),
            ("PT1H", 3600),
            ("PT0S", 0),
            ("PT1H0M0S", 3600),
        ],
        ids=[
            "minutes-seconds",
            "hours-minutes-seconds",
            "minutes-only",
            "seconds-only",
            "hours-only",
            "zero",
            "full-zero",
        ],
    )
    def test_valid_durations(self, give, want):
        assert parse_iso_duration(give) == want

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            ("", 0),
            ("P1D", 0),
            ("invalid", 0),
            ("3M45S", 0),
        ],
        ids=["empty-string", "date-duration", "garbage", "no-pt-prefix"],
    )
    def test_invalid_returns_zero(self, give, want):
        assert parse_iso_duration(give) == want

    @pytest.mark.edge_cases
    def test_none_input(self):
        assert parse_iso_duration(None) == 0


# ─── parse_date_range ────────────────────────────────────────────────────────


class TestParseDateRange:
    @pytest.mark.unit
    def test_all_returns_epoch(self):
        result = parse_date_range("all")
        assert result == datetime(1970, 1, 1, tzinfo=timezone.utc)

    @pytest.mark.unit
    def test_all_case_insensitive(self):
        result = parse_date_range("ALL")
        assert result == datetime(1970, 1, 1, tzinfo=timezone.utc)

    @pytest.mark.unit
    def test_absolute_date(self):
        result = parse_date_range("20230615")
        assert result == datetime(2023, 6, 15, tzinfo=timezone.utc)

    @pytest.mark.unit
    def test_relative_months(self):
        result = parse_date_range("today-6months")
        assert result.tzinfo == timezone.utc
        now = datetime.now(timezone.utc)
        diff_days = (now - result).days
        assert 170 < diff_days < 200

    @pytest.mark.unit
    def test_relative_years(self):
        result = parse_date_range("today-2years")
        assert result.tzinfo == timezone.utc
        now = datetime.now(timezone.utc)
        diff_days = (now - result).days
        assert 700 < diff_days < 740

    @pytest.mark.unit
    def test_relative_singular_year(self):
        result = parse_date_range("today-1year")
        assert result.tzinfo == timezone.utc
        now = datetime.now(timezone.utc)
        diff_days = (now - result).days
        assert 360 < diff_days < 370

    @pytest.mark.unit
    def test_invalid_falls_back_to_6months(self):
        result = parse_date_range("nonsense")
        now = datetime.now(timezone.utc)
        diff_days = (now - result).days
        assert 170 < diff_days < 200

    @pytest.mark.edge_cases
    def test_relative_zeroes_time_components(self):
        result = parse_date_range("today-3months")
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0


# ─── estimate_gb ─────────────────────────────────────────────────────────────


class TestEstimateGb:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want_approx",
        [
            (0, 0.001),
            (60, 60 * 1.8 / 8 / 1024),
            (3600, 3600 * 1.8 / 8 / 1024),
            (600, 600 * 1.8 / 8 / 1024),
        ],
        ids=["zero-floors-to-min", "one-minute", "one-hour", "ten-minutes"],
    )
    def test_estimates(self, give, want_approx):
        result = estimate_gb(give)
        assert result == pytest.approx(want_approx, rel=1e-6)

    @pytest.mark.edge_cases
    def test_negative_duration_floors(self):
        result = estimate_gb(-100)
        assert result == 0.001


# ─── passes_duration_filter ──────────────────────────────────────────────────


class TestPassesDurationFilter:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            ({"duration_seconds": 120}, True),
            ({"duration_seconds": 60}, True),
            ({"duration_seconds": 59}, False),
            ({"duration_seconds": 0}, False),
            ({}, False),
        ],
        ids=["above-min", "exact-min", "below-min", "zero", "missing-key"],
    )
    def test_min_duration(self, give, want):
        assert passes_duration_filter(give, min_duration_s=60) == want

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            ({"duration_seconds": 3600}, True),
            ({"duration_seconds": 3601}, False),
            ({"duration_seconds": 120}, True),
        ],
        ids=["at-max", "above-max", "well-below-max"],
    )
    def test_max_duration(self, give, want):
        assert passes_duration_filter(give, min_duration_s=60, max_duration_s=3600) == want

    @pytest.mark.unit
    def test_no_max_allows_any(self):
        assert passes_duration_filter({"duration_seconds": 99999}, min_duration_s=60) is True


# ─── score_video (via VideoScorer) ───────────────────────────────────────────


class TestScoreVideo:
    WEIGHTS = {"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}
    HALF_LIFE = 90.0

    def _score(self, video):
        return VideoScorer(self.WEIGHTS, self.HALF_LIFE).score_video(video)

    @pytest.mark.unit
    def test_viral_video_scores_high(self):
        video = {
            "view_count": 10_000_000,
            "like_count": 500_000,
            "comment_count": 50_000,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        assert self._score(video) > 3.0

    @pytest.mark.unit
    def test_old_video_scores_lower_freshness(self):
        recent = {
            "view_count": 100_000,
            "like_count": 5_000,
            "comment_count": 500,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        old = {
            "view_count": 100_000,
            "like_count": 5_000,
            "comment_count": 500,
            "published_at": "2020-01-01T00:00:00Z",
        }
        assert self._score(recent) > self._score(old)

    @pytest.mark.unit
    def test_zero_views_does_not_crash(self):
        assert self._score({"view_count": 0, "like_count": 0, "comment_count": 0}) >= 0

    @pytest.mark.unit
    def test_missing_published_at_uses_half_life_age(self):
        score = self._score({"view_count": 1000, "like_count": 50, "comment_count": 5})
        freshness_component = 0.30 * 0.5
        assert score > freshness_component

    @pytest.mark.unit
    def test_invalid_published_at_does_not_crash(self):
        video = {"view_count": 1000, "like_count": 50, "comment_count": 5, "published_at": "not-a-date"}
        assert self._score(video) >= 0

    @pytest.mark.unit
    def test_popularity_scales_with_views(self):
        low = {"view_count": 100, "like_count": 0, "comment_count": 0}
        high = {"view_count": 1_000_000, "like_count": 0, "comment_count": 0}
        assert self._score(high) > self._score(low)

    @pytest.mark.unit
    def test_pure_function_deterministic(self):
        video = {
            "view_count": 50_000,
            "like_count": 2_000,
            "comment_count": 200,
            "published_at": "2025-01-01T00:00:00Z",
        }
        s1 = self._score(video)
        s2 = self._score(video)
        assert s1 == pytest.approx(s2, rel=1e-9)


# ─── ese_score ───────────────────────────────────────────────────────────────


class TestEseScore:
    WEIGHTS = {"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}
    HALF_LIFE = 90.0

    @pytest.mark.unit
    def test_alpha_zero_equals_raw_score(self):
        s = VideoScorer(self.WEIGHTS, self.HALF_LIFE)
        video = {
            "view_count": 100_000,
            "like_count": 5_000,
            "comment_count": 500,
            "duration_seconds": 600,
            "published_at": "2025-01-01T00:00:00Z",
        }
        raw = s.score_video(video)
        ese = s.ese_score(video, alpha=0.0)
        assert ese == pytest.approx(raw, rel=1e-6)

    @pytest.mark.unit
    def test_higher_alpha_penalizes_long_videos(self):
        s = VideoScorer(self.WEIGHTS, self.HALF_LIFE)
        short = {
            "view_count": 100_000,
            "like_count": 5_000,
            "comment_count": 500,
            "duration_seconds": 120,
            "published_at": "2025-01-01T00:00:00Z",
        }
        long = {**short, "duration_seconds": 7200}
        assert s.ese_score(short, alpha=0.0) == pytest.approx(s.ese_score(long, alpha=0.0), rel=1e-6)
        assert s.ese_score(short, alpha=0.3) > s.ese_score(long, alpha=0.3)


# ─── compute_diff (inline — too small for its own module) ────────────────────


class TestComputeDiff:
    @staticmethod
    def _diff(desired, existing):
        return desired - existing, existing - desired

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            (({"a", "b", "c"}, {"b", "c", "d"}), ({"a"}, {"d"})),
            (({"a", "b"}, set()), ({"a", "b"}, set())),
            ((set(), {"a", "b"}), (set(), {"a", "b"})),
            (({"a", "b"}, {"a", "b"}), (set(), set())),
            ((set(), set()), (set(), set())),
        ],
        ids=["overlap", "all-new", "all-removed", "identical", "both-empty"],
    )
    def test_diff(self, give, want):
        desired, existing = give
        want_dl, want_rm = want
        to_download, to_remove = self._diff(desired, existing)
        assert to_download == want_dl
        assert to_remove == want_rm


# ─── load_config (via AppConfig) ─────────────────────────────────────────────


class TestLoadConfig:
    @pytest.mark.unit
    def test_returns_all_sections(self):
        cfg = AppConfig()
        assert cfg.api is not None
        assert cfg.quota is not None
        assert cfg.scoring is not None
        assert cfg.sources is not None
        assert cfg.db is not None

    @pytest.mark.unit
    def test_scoring_weights_sum_to_one(self):
        cfg = AppConfig()
        total = sum(cfg.scoring_weights.values())
        assert total == pytest.approx(1.0, abs=0.01)

    @pytest.mark.unit
    def test_default_values_present(self):
        cfg = AppConfig()
        assert cfg.api["page_size"] == 50
        assert cfg.api["max_retries"] == 3
        assert cfg.quota["daily_limit"] == 10000


# ─── load_env (via AppConfig) ────────────────────────────────────────────────


class TestLoadEnv:
    @pytest.mark.unit
    def test_loads_simple_vars(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_KEY=test_value\nANOTHER=hello\n")
        monkeypatch.delenv("TEST_KEY", raising=False)
        monkeypatch.delenv("ANOTHER", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ["TEST_KEY"] == "test_value"
        assert os.environ["ANOTHER"] == "hello"

    @pytest.mark.unit
    def test_skips_comments_and_blanks(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("# comment\n\nKEY=val\n")
        monkeypatch.delenv("KEY", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ["KEY"] == "val"

    @pytest.mark.unit
    def test_does_not_overwrite_existing(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("EXISTING_KEY=new_value\n")
        monkeypatch.setenv("EXISTING_KEY", "original")

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ["EXISTING_KEY"] == "original"

    @pytest.mark.unit
    def test_missing_env_file(self, tmp_path):
        config = AppConfig(env_file=tmp_path / "nonexistent")
        config.load_env()  # should not raise


# ─── enrich_videos (via YouTubeClient) ──────────────────────────────────────


class TestEnrichVideos:
    @pytest.mark.unit
    def test_parses_api_response(self, mocker):
        from src.services.youtube import YouTubeClient

        client = YouTubeClient("fake-key", {"enrichment_batch_size": 50})
        mocker.patch.object(
            client,
            "api_get",
            return_value={
                "items": [
                    {
                        "id": "vid1",
                        "contentDetails": {"duration": "PT5M30S"},
                        "statistics": {"viewCount": "100000", "likeCount": "5000", "commentCount": "200"},
                    },
                    {
                        "id": "vid2",
                        "contentDetails": {"duration": "PT1H"},
                        "statistics": {"viewCount": "50000", "likeCount": "1000", "commentCount": "100"},
                    },
                ]
            },
        )

        details, calls = client.enrich_videos(["vid1", "vid2"])
        assert calls == 1
        assert details["vid1"]["duration_seconds"] == 330
        assert details["vid1"]["view_count"] == 100000
        assert details["vid2"]["duration_seconds"] == 3600

    @pytest.mark.unit
    def test_batches_large_lists(self, mocker):
        from src.services.youtube import YouTubeClient

        client = YouTubeClient("fake-key", {"enrichment_batch_size": 50})
        mocker.patch.object(client, "api_get", return_value={"items": []})
        _, calls = client.enrich_videos([f"v{i}" for i in range(120)])
        assert calls == 3

    @pytest.mark.unit
    def test_empty_list(self, mocker):
        from src.services.youtube import YouTubeClient

        client = YouTubeClient("fake-key", {})
        details, calls = client.enrich_videos([])
        assert details == {}
        assert calls == 0

    @pytest.mark.unit
    def test_missing_stats_default_to_zero(self, mocker):
        from src.services.youtube import YouTubeClient

        client = YouTubeClient("fake-key", {"enrichment_batch_size": 50})
        mocker.patch.object(
            client,
            "api_get",
            return_value={"items": [{"id": "vid1", "contentDetails": {"duration": "PT1M"}, "statistics": {}}]},
        )
        details, _ = client.enrich_videos(["vid1"])
        assert details["vid1"]["view_count"] == 0


# ─── fetch_desired_videos (via YouTubeClient) ───────────────────────────────


class TestFetchDesiredVideos:
    @pytest.mark.unit
    def test_stops_at_target_count(self, mocker):
        from src.services.youtube import YouTubeClient

        client = YouTubeClient("fake-key", {"page_size": 50, "enrichment_batch_size": 50})

        playlist_response = {
            "items": [
                {
                    "snippet": {
                        "resourceId": {"videoId": f"v{i}"},
                        "title": f"Video {i}",
                        "publishedAt": "2025-06-01T00:00:00Z",
                        "description": "",
                        "thumbnails": {},
                    }
                }
                for i in range(10)
            ],
        }
        enrich_response = {
            "items": [
                {
                    "id": f"v{i}",
                    "contentDetails": {"duration": "PT5M"},
                    "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "5"},
                }
                for i in range(10)
            ]
        }
        mocker.patch.object(client, "api_get", side_effect=[playlist_response, enrich_response])

        cutoff = datetime(1970, 1, 1, tzinfo=timezone.utc)
        videos, total_fetched, quota = client.fetch_playlist_videos("UUxxx", target_count=5, date_cutoff=cutoff)
        assert len(videos) == 5

    @pytest.mark.unit
    def test_stops_at_date_boundary(self, mocker):
        from src.services.youtube import YouTubeClient

        client = YouTubeClient("fake-key", {"page_size": 50, "enrichment_batch_size": 50})

        items = []
        for i in range(10):
            date = "2025-06-01T00:00:00Z" if i < 3 else "2020-01-01T00:00:00Z"
            items.append(
                {
                    "snippet": {
                        "resourceId": {"videoId": f"v{i}"},
                        "title": f"Video {i}",
                        "publishedAt": date,
                        "description": "",
                        "thumbnails": {},
                    }
                }
            )

        playlist_response = {"items": items}
        enrich_response = {
            "items": [
                {
                    "id": f"v{i}",
                    "contentDetails": {"duration": "PT5M"},
                    "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "5"},
                }
                for i in range(3)
            ]
        }
        mocker.patch.object(client, "api_get", side_effect=[playlist_response, enrich_response])

        cutoff = datetime(2024, 1, 1, tzinfo=timezone.utc)
        videos, _, _ = client.fetch_playlist_videos("UUxxx", target_count=50, date_cutoff=cutoff)
        assert len(videos) == 3
