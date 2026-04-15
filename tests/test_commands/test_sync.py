"""Tests for SyncCommand — unified fetch → calibrate → score → dedup → enqueue."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest
import requests

from src.commands.sync import SyncCommand
from src.config import AppConfig
from src.scoring import VideoScorer
from src.services.youtube import uploads_playlist_id


def _make_channel(channel_id: str = "UC123", **overrides) -> dict:
    base = {
        "curated_id": 1,
        "channel_id": channel_id,
        "title": "Test Channel",
        "sync_mode": "sync",
        "scoring_alpha": 0.3,
        "min_duration_seconds": 60,
        "max_duration_seconds": 3600,
        "last_full_refresh_at": None,
    }
    base.update(overrides)
    return base


def _make_video(video_id: str, **overrides) -> dict:
    base = {
        "video_id": video_id,
        "title": f"Video {video_id}",
        "duration_seconds": 300,
        "published_at": "2026-03-15T00:00:00Z",
        "like_count": 2500,
        "comment_count": 250,
    }
    base.update(overrides)
    return base


@pytest.fixture
def config():
    cfg = AppConfig()
    cfg.api["max_workers"] = 1
    cfg.producer["channels_per_run"] = 2
    return cfg


@pytest.fixture
def scorer():
    return VideoScorer()


@pytest.fixture
def db():
    mock = MagicMock()
    mock.fetch_existing_r2_with_bytes.return_value = {}
    return mock


@pytest.fixture
def youtube():
    mock = MagicMock()
    mock.fetch_playlist_videos.return_value = ([], 0, 0)
    return mock


# ─── Pure helpers ────────────────────────────────────────────────────────────


class TestDurationBuckets:
    @pytest.mark.unit
    def test_empty(self):
        result = SyncCommand._duration_buckets([])
        assert all(v == 0 for v in result.values())
        assert len(result) == 8

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give_seconds, want_bucket",
        [
            (30, "under_1m"),
            (120, "1_5m"),
            (450, "5_10m"),
            (900, "10_20m"),
            (1500, "20_30m"),
            (2400, "30_60m"),
            (5400, "1_2h"),
            (9000, "over_2h"),
        ],
        ids=["under-1m", "1-5m", "5-10m", "10-20m", "20-30m", "30-60m", "1-2h", "over-2h"],
    )
    def test_single_video_correct_bucket(self, give_seconds, want_bucket):
        videos = [{"duration_seconds": give_seconds}]
        result = SyncCommand._duration_buckets(videos)
        assert result[want_bucket] == 1
        assert sum(result.values()) == 1

    @pytest.mark.unit
    def test_multiple_videos(self):
        videos = [
            {"duration_seconds": 30},
            {"duration_seconds": 30},
            {"duration_seconds": 600},
            {"duration_seconds": 3700},
        ]
        result = SyncCommand._duration_buckets(videos)
        assert result["under_1m"] == 2
        assert result["10_20m"] == 1
        assert result["1_2h"] == 1
        assert sum(result.values()) == 4

    @pytest.mark.edge_cases
    def test_boundary_values(self):
        videos = [
            {"duration_seconds": 59},
            {"duration_seconds": 60},
            {"duration_seconds": 299},
            {"duration_seconds": 300},
            {"duration_seconds": 3599},
            {"duration_seconds": 3600},
            {"duration_seconds": 7199},
            {"duration_seconds": 7200},
        ]
        result = SyncCommand._duration_buckets(videos)
        assert result["under_1m"] == 1
        assert result["1_5m"] == 2
        assert result["5_10m"] == 1
        assert result["30_60m"] == 1
        assert result["1_2h"] == 2
        assert result["over_2h"] == 1

    @pytest.mark.edge_cases
    def test_missing_duration_key(self):
        result = SyncCommand._duration_buckets([{}])
        assert result["under_1m"] == 1


class TestPassingCounts:
    @pytest.mark.unit
    def test_empty(self):
        result = SyncCommand._passing_counts([])
        assert all(v == 0 for v in result.values())

    @pytest.mark.unit
    def test_all_filters(self):
        videos = [
            {"duration_seconds": 30},
            {"duration_seconds": 120},
            {"duration_seconds": 600},
            {"duration_seconds": 1500},
            {"duration_seconds": 3000},
            {"duration_seconds": 5000},
        ]
        result = SyncCommand._passing_counts(videos)
        assert result["min_60s"] == 5
        assert result["min_300s"] == 4
        assert result["min_300s_max_1800s"] == 2
        assert result["min_300s_max_3600s"] == 3
        assert result["min_60s_max_1800s"] == 3
        assert result["min_60s_max_3600s"] == 4

    @pytest.mark.edge_cases
    def test_boundary_60s(self):
        videos = [{"duration_seconds": 59}, {"duration_seconds": 60}]
        result = SyncCommand._passing_counts(videos)
        assert result["min_60s"] == 1


class TestComputeCadence:
    @pytest.mark.unit
    def test_empty_videos(self):
        result = SyncCommand._compute_cadence([])
        assert result["avg_gap_days"] is None
        assert result["median_gap_days"] is None
        assert result["posts_per_week"] == 0

    @pytest.mark.unit
    def test_all_below_60s_excluded(self):
        videos = [
            {"duration_seconds": 30, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 45, "published_at": "2025-06-02T00:00:00Z"},
        ]
        result = SyncCommand._compute_cadence(videos)
        assert result["posts_per_week"] == 0

    @pytest.mark.unit
    def test_single_video_no_gaps(self):
        videos = [{"duration_seconds": 120, "published_at": "2025-06-01T00:00:00Z"}]
        result = SyncCommand._compute_cadence(videos)
        assert result["avg_gap_days"] is None
        assert result["avg_duration_seconds"] == 120
        assert result["median_duration_seconds"] == 120

    @pytest.mark.unit
    def test_regular_posting_schedule(self):
        videos = [{"duration_seconds": 600, "published_at": f"2025-06-{1 + i * 7:02d}T00:00:00Z"} for i in range(5)]
        result = SyncCommand._compute_cadence(videos)
        assert result["avg_gap_days"] == pytest.approx(7.0, abs=0.1)
        assert result["median_gap_days"] == pytest.approx(7.0, abs=0.1)
        assert result["posts_per_week"] == pytest.approx(1.25, abs=0.1)

    @pytest.mark.unit
    def test_irregular_posting(self):
        videos = [
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "2025-06-02T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "2025-06-15T00:00:00Z"},
        ]
        result = SyncCommand._compute_cadence(videos)
        assert result["avg_gap_days"] == pytest.approx(7.0, abs=0.1)
        assert result["posts_per_week"] > 0

    @pytest.mark.edge_cases
    def test_missing_published_at_excluded(self):
        videos = [
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 300, "published_at": ""},
            {"duration_seconds": 300, "published_at": "2025-06-08T00:00:00Z"},
        ]
        result = SyncCommand._compute_cadence(videos)
        assert result["avg_gap_days"] == pytest.approx(7.0, abs=0.1)

    @pytest.mark.edge_cases
    def test_invalid_published_at_excluded(self):
        videos = [
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "not-a-date"},
            {"duration_seconds": 300, "published_at": "2025-06-08T00:00:00Z"},
        ]
        result = SyncCommand._compute_cadence(videos)
        assert result["avg_gap_days"] == pytest.approx(7.0, abs=0.1)

    @pytest.mark.unit
    def test_duration_stats(self):
        videos = [
            {"duration_seconds": 120, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 600, "published_at": "2025-06-02T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "2025-06-03T00:00:00Z"},
        ]
        result = SyncCommand._compute_cadence(videos)
        assert result["avg_duration_seconds"] == 340
        assert result["median_duration_seconds"] == 300


# ─── process_channel ─────────────────────────────────────────────────────────


class TestProcessChannel:
    @pytest.mark.unit
    def test_full_pipeline_enqueues_only_new_eligible_videos(self, config, scorer, db, youtube):
        youtube.fetch_playlist_videos.return_value = (
            [
                _make_video("v_existing"),
                _make_video("v_new"),
                _make_video("v_short", duration_seconds=30),
            ],
            3,
            5,
        )
        db.fetch_existing_r2_with_bytes.return_value = {
            "v_existing": {"storage_bytes": 1000, "score": 1.0},
        }

        cmd = SyncCommand(config, db, scorer, youtube)
        result = cmd.process_channel(_make_channel(), dry_run=False, verbose=False)

        youtube.fetch_playlist_videos.assert_called_once()
        fetch_args = youtube.fetch_playlist_videos.call_args.args
        assert fetch_args[0] == uploads_playlist_id("UC123")
        assert fetch_args[1] == 100_000
        assert fetch_args[2] == datetime(1970, 1, 1, tzinfo=timezone.utc)
        assert fetch_args[3] == config.producer["early_stop_tolerance"]

        assert result["channel_id"] == "UC123"
        assert result["title"] == "Test Channel"
        assert result["total_videos"] == 3
        assert result["eligible"] == 2
        assert result["existing"] == 1
        assert result["downloads"] == 1
        assert result["quota_used"] == 5
        assert result["scores_updated"] == 1
        assert "cadence" in result

        db.replace_channel_jobs.assert_called_once()
        jobs = db.replace_channel_jobs.call_args.args[1]
        assert [job["video_id"] for job in jobs] == ["v_new"]
        assert jobs[0]["channel_id"] == "UC123"
        assert jobs[0]["metadata"]["video_id"] == "v_new"
        assert jobs[0]["score"] > 0
        assert jobs[0]["published_at"] == "2026-03-15T00:00:00Z"

    @pytest.mark.unit
    def test_updates_scores_on_existing_r2_videos(self, config, scorer, db, youtube):
        youtube.fetch_playlist_videos.return_value = (
            [_make_video("v1"), _make_video("v2")],
            2,
            2,
        )
        db.fetch_existing_r2_with_bytes.return_value = {
            "v1": {"storage_bytes": 1000},
            "v2": {"storage_bytes": 2000},
        }

        cmd = SyncCommand(config, db, scorer, youtube)
        cmd.process_channel(_make_channel(), dry_run=True, verbose=False)

        db.replace_channel_jobs.assert_called_once_with("UC123", [], dry_run=True)
        db.update_video_scores.assert_called_once()
        assert set(db.update_video_scores.call_args.args[0]) == {"v1", "v2"}
        assert db.update_video_scores.call_args.args[1] is True

    @pytest.mark.unit
    def test_upserts_calibration_data(self, config, scorer, db, youtube):
        youtube.fetch_playlist_videos.return_value = (
            [
                _make_video("v1", duration_seconds=30),
                _make_video("v2", duration_seconds=600),
                _make_video("v3", duration_seconds=3600),
            ],
            3,
            4,
        )

        cmd = SyncCommand(config, db, scorer, youtube)
        cmd.process_channel(_make_channel(), dry_run=False, verbose=False)

        db.upsert_channel_calibration.assert_called_once()
        call_kwargs = db.upsert_channel_calibration.call_args.kwargs
        assert call_kwargs["channel_id"] == "UC123"
        assert call_kwargs["total_videos_sampled"] == 3
        assert call_kwargs["duration_buckets"]["under_1m"] == 1
        assert call_kwargs["passing"]["min_60s"] == 2
        assert "posts_per_week" in call_kwargs["cadence"]

    @pytest.mark.unit
    def test_calibration_failure_does_not_block_pipeline(self, config, scorer, db, youtube, capsys):
        youtube.fetch_playlist_videos.return_value = (
            [_make_video("v1")],
            1,
            2,
        )
        db.upsert_channel_calibration.side_effect = Exception("DB timeout")

        cmd = SyncCommand(config, db, scorer, youtube)
        result = cmd.process_channel(_make_channel(), dry_run=False, verbose=False)

        assert "WARNING" in capsys.readouterr().out
        assert result["downloads"] == 1
        db.replace_channel_jobs.assert_called_once()

    @pytest.mark.unit
    def test_returns_error_summary_on_http_error(self, config, scorer, db, youtube):
        response = requests.Response()
        response.status_code = 403
        youtube.fetch_playlist_videos.side_effect = requests.exceptions.HTTPError(
            "quota exceeded", response=response
        )

        cmd = SyncCommand(config, db, scorer, youtube)
        result = cmd.process_channel(_make_channel(), dry_run=False, verbose=False)

        assert result["error"] == "quota exceeded"
        assert result["downloads"] == 0
        db.replace_channel_jobs.assert_not_called()

    @pytest.mark.unit
    def test_empty_playlist(self, config, scorer, db, youtube):
        youtube.fetch_playlist_videos.return_value = ([], 0, 1)

        cmd = SyncCommand(config, db, scorer, youtube)
        result = cmd.process_channel(_make_channel(), dry_run=False, verbose=False)

        assert result["total_videos"] == 0
        assert result["downloads"] == 0
        db.replace_channel_jobs.assert_called_once_with("UC123", [], dry_run=False)


# ─── reset ───────────────────────────────────────────────────────────────────


class TestReset:
    @pytest.mark.unit
    def test_clears_queue_and_staging(self, config, scorer, db, youtube, tmp_path):
        staging = tmp_path / "downloads" / "staging"
        staging.mkdir(parents=True)
        (staging / "leftover.mp4").write_text("junk")
        config.project_root = tmp_path

        db.clear_sync_queue.return_value = 5

        cmd = SyncCommand(config, db, scorer, youtube)
        cmd._reset(dry_run=False)

        db.clear_sync_queue.assert_called_once()
        assert staging.exists()
        assert list(staging.iterdir()) == []

    @pytest.mark.unit
    def test_dry_run_skips_reset(self, config, scorer, db, youtube, capsys):
        cmd = SyncCommand(config, db, scorer, youtube)
        cmd._reset(dry_run=True)

        db.clear_sync_queue.assert_not_called()
        assert "DRY RUN" in capsys.readouterr().out


# ─── run orchestration ───────────────────────────────────────────────────────


class TestSyncCommandRun:
    @pytest.mark.integration
    def test_run_single_channel_and_updates_refresh_timestamp(self, config, scorer, db, youtube):
        channels = [
            _make_channel("UC111", curated_id="c1", title="First"),
            _make_channel("UC222", curated_id="c2", title="Second"),
        ]
        db.fetch_curated_channels.return_value = channels

        cmd = SyncCommand(config, db, scorer, youtube)
        cmd.process_channel = MagicMock(return_value={
            "channel_id": "UC222",
            "title": "Second",
            "sync_mode": "sync",
            "total_videos": 10,
            "eligible": 8,
            "existing": 3,
            "downloads": 5,
            "scores_updated": 3,
            "quota_used": 9,
            "cadence": {"posts_per_week": 1.5},
        })

        cmd.run(channel="UC222", dry_run=False, verbose=True)

        cmd.process_channel.assert_called_once_with(channels[1], False, True)
        db.update_full_refresh_timestamp.assert_called_once_with(["c2"], False)

    @pytest.mark.integration
    def test_run_skips_refresh_timestamp_for_error_summary(self, config, scorer, db, youtube):
        channels = [
            _make_channel("UC111", curated_id="c1", title="First"),
            _make_channel("UC222", curated_id="c2", title="Second"),
        ]
        db.fetch_curated_channels.return_value = channels

        cmd = SyncCommand(config, db, scorer, youtube)
        cmd.process_channel = MagicMock(side_effect=[
            {
                "channel_id": "UC111",
                "title": "First",
                "sync_mode": "sync",
                "total_videos": 10,
                "eligible": 8,
                "existing": 3,
                "downloads": 5,
                "scores_updated": 3,
                "quota_used": 9,
                "cadence": {"posts_per_week": 1.0},
            },
            {
                "channel_id": "UC222",
                "title": "Second",
                "error": "quota exceeded",
                "total_videos": 0,
                "eligible": 0,
                "existing": 0,
                "downloads": 0,
                "quota_used": 0,
            },
        ])

        cmd.run(dry_run=False, verbose=False)

        db.update_full_refresh_timestamp.assert_called_once_with(["c1"], False)

    @pytest.mark.integration
    def test_run_all_channels_skips_rotation(self, config, scorer, db, youtube):
        channels = [
            _make_channel("UC1", curated_id="c1", title="A"),
            _make_channel("UC2", curated_id="c2", title="B"),
            _make_channel("UC3", curated_id="c3", title="C"),
        ]
        db.fetch_curated_channels.return_value = channels

        cmd = SyncCommand(config, db, scorer, youtube)
        cmd.process_channel = MagicMock(return_value={
            "channel_id": "UC1", "title": "A", "sync_mode": "sync",
            "total_videos": 0, "eligible": 0, "existing": 0,
            "downloads": 0, "scores_updated": 0, "quota_used": 0,
            "cadence": {},
        })

        cmd.run(all_channels=True, dry_run=False, verbose=False)

        assert cmd.process_channel.call_count == 3

    @pytest.mark.integration
    def test_run_no_channels_found(self, config, scorer, db, youtube, capsys):
        db.fetch_curated_channels.return_value = []

        cmd = SyncCommand(config, db, scorer, youtube)
        cmd.run()

        assert "No curated channels found" in capsys.readouterr().out

    @pytest.mark.integration
    def test_run_channel_not_found(self, config, scorer, db, youtube, capsys):
        db.fetch_curated_channels.return_value = [_make_channel("UC111")]

        cmd = SyncCommand(config, db, scorer, youtube)
        cmd.run(channel="UC_MISSING")

        assert "not found" in capsys.readouterr().out

    @pytest.mark.unit
    def test_select_next_channel_picks_oldest(self, config, scorer, db, youtube):
        cmd = SyncCommand(config, db, scorer, youtube)
        selected = cmd._select_next_channel(
            [
                _make_channel("UC1", title="Newest", last_full_refresh_at="2026-03-20T00:00:00Z"),
                _make_channel("UC2", title="Stale", last_full_refresh_at="2026-03-01T00:00:00Z"),
                _make_channel("UC3", title="Never", last_full_refresh_at=None),
            ]
        )

        assert len(selected) == 1
        assert selected[0]["channel_id"] == "UC3"
