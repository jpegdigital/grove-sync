"""Tests for PlanCommand's current playlist-only planning flow."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import requests

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


class TestPlanCommandProcessChannel:
    @pytest.mark.unit
    def test_process_channel_enqueues_only_missing_eligible_videos(self, config, scorer, db, youtube):
        from src.commands.plan import PlanCommand

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

        cmd = PlanCommand(config, db, scorer, youtube)
        result = cmd.process_channel(_make_channel(), dry_run=False, verbose=False)

        youtube.fetch_playlist_videos.assert_called_once()
        fetch_args = youtube.fetch_playlist_videos.call_args.args
        db.replace_channel_jobs.assert_called_once()
        jobs = db.replace_channel_jobs.call_args.args[1]

        assert fetch_args[0] == uploads_playlist_id("UC123")
        assert fetch_args[1] == 100_000
        assert fetch_args[2] == datetime(1970, 1, 1, tzinfo=timezone.utc)
        assert fetch_args[3] == config.producer["early_stop_tolerance"]
        assert result == {
            "channel_id": "UC123",
            "title": "Test Channel",
            "total_videos": 3,
            "eligible": 2,
            "existing": 1,
            "downloads": 1,
            "quota_used": 5,
        }
        assert [job["video_id"] for job in jobs] == ["v_new"]
        assert jobs[0]["channel_id"] == "UC123"
        assert jobs[0]["metadata"]["video_id"] == "v_new"
        assert jobs[0]["score"] > 0
        assert jobs[0]["published_at"] == "2026-03-15T00:00:00Z"
        db.update_video_scores.assert_called_once()
        score_updates = db.update_video_scores.call_args.args[0]
        assert set(score_updates) == {"v_existing"}
        assert score_updates["v_existing"] > 0

    @pytest.mark.unit
    def test_process_channel_batches_existing_score_updates(self, config, scorer, db, youtube):
        from src.commands.plan import PlanCommand

        youtube.fetch_playlist_videos.return_value = (
            [_make_video("v1"), _make_video("v2")],
            2,
            2,
        )
        db.fetch_existing_r2_with_bytes.return_value = {
            "v1": {"storage_bytes": 1000},
            "v2": {"storage_bytes": 2000},
        }

        cmd = PlanCommand(config, db, scorer, youtube)
        cmd.process_channel(_make_channel(), dry_run=True, verbose=False)

        db.replace_channel_jobs.assert_called_once_with("UC123", [], dry_run=True)
        db.update_video_scores.assert_called_once()
        assert set(db.update_video_scores.call_args.args[0]) == {"v1", "v2"}
        assert db.update_video_scores.call_args.args[1] is True

    @pytest.mark.unit
    def test_process_channel_returns_error_summary_on_http_error(self, config, scorer, db, youtube):
        from src.commands.plan import PlanCommand

        response = requests.Response()
        response.status_code = 403
        error = requests.exceptions.HTTPError("quota exceeded", response=response)
        youtube.fetch_playlist_videos.side_effect = error

        cmd = PlanCommand(config, db, scorer, youtube)
        result = cmd.process_channel(_make_channel(), dry_run=False, verbose=False)

        assert result["error"] == "quota exceeded"
        assert result["downloads"] == 0
        db.replace_channel_jobs.assert_not_called()


class TestPlanCommandRun:
    @pytest.mark.integration
    def test_run_processes_named_channel_and_updates_refresh_timestamp_only_for_success(self, config, scorer, db, youtube):
        from src.commands.plan import PlanCommand

        channels = [
            _make_channel("UC111", curated_id="c1", title="First"),
            _make_channel("UC222", curated_id="c2", title="Second"),
        ]
        db.fetch_curated_channels.return_value = channels

        cmd = PlanCommand(config, db, scorer, youtube)
        cmd.process_channel = MagicMock(return_value={
            "channel_id": "UC222",
            "title": "Second",
            "total_videos": 10,
            "eligible": 8,
            "existing": 3,
            "downloads": 5,
            "quota_used": 9,
        })

        cmd.run(channel="UC222", dry_run=False, verbose=True)

        cmd.process_channel.assert_called_once_with(channels[1], False, True)
        db.update_full_refresh_timestamp.assert_called_once_with(["c2"], False)

    @pytest.mark.integration
    def test_run_skips_refresh_timestamp_for_error_summary(self, config, scorer, db, youtube):
        from src.commands.plan import PlanCommand

        channels = [
            _make_channel("UC111", curated_id="c1", title="First"),
            _make_channel("UC222", curated_id="c2", title="Second"),
        ]
        db.fetch_curated_channels.return_value = channels

        cmd = PlanCommand(config, db, scorer, youtube)
        cmd.process_channel = MagicMock(side_effect=[
            {
                "channel_id": "UC111",
                "title": "First",
                "total_videos": 10,
                "eligible": 8,
                "existing": 3,
                "downloads": 5,
                "quota_used": 9,
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

    @pytest.mark.unit
    def test_select_rolling_channels_prefers_oldest_refresh(self, config, scorer, db, youtube):
        from src.commands.plan import PlanCommand

        cmd = PlanCommand(config, db, scorer, youtube)
        selected = cmd._select_rolling_channels(
            [
                _make_channel("UC1", title="Newest", last_full_refresh_at="2026-03-20T00:00:00Z"),
                _make_channel("UC2", title="Stale", last_full_refresh_at="2026-03-01T00:00:00Z"),
                _make_channel("UC3", title="Never", last_full_refresh_at=None),
            ]
        )

        assert [channel["channel_id"] for channel in selected] == ["UC3", "UC2"]
