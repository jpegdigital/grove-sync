"""Tests for CalibrateCommand — pure helpers, sampling logic, and integration flows."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.commands.calibrate import CalibrateCommand
from src.config import AppConfig
from src.scoring import VideoScorer
from src.services.db import SyncDatabase
from src.services.youtube import YouTubeClient

# ─── Fixtures ────────────────────────────────────────────────────────────────


def _mock_supabase():
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
    mock.execute.return_value = SimpleNamespace(data=[])
    return mock


@pytest.fixture
def config():
    return AppConfig()


@pytest.fixture
def scorer(config):
    return VideoScorer(config.scoring_weights, config.freshness_half_life_days)


@pytest.fixture
def cmd(config, scorer, tmp_path):
    client = _mock_supabase()
    db = SyncDatabase(client)
    yt = YouTubeClient("fake-key", config.api)
    return CalibrateCommand(config, db, yt, scorer, output_dir=tmp_path)


# ─── _duration_buckets ──────────────────────────────────────────────────────


class TestDurationBuckets:
    @pytest.mark.unit
    def test_empty(self):
        result = CalibrateCommand._duration_buckets([])
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
        result = CalibrateCommand._duration_buckets(videos)
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
        result = CalibrateCommand._duration_buckets(videos)
        assert result["under_1m"] == 2
        assert result["10_20m"] == 1  # 600s = 10m exactly → 10_20m bucket
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
        result = CalibrateCommand._duration_buckets(videos)
        assert result["under_1m"] == 1  # 59
        assert result["1_5m"] == 2  # 60, 299
        assert result["5_10m"] == 1  # 300
        assert result["30_60m"] == 1  # 3599
        assert result["1_2h"] == 2  # 3600, 7199
        assert result["over_2h"] == 1  # 7200

    @pytest.mark.edge_cases
    def test_zero_duration(self):
        videos = [{"duration_seconds": 0}]
        result = CalibrateCommand._duration_buckets(videos)
        assert result["under_1m"] == 1

    @pytest.mark.edge_cases
    def test_missing_duration_key(self):
        videos = [{}]
        result = CalibrateCommand._duration_buckets(videos)
        assert result["under_1m"] == 1


# ─── _passing_counts ────────────────────────────────────────────────────────


class TestPassingCounts:
    @pytest.mark.unit
    def test_empty(self):
        result = CalibrateCommand._passing_counts([])
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
        result = CalibrateCommand._passing_counts(videos)
        assert result["min_60s"] == 5  # all except 30
        assert result["min_300s"] == 4  # 600, 1500, 3000, 5000
        assert result["min_300s_max_1800s"] == 2  # 600, 1500
        assert result["min_300s_max_3600s"] == 3  # 600, 1500, 3000
        assert result["min_60s_max_1800s"] == 3  # 120, 600, 1500
        assert result["min_60s_max_3600s"] == 4  # 120, 600, 1500, 3000

    @pytest.mark.edge_cases
    def test_boundary_60s(self):
        videos = [{"duration_seconds": 59}, {"duration_seconds": 60}]
        result = CalibrateCommand._passing_counts(videos)
        assert result["min_60s"] == 1


# ─── _compute_cadence ───────────────────────────────────────────────────────


class TestComputeCadence:
    @pytest.mark.unit
    def test_empty_videos(self):
        result = CalibrateCommand._compute_cadence([])
        assert result["avg_gap_days"] is None
        assert result["median_gap_days"] is None
        assert result["posts_per_week"] == 0

    @pytest.mark.unit
    def test_all_below_60s_excluded(self):
        videos = [
            {"duration_seconds": 30, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 45, "published_at": "2025-06-02T00:00:00Z"},
        ]
        result = CalibrateCommand._compute_cadence(videos)
        assert result["posts_per_week"] == 0

    @pytest.mark.unit
    def test_single_video_no_gaps(self):
        videos = [{"duration_seconds": 120, "published_at": "2025-06-01T00:00:00Z"}]
        result = CalibrateCommand._compute_cadence(videos)
        assert result["avg_gap_days"] is None  # no gaps with 1 video
        assert result["avg_duration_seconds"] == 120
        assert result["median_duration_seconds"] == 120

    @pytest.mark.unit
    def test_regular_posting_schedule(self):
        # Videos every 7 days for 5 weeks
        videos = [{"duration_seconds": 600, "published_at": f"2025-06-{1 + i * 7:02d}T00:00:00Z"} for i in range(5)]
        result = CalibrateCommand._compute_cadence(videos)
        assert result["avg_gap_days"] == pytest.approx(7.0, abs=0.1)
        assert result["median_gap_days"] == pytest.approx(7.0, abs=0.1)
        assert result["posts_per_week"] == pytest.approx(1.25, abs=0.1)  # 5 posts / 28 days * 7

    @pytest.mark.unit
    def test_irregular_posting(self):
        videos = [
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "2025-06-02T00:00:00Z"},  # 1 day gap
            {"duration_seconds": 300, "published_at": "2025-06-15T00:00:00Z"},  # 13 day gap
        ]
        result = CalibrateCommand._compute_cadence(videos)
        assert result["avg_gap_days"] == pytest.approx(7.0, abs=0.1)
        assert result["median_gap_days"] is not None
        assert result["posts_per_week"] > 0

    @pytest.mark.edge_cases
    def test_missing_published_at_excluded(self):
        videos = [
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 300, "published_at": ""},
            {"duration_seconds": 300, "published_at": "2025-06-08T00:00:00Z"},
        ]
        result = CalibrateCommand._compute_cadence(videos)
        # Only 2 qualifying videos
        assert result["avg_gap_days"] == pytest.approx(7.0, abs=0.1)

    @pytest.mark.edge_cases
    def test_invalid_published_at_excluded(self):
        videos = [
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "not-a-date"},
            {"duration_seconds": 300, "published_at": "2025-06-08T00:00:00Z"},
        ]
        result = CalibrateCommand._compute_cadence(videos)
        assert result["avg_gap_days"] == pytest.approx(7.0, abs=0.1)

    @pytest.mark.unit
    def test_duration_stats(self):
        videos = [
            {"duration_seconds": 120, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 600, "published_at": "2025-06-02T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "2025-06-03T00:00:00Z"},
        ]
        result = CalibrateCommand._compute_cadence(videos)
        assert result["avg_duration_seconds"] == 340
        assert result["median_duration_seconds"] == 300


# ─── _sample_channel (mocked API) ───────────────────────────────────────────


class TestSampleChannel:
    @pytest.mark.integration
    def test_single_page_sampling(self, cmd, mocker):
        playlist_response = {
            "items": [
                {
                    "snippet": {
                        "resourceId": {"videoId": f"v{i}"},
                        "title": f"Video {i}",
                        "publishedAt": f"2025-06-{i + 1:02d}T00:00:00Z",
                    }
                }
                for i in range(5)
            ],
            # No nextPageToken → single page
        }
        enrich_response = {
            "items": [
                {
                    "id": f"v{i}",
                    "contentDetails": {"duration": "PT10M"},
                    "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "5"},
                }
                for i in range(5)
            ]
        }

        mocker.patch.object(cmd.yt, "api_get", side_effect=[playlist_response, enrich_response])

        channel = {"channel_id": "UC123", "title": "Test Channel", "custom_url": "@test"}
        result = cmd._sample_channel(channel, max_pages=20)

        assert result["summary"]["total_videos_sampled"] == 5
        assert result["summary"]["pages_fetched"] == 1
        assert result["summary"]["playlist_exhausted"] is True
        assert result["summary"]["quota_used"] == 2  # 1 playlist + 1 enrich
        assert len(result["videos"]) == 5
        # All videos should have scores
        assert all("score" in v for v in result["videos"])

    @pytest.mark.integration
    def test_respects_max_pages(self, cmd, mocker):
        def make_page(page_num, has_next):
            resp = {
                "items": [
                    {
                        "snippet": {
                            "resourceId": {"videoId": f"v{page_num}_{i}"},
                            "title": f"Video {page_num}_{i}",
                            "publishedAt": "2025-06-01T00:00:00Z",
                        }
                    }
                    for i in range(3)
                ],
            }
            if has_next:
                resp["nextPageToken"] = f"token_{page_num + 1}"
            return resp

        enrich_resp = {
            "items": [
                {
                    "id": f"v{p}_{i}",
                    "contentDetails": {"duration": "PT5M"},
                    "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "1"},
                }
                for p in range(2)
                for i in range(3)
            ]
        }

        # 2 pages of playlist + 2 enrichment calls
        mocker.patch.object(
            cmd.yt,
            "api_get",
            side_effect=[
                make_page(0, has_next=True),
                enrich_resp,
                make_page(1, has_next=True),
                enrich_resp,
            ],
        )

        channel = {"channel_id": "UC123", "title": "Test"}
        result = cmd._sample_channel(channel, max_pages=2)

        assert result["summary"]["pages_fetched"] == 2
        assert result["summary"]["playlist_exhausted"] is False

    @pytest.mark.integration
    def test_empty_playlist(self, cmd, mocker):
        mocker.patch.object(cmd.yt, "api_get", return_value={"items": []})

        channel = {"channel_id": "UC123", "title": "Empty Channel"}
        result = cmd._sample_channel(channel, max_pages=20)

        assert result["summary"]["total_videos_sampled"] == 0
        assert result["videos"] == []
        assert result["summary"]["duration_stats"]["min"] == 0

    @pytest.mark.integration
    def test_summary_stats_computed(self, cmd, mocker):
        playlist_response = {
            "items": [
                {
                    "snippet": {
                        "resourceId": {"videoId": f"v{i}"},
                        "title": f"Video {i}",
                        "publishedAt": "2025-06-01T00:00:00Z",
                    }
                }
                for i in range(3)
            ],
        }
        enrich_response = {
            "items": [
                {
                    "id": "v0",
                    "contentDetails": {"duration": "PT30S"},
                    "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "1"},
                },
                {
                    "id": "v1",
                    "contentDetails": {"duration": "PT10M"},
                    "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "5"},
                },
                {
                    "id": "v2",
                    "contentDetails": {"duration": "PT1H"},
                    "statistics": {"viewCount": "10000", "likeCount": "500", "commentCount": "50"},
                },
            ]
        }
        mocker.patch.object(cmd.yt, "api_get", side_effect=[playlist_response, enrich_response])

        channel = {"channel_id": "UC123", "title": "Mixed"}
        result = cmd._sample_channel(channel, max_pages=20)

        summary = result["summary"]
        assert summary["duration_buckets"]["under_1m"] == 1
        assert summary["duration_buckets"]["10_20m"] == 1  # PT10M = 600s → 10_20m
        assert summary["duration_buckets"]["1_2h"] == 1  # PT1H = 3600s → 1_2h bucket
        assert summary["passing_filter_counts"]["min_60s"] == 2
        assert summary["duration_stats"]["min"] == 30
        assert summary["duration_stats"]["max"] == 3600


# ─── _upsert_calibration (mocked DB) ────────────────────────────────────────


class TestUpsertCalibration:
    @pytest.mark.integration
    def test_upserts_to_db(self, cmd):
        summary = {
            "channel_id": "UC123",
            "total_videos_sampled": 50,
            "passing_filter_counts": {"min_60s": 40, "min_60s_max_3600s": 35, "min_300s": 30, "min_300s_max_3600s": 25},
            "duration_buckets": {"under_1m": 5, "1_5m": 10},
        }
        videos = [
            {"duration_seconds": 600, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 600, "published_at": "2025-06-08T00:00:00Z"},
        ]

        cmd._upsert_calibration(summary, videos)

        # Verify upsert was called on channel_calibration table
        cmd.db.client.table.assert_called_with("channel_calibration")
        cmd.db.client.upsert.assert_called_once()
        record = cmd.db.client.upsert.call_args[0][0]
        assert record["channel_id"] == "UC123"
        assert record["total_videos_sampled"] == 50
        assert record["passing_min60"] == 40


# ─── Full run integration (mocked everything) ───────────────────────────────


class TestCalibrateCommandRun:
    @pytest.mark.integration
    def test_writes_output_files(self, cmd, mocker, tmp_path):
        mocker.patch.object(
            cmd.db,
            "fetch_curated_channels",
            return_value=[{"channel_id": "UC123", "title": "TestCh", "custom_url": "@test"}],
        )

        playlist_response = {
            "items": [
                {
                    "snippet": {
                        "resourceId": {"videoId": "v1"},
                        "title": "Video 1",
                        "publishedAt": "2025-06-01T00:00:00Z",
                    }
                }
            ],
        }
        enrich_response = {
            "items": [
                {
                    "id": "v1",
                    "contentDetails": {"duration": "PT5M"},
                    "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "5"},
                }
            ]
        }
        mocker.patch.object(cmd.yt, "api_get", side_effect=[playlist_response, enrich_response])
        mocker.patch.object(cmd, "_upsert_calibration")

        cmd.run(max_pages=5)

        # Per-channel JSON should exist
        json_files = list(tmp_path.glob("*.json"))
        assert len(json_files) >= 1

        # Summary JSON should exist
        summary_path = tmp_path / "_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["channels_sampled"] == 1
        assert summary["total_videos_sampled"] == 1

    @pytest.mark.integration
    def test_single_channel_filter(self, cmd, mocker):
        mocker.patch.object(
            cmd.db,
            "fetch_curated_channels",
            return_value=[
                {"channel_id": "UC111", "title": "Chan A"},
                {"channel_id": "UC222", "title": "Chan B"},
            ],
        )

        playlist_response = {
            "items": [
                {
                    "snippet": {
                        "resourceId": {"videoId": "v1"},
                        "title": "Video",
                        "publishedAt": "2025-06-01T00:00:00Z",
                    }
                }
            ]
        }
        enrich_response = {
            "items": [
                {
                    "id": "v1",
                    "contentDetails": {"duration": "PT5M"},
                    "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "1"},
                }
            ]
        }
        mocker.patch.object(cmd.yt, "api_get", side_effect=[playlist_response, enrich_response])
        mocker.patch.object(cmd, "_upsert_calibration")

        cmd.run(max_pages=1, channel="UC222")

        # Should only have processed UC222
        summary = json.loads((cmd.output_dir / "_summary.json").read_text(encoding="utf-8"))
        assert summary["channels_sampled"] == 1

    @pytest.mark.integration
    def test_channel_not_found(self, cmd, mocker, capsys):
        mocker.patch.object(
            cmd.db,
            "fetch_curated_channels",
            return_value=[{"channel_id": "UC111", "title": "Chan A"}],
        )

        cmd.run(max_pages=1, channel="UC_NONEXISTENT")

        output = capsys.readouterr().out
        assert "not found" in output

    @pytest.mark.integration
    def test_no_channels(self, cmd, mocker, capsys):
        mocker.patch.object(cmd.db, "fetch_curated_channels", return_value=[])

        cmd.run(max_pages=1)

        # Should print summary with 0 channels — _summary.json should exist
        assert (cmd.output_dir / "_summary.json").exists()

    @pytest.mark.integration
    def test_db_upsert_failure_continues(self, cmd, mocker, capsys):
        mocker.patch.object(
            cmd.db,
            "fetch_curated_channels",
            return_value=[{"channel_id": "UC123", "title": "TestCh"}],
        )

        playlist_response = {
            "items": [
                {
                    "snippet": {
                        "resourceId": {"videoId": "v1"},
                        "title": "Video",
                        "publishedAt": "2025-06-01T00:00:00Z",
                    }
                }
            ]
        }
        enrich_response = {
            "items": [
                {
                    "id": "v1",
                    "contentDetails": {"duration": "PT5M"},
                    "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "1"},
                }
            ]
        }
        mocker.patch.object(cmd.yt, "api_get", side_effect=[playlist_response, enrich_response])
        mocker.patch.object(cmd, "_upsert_calibration", side_effect=Exception("DB error"))

        cmd.run(max_pages=1)

        output = capsys.readouterr().out
        assert "WARNING" in output
        assert "DB error" in output
        # Should still write output files despite DB failure
        assert (cmd.output_dir / "_summary.json").exists()
