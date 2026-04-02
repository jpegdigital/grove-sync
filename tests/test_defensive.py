"""Defensive tests — edge conditions that may expose real bugs in the codebase.

These tests are written to FAIL if the code has the bug. Do not fix the code
until the user reviews which failures are real issues vs. acceptable behavior.
"""

import math
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.commands.calibrate import CalibrateCommand
from src.config import AppConfig
from src.scoring import VideoScorer
from src.services.db import SyncDatabase
from src.services.hls import HlsPipeline
from src.services.storage import R2Storage
from src.services.youtube import YouTubeClient, parse_date_range, parse_iso_duration

# ═════════════════════════════════════════════════════════════════════════════
# VideoScorer: division by zero and math domain errors
# ═════════════════════════════════════════════════════════════════════════════


class TestScorerZeroDivision:
    """half_life_days=0 causes ZeroDivisionError in math.exp(... / 0)."""

    @pytest.mark.safety
    def test_half_life_zero_does_not_crash(self):
        scorer = VideoScorer({"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}, half_life_days=0)
        video = {"view_count": 1000, "like_count": 50, "comment_count": 5, "published_at": "2025-01-01T00:00:00Z"}
        # This should not raise ZeroDivisionError
        score = scorer.score_video(video)
        assert isinstance(score, float)
        assert not math.isnan(score)
        assert not math.isinf(score)

    @pytest.mark.safety
    def test_half_life_negative_does_not_crash(self):
        scorer = VideoScorer({"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}, half_life_days=-90)
        video = {"view_count": 1000, "like_count": 50, "comment_count": 5}
        score = scorer.score_video(video)
        assert isinstance(score, float)
        assert not math.isnan(score)

    @pytest.mark.safety
    def test_ese_score_zero_half_life(self):
        scorer = VideoScorer({"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}, half_life_days=0)
        video = {"view_count": 1000, "like_count": 50, "comment_count": 5, "duration_seconds": 300}
        result = scorer.ese_score(video, alpha=0.3)
        assert isinstance(result, float)
        assert not math.isnan(result)


# ═════════════════════════════════════════════════════════════════════════════
# parse_iso_duration: missing number before unit letter
# ═════════════════════════════════════════════════════════════════════════════


class TestParseIsoDurationMalformed:
    """YouTube sometimes returns malformed durations."""

    @pytest.mark.safety
    def test_no_number_before_unit(self):
        """'PTM' — M with no preceding number should not crash."""
        result = parse_iso_duration("PTM")
        assert isinstance(result, int)

    @pytest.mark.safety
    def test_no_number_before_seconds(self):
        """'PTS' — S with no number."""
        result = parse_iso_duration("PTS")
        assert isinstance(result, int)

    @pytest.mark.safety
    def test_no_number_before_hours(self):
        """'PTH' — H with no number."""
        result = parse_iso_duration("PTH")
        assert isinstance(result, int)

    @pytest.mark.safety
    def test_mixed_malformed(self):
        """'PT1HM30S' — missing number before M."""
        result = parse_iso_duration("PT1HM30S")
        assert isinstance(result, int)


# ═════════════════════════════════════════════════════════════════════════════
# AppConfig.load_env: quoted values in .env files
# ═════════════════════════════════════════════════════════════════════════════


class TestLoadEnvQuotedValues:
    """Many .env files use quotes: KEY="value" or KEY='value'.
    The current parser does NOT strip quotes, which means the value
    includes the literal quote characters."""

    @pytest.mark.safety
    def test_double_quoted_value_stripped(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text('MY_KEY="my_value"\n')
        monkeypatch.delenv("MY_KEY", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        # Value should be 'my_value', not '"my_value"'
        assert os.environ["MY_KEY"] == "my_value"

    @pytest.mark.safety
    def test_single_quoted_value_stripped(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("MY_KEY='my_value'\n")
        monkeypatch.delenv("MY_KEY", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ["MY_KEY"] == "my_value"


# ═════════════════════════════════════════════════════════════════════════════
# AppConfig._load_yaml: YAML contains non-dict at top level or in sections
# ═════════════════════════════════════════════════════════════════════════════


class TestLoadYamlMalformed:
    @pytest.mark.safety
    def test_yaml_with_list_at_top_level(self, tmp_path):
        """YAML file that is a list instead of a dict should not crash."""
        config_file = tmp_path / "bad.yaml"
        config_file.write_text("- item1\n- item2\n")
        defaults = AppConfig._producer_defaults()
        # Should not raise TypeError on iteration
        result = AppConfig._load_yaml(config_file, defaults)
        assert result == defaults

    @pytest.mark.safety
    def test_yaml_section_is_list_not_dict(self, tmp_path):
        """A section that is a list instead of dict would crash on .update()."""
        config_file = tmp_path / "bad.yaml"
        config_file.write_text("api:\n  - page_size: 25\n")
        defaults = AppConfig._producer_defaults()
        # The .update() call expects a dict, but gets a list
        result = AppConfig._load_yaml(config_file, defaults)
        assert "api" in result


# ═════════════════════════════════════════════════════════════════════════════
# measure_peak_bandwidth: target_duration=0 causes division issues
# ═════════════════════════════════════════════════════════════════════════════


class TestMeasurePeakBandwidthZeroDuration:
    @pytest.mark.safety
    def test_target_duration_zero(self, tmp_path):
        """target_duration=0 → min_dur=0, max_dur=0, bitrate division by total_dur=0."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        (hls_dir / "init.mp4").write_bytes(b"\x00" * 100)
        (hls_dir / "seg_000.m4s").write_bytes(b"\x00" * 50_000)

        playlist = (
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:6\n"
            '#EXT-X-MAP:URI="init.mp4"\n#EXTINF:6.0,\nseg_000.m4s\n#EXT-X-ENDLIST\n'
        )
        (hls_dir / "playlist.m3u8").write_text(playlist, encoding="utf-8")

        # Should not raise ZeroDivisionError
        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=0)
        assert result is None or isinstance(result, int)

    @pytest.mark.safety
    def test_target_duration_negative(self, tmp_path):
        """Negative target_duration → negative min_dur/max_dur, all segments skipped."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        (hls_dir / "seg_000.m4s").write_bytes(b"\x00" * 50_000)

        playlist = "#EXTM3U\n#EXTINF:6.0,\nseg_000.m4s\n#EXT-X-ENDLIST\n"
        (hls_dir / "playlist.m3u8").write_text(playlist, encoding="utf-8")

        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=-1)
        assert result is None or isinstance(result, int)


# ═════════════════════════════════════════════════════════════════════════════
# SyncDatabase.fail_job: race condition — job deleted between SELECT and UPDATE
# ═════════════════════════════════════════════════════════════════════════════


class TestFailJobRaceCondition:
    @pytest.mark.safety
    def test_job_deleted_before_select(self):
        """If the job is already gone, fail_job should not crash on empty resp.data."""
        mock = MagicMock()
        mock.table.return_value = mock
        mock.select.return_value = mock
        mock.update.return_value = mock
        mock.eq.return_value = mock
        # SELECT returns None (job doesn't exist)
        mock.execute.side_effect = [
            SimpleNamespace(data=None),
            SimpleNamespace(data=[]),
        ]

        db = SyncDatabase(mock)
        # Should not raise IndexError or TypeError
        db.fail_job("deleted-job-id", "some error")


# ═════════════════════════════════════════════════════════════════════════════
# R2Storage.build_r2_key: special characters in video_id or handle
# ═════════════════════════════════════════════════════════════════════════════


class TestR2KeySpecialChars:
    @pytest.mark.safety
    def test_video_id_with_hyphens_and_underscores(self):
        """YouTube video IDs can contain hyphens and underscores."""
        key = R2Storage.build_r2_key("@chan", "2025-06-01T00:00:00Z", "dQw4w9WgXcQ", "master.m3u8")
        assert "dQw4w9WgXcQ" in key

    @pytest.mark.safety
    def test_handle_with_dots_and_hyphens(self):
        """Some handles have dots or hyphens."""
        key = R2Storage.build_r2_key("@my.channel-name", "2025-06-01T00:00:00Z", "vid1", "master.m3u8")
        assert key.startswith("my.channel-name/")

    @pytest.mark.safety
    def test_handle_with_unicode(self):
        """Non-ASCII handle shouldn't crash (even if R2 may reject it later)."""
        key = R2Storage.build_r2_key("@日本語チャンネル", "2025-06-01T00:00:00Z", "vid1", "master.m3u8")
        assert "vid1" in key

    @pytest.mark.safety
    def test_relative_path_with_path_traversal(self):
        """Relative path with .. should be passed through (caller's responsibility)."""
        key = R2Storage.build_r2_key("chan", "2025-06-01T00:00:00Z", "vid1", "../../../etc/passwd")
        # This is a potential security issue — the key should not allow traversal
        # For now just verify it doesn't crash
        assert isinstance(key, str)


# ═════════════════════════════════════════════════════════════════════════════
# CalibrateCommand._compute_cadence: all videos at same timestamp
# ═════════════════════════════════════════════════════════════════════════════


class TestComputeCadenceSameTimestamp:
    @pytest.mark.safety
    def test_all_videos_same_timestamp(self):
        """All videos published at exact same time → span_days=0 before the max(..., 1) clamp.
        But gaps would all be 0.0 days. posts_per_week would be len/1*7 = huge."""
        videos = [
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
            {"duration_seconds": 300, "published_at": "2025-06-01T00:00:00Z"},
        ]
        result = CalibrateCommand._compute_cadence(videos)
        # Should not produce infinity or unreasonable values
        assert result["posts_per_week"] < 1000  # sanity check
        assert result["avg_gap_days"] is not None
        assert result["avg_gap_days"] >= 0


# ═════════════════════════════════════════════════════════════════════════════
# YouTubeClient: API returns 200 with non-JSON body
# ═════════════════════════════════════════════════════════════════════════════


class TestApiGetBadJsonResponse:
    @pytest.mark.safety
    def test_200_with_invalid_json(self, mocker):
        """YouTube returns 200 but body isn't valid JSON (rare but happens)."""
        client = YouTubeClient("key", {"max_retries": 0})

        mock_resp = MagicMock(status_code=200)
        mock_resp.json.side_effect = ValueError("No JSON object could be decoded")
        mocker.patch("src.services.youtube.requests.get", return_value=mock_resp)

        # Should raise a clear error, not return garbage
        with pytest.raises((ValueError, Exception)):
            client.api_get("https://example.com", {})


# ═════════════════════════════════════════════════════════════════════════════
# YouTubeClient.enrich_videos: API response missing 'items' key entirely
# ═════════════════════════════════════════════════════════════════════════════


class TestEnrichVideosMissingItemsKey:
    @pytest.mark.safety
    def test_response_has_no_items_key(self, mocker):
        """API returns {} or {"error": ...} with no 'items' key at all."""
        client = YouTubeClient("key", {"enrichment_batch_size": 50})
        mocker.patch.object(client, "api_get", return_value={})

        details, calls = client.enrich_videos(["v1", "v2"])
        # Should not crash — just return empty details
        assert details == {}
        assert calls == 1


# ═════════════════════════════════════════════════════════════════════════════
# VideoScorer: NaN/inf propagation from bad video data
# ═════════════════════════════════════════════════════════════════════════════


class TestScorerNanInfPropagation:
    WEIGHTS = {"popularity": 0.35, "engagement": 0.35, "freshness": 0.30}

    @pytest.mark.safety
    def test_nan_view_count(self):
        """If view_count is somehow NaN (from bad float coercion), score should not be NaN."""
        scorer = VideoScorer(self.WEIGHTS, 90)
        video = {"view_count": float("nan"), "like_count": 0, "comment_count": 0}
        score = scorer.score_video(video)
        assert not math.isnan(score)

    @pytest.mark.safety
    def test_inf_view_count(self):
        scorer = VideoScorer(self.WEIGHTS, 90)
        video = {"view_count": float("inf"), "like_count": 0, "comment_count": 0}
        score = scorer.score_video(video)
        assert not math.isinf(score) or score > 0  # inf score is debatable but shouldn't crash


# ═════════════════════════════════════════════════════════════════════════════
# parse_date_range: edge cases that could produce unexpected results
# ═════════════════════════════════════════════════════════════════════════════


class TestParseDateRangeDefensive:
    @pytest.mark.safety
    def test_today_zero_months(self):
        """'today-0months' → should return roughly now, not crash."""
        result = parse_date_range("today-0months")
        now = datetime.now(timezone.utc)
        diff = abs((now - result).total_seconds())
        assert diff < 86400  # within 1 day of now

    @pytest.mark.safety
    def test_today_zero_years(self):
        result = parse_date_range("today-0years")
        now = datetime.now(timezone.utc)
        diff = abs((now - result).total_seconds())
        assert diff < 86400

    @pytest.mark.safety
    def test_very_large_relative(self):
        """'today-9999years' should not crash (even if result is before epoch)."""
        result = parse_date_range("today-9999years")
        assert result.tzinfo == timezone.utc


# ═════════════════════════════════════════════════════════════════════════════
# HlsPipeline.parse_info_json: binary / non-UTF8 file content
# ═════════════════════════════════════════════════════════════════════════════


class TestParseInfoJsonBinary:
    @pytest.mark.safety
    def test_binary_file_content(self, tmp_path):
        """An info.json that is actually binary garbage should return None, not crash."""
        path = tmp_path / "video.info.json"
        path.write_bytes(b"\x00\xff\xfe\x80" * 100)
        result = HlsPipeline.parse_info_json(path)
        assert result is None

    @pytest.mark.safety
    def test_latin1_encoded_file(self, tmp_path):
        """File saved in Latin-1 with non-ASCII chars might fail UTF-8 decode."""
        path = tmp_path / "video.info.json"
        path.write_bytes('{"title": "caf\xe9"}'.encode("latin-1"))
        result = HlsPipeline.parse_info_json(path)
        # Should return None (UnicodeDecodeError caught) or parse if it happens to be valid
        assert result is None or isinstance(result, dict)


# ═════════════════════════════════════════════════════════════════════════════
# Config shallow merge: nested dict override loses sibling keys
# ═════════════════════════════════════════════════════════════════════════════


class TestConfigShallowMergeBug:
    @pytest.mark.safety
    def test_overriding_nested_dict_preserves_siblings(self, tmp_path):
        """If YAML overrides scoring.weights.popularity, the other weights
        (engagement, freshness) should NOT be lost. The current shallow merge
        does defaults[section].update(file_cfg[section]) which REPLACES the
        entire 'weights' sub-dict."""
        config_file = tmp_path / "producer.yaml"
        config_file.write_text("scoring:\n  weights:\n    popularity: 0.5\n")
        defaults = AppConfig._producer_defaults()
        result = AppConfig._load_yaml(config_file, defaults)

        weights = result["scoring"]["weights"]
        # After merge, engagement and freshness should still exist
        assert "engagement" in weights, "Shallow merge lost 'engagement' key"
        assert "freshness" in weights, "Shallow merge lost 'freshness' key"
        assert weights["popularity"] == 0.5


# ═════════════════════════════════════════════════════════════════════════════
# YouTubeClient: quota double-counting
# ═════════════════════════════════════════════════════════════════════════════


class TestQuotaDoubleCount:
    @pytest.mark.safety
    def test_fetch_search_does_not_double_count_enrich_quota(self, mocker):
        """fetch_search_videos calls enrich_videos which adds to self.quota_used,
        then fetch_search_videos ALSO adds to self.quota_used. The enrich quota
        may be counted twice."""
        client = YouTubeClient("key", {"enrichment_batch_size": 50})

        search_resp = {
            "items": [
                {
                    "id": {"videoId": "v1"},
                    "snippet": {"title": "V1", "publishedAt": "", "description": "", "thumbnails": {}},
                }
            ]
        }
        enrich_resp = {
            "items": [
                {
                    "id": "v1",
                    "contentDetails": {"duration": "PT5M"},
                    "statistics": {"viewCount": "100", "likeCount": "5", "commentCount": "1"},
                }
            ]
        }
        mocker.patch.object(client, "api_get", side_effect=[search_resp, enrich_resp])

        videos, quota = client.fetch_search_videos("UC123", "viewCount")

        # quota should be 101 (100 for search + 1 for enrich)
        assert quota == 101
        # But self.quota_used might be 102 if enrich_videos also adds to it
        # and then fetch_search_videos adds quota_used again
        assert client.quota_used == 101, (
            f"Expected quota_used=101, got {client.quota_used}. Enrich quota is being double-counted."
        )
