"""Integration and edge-case tests for sync_consumer.

Integration tests exercise multi-function flows with mocked external boundaries
(subprocess, R2, Supabase). Edge-case tests push pure functions to boundary conditions.
"""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.commands.process import ProcessCommand
from src.config import AppConfig, ConfigError
from src.services.db import SyncDatabase
from src.services.hls import HlsPipeline
from src.services.storage import R2Storage

# ─── Supabase mock helper ───────────────────────────────────────────────────


def _mock_supabase_chain():
    """Create a mock that supports chained Supabase client calls."""
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


# ─── validate_env ────────────────────────────────────────────────────────────


class TestValidateEnv:
    REQUIRED_VARS = [
        "NEXT_PUBLIC_SUPABASE_URL",
        "SUPABASE_SECRET_KEY",
        "R2_ACCOUNT_ID",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
    ]

    @pytest.mark.safety
    def test_raises_when_all_missing(self, monkeypatch):
        for var in self.REQUIRED_VARS:
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(ConfigError):
            AppConfig().validate_consumer_env()

    @pytest.mark.safety
    def test_raises_when_one_missing(self, monkeypatch):
        for var in self.REQUIRED_VARS:
            monkeypatch.setenv(var, "test-value")
        monkeypatch.delenv("R2_BUCKET_NAME")
        with pytest.raises(ConfigError):
            AppConfig().validate_consumer_env()

    @pytest.mark.integration
    def test_passes_when_all_set(self, monkeypatch):
        for var in self.REQUIRED_VARS:
            monkeypatch.setenv(var, "test-value")
        AppConfig().validate_consumer_env()  # should not raise


# ─── resolve_channel_handle ──────────────────────────────────────────────────


class TestResolveChannelHandle:
    @pytest.mark.integration
    def test_uses_metadata_handle(self):
        client = _mock_supabase_chain()
        db = SyncDatabase(client)
        job = {"metadata": {"handle": "@testchannel"}, "channel_id": "UC123"}
        assert db.resolve_channel_handle(job) == "@testchannel"

    @pytest.mark.integration
    def test_uses_metadata_channel_handle(self):
        client = _mock_supabase_chain()
        db = SyncDatabase(client)
        job = {"metadata": {"channel_handle": "@alt_handle"}, "channel_id": "UC123"}
        assert db.resolve_channel_handle(job) == "@alt_handle"

    @pytest.mark.integration
    def test_falls_back_to_db_lookup(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=[{"custom_url": "@fromdb"}])
        db = SyncDatabase(client)
        job = {"metadata": {}, "channel_id": "UC123"}
        assert db.resolve_channel_handle(job) == "@fromdb"

    @pytest.mark.integration
    def test_returns_unknown_when_all_fail(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=[])
        db = SyncDatabase(client)
        job = {"metadata": {}, "channel_id": "UC123"}
        assert db.resolve_channel_handle(job) == "unknown"

    @pytest.mark.edge_cases
    def test_none_metadata(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=[])
        db = SyncDatabase(client)
        job = {"metadata": None, "channel_id": "UC123"}
        assert db.resolve_channel_handle(job) == "unknown"

    @pytest.mark.edge_cases
    def test_no_channel_id(self):
        client = _mock_supabase_chain()
        db = SyncDatabase(client)
        job = {"metadata": {}}
        assert db.resolve_channel_handle(job) == "unknown"


# ─── Queue operations ───────────────────────────────────────────────────────


class TestClaimJobs:
    @pytest.mark.integration
    def test_returns_job_list(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(
            data=[
                {"id": "j1", "video_id": "v1", "action": "download"},
                {"id": "j2", "video_id": "v2", "action": "remove"},
            ]
        )
        db = SyncDatabase(client)
        result = db.claim_jobs(batch_size=50, max_attempts=3)
        assert len(result) == 2
        assert result[0]["action"] == "download"

    @pytest.mark.integration
    def test_returns_empty_on_no_jobs(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=None)
        db = SyncDatabase(client)
        result = db.claim_jobs(batch_size=50, max_attempts=3)
        assert result == []


class TestFailJob:
    @pytest.mark.integration
    def test_increments_attempts(self):
        client = _mock_supabase_chain()
        # First call (select attempts) returns 2
        # Second call (update) returns success
        client.execute.side_effect = [
            SimpleNamespace(data=[{"attempts": 2}]),
            SimpleNamespace(data=[]),
        ]

        db = SyncDatabase(client)
        db.fail_job("job123", "Something went wrong")

        # Verify update was called with attempts=3
        update_call = client.update.call_args[0][0]
        assert update_call["attempts"] == 3
        assert update_call["status"] == "pending"
        assert update_call["started_at"] is None

    @pytest.mark.edge_cases
    def test_truncates_long_error_message(self):
        client = _mock_supabase_chain()
        client.execute.side_effect = [
            SimpleNamespace(data=[{"attempts": 0}]),
            SimpleNamespace(data=[]),
        ]

        db = SyncDatabase(client)
        long_error = "x" * 2000
        db.fail_job("job123", long_error)

        update_call = client.update.call_args[0][0]
        assert len(update_call["error"]) == 1000

    @pytest.mark.edge_cases
    def test_no_existing_attempts_defaults_to_zero(self):
        client = _mock_supabase_chain()
        client.execute.side_effect = [
            SimpleNamespace(data=[{}]),  # no "attempts" key
            SimpleNamespace(data=[]),
        ]

        db = SyncDatabase(client)
        db.fail_job("job123", "error")

        update_call = client.update.call_args[0][0]
        assert update_call["attempts"] == 1


class TestCompleteJob:
    @pytest.mark.integration
    def test_deletes_job(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=[])

        db = SyncDatabase(client)
        db.complete_job("job123")

        client.table.assert_called_with("sync_queue")
        client.delete.assert_called_once()


class TestResetStaleLocks:
    @pytest.mark.integration
    def test_returns_count(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=5)

        db = SyncDatabase(client)
        result = db.reset_stale_locks(stale_lock_minutes=60)
        assert result == 5

    @pytest.mark.edge_cases
    def test_non_int_response_returns_zero(self):
        client = _mock_supabase_chain()
        client.execute.return_value = SimpleNamespace(data=None)

        db = SyncDatabase(client)
        result = db.reset_stale_locks(stale_lock_minutes=60)
        assert result == 0


# ─── cleanup_staging ─────────────────────────────────────────────────────────


class TestCleanupStaging:
    @pytest.mark.integration
    def test_removes_directory_tree(self, tmp_path):
        staging = tmp_path / "staging" / "vid123"
        staging.mkdir(parents=True)
        (staging / "video.mp4").write_bytes(b"\x00" * 100)
        (staging / "sub_dir").mkdir()
        (staging / "sub_dir" / "file.txt").write_text("data")

        ProcessCommand._cleanup_staging(staging)
        assert not staging.exists()

    @pytest.mark.edge_cases
    def test_nonexistent_dir_does_not_raise(self, tmp_path):
        ProcessCommand._cleanup_staging(tmp_path / "nonexistent")
        # should not raise


# ─── generate_master_playlist edge cases ─────────────────────────────────────


class TestGenerateMasterPlaylistEdgeCases:
    @pytest.mark.edge_cases
    def test_single_tier_with_unusual_resolution(self):
        tiers = [
            {
                "label": "360p",
                "bandwidth": 800000,
                "resolution": "640x360",
                "codecs": "avc1.42001e,mp4a.40.2",
            },
        ]
        content = HlsPipeline.generate_master_playlist(tiers)
        assert "RESOLUTION=640x360" in content
        assert "BANDWIDTH=800000" in content
        assert "360p/playlist.m3u8" in content

    @pytest.mark.edge_cases
    def test_many_tiers(self):
        tiers = [
            {
                "label": f"{h}p",
                "bandwidth": h * 3000,
                "resolution": f"{int(h * 16 / 9)}x{h}",
                "codecs": "avc1.640028,mp4a.40.2",
            }
            for h in [240, 360, 480, 720, 1080, 1440, 2160]
        ]
        content = HlsPipeline.generate_master_playlist(tiers)
        assert content.count("EXT-X-STREAM-INF") == 7
        assert "2160p/playlist.m3u8" in content

    @pytest.mark.edge_cases
    def test_special_characters_in_codecs(self):
        """Codecs with special chars should be properly quoted."""
        tiers = [
            {
                "label": "720p",
                "bandwidth": 2500000,
                "resolution": "1280x720",
                "codecs": "avc1.640028,mp4a.40.2",
            },
        ]
        content = HlsPipeline.generate_master_playlist(tiers)
        # Codecs must be double-quoted in the playlist
        assert 'CODECS="avc1.640028,mp4a.40.2"' in content

    @pytest.mark.edge_cases
    def test_trailing_newline(self):
        tiers = [
            {
                "label": "720p",
                "bandwidth": 2500000,
                "resolution": "1280x720",
                "codecs": "avc1.640028,mp4a.40.2",
            },
        ]
        content = HlsPipeline.generate_master_playlist(tiers)
        assert content.endswith("\n")


# ─── build_r2_key_hls edge cases ────────────────────────────────────────────


class TestBuildR2KeyHlsEdgeCases:
    @pytest.mark.edge_cases
    @pytest.mark.parametrize(
        "give_handle, want_prefix",
        [
            ("@@@triple_at", "triple_at"),
            ("", ""),
            ("simple", "simple"),
        ],
        ids=["multiple-at-signs", "empty-handle", "no-at-sign"],
    )
    def test_handle_edge_cases(self, give_handle, want_prefix):
        key = R2Storage.build_r2_key(give_handle, "2025-06-15T00:00:00Z", "vid1", "master.m3u8")
        assert key.startswith(f"{want_prefix}/2025-06/")

    @pytest.mark.edge_cases
    def test_deeply_nested_relative_path(self):
        key = R2Storage.build_r2_key("chan", "2025-06-15T00:00:00Z", "vid1", "720p/segments/seg_000.m4s")
        assert key == "chan/2025-06/vid1/720p/segments/seg_000.m4s"

    @pytest.mark.edge_cases
    def test_published_at_with_microseconds(self):
        key = R2Storage.build_r2_key("chan", "2025-06-15T12:30:45.123456+00:00", "vid1", "master.m3u8")
        assert key == "chan/2025-06/vid1/master.m3u8"

    @pytest.mark.edge_cases
    def test_published_at_with_offset(self):
        key = R2Storage.build_r2_key("chan", "2025-06-15T12:30:45+05:30", "vid1", "master.m3u8")
        assert key == "chan/2025-06/vid1/master.m3u8"

    @pytest.mark.edge_cases
    def test_december_month_padding(self):
        key = R2Storage.build_r2_key("chan", "2025-12-01T00:00:00Z", "vid1", "master.m3u8")
        assert "2025-12" in key

    @pytest.mark.edge_cases
    def test_january_month_padding(self):
        key = R2Storage.build_r2_key("chan", "2025-01-15T00:00:00Z", "vid1", "master.m3u8")
        assert "2025-01" in key


# ─── build_codec_string edge cases ──────────────────────────────────────────


class TestBuildCodecStringEdgeCases:
    @pytest.mark.edge_cases
    def test_both_streams_missing(self):
        result = HlsPipeline.build_codec_string({"video": None, "audio": None})
        assert result == "avc1.640028,mp4a.40.2"

    @pytest.mark.edge_cases
    def test_empty_probe_dict(self):
        result = HlsPipeline.build_codec_string({})
        assert result == "avc1.640028,mp4a.40.2"

    @pytest.mark.edge_cases
    def test_video_missing_profile(self):
        result = HlsPipeline.build_codec_string(
            {
                "video": {"codec_name": "h264", "level": 40},
                "audio": {"codec_name": "aac", "profile": "LC"},
            }
        )
        # Missing profile defaults to High (64)
        assert result.startswith("avc1.64")

    @pytest.mark.edge_cases
    def test_video_missing_level(self):
        result = HlsPipeline.build_codec_string(
            {
                "video": {"codec_name": "h264", "profile": "High"},
                "audio": {"codec_name": "aac", "profile": "LC"},
            }
        )
        # Missing level defaults to 31
        assert result.startswith("avc1.6400")

    @pytest.mark.edge_cases
    def test_audio_missing_profile(self):
        result = HlsPipeline.build_codec_string(
            {
                "video": {"codec_name": "h264", "profile": "High", "level": 40},
                "audio": {"codec_name": "aac"},
            }
        )
        # Missing audio profile defaults to LC (2)
        assert result.endswith("mp4a.40.2")

    @pytest.mark.edge_cases
    def test_non_aac_audio_uses_fallback(self):
        result = HlsPipeline.build_codec_string(
            {
                "video": {"codec_name": "h264", "profile": "High", "level": 40},
                "audio": {"codec_name": "opus", "profile": ""},
            }
        )
        assert result.endswith("mp4a.40.2")

    @pytest.mark.edge_cases
    def test_high_10_profile(self):
        result = HlsPipeline.build_codec_string(
            {
                "video": {"codec_name": "h264", "profile": "High 10", "level": 41},
                "audio": {"codec_name": "aac", "profile": "LC"},
            }
        )
        assert result.startswith("avc1.6e0029")


# ─── parse_info_json edge cases ──────────────────────────────────────────────


class TestParseInfoJsonEdgeCases:
    @pytest.mark.edge_cases
    def test_upload_date_wrong_length(self, tmp_path):
        info = {"upload_date": "2025"}
        path = tmp_path / "video.info.json"
        path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(path)
        assert result["published_at"] is None

    @pytest.mark.edge_cases
    def test_upload_date_none(self, tmp_path):
        info = {"upload_date": None}
        path = tmp_path / "video.info.json"
        path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(path)
        assert result["published_at"] is None

    @pytest.mark.edge_cases
    def test_empty_chapters_list(self, tmp_path):
        info = {"chapters": []}
        path = tmp_path / "video.info.json"
        path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(path)
        # Empty list is falsy, should result in None
        assert result["chapters"] is None

    @pytest.mark.edge_cases
    def test_unicode_title(self, tmp_path):
        info = {"title": "日本語タイトル 🎵 émojis & spëcial chars"}
        path = tmp_path / "video.info.json"
        path.write_text(json.dumps(info, ensure_ascii=False), encoding="utf-8")

        result = HlsPipeline.parse_info_json(path)
        assert result["title"] == "日本語タイトル 🎵 émojis & spëcial chars"

    @pytest.mark.edge_cases
    def test_very_large_info_json(self, tmp_path):
        info = {
            "title": "Test",
            "description": "x" * 100_000,
            "tags": [f"tag{i}" for i in range(1000)],
        }
        path = tmp_path / "video.info.json"
        path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(path)
        assert result is not None
        assert len(result["description"]) == 100_000

    @pytest.mark.edge_cases
    def test_null_values_for_all_fields(self, tmp_path):
        info = {
            "title": None,
            "fulltitle": None,
            "description": None,
            "duration": None,
            "view_count": None,
            "thumbnail": None,
            "uploader_id": None,
            "tags": None,
            "categories": None,
        }
        path = tmp_path / "video.info.json"
        path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(path)
        assert result["title"] == "Untitled"
        assert result["description"] == ""
        assert result["tags"] == []
        assert result["thumbnail_url"] == ""


# ─── measure_peak_bandwidth edge cases ──────────────────────────────────────


class TestMeasurePeakBandwidthEdgeCases:
    @pytest.mark.edge_cases
    def test_single_segment(self, tmp_path):
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        (hls_dir / "init.mp4").write_bytes(b"\x00" * 500)
        (hls_dir / "seg_000.m4s").write_bytes(b"\x00" * 100_000)

        playlist = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:7\n"
            "#EXT-X-TARGETDURATION:6\n"
            '#EXT-X-MAP:URI="init.mp4"\n'
            "#EXTINF:5.5,\n"
            "seg_000.m4s\n"
            "#EXT-X-ENDLIST\n"
        )
        (hls_dir / "playlist.m3u8").write_text(playlist, encoding="utf-8")

        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=6)
        assert result is not None
        assert result > 0

    @pytest.mark.edge_cases
    def test_missing_segment_files(self, tmp_path):
        """Playlist references segments that don't exist on disk."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()

        playlist = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg_000.m4s\n#EXT-X-ENDLIST\n"
        (hls_dir / "playlist.m3u8").write_text(playlist, encoding="utf-8")

        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=6)
        assert result is None  # no segments found

    @pytest.mark.edge_cases
    def test_no_init_file(self, tmp_path):
        """Bandwidth calculation should work without init.mp4 (init_size=0)."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        (hls_dir / "seg_000.m4s").write_bytes(b"\x00" * 200_000)

        playlist = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg_000.m4s\n#EXT-X-ENDLIST\n"
        (hls_dir / "playlist.m3u8").write_text(playlist, encoding="utf-8")

        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=6)
        assert result is not None
        # 200KB over 6s = ~267 kbps
        expected = int((200_000 * 8) / 6.0)
        assert abs(result - expected) < 1000

    @pytest.mark.edge_cases
    def test_varying_segment_durations(self, tmp_path):
        """Segments with wildly different durations."""
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        (hls_dir / "init.mp4").write_bytes(b"\x00" * 100)

        # Short segment (2s), normal segment (6s), long segment (10s)
        (hls_dir / "seg_000.m4s").write_bytes(b"\x00" * 50_000)
        (hls_dir / "seg_001.m4s").write_bytes(b"\x00" * 150_000)
        (hls_dir / "seg_002.m4s").write_bytes(b"\x00" * 250_000)

        playlist = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:7\n"
            "#EXT-X-TARGETDURATION:10\n"
            '#EXT-X-MAP:URI="init.mp4"\n'
            "#EXTINF:2.0,\n"
            "seg_000.m4s\n"
            "#EXTINF:6.0,\n"
            "seg_001.m4s\n"
            "#EXTINF:10.0,\n"
            "seg_002.m4s\n"
            "#EXT-X-ENDLIST\n"
        )
        (hls_dir / "playlist.m3u8").write_text(playlist, encoding="utf-8")

        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=6)
        assert result is not None
        assert result > 0


# ─── load_config edge cases ──────────────────────────────────────────────────


class TestConsumerLoadConfigEdgeCases:
    @pytest.mark.edge_cases
    def test_config_file_override(self, tmp_path):
        """Values from consumer.yaml should override defaults."""
        config_content = """
consumer:
  batch_size: 200
  max_attempts: 5
"""
        config_file = tmp_path / "consumer.yaml"
        config_file.write_text(config_content)

        cfg = AppConfig._load_yaml(config_file, AppConfig._consumer_defaults())
        assert cfg["consumer"]["batch_size"] == 200
        assert cfg["consumer"]["max_attempts"] == 5
        # Non-overridden defaults should remain
        assert cfg["consumer"]["stale_lock_minutes"] == 60

    @pytest.mark.edge_cases
    def test_empty_config_file(self, tmp_path):
        config_file = tmp_path / "consumer.yaml"
        config_file.write_text("")

        cfg = AppConfig._load_yaml(config_file, AppConfig._consumer_defaults())
        # All defaults should still be present
        assert cfg["consumer"]["batch_size"] == 50

    @pytest.mark.edge_cases
    def test_missing_config_file_uses_defaults(self, tmp_path):
        cfg = AppConfig._load_yaml(tmp_path / "nonexistent.yaml", AppConfig._consumer_defaults())
        assert cfg["consumer"]["batch_size"] == 50
        assert len(cfg["hls"]["tiers"]) >= 2


# ─── Full HLS master playlist -> R2 key integration ──────────────────────────


class TestHlsPipelineIntegration:
    @pytest.mark.integration
    def test_generate_playlist_then_build_keys(self):
        """End-to-end: generate master playlist content, then verify R2 keys match."""
        tiers = [
            {
                "label": "480p",
                "bandwidth": 1200000,
                "resolution": "854x480",
                "codecs": "avc1.4d401e,mp4a.40.2",
            },
            {
                "label": "720p",
                "bandwidth": 2500000,
                "resolution": "1280x720",
                "codecs": "avc1.640028,mp4a.40.2",
            },
        ]

        content = HlsPipeline.generate_master_playlist(tiers)

        # Verify playlist references the right tier paths
        assert "480p/playlist.m3u8" in content
        assert "720p/playlist.m3u8" in content

        # Build R2 keys for the files that would be uploaded
        channel = "@mychannel"
        pub = "2025-06-15T00:00:00Z"
        vid = "abc123"

        master_key = R2Storage.build_r2_key(channel, pub, vid, "master.m3u8")
        tier_480_key = R2Storage.build_r2_key(channel, pub, vid, "480p/playlist.m3u8")
        tier_720_key = R2Storage.build_r2_key(channel, pub, vid, "720p/playlist.m3u8")
        seg_key = R2Storage.build_r2_key(channel, pub, vid, "720p/seg_000.m4s")

        assert master_key == "mychannel/2025-06/abc123/master.m3u8"
        assert tier_480_key == "mychannel/2025-06/abc123/480p/playlist.m3u8"
        assert tier_720_key == "mychannel/2025-06/abc123/720p/playlist.m3u8"
        assert seg_key == "mychannel/2025-06/abc123/720p/seg_000.m4s"

    @pytest.mark.integration
    def test_codec_string_used_in_playlist(self):
        """Codec string from ffprobe data should produce valid playlist content."""
        probe = {
            "video": {"codec_name": "h264", "profile": "Main", "level": 31},
            "audio": {"codec_name": "aac", "profile": "LC"},
        }
        codecs = HlsPipeline.build_codec_string(probe)
        assert codecs == "avc1.4d001f,mp4a.40.2"

        tier = {
            "label": "720p",
            "bandwidth": 2500000,
            "resolution": "1280x720",
            "codecs": codecs,
        }
        content = HlsPipeline.generate_master_playlist([tier])
        assert f'CODECS="{codecs}"' in content

    @pytest.mark.integration
    def test_ffmpeg_cmd_matches_config(self):
        """Config-driven segment duration should propagate to ffmpeg cmd."""
        cfg = AppConfig()
        segment_dur = cfg.hls["segment_duration"]

        cmd = HlsPipeline.build_ffmpeg_remux_cmd(
            Path("/tmp/video.mp4"),
            Path("/tmp/hls"),
            segment_duration=segment_dur,
        )

        idx = cmd.index("-hls_time")
        assert cmd[idx + 1] == str(segment_dur)

    @pytest.mark.integration
    def test_format_selector_per_tier_from_config(self):
        """Each configured tier should produce a valid format selector."""
        cfg = AppConfig()
        for tier in cfg.hls["tiers"]:
            fmt = HlsPipeline.build_format_selector(tier)
            assert f"height<={tier['height']}" in fmt
            assert "ext=mp4" in fmt


# ─── build_format_selector edge cases ───────────────────────────────────────


class TestBuildFormatSelectorEdgeCases:
    @pytest.mark.edge_cases
    def test_very_low_resolution(self):
        result = HlsPipeline.build_format_selector({"label": "144p", "height": 144, "bandwidth": 200000})
        assert "height<=144" in result

    @pytest.mark.edge_cases
    def test_4k_resolution(self):
        result = HlsPipeline.build_format_selector({"label": "2160p", "height": 2160, "bandwidth": 15000000})
        assert "height<=2160" in result


# ─── build_ffmpeg_remux_cmd edge cases ──────────────────────────────────────


class TestBuildFfmpegRemuxCmdEdgeCases:
    @pytest.mark.edge_cases
    def test_path_with_spaces(self, tmp_path):
        input_path = tmp_path / "my videos" / "video file.mp4"
        output_dir = tmp_path / "hls output"
        cmd = HlsPipeline.build_ffmpeg_remux_cmd(input_path, output_dir)
        # Paths should be passed as strings (subprocess handles quoting)
        assert str(input_path) in cmd
        assert str(output_dir / "playlist.m3u8") in cmd

    @pytest.mark.edge_cases
    def test_very_small_segment_duration(self, tmp_path):
        cmd = HlsPipeline.build_ffmpeg_remux_cmd(tmp_path / "v.mp4", tmp_path / "out", segment_duration=1)
        idx = cmd.index("-hls_time")
        assert cmd[idx + 1] == "1"

    @pytest.mark.edge_cases
    def test_large_segment_duration(self, tmp_path):
        cmd = HlsPipeline.build_ffmpeg_remux_cmd(tmp_path / "v.mp4", tmp_path / "out", segment_duration=60)
        idx = cmd.index("-hls_time")
        assert cmd[idx + 1] == "60"
