"""Tests for sync_consumer — pure functions, builders, parsers, and HLS pipeline helpers."""

import json

import pytest

from src.config import AppConfig
from src.services.hls import HlsPipeline
from src.services.storage import R2Storage

# ─── build_format_selector ──────────────────────────────────────────────────


class TestBuildFormatSelector:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want_contains",
        [
            ({"label": "480p", "height": 480, "bandwidth": 1200000}, "height<=480"),
            ({"label": "720p", "height": 720, "bandwidth": 2500000}, "height<=720"),
            ({"label": "1080p", "height": 1080, "bandwidth": 5000000}, "height<=1080"),
        ],
        ids=["480p", "720p", "1080p"],
    )
    def test_contains_height_filter(self, give, want_contains):
        result = HlsPipeline.build_format_selector(give)
        assert want_contains in result

    @pytest.mark.unit
    def test_prefers_mp4_with_h264(self):
        result = HlsPipeline.build_format_selector({"label": "720p", "height": 720, "bandwidth": 2500000})
        assert "ext=mp4" in result
        assert "avc|h264" in result

    @pytest.mark.unit
    def test_includes_audio_fallback(self):
        result = HlsPipeline.build_format_selector({"label": "720p", "height": 720, "bandwidth": 2500000})
        assert "ba[ext=m4a]" in result


# ─── build_ffmpeg_remux_cmd ──────────────────────────────────────────────────


class TestBuildFfmpegRemuxCmd:
    @pytest.mark.unit
    def test_contains_required_flags(self, tmp_path):
        input_path = tmp_path / "video.mp4"
        output_dir = tmp_path / "hls"

        cmd = HlsPipeline.build_ffmpeg_remux_cmd(input_path, output_dir, segment_duration=6)

        assert cmd[0] == "ffmpeg"
        assert "-c" in cmd
        assert "copy" in cmd
        assert "-f" in cmd
        assert "hls" in cmd
        assert "-hls_segment_type" in cmd
        assert "fmp4" in cmd

    @pytest.mark.unit
    def test_uses_segment_duration(self, tmp_path):
        cmd = HlsPipeline.build_ffmpeg_remux_cmd(tmp_path / "v.mp4", tmp_path / "out", segment_duration=10)
        idx = cmd.index("-hls_time")
        assert cmd[idx + 1] == "10"

    @pytest.mark.unit
    def test_output_paths(self, tmp_path):
        output_dir = tmp_path / "hls"
        cmd = HlsPipeline.build_ffmpeg_remux_cmd(tmp_path / "v.mp4", output_dir)

        # Playlist path should end with playlist.m3u8
        playlist_arg = cmd[-1]
        assert playlist_arg.endswith("playlist.m3u8")

        # Segment pattern should contain seg_%03d.m4s
        seg_idx = cmd.index("-hls_segment_filename")
        assert "seg_%03d.m4s" in cmd[seg_idx + 1]

    @pytest.mark.unit
    def test_vod_playlist_type(self, tmp_path):
        cmd = HlsPipeline.build_ffmpeg_remux_cmd(tmp_path / "v.mp4", tmp_path / "out")
        idx = cmd.index("-hls_playlist_type")
        assert cmd[idx + 1] == "vod"

    @pytest.mark.unit
    def test_init_filename(self, tmp_path):
        cmd = HlsPipeline.build_ffmpeg_remux_cmd(tmp_path / "v.mp4", tmp_path / "out")
        idx = cmd.index("-hls_fmp4_init_filename")
        assert cmd[idx + 1] == "init.mp4"


# ─── generate_master_playlist ────────────────────────────────────────────────


class TestGenerateMasterPlaylist:
    @pytest.mark.unit
    def test_single_tier(self):
        tiers = [
            {"label": "720p", "bandwidth": 2500000, "resolution": "1280x720", "codecs": "avc1.640028,mp4a.40.2"},
        ]
        content = HlsPipeline.generate_master_playlist(tiers)
        assert "#EXTM3U" in content
        assert "#EXT-X-VERSION:7" in content
        assert "BANDWIDTH=2500000" in content
        assert "RESOLUTION=1280x720" in content
        assert "720p/playlist.m3u8" in content

    @pytest.mark.unit
    def test_multi_tier(self):
        tiers = [
            {"label": "480p", "bandwidth": 1200000, "resolution": "854x480", "codecs": "avc1.4d401e,mp4a.40.2"},
            {"label": "720p", "bandwidth": 2500000, "resolution": "1280x720", "codecs": "avc1.640028,mp4a.40.2"},
        ]
        content = HlsPipeline.generate_master_playlist(tiers)
        assert "480p/playlist.m3u8" in content
        assert "720p/playlist.m3u8" in content
        lines = content.strip().split("\n")
        # Should have: EXTM3U, VERSION, INDEPENDENT-SEGMENTS, then 2 x (STREAM-INF + URI)
        assert len(lines) == 7

    @pytest.mark.safety
    def test_empty_tiers_raises(self):
        with pytest.raises(ValueError, match="No tiers available"):
            HlsPipeline.generate_master_playlist([])

    @pytest.mark.unit
    def test_includes_codecs_quoted(self):
        tiers = [
            {"label": "720p", "bandwidth": 2500000, "resolution": "1280x720", "codecs": "avc1.640028,mp4a.40.2"},
        ]
        content = HlsPipeline.generate_master_playlist(tiers)
        assert 'CODECS="avc1.640028,mp4a.40.2"' in content

    @pytest.mark.unit
    def test_independent_segments(self):
        tiers = [
            {"label": "720p", "bandwidth": 2500000, "resolution": "1280x720", "codecs": "avc1.640028,mp4a.40.2"},
        ]
        content = HlsPipeline.generate_master_playlist(tiers)
        assert "#EXT-X-INDEPENDENT-SEGMENTS" in content


# ─── build_r2_key_hls ───────────────────────────────────────────────────────


class TestBuildR2KeyHls:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            (
                ("@testchannel", "2025-06-15T12:00:00Z", "abc123", "master.m3u8"),
                "testchannel/2025-06/abc123/master.m3u8",
            ),
            (
                ("testchannel", "2025-06-15T12:00:00Z", "abc123", "720p/playlist.m3u8"),
                "testchannel/2025-06/abc123/720p/playlist.m3u8",
            ),
            (
                ("@chan", "2025-01-01T00:00:00Z", "vid1", "480p/seg_000.m4s"),
                "chan/2025-01/vid1/480p/seg_000.m4s",
            ),
        ],
        ids=["strips-at-sign", "no-at-sign", "segment-path"],
    )
    def test_key_format(self, give, want):
        handle, published, vid_id, rel_path = give
        assert R2Storage.build_r2_key(handle, published, vid_id, rel_path) == want

    @pytest.mark.unit
    def test_none_published_at(self):
        key = R2Storage.build_r2_key("chan", None, "vid1", "master.m3u8")
        assert key == "chan/unknown-00/vid1/master.m3u8"

    @pytest.mark.unit
    def test_invalid_published_at(self):
        key = R2Storage.build_r2_key("chan", "not-a-date", "vid1", "master.m3u8")
        assert key == "chan/unknown-00/vid1/master.m3u8"


# ─── build_codec_string ─────────────────────────────────────────────────────


class TestBuildCodecString:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "give, want",
        [
            (
                {
                    "video": {"codec_name": "h264", "profile": "High", "level": 40},
                    "audio": {"codec_name": "aac", "profile": "LC"},
                },
                "avc1.640028,mp4a.40.2",
            ),
            (
                {
                    "video": {"codec_name": "h264", "profile": "Main", "level": 31},
                    "audio": {"codec_name": "aac", "profile": "LC"},
                },
                "avc1.4d001f,mp4a.40.2",
            ),
            (
                {
                    "video": {"codec_name": "h264", "profile": "Constrained Baseline", "level": 30},
                    "audio": {"codec_name": "aac", "profile": "HE-AAC"},
                },
                "avc1.42c01e,mp4a.40.5",
            ),
            (
                {
                    "video": {"codec_name": "h264", "profile": "Baseline", "level": 31},
                    "audio": {"codec_name": "aac", "profile": "HE-AACv2"},
                },
                "avc1.42001f,mp4a.40.29",
            ),
        ],
        ids=["high-40-aac-lc", "main-31-aac-lc", "constrained-baseline-he-aac", "baseline-he-aacv2"],
    )
    def test_known_profiles(self, give, want):
        assert HlsPipeline.build_codec_string(give) == want

    @pytest.mark.unit
    def test_missing_video_uses_fallback(self):
        result = HlsPipeline.build_codec_string({"video": None, "audio": {"codec_name": "aac", "profile": "LC"}})
        assert result == "avc1.640028,mp4a.40.2"

    @pytest.mark.unit
    def test_missing_audio_uses_fallback(self):
        probe = {"video": {"codec_name": "h264", "profile": "High", "level": 40}, "audio": None}
        result = HlsPipeline.build_codec_string(probe)
        assert result == "avc1.640028,mp4a.40.2"

    @pytest.mark.unit
    def test_unknown_profile_defaults_high(self):
        result = HlsPipeline.build_codec_string(
            {
                "video": {"codec_name": "h264", "profile": "SomeWeirdProfile", "level": 40},
                "audio": {"codec_name": "aac", "profile": "LC"},
            }
        )
        assert result.startswith("avc1.6400")

    @pytest.mark.edge_cases
    def test_non_h264_video_uses_fallback(self):
        result = HlsPipeline.build_codec_string(
            {
                "video": {"codec_name": "vp9", "profile": "0", "level": 40},
                "audio": {"codec_name": "aac", "profile": "LC"},
            }
        )
        assert result == "avc1.640028,mp4a.40.2"


# ─── parse_info_json ─────────────────────────────────────────────────────────


class TestParseInfoJson:
    @pytest.mark.unit
    def test_parses_full_info(self, tmp_path):
        info = {
            "title": "Test Video",
            "fulltitle": "Test Video Full",
            "description": "A description",
            "duration": 300,
            "view_count": 10000,
            "like_count": 500,
            "comment_count": 50,
            "upload_date": "20250615",
            "thumbnail": "https://example.com/thumb.jpg",
            "uploader_id": "@testchannel",
            "tags": ["tag1", "tag2"],
            "categories": ["Education"],
            "chapters": [
                {"title": "Intro", "start_time": 0, "end_time": 30},
                {"title": "Main", "start_time": 30, "end_time": 300},
            ],
            "width": 1920,
            "height": 1080,
            "fps": 30,
            "language": "en",
            "webpage_url": "https://youtube.com/watch?v=abc123",
        }
        info_path = tmp_path / "video.info.json"
        info_path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(info_path)

        assert result["title"] == "Test Video"
        assert result["description"] == "A description"
        assert result["duration_seconds"] == 300
        assert result["view_count"] == 10000
        assert result["published_at"] == "2025-06-15T00:00:00Z"
        assert result["handle"] == "@testchannel"
        assert result["tags"] == ["tag1", "tag2"]
        assert result["width"] == 1920
        assert result["chapters"] is not None
        parsed_chapters = json.loads(result["chapters"])
        assert len(parsed_chapters) == 2

    @pytest.mark.unit
    def test_missing_fields_have_defaults(self, tmp_path):
        info = {}
        info_path = tmp_path / "video.info.json"
        info_path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(info_path)

        assert result["title"] == "Untitled"
        assert result["description"] == ""
        assert result["thumbnail_url"] == ""
        assert result["tags"] == []
        assert result["chapters"] is None

    @pytest.mark.unit
    def test_invalid_json_returns_none(self, tmp_path):
        info_path = tmp_path / "bad.info.json"
        info_path.write_text("not json at all", encoding="utf-8")

        result = HlsPipeline.parse_info_json(info_path)
        assert result is None

    @pytest.mark.unit
    def test_nonexistent_file_returns_none(self, tmp_path):
        result = HlsPipeline.parse_info_json(tmp_path / "nonexistent.info.json")
        assert result is None

    @pytest.mark.unit
    def test_upload_date_parsing(self, tmp_path):
        info = {"upload_date": "20231225"}
        info_path = tmp_path / "video.info.json"
        info_path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(info_path)
        assert result["published_at"] == "2023-12-25T00:00:00Z"

    @pytest.mark.edge_cases
    def test_no_chapters_or_invalid_chapters(self, tmp_path):
        info = {"chapters": "not a list"}
        info_path = tmp_path / "video.info.json"
        info_path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(info_path)
        assert result["chapters"] is None

    @pytest.mark.edge_cases
    def test_fulltitle_fallback(self, tmp_path):
        info = {"fulltitle": "Full Title Only"}
        info_path = tmp_path / "video.info.json"
        info_path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(info_path)
        assert result["title"] == "Full Title Only"


# ─── measure_peak_bandwidth ─────────────────────────────────────────────────


class TestMeasurePeakBandwidth:
    @pytest.mark.unit
    def test_calculates_from_playlist(self, tmp_path):
        # Create a realistic HLS playlist and segment files
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()

        # Write init.mp4 (small)
        (hls_dir / "init.mp4").write_bytes(b"\x00" * 1000)

        # Write segments of known sizes
        for i in range(3):
            (hls_dir / f"seg_{i:03d}.m4s").write_bytes(b"\x00" * 500_000)

        # Write playlist
        playlist_content = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:7\n"
            "#EXT-X-TARGETDURATION:6\n"
            '#EXT-X-MAP:URI="init.mp4"\n'
            "#EXTINF:6.0,\n"
            "seg_000.m4s\n"
            "#EXTINF:6.0,\n"
            "seg_001.m4s\n"
            "#EXTINF:5.5,\n"
            "seg_002.m4s\n"
            "#EXT-X-ENDLIST\n"
        )
        (hls_dir / "playlist.m3u8").write_text(playlist_content, encoding="utf-8")

        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=6)
        assert result is not None
        assert result > 0
        # 500KB segment + 1KB init over 6 seconds ~ ~668 kbps
        assert 500_000 < result < 1_000_000

    @pytest.mark.unit
    def test_nonexistent_playlist_returns_none(self, tmp_path):
        result = HlsPipeline.measure_peak_bandwidth(tmp_path, target_duration=6)
        assert result is None

    @pytest.mark.unit
    def test_empty_playlist_returns_none(self, tmp_path):
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        (hls_dir / "playlist.m3u8").write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")

        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=6)
        assert result is None


# ─── load_config ─────────────────────────────────────────────────────────────


class TestConsumerLoadConfig:
    @pytest.mark.unit
    def test_returns_all_sections(self):
        cfg = AppConfig()
        assert cfg.consumer is not None
        assert cfg.ytdlp is not None
        assert cfg.hls is not None
        assert cfg.r2 is not None

    @pytest.mark.unit
    def test_default_tiers(self):
        cfg = AppConfig()
        tiers = cfg.hls["tiers"]
        labels = [t["label"] for t in tiers]
        assert "480p" in labels
        assert "720p" in labels

    @pytest.mark.unit
    def test_default_consumer_settings(self):
        cfg = AppConfig()
        assert cfg.consumer["batch_size"] == 50
        assert cfg.consumer["max_attempts"] == 3
        assert cfg.consumer["stale_lock_minutes"] == 60

    @pytest.mark.unit
    def test_segment_duration_default(self):
        cfg = AppConfig()
        assert cfg.hls["segment_duration"] == 6
        assert cfg.hls["segment_type"] == "fmp4"
