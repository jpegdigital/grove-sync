"""Tests for HlsPipeline class — verifies class methods match original function behavior."""

import json

import pytest

from src.services.hls import HlsPipeline


@pytest.fixture
def pipeline():
    config = {
        "ytdlp": {"merge_output_format": "mp4"},
        "hls": {
            "tiers": [
                {"label": "480p", "height": 480, "bandwidth": 1200000},
                {"label": "720p", "height": 720, "bandwidth": 2500000},
            ],
            "segment_duration": 6,
            "min_tiers": 1,
        },
        "consumer": {"throttle_min_seconds": 0, "throttle_max_seconds": 0},
    }
    return HlsPipeline(config)


class TestHlsPipelineBuildFormatSelector:
    @pytest.mark.unit
    def test_height_filter(self):
        result = HlsPipeline.build_format_selector({"label": "720p", "height": 720, "bandwidth": 2500000})
        assert "height<=720" in result
        assert "ext=mp4" in result


class TestHlsPipelineBuildFfmpegCmd:
    @pytest.mark.unit
    def test_required_flags(self, tmp_path):
        cmd = HlsPipeline.build_ffmpeg_remux_cmd(tmp_path / "v.mp4", tmp_path / "out", 6)
        assert cmd[0] == "ffmpeg"
        assert "hls" in cmd
        assert "fmp4" in cmd
        assert "vod" in cmd


class TestHlsPipelineGenerateMasterPlaylist:
    @pytest.mark.unit
    def test_single_tier(self):
        tiers = [{"label": "720p", "bandwidth": 2500000, "resolution": "1280x720", "codecs": "avc1.640028,mp4a.40.2"}]
        content = HlsPipeline.generate_master_playlist(tiers)
        assert "#EXTM3U" in content
        assert "BANDWIDTH=2500000" in content
        assert "720p/playlist.m3u8" in content

    @pytest.mark.safety
    def test_empty_raises(self):
        with pytest.raises(ValueError):
            HlsPipeline.generate_master_playlist([])


class TestHlsPipelineBuildCodecString:
    @pytest.mark.unit
    def test_high_profile(self):
        probe = {
            "video": {"codec_name": "h264", "profile": "High", "level": 40},
            "audio": {"codec_name": "aac", "profile": "LC"},
        }
        assert HlsPipeline.build_codec_string(probe) == "avc1.640028,mp4a.40.2"

    @pytest.mark.unit
    def test_fallback(self):
        assert HlsPipeline.build_codec_string({"video": None, "audio": None}) == "avc1.640028,mp4a.40.2"


class TestHlsPipelineParseInfoJson:
    @pytest.mark.unit
    def test_parses_full(self, tmp_path):
        info = {
            "title": "Test",
            "description": "desc",
            "duration": 300,
            "upload_date": "20250615",
            "view_count": 1000,
        }
        path = tmp_path / "video.info.json"
        path.write_text(json.dumps(info), encoding="utf-8")

        result = HlsPipeline.parse_info_json(path)
        assert result["title"] == "Test"
        assert result["published_at"] == "2025-06-15T00:00:00Z"

    @pytest.mark.unit
    def test_invalid_returns_none(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("not json")
        assert HlsPipeline.parse_info_json(path) is None


class TestHlsPipelineMeasurePeakBandwidth:
    @pytest.mark.unit
    def test_calculates(self, tmp_path):
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        (hls_dir / "init.mp4").write_bytes(b"\x00" * 1000)
        (hls_dir / "seg_000.m4s").write_bytes(b"\x00" * 500_000)

        playlist = (
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:6\n"
            '#EXT-X-MAP:URI="init.mp4"\n#EXTINF:6.0,\nseg_000.m4s\n#EXT-X-ENDLIST\n'
        )
        (hls_dir / "playlist.m3u8").write_text(playlist, encoding="utf-8")

        result = HlsPipeline.measure_peak_bandwidth(hls_dir, target_duration=6)
        assert result is not None
        assert result > 0

    @pytest.mark.unit
    def test_missing_playlist_returns_none(self, tmp_path):
        assert HlsPipeline.measure_peak_bandwidth(tmp_path, target_duration=6) is None


class TestHlsPipelineExtractTierMetadata:
    @pytest.mark.unit
    def test_returns_defaults_when_no_files(self, pipeline):
        tier = {"label": "720p", "height": 720, "bandwidth": 2500000}
        meta = pipeline.extract_tier_metadata(tier)
        assert meta["bandwidth"] == 2500000
        assert meta["codecs"] == "avc1.640028,mp4a.40.2"
        assert "x" in meta["resolution"]
