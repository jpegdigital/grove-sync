"""Tests for R2Storage class."""

import pytest

from src.services.storage import R2Storage


class TestR2StorageBuildKey:
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
        ],
        ids=["strips-at", "nested-path"],
    )
    def test_key_format(self, give, want):
        assert R2Storage.build_r2_key(*give) == want

    @pytest.mark.unit
    def test_none_published(self):
        key = R2Storage.build_r2_key("chan", None, "v1", "master.m3u8")
        assert key == "chan/unknown-00/v1/master.m3u8"

    @pytest.mark.unit
    def test_invalid_date(self):
        key = R2Storage.build_r2_key("chan", "bad-date", "v1", "master.m3u8")
        assert key == "chan/unknown-00/v1/master.m3u8"
