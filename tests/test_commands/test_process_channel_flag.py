from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.commands.process import ProcessCommand, main
from src.config import AppConfig


@pytest.fixture
def config(tmp_path):
    cfg = AppConfig()
    cfg.project_root = tmp_path
    cfg._consumer["consumer"]["throttle_min_seconds"] = 0
    cfg._consumer["consumer"]["throttle_max_seconds"] = 0
    cfg._consumer["consumer"]["video_throttle_min_seconds"] = 0
    cfg._consumer["consumer"]["video_throttle_max_seconds"] = 0
    return cfg


@pytest.fixture
def db():
    mock = MagicMock()
    mock.fetch_curated_channels.return_value = []
    return mock


@pytest.fixture
def storage():
    return MagicMock()


@pytest.fixture
def hls():
    return MagicMock()


class TestProcessChannelFlag:
    @pytest.mark.unit
    def test_run_filters_to_requested_channel(self, config, db, storage, hls, tmp_path):
        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        cmd._recover_on_startup = MagicMock()
        cmd._print_summary = MagicMock()
        cmd._process_channel = MagicMock(return_value={
            "downloaded": 0,
            "uploaded": 0,
            "evicted": 0,
            "skipped": 0,
        })

        db.fetch_curated_channels.return_value = [
            {"channel_id": "UC1", "title": "One", "storage_budget_gb": 10.0, "catalog_fraction": 0.6, "sync_mode": "sync"},
            {"channel_id": "UC2", "title": "Two", "storage_budget_gb": 10.0, "catalog_fraction": 0.6, "sync_mode": "sync"},
        ]

        cmd.run(channel="UC2", dry_run=True, verbose=False)

        cmd._process_channel.assert_called_once_with(
            "UC2",
            db.fetch_curated_channels.return_value[1],
            dry_run=True,
            verbose=False,
            limit=None,
        )

    @pytest.mark.unit
    def test_run_channel_not_found_skips_processing(self, config, db, storage, hls, tmp_path, capsys):
        cmd = ProcessCommand(config, db, storage, hls, staging_dir=tmp_path)
        cmd._recover_on_startup = MagicMock()
        cmd._print_summary = MagicMock()
        cmd._process_channel = MagicMock()

        db.fetch_curated_channels.return_value = [
            {"channel_id": "UC1", "title": "One", "storage_budget_gb": 10.0, "catalog_fraction": 0.6, "sync_mode": "sync"},
        ]

        cmd.run(channel="UC404", dry_run=True, verbose=False)

        assert "not found" in capsys.readouterr().out
        cmd._process_channel.assert_not_called()
        cmd._print_summary.assert_not_called()

    @pytest.mark.unit
    def test_main_passes_channel_argument(self, monkeypatch, tmp_path):
        captured = {}
        fake_config = AppConfig()
        fake_config.project_root = tmp_path
        fake_config._consumer = {}
        fake_config.validate_consumer_env = MagicMock()
        fake_config.get_env = MagicMock(return_value="test")

        class FakeProcessCommand:
            def __init__(self, config, db, storage, hls):
                captured["constructed"] = True

            def run(self, limit=None, dry_run=False, verbose=False, channel=None):
                captured["run"] = {
                    "limit": limit,
                    "dry_run": dry_run,
                    "verbose": verbose,
                    "channel": channel,
                }

        monkeypatch.setattr("src.commands.process.AppConfig.load", staticmethod(lambda: fake_config))
        monkeypatch.setattr("src.commands.process._check_ffmpeg", lambda: None)
        monkeypatch.setattr("src.commands.process.ProcessCommand", FakeProcessCommand)
        monkeypatch.setattr("src.commands.process.R2Storage", SimpleNamespace(from_env=lambda: object()))
        monkeypatch.setattr("src.commands.process.HlsPipeline", lambda *args, **kwargs: object())
        monkeypatch.setattr("sys.modules", {
            **__import__("sys").modules,
            "supabase": SimpleNamespace(create_client=lambda *args, **kwargs: object()),
        })
        monkeypatch.setattr("sys.argv", ["process.py", "--channel", "UC2", "--dry-run"])

        main()

        assert captured["constructed"] is True
        assert captured["run"] == {
            "limit": None,
            "dry_run": True,
            "verbose": False,
            "channel": "UC2",
        }
