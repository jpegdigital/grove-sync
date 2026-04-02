"""Tests for AppConfig class."""

import os

import pytest

from src.config import AppConfig, ConfigError


class TestAppConfigLoad:
    @pytest.mark.unit
    def test_loads_producer_defaults(self):
        config = AppConfig()
        assert config.api["page_size"] == 50
        assert config.api["max_retries"] == 3
        assert config.scoring_weights["popularity"] == 0.25
        assert config.scoring_weights["velocity"] == 0.20
        assert config.scoring_weights["reach"] == 0.15

    @pytest.mark.unit
    def test_loads_consumer_defaults(self):
        config = AppConfig()
        assert config.consumer["batch_size"] == 50
        assert config.hls["segment_duration"] == 6
        assert len(config.hls["tiers"]) >= 2

    @pytest.mark.unit
    def test_scoring_weights_sum_to_one(self):
        config = AppConfig()
        total = sum(config.scoring_weights.values())
        assert total == pytest.approx(1.0, abs=0.01)

    @pytest.mark.unit
    def test_freshness_half_life(self):
        config = AppConfig()
        assert config.freshness_half_life_days == 90


class TestAppConfigEnv:
    @pytest.mark.unit
    def test_load_env(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_APP_CONFIG=hello\n")
        monkeypatch.delenv("TEST_APP_CONFIG", raising=False)

        config = AppConfig(env_file=env_file)
        config.load_env()
        assert os.environ["TEST_APP_CONFIG"] == "hello"

    @pytest.mark.unit
    def test_get_env_returns_value(self, monkeypatch):
        monkeypatch.setenv("MY_KEY", "my_value")
        assert AppConfig.get_env("MY_KEY") == "my_value"

    @pytest.mark.safety
    def test_get_env_raises_on_missing(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_KEY_XYZ", raising=False)
        with pytest.raises(ConfigError, match="NONEXISTENT_KEY_XYZ"):
            AppConfig.get_env("NONEXISTENT_KEY_XYZ")

    @pytest.mark.safety
    def test_get_env_raises_on_empty(self, monkeypatch):
        monkeypatch.setenv("EMPTY_KEY", "")
        with pytest.raises(ConfigError):
            AppConfig.get_env("EMPTY_KEY")


class TestAppConfigValidation:
    @pytest.mark.safety
    def test_validate_producer_env_raises_on_missing(self, monkeypatch):
        monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
        config = AppConfig()
        with pytest.raises(ConfigError):
            config.validate_producer_env()

    @pytest.mark.safety
    def test_validate_consumer_env_raises_on_missing(self, monkeypatch):
        monkeypatch.delenv("R2_BUCKET_NAME", raising=False)
        config = AppConfig()
        with pytest.raises(ConfigError):
            config.validate_consumer_env()

    @pytest.mark.integration
    def test_validate_producer_env_passes_when_set(self, monkeypatch):
        for key in ("YOUTUBE_API_KEY", "NEXT_PUBLIC_SUPABASE_URL", "SUPABASE_SECRET_KEY"):
            monkeypatch.setenv(key, "test")
        config = AppConfig()
        config.validate_producer_env()

    @pytest.mark.integration
    def test_validate_consumer_env_passes_when_set(self, monkeypatch):
        for key in (
            "NEXT_PUBLIC_SUPABASE_URL",
            "SUPABASE_SECRET_KEY",
            "R2_ACCOUNT_ID",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_BUCKET_NAME",
        ):
            monkeypatch.setenv(key, "test")
        config = AppConfig()
        config.validate_consumer_env()


class TestAppConfigYamlOverride:
    @pytest.mark.integration
    def test_producer_yaml_overrides_defaults(self, tmp_path, monkeypatch):
        config_file = tmp_path / "producer.yaml"
        config_file.write_text("api:\n  page_size: 25\n  max_retries: 5\n")
        producer_cfg = AppConfig._load_yaml(config_file, AppConfig._producer_defaults())
        assert producer_cfg["api"]["page_size"] == 25
        assert producer_cfg["api"]["max_retries"] == 5
        # Non-overridden should remain
        assert producer_cfg["api"]["max_workers"] == 8

    @pytest.mark.edge_cases
    def test_empty_yaml_uses_defaults(self, tmp_path):
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")
        cfg = AppConfig._load_yaml(config_file, AppConfig._producer_defaults())
        assert cfg["api"]["page_size"] == 50

    @pytest.mark.edge_cases
    def test_missing_yaml_uses_defaults(self, tmp_path):
        cfg = AppConfig._load_yaml(tmp_path / "nonexistent.yaml", AppConfig._producer_defaults())
        assert cfg["api"]["page_size"] == 50
