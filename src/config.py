"""Application configuration — env loading, YAML config, validation."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class ConfigError(Exception):
    """Raised when a required config value is missing or invalid."""


def _deep_merge(base: dict, override: dict) -> None:
    """Recursively merge override into base, preserving sibling keys in nested dicts."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


class AppConfig:
    """Loads and holds all application configuration.

    Merges YAML config files over hardcoded defaults. Loads .env into os.environ.
    Provides typed access to config sections.
    """

    def __init__(
        self,
        producer_cfg: dict | None = None,
        consumer_cfg: dict | None = None,
        env_file: Path | None = None,
    ):
        self.project_root = PROJECT_ROOT
        self.env_file = env_file or (PROJECT_ROOT / ".env")

        self._producer: dict = producer_cfg or self._load_yaml(
            PROJECT_ROOT / "config" / "producer.yaml",
            self._producer_defaults(),
        )
        self._consumer: dict = consumer_cfg or self._load_yaml(
            PROJECT_ROOT / "config" / "consumer.yaml",
            self._consumer_defaults(),
        )

    @classmethod
    def load(cls, env_file: Path | None = None) -> AppConfig:
        """Standard factory: load .env, then build config from YAML files."""
        config = cls(env_file=env_file)
        config.load_env()
        return config

    # ── Env ──────────────────────────────────────────────────────────────

    def load_env(self) -> None:
        """Load .env file into os.environ (setdefault — won't overwrite)."""
        if not self.env_file.exists():
            return
        with open(self.env_file) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                    value = value[1:-1]
                os.environ.setdefault(key.strip(), value)

    @staticmethod
    def get_env(key: str) -> str:
        """Get a required environment variable or raise ConfigError."""
        val = os.environ.get(key)
        if not val:
            raise ConfigError(f"Required environment variable not set: {key}")
        return val

    def validate_producer_env(self) -> None:
        """Validate env vars required by the producer."""
        for key in ("YOUTUBE_API_KEY", "NEXT_PUBLIC_SUPABASE_URL", "SUPABASE_SECRET_KEY"):
            self.get_env(key)

    def validate_consumer_env(self) -> None:
        """Validate env vars required by the consumer."""
        for key in (
            "NEXT_PUBLIC_SUPABASE_URL",
            "SUPABASE_SECRET_KEY",
            "R2_ACCOUNT_ID",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_BUCKET_NAME",
        ):
            self.get_env(key)

    # ── Producer config accessors ────────────────────────────────────────

    @property
    def producer(self) -> dict:
        return self._producer["producer"]

    @property
    def api(self) -> dict:
        return self._producer["api"]

    @property
    def quota(self) -> dict:
        return self._producer["quota"]


    @property
    def db(self) -> dict:
        return self._producer["db"]

    # ── Consumer config accessors ────────────────────────────────────────

    @property
    def consumer(self) -> dict:
        return self._consumer["consumer"]

    @property
    def ytdlp(self) -> dict:
        return self._consumer["ytdlp"]

    @property
    def hls(self) -> dict:
        return self._consumer["hls"]

    @property
    def r2(self) -> dict:
        return self._consumer["r2"]

    # ── YAML loading ─────────────────────────────────────────────────────

    @staticmethod
    def _load_yaml(config_file: Path, defaults: dict) -> dict:
        """Load a YAML config file and deep-merge over defaults."""
        if config_file.exists():
            with open(config_file) as f:
                file_cfg = yaml.safe_load(f) or {}
            if not isinstance(file_cfg, dict):
                return defaults
            for section in defaults:
                if section in file_cfg and isinstance(file_cfg[section], dict):
                    _deep_merge(defaults[section], file_cfg[section])
        return defaults

    @staticmethod
    def _producer_defaults() -> dict:
        return {
            "producer": {
                "early_stop_tolerance": 3,
                "channels_per_run": 5,
            },
            "api": {
                "page_size": 50,
                "enrichment_batch_size": 50,
                "max_workers": 8,
                "max_retries": 3,
                "retry_backoff_base": 2,
            },
            "quota": {
                "daily_limit": 10000,
                "warn_threshold": 8000,
            },
            "sources": {
                "popular": {
                    "duration_floor": 60,
                },
                "rated": {
                    "duration_floor": 60,
                },
            },
            "db": {
                "page_size": 1000,
                "enqueue_batch_size": 100,
                "overflow_limit": 20,
            },
        }

    @staticmethod
    def _consumer_defaults() -> dict:
        return {
            "consumer": {
                "batch_size": 50,
                "max_attempts": 3,
                "stale_lock_minutes": 60,
                "throttle_min_seconds": 3,
                "throttle_max_seconds": 8,
                "video_throttle_min_seconds": 5,
                "video_throttle_max_seconds": 15,
            },
            "ytdlp": {
                "format": (
                    "bv[height<=%(max_height)s][ext=mp4][vcodec~='^(avc|h264)']+ba[ext=m4a]"
                    "/b[height<=%(max_height)s][ext=mp4][vcodec~='^(avc|h264)']"
                ),
                "max_height": 1080,
                "merge_output_format": "mp4",
                "faststart": True,
                "write_thumbnail": True,
                "write_subs": True,
                "write_auto_subs": True,
                "sub_langs": "en",
                "sub_format": "vtt",
                "write_info_json": True,
                "remote_components": "ejs:github,ejs:npm",
                "match_filters": "!is_live & !is_upcoming & !post_live",
                "sleep_interval_subtitles": 5,
                "min_height": 361,
            },
            "hls": {
                "tiers": [
                    {"label": "480p", "height": 480, "bandwidth": 1200000},
                    {"label": "720p", "height": 720, "bandwidth": 2500000},
                ],
                "segment_duration": 6,
                "segment_type": "fmp4",
                "min_tiers": 1,
            },
            "r2": {
                "key_template": "{handle}/{year}-{month}/{video_id}.{ext}",
            },
        }
