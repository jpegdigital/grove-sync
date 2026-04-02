"""Data models for grove-sync — typed dataclasses replacing implicit dict shapes."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Video:
    """A video with metadata — used throughout producer and consumer."""

    video_id: str
    title: str = ""
    published_at: str = ""
    description: str = ""
    thumbnail_url: str = ""
    duration_seconds: int = 0
    duration_iso: str = ""
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    score: float = 0.0
    storage_bytes: int | None = None


@dataclass
class ChannelConfig:
    """Per-channel sync config from curated_channels table."""

    curated_id: int
    channel_id: str
    title: str = ""
    custom_url: str = ""
    sync_mode: str = "sync"
    storage_budget_gb: float = 10.0
    catalog_fraction: float = 0.6
    scoring_alpha: float = 0.3
    min_duration_seconds: int = 60
    max_duration_seconds: int | None = None
    date_range_override: str | None = None
    last_full_refresh_at: str | None = None
    median_gap_days: float | None = None

    @property
    def is_archive(self) -> bool:
        return self.sync_mode == "archive"

    @property
    def catalog_budget_gb(self) -> float:
        return self.storage_budget_gb * self.catalog_fraction

    @property
    def fresh_budget_base_gb(self) -> float:
        return self.storage_budget_gb * (1.0 - self.catalog_fraction)


@dataclass
class SyncJob:
    """A job in the sync_queue."""

    video_id: str
    channel_id: str
    metadata: dict = field(default_factory=dict)
    id: str | None = None
    score: float = 0.0
    storage_bytes: int | None = None
    status: str = "pending"


@dataclass
class DownloadResult:
    """Result of downloading and measuring a single video."""

    video_id: str
    channel_id: str
    score: float
    storage_bytes: int
    staging_dir: Path
    job_id: str
    published_at: str = ""
    info_data: dict = field(default_factory=dict)
    remuxed_tiers: list = field(default_factory=list)
    sidecar_files: list[Path] = field(default_factory=list)


@dataclass
class ChannelResult:
    """Summary returned after processing a channel."""

    channel_id: str
    title: str
    mode: str
    desired: int = 0
    existing: int = 0
    downloads: int = 0
    removals: int = 0
    quota_used: int = 0
    error: str | None = None
