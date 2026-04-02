"""VideoFetcher — cache-aware video candidate fetching from YouTube API.

Owns both the YouTube API calls and local JSON cache persistence.
The caller explicitly chooses the source via from_cache parameter.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.services.youtube import YouTubeClient


class VideoFetcher:
    """Fetches video candidates from YouTube API or local JSON cache.

    Not a transparent cache-aside — the caller explicitly chooses the source.
    Cache is always written after a successful API fetch.
    """

    def __init__(self, youtube: YouTubeClient, cache_dir: Path):
        self.yt = youtube
        self.cache_dir = cache_dir

    def fetch_playlist(
        self,
        channel_id: str,
        playlist_id: str,
        target_count: int,
        date_cutoff: datetime,
        early_stop: int,
        *,
        from_cache: bool = False,
    ) -> tuple[list[dict], int]:
        """Fetch recent candidates from a channel's uploads playlist.

        Returns (candidates, quota_used). quota_used is 0 when from_cache=True.
        Exceptions from the YouTube API propagate to the caller.
        """
        if from_cache:
            cached = self._load(channel_id, "recent")
            if cached:
                return cached.get("candidates", []), 0
            return [], 0

        candidates, _, quota = self.yt.fetch_playlist_videos(
            playlist_id, target_count, date_cutoff, early_stop
        )
        if candidates:
            self._save(channel_id, "recent", {"candidates": candidates})
        return candidates, quota

    def fetch_search_pair(
        self,
        channel_id: str,
        *,
        from_cache: bool = False,
        search_max_results: int = 50,
    ) -> tuple[list[dict], list[dict], int]:
        """Fetch popular + rated candidates via two YouTube search calls.

        Both results are cached atomically under one key ("full").
        Returns (popular, rated, quota_used). quota_used is 0 when from_cache=True.
        If either search fails, the exception propagates and nothing is cached.
        """
        if from_cache:
            cached = self._load(channel_id, "full")
            if cached:
                return cached.get("popular", []), cached.get("rated", []), 0
            return [], [], 0

        popular, pop_q = self.yt.fetch_search_videos(channel_id, "viewCount", search_max_results)
        rated, rat_q = self.yt.fetch_search_videos(channel_id, "rating", search_max_results)
        if popular or rated:
            self._save(channel_id, "full", {"popular": popular, "rated": rated})
        return popular, rated, pop_q + rat_q

    # ── Cache I/O ───────────────────────────────────────────────────────────

    def _load(self, channel_id: str, mode: str) -> dict | None:
        """Load cached data from disk. Returns None on miss or error."""
        path = self.cache_dir / f"{channel_id}_{mode}.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    def _save(self, channel_id: str, mode: str, data: dict) -> None:
        """Write cache file with metadata envelope."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "channel_id": channel_id,
            "mode": mode,
            "cached_at": datetime.now(timezone.utc).isoformat(),
            **data,
        }
        path = self.cache_dir / f"{channel_id}_{mode}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
