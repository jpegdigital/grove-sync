"""YouTube Data API v3 client with retry, batching, and quota tracking."""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone

import requests
from dateutil.relativedelta import relativedelta

API_BASE = "https://www.googleapis.com/youtube/v3"


class YouTubeClient:
    """YouTube Data API client with exponential backoff and quota tracking.

    All quota usage is accumulated in self.quota_used for the lifetime of
    the client instance.
    """

    def __init__(self, api_key: str, config: dict):
        self.api_key = api_key
        self.page_size: int = config.get("page_size", 50)
        self.batch_size: int = config.get("enrichment_batch_size", 50)
        self.max_retries: int = config.get("max_retries", 3)
        self.backoff_base: int = config.get("retry_backoff_base", 2)
        self.quota_used: int = 0

    # ── HTTP ─────────────────────────────────────────────────────────────

    def api_get(self, url: str, params: dict) -> dict:
        """GET with exponential backoff on 429/5xx."""
        for attempt in range(self.max_retries + 1):
            resp = requests.get(url, params=params)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503) and attempt < self.max_retries:
                wait = self.backoff_base ** (attempt + 1)
                print(f"      Retry {attempt + 1}/{self.max_retries} after {wait}s (HTTP {resp.status_code})")
                time.sleep(wait)
                continue
            resp.raise_for_status()

        resp.raise_for_status()
        return {}

    # ── Enrichment ───────────────────────────────────────────────────────

    def enrich_videos(self, video_ids: list[str]) -> tuple[dict[str, dict], int]:
        """Batch-fetch duration and stats via videos.list.

        Returns (details_dict, quota_used).
        Cost: 1 quota unit per batch.
        """
        details: dict[str, dict] = {}
        calls = 0

        for i in range(0, len(video_ids), self.batch_size):
            chunk = video_ids[i : i + self.batch_size]
            params = {
                "part": "contentDetails,statistics",
                "id": ",".join(chunk),
                "key": self.api_key,
            }
            data = self.api_get(f"{API_BASE}/videos", params)
            calls += 1

            for item in data.get("items", []):
                duration_iso = item.get("contentDetails", {}).get("duration", "")
                stats = item.get("statistics", {})
                details[item["id"]] = {
                    "duration_iso": duration_iso,
                    "duration_seconds": parse_iso_duration(duration_iso),
                    "view_count": int(stats.get("viewCount", 0)),
                    "like_count": int(stats.get("likeCount", 0)),
                    "comment_count": int(stats.get("commentCount", 0)),
                }

        self.quota_used += calls
        return details, calls

    # ── Playlist fetch ───────────────────────────────────────────────────

    def fetch_playlist_videos(
        self,
        playlist_id: str,
        target_count: int,
        date_cutoff: datetime,
        early_stop_tolerance: int = 3,
    ) -> tuple[list[dict], int, int]:
        """Fetch and enrich videos page-by-page from a playlist.

        Returns (videos, total_fetched, quota_used).
        """
        desired: list[dict] = []
        page_token = None
        total_fetched = 0
        total_quota = 0
        consecutive_past_cutoff = 0

        while len(desired) < target_count:
            params = {
                "part": "snippet",
                "playlistId": playlist_id,
                "maxResults": self.page_size,
                "key": self.api_key,
            }
            if page_token:
                params["pageToken"] = page_token

            data = self.api_get(f"{API_BASE}/playlistItems", params)
            total_quota += 1

            raw_items = []
            hit_date_boundary = False

            for item in data.get("items", []):
                snippet = item["snippet"]
                vid_id = snippet.get("resourceId", {}).get("videoId")
                if not vid_id:
                    continue

                published_at = snippet.get("publishedAt", "")

                if published_at:
                    try:
                        pub_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                        if pub_dt < date_cutoff:
                            consecutive_past_cutoff += 1
                            if consecutive_past_cutoff >= early_stop_tolerance:
                                hit_date_boundary = True
                                break
                            continue
                        else:
                            consecutive_past_cutoff = 0
                    except ValueError:
                        consecutive_past_cutoff = 0

                raw_items.append(
                    {
                        "video_id": vid_id,
                        "title": snippet.get("title", ""),
                        "published_at": published_at,
                        "description": snippet.get("description", ""),
                        "thumbnail_url": (
                            snippet.get("thumbnails", {}).get("high", {}).get("url")
                            or snippet.get("thumbnails", {}).get("medium", {}).get("url")
                            or snippet.get("thumbnails", {}).get("default", {}).get("url")
                            or ""
                        ),
                    }
                )

            total_fetched += len(raw_items)

            if raw_items:
                video_ids = [v["video_id"] for v in raw_items]
                details, enrich_calls = self.enrich_videos(video_ids)
                total_quota += enrich_calls

                for v in raw_items:
                    enriched = {**v, **details.get(v["video_id"], {})}
                    desired.append(enriched)
                    if len(desired) >= target_count:
                        break

            if hit_date_boundary:
                break

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        self.quota_used += total_quota
        return desired, total_fetched, total_quota

    # ── Search fetch ─────────────────────────────────────────────────────

    def fetch_search_videos(
        self,
        channel_id: str,
        order: str,
        max_results: int = 50,
    ) -> tuple[list[dict], int]:
        """Fetch videos via search.list for a channel with a given sort order.

        Returns (enriched_videos, quota_used).
        Cost: 100 units for search.list + enrichment batches.
        """
        params = {
            "part": "snippet",
            "channelId": channel_id,
            "type": "video",
            "order": order,
            "maxResults": min(max_results, 50),
            "key": self.api_key,
        }

        data = self.api_get(f"{API_BASE}/search", params)
        quota_used = 100

        raw_items = []
        for item in data.get("items", []):
            vid_id = item.get("id", {}).get("videoId")
            if not vid_id:
                continue
            snippet = item.get("snippet", {})
            raw_items.append(
                {
                    "video_id": vid_id,
                    "title": snippet.get("title", ""),
                    "published_at": snippet.get("publishedAt", ""),
                    "description": snippet.get("description", ""),
                    "thumbnail_url": (
                        snippet.get("thumbnails", {}).get("high", {}).get("url")
                        or snippet.get("thumbnails", {}).get("medium", {}).get("url")
                        or snippet.get("thumbnails", {}).get("default", {}).get("url")
                        or ""
                    ),
                }
            )

        if not raw_items:
            self.quota_used += quota_used
            return [], quota_used

        video_ids = [v["video_id"] for v in raw_items]
        details, enrich_calls = self.enrich_videos(video_ids)
        # enrich_videos already added enrich_calls to self.quota_used,
        # so only add the search cost (100) to self.quota_used here
        quota_used += enrich_calls

        enriched: list[dict] = []
        for v in raw_items:
            enriched.append({**v, **details.get(v["video_id"], {})})

        self.quota_used += 100  # only the search.list cost (enrich already tracked)
        return enriched, quota_used


# ─── YouTube-domain helpers (pure) ───────────────────────────────────────────


def uploads_playlist_id(channel_id: str) -> str:
    """Convert channel ID (UC...) to uploads playlist ID (UU...)."""
    return "UU" + channel_id[2:]


def parse_iso_duration(iso: str) -> int:
    """Parse ISO 8601 duration like PT3M45S to seconds."""
    if not iso or not iso.startswith("PT"):
        return 0
    s = iso[2:]
    hours = minutes = seconds = 0
    for unit, name in [("H", "hours"), ("M", "minutes"), ("S", "seconds")]:
        if unit in s:
            val, s = s.split(unit, 1)
            if not val.isdigit():
                continue
            if name == "hours":
                hours = int(val)
            elif name == "minutes":
                minutes = int(val)
            elif name == "seconds":
                seconds = int(val)
    return hours * 3600 + minutes * 60 + seconds


def parse_date_range(override_str: str) -> datetime:
    """Convert date range string to a UTC datetime cutoff.

    Supported formats:
        "all" (no date filtering)
        "today-6months", "today-2years" (relative)
        "19700101" (absolute date, YYYYMMDD)
    """
    now = datetime.now(timezone.utc)

    if override_str.lower() == "all":
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    if re.match(r"^\d{8}$", override_str):
        return datetime.strptime(override_str, "%Y%m%d").replace(tzinfo=timezone.utc)

    match = re.match(r"^today-(\d+)(months?|years?)$", override_str)
    if not match:
        return now - relativedelta(months=6)

    amount = int(match.group(1))
    unit = match.group(2)

    try:
        if unit.startswith("year"):
            result = (now - relativedelta(years=amount)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            result = (now - relativedelta(months=amount)).replace(hour=0, minute=0, second=0, microsecond=0)
        return result
    except (ValueError, OverflowError):
        return datetime(1, 1, 1, tzinfo=timezone.utc)
