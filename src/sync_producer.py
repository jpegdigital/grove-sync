"""
Video Sync Producer — multi-source scored video discovery.

Supports two run modes:
  - recent (daily): Playlist only. Scores recent videos, preserves reserved
    (popular/rated) slots from prior full runs. ~326 quota for 52 channels.
  - full (weekly): Popular (viewCount) + Rated (rating) + Recent (playlist).
    Deduplicates, scores with popularity+engagement+freshness algorithm,
    guarantees minimum slots per source. ~5,500 quota for 52 channels.

Scoring: log-scaled views (popularity) + like/comment rate (engagement) +
exponential decay with 90-day half-life (freshness). Configurable weights
and source minimums in config/producer.yaml.

Per-channel overrides from curated_channels:
  - date_range_override: widen/narrow the date window (e.g. "today-2years")
  - min_duration_override: custom duration floor in seconds

Usage:
    uv run python scripts/sync_producer.py [OPTIONS]

Options:
    --mode {recent,full}   Run mode (default: recent)
    --channel CHANNEL_ID   Run for a single channel only
    --dry-run              Preview what would be enqueued, don't write
    --verbose              Show per-video scoring and source breakdowns
"""

import argparse
import math
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from dateutil.relativedelta import relativedelta

# ─── Project Setup ────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
CONFIG_FILE = PROJECT_ROOT / "config" / "producer.yaml"

API_BASE = "https://www.googleapis.com/youtube/v3"

# Fix Windows console encoding for unicode channel titles
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ─── Config ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    """Load producer config from config/producer.yaml with sensible defaults."""
    defaults = {
        "producer": {
            "max_videos_per_channel": 250,
            "min_duration_seconds": 300,
            "default_date_range": "today-6months",
            "early_stop_tolerance": 3,
            "full_refresh_percentage": 0.10,
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
        "scoring": {
            "weights": {
                "popularity": 0.35,
                "engagement": 0.35,
                "freshness": 0.30,
            },
            "freshness_half_life_days": 90,
        },
        "sources": {
            "popular": {
                "min_percentage": 0.20,
                "duration_floor": 60,
            },
            "rated": {
                "min_percentage": 0.20,
                "duration_floor": 60,
            },
        },
        "db": {
            "page_size": 1000,
            "enqueue_batch_size": 100,
        },
    }
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            file_cfg = yaml.safe_load(f) or {}
        # Deep merge: file values override defaults
        for section in defaults:
            if section in file_cfg:
                defaults[section].update(file_cfg[section])
    return defaults


# Global config — loaded once at import, overridable in tests
CFG = load_config()


# ─── Env ──────────────────────────────────────────────────────────────────────

def load_env() -> None:
    """Load .env file into os.environ."""
    if not ENV_FILE.exists():
        return
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def get_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        print(f"Error: {key} not set")
        sys.exit(2)
    return val


def get_supabase_client():
    from supabase import create_client
    return create_client(
        get_env("NEXT_PUBLIC_SUPABASE_URL"),
        get_env("SUPABASE_SECRET_KEY"),
    )


# ─── API helpers with retry ──────────────────────────────────────────────────

def api_get(url: str, params: dict) -> dict:
    """GET with exponential backoff on 429/5xx."""
    max_retries = CFG["api"]["max_retries"]
    backoff_base = CFG["api"]["retry_backoff_base"]

    for attempt in range(max_retries + 1):
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503) and attempt < max_retries:
            wait = backoff_base ** (attempt + 1)
            print(f"      Retry {attempt + 1}/{max_retries} after {wait}s (HTTP {resp.status_code})")
            time.sleep(wait)
            continue
        resp.raise_for_status()

    # Should not reach here, but just in case
    resp.raise_for_status()
    return {}


# ─── uploads_playlist_id ──────────────────────────────────────────────────────

def uploads_playlist_id(channel_id: str) -> str:
    """Convert channel ID (UC...) to uploads playlist ID (UU...)."""
    return "UU" + channel_id[2:]


# ─── parse_iso_duration ───────────────────────────────────────────────────────

def parse_iso_duration(iso: str) -> int:
    """Parse ISO 8601 duration like PT3M45S to seconds."""
    if not iso or not iso.startswith("PT"):
        return 0
    s = iso[2:]
    hours = minutes = seconds = 0
    for unit, name in [("H", "hours"), ("M", "minutes"), ("S", "seconds")]:
        if unit in s:
            val, s = s.split(unit, 1)
            if name == "hours":
                hours = int(val)
            elif name == "minutes":
                minutes = int(val)
            elif name == "seconds":
                seconds = int(val)
    return hours * 3600 + minutes * 60 + seconds


# ─── parse_date_range ────────────────────────────────────────────────────────

def parse_date_range(override_str: str | None) -> datetime:
    """Convert date range string to a UTC datetime cutoff (start of day, inclusive).

    Supported formats:
        "all" (no date filtering)
        "today-6months", "today-2years", "today-1years" (relative)
        "19700101" (absolute date, YYYYMMDD)
        None → uses default_date_range from config
    """
    if not override_str:
        override_str = CFG["producer"]["default_date_range"]

    now = datetime.now(timezone.utc)

    # "all" means no date filtering — use epoch as cutoff
    if override_str.lower() == "all":
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    # Absolute date format: YYYYMMDD
    if re.match(r"^\d{8}$", override_str):
        return datetime.strptime(override_str, "%Y%m%d").replace(tzinfo=timezone.utc)

    # Relative format: today-Nunit
    match = re.match(r"^today-(\d+)(months?|years?)$", override_str)
    if not match:
        # Fall back to 6 months
        return now - relativedelta(months=6)

    amount = int(match.group(1))
    unit = match.group(2)

    if unit.startswith("year"):
        return (now - relativedelta(years=amount)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    else:
        return (now - relativedelta(months=amount)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )


# ─── enrich_videos ────────────────────────────────────────────────────────────

def enrich_videos(
    api_key: str, video_ids: list[str]
) -> tuple[dict[str, dict], int]:
    """Batch-fetch duration and stats via videos.list.

    Returns (details_dict, quota_used) where details_dict is keyed by video_id
    with {duration_seconds, duration_iso, view_count, like_count, comment_count}.
    Cost: 1 quota unit per batch of enrichment_batch_size videos.
    """
    batch_size = CFG["api"]["enrichment_batch_size"]
    details: dict[str, dict] = {}
    calls = 0

    for i in range(0, len(video_ids), batch_size):
        chunk = video_ids[i : i + batch_size]
        params = {
            "part": "contentDetails,statistics",
            "id": ",".join(chunk),
            "key": api_key,
        }
        data = api_get(f"{API_BASE}/videos", params)
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

    return details, calls


# ─── Scoring ─────────────────────────────────────────────────────────────────

def score_video(video: dict, weights: dict, half_life_days: float) -> float:
    """Compute a weighted score for a video.

    Signals:
      popularity  = log10(max(views, 1))              → range ~0-8
      engagement  = (like_rate*0.7 + comment_rate*0.3)*100  → range ~0-5
      freshness   = exp(-age_days * ln(2) / half_life)      → range 0-1

    Returns the weighted sum.  Pure function — no side effects.
    """
    views = max(int(video.get("view_count", 0)), 1)
    likes = int(video.get("like_count", 0))
    comments = int(video.get("comment_count", 0))

    # Popularity: log-scaled view count
    popularity = math.log10(views)

    # Engagement: like_rate and comment_rate relative to views
    like_rate = likes / views if views > 0 else 0.0
    comment_rate = comments / views if views > 0 else 0.0
    engagement = (like_rate * 0.7 + comment_rate * 0.3) * 100

    # Freshness: exponential decay based on age
    published_at = video.get("published_at", "")
    if published_at:
        try:
            pub_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            age_days = max((datetime.now(timezone.utc) - pub_dt).total_seconds() / 86400, 0)
        except (ValueError, TypeError):
            age_days = half_life_days  # unknown age → 50% freshness
    else:
        age_days = half_life_days

    freshness = math.exp(-age_days * math.log(2) / half_life_days)

    w = weights
    return (
        w.get("popularity", 0.35) * popularity
        + w.get("engagement", 0.35) * engagement
        + w.get("freshness", 0.30) * freshness
    )


def select_desired_set(
    popular: list[dict],
    rated: list[dict],
    recent: list[dict],
    max_count: int,
    source_cfg: dict,
) -> list[dict]:
    """Select the final desired set with source guarantees and deduplication.

    1. Deduplicate across all three sources by video_id (merge source_tags).
    2. Guarantee min_percentage from each source, sorted by score within source.
    3. Fill remaining slots by top score from the full pool.
    4. Attach source_tags list to each video.

    Returns list of video dicts, each with a 'source_tags' key.
    """
    # Tag each video with its source(s)
    seen: dict[str, dict] = {}  # video_id → merged video dict

    def _add_source(videos: list[dict], tag: str) -> None:
        for v in videos:
            vid = v["video_id"]
            if vid in seen:
                # Merge source tag
                if tag not in seen[vid]["source_tags"]:
                    seen[vid]["source_tags"].append(tag)
                # Keep highest score
                if v.get("_score", 0) > seen[vid].get("_score", 0):
                    seen[vid]["_score"] = v.get("_score", 0)
            else:
                seen[vid] = {**v, "source_tags": [tag]}

    _add_source(popular, "popular")
    _add_source(rated, "rated")
    _add_source(recent, "recent")

    # Calculate minimum slots per source
    pop_min = int(max_count * source_cfg.get("popular", {}).get("min_percentage", 0.20))
    rated_min = int(max_count * source_cfg.get("rated", {}).get("min_percentage", 0.20))

    # Build per-source pools (deduplicated, sorted by score desc)
    def _source_pool(tag: str) -> list[dict]:
        return sorted(
            [v for v in seen.values() if tag in v.get("source_tags", [])],
            key=lambda v: v.get("_score", 0),
            reverse=True,
        )

    pop_pool = _source_pool("popular")
    rated_pool = _source_pool("rated")

    # Guarantee minimum from each source
    selected_ids: set[str] = set()
    result: list[dict] = []

    def _fill_from(pool: list[dict], count: int) -> None:
        added = 0
        for v in pool:
            if added >= count:
                break
            if v["video_id"] not in selected_ids:
                selected_ids.add(v["video_id"])
                result.append(v)
                added += 1

    _fill_from(pop_pool, pop_min)
    _fill_from(rated_pool, rated_min)

    # Fill remaining slots from full pool by score
    remaining = max_count - len(result)
    if remaining > 0:
        full_pool = sorted(
            seen.values(),
            key=lambda v: v.get("_score", 0),
            reverse=True,
        )
        for v in full_pool:
            if len(result) >= max_count:
                break
            if v["video_id"] not in selected_ids:
                selected_ids.add(v["video_id"])
                result.append(v)

    return result


# ─── fetch_desired_videos ────────────────────────────────────────────────────

def passes_duration_filter(video: dict, min_duration_s: int) -> bool:
    """Check if a video passes the duration rules."""
    duration = video.get("duration_seconds", 0)
    return duration >= 60 and duration >= min_duration_s


def fetch_desired_videos(
    api_key: str,
    playlist_id: str,
    target_count: int,
    min_duration_s: int,
    date_cutoff: datetime,
) -> tuple[list[dict], int, int]:
    """Fetch, enrich, and filter videos page-by-page until we have target_count
    videos that pass the duration filter, hit the date boundary, or exhaust
    the channel — whichever comes first.

    Returns (desired_videos, total_fetched, quota_used).
    """
    page_size = CFG["api"]["page_size"]
    tolerance = CFG["producer"]["early_stop_tolerance"]

    desired: list[dict] = []
    page_token = None
    total_fetched = 0
    total_quota = 0
    consecutive_past_cutoff = 0

    while len(desired) < target_count:
        # Step 1: Fetch one page of playlist items
        params = {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": page_size,
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token

        data = api_get(f"{API_BASE}/playlistItems", params)
        total_quota += 1

        # Parse playlist items, checking date cutoff as we go
        raw_items = []
        hit_date_boundary = False

        for item in data.get("items", []):
            snippet = item["snippet"]
            vid_id = snippet.get("resourceId", {}).get("videoId")
            if not vid_id:
                continue

            published_at = snippet.get("publishedAt", "")

            # Check date cutoff (playlist is reverse-chronological)
            if published_at:
                try:
                    pub_dt = datetime.fromisoformat(
                        published_at.replace("Z", "+00:00")
                    )
                    if pub_dt < date_cutoff:
                        consecutive_past_cutoff += 1
                        if consecutive_past_cutoff >= tolerance:
                            hit_date_boundary = True
                            break
                        continue  # skip but keep going
                    else:
                        consecutive_past_cutoff = 0
                except ValueError:
                    consecutive_past_cutoff = 0

            raw_items.append({
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
            })

        total_fetched += len(raw_items)

        if raw_items:
            # Step 2: Enrich this page
            video_ids = [v["video_id"] for v in raw_items]
            details, enrich_calls = enrich_videos(api_key, video_ids)
            total_quota += enrich_calls

            # Step 3: Merge enrichment and filter by duration
            for v in raw_items:
                enriched = {**v, **details.get(v["video_id"], {})}
                if passes_duration_filter(enriched, min_duration_s):
                    desired.append(enriched)
                    if len(desired) >= target_count:
                        break

        # Stop if we hit the date boundary
        if hit_date_boundary:
            break

        # Check for next page
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return desired, total_fetched, total_quota


# ─── fetch_search_videos ────────────────────────────────────────────────────

def fetch_search_videos(
    api_key: str,
    channel_id: str,
    order: str,
    duration_floor: int,
) -> tuple[list[dict], int]:
    """Fetch videos via search.list for a channel with a given sort order.

    Calls search.list with channelId + order (viewCount or rating) + type=video
    + maxResults=50, then enriches via enrich_videos() and filters by duration.

    Returns (enriched_videos, quota_used).
    Cost: 100 units for search.list + enrichment batches.
    """
    params = {
        "part": "snippet",
        "channelId": channel_id,
        "type": "video",
        "order": order,
        "maxResults": 50,
        "key": api_key,
    }

    data = api_get(f"{API_BASE}/search", params)
    quota_used = 100  # search.list costs 100 units

    # Extract video IDs from search results
    raw_items = []
    for item in data.get("items", []):
        vid_id = item.get("id", {}).get("videoId")
        if not vid_id:
            continue
        snippet = item.get("snippet", {})
        raw_items.append({
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
        })

    if not raw_items:
        return [], quota_used

    # Enrich with duration + stats
    video_ids = [v["video_id"] for v in raw_items]
    details, enrich_calls = enrich_videos(api_key, video_ids)
    quota_used += enrich_calls

    # Merge enrichment and filter by duration floor
    enriched: list[dict] = []
    for v in raw_items:
        merged = {**v, **details.get(v["video_id"], {})}
        duration = merged.get("duration_seconds", 0)
        if duration >= duration_floor:
            enriched.append(merged)

    return enriched, quota_used


# ─── DB fetchers ──────────────────────────────────────────────────────────────

def fetch_curated_channels(client) -> list[dict]:
    """Fetch all curated channels with overrides and channel info."""
    resp = (
        client.table("curated_channels")
        .select("id, channel_id, date_range_override, min_duration_override, max_videos_override, last_full_refresh_at, channels(youtube_id, title, custom_url)")
        .order("display_order")
        .execute()
    )
    results = []
    for row in resp.data or []:
        ch = row.get("channels")
        if not ch:
            continue
        results.append({
            "curated_id": row["id"],
            "channel_id": row["channel_id"],
            "title": ch.get("title", ""),
            "custom_url": ch.get("custom_url", ""),
            "date_range_override": row.get("date_range_override"),
            "min_duration_override": row.get("min_duration_override"),
            "max_videos_override": row.get("max_videos_override"),
            "last_full_refresh_at": row.get("last_full_refresh_at"),
        })
    return results


def fetch_reserved_video_ids(client, channel_id: str) -> set[str]:
    """Fetch youtube_ids with popular/rated source_tags — these are frozen in recent mode.

    Reserved videos are those discovered by a prior full run and tagged as
    'popular' or 'rated'. Daily (recent) runs preserve these slots.
    """
    resp = (
        client.table("videos")
        .select("youtube_id")
        .eq("channel_id", channel_id)
        .filter("r2_synced_at", "not.is", "null")
        .overlaps("source_tags", ["popular", "rated"])
        .execute()
    )
    return {row["youtube_id"] for row in (resp.data or [])}


def fetch_existing_video_ids(client, channel_id: str) -> set[str]:
    """Fetch youtube_ids for R2-synced videos in a channel."""
    video_ids: set[str] = set()
    offset = 0
    page_size = CFG["db"]["page_size"]

    while True:
        resp = (
            client.table("videos")
            .select("youtube_id")
            .eq("channel_id", channel_id)
            .filter("r2_synced_at", "not.is", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = resp.data or []
        for row in rows:
            video_ids.add(row["youtube_id"])
        if len(rows) < page_size:
            break
        offset += page_size

    return video_ids


def fetch_existing_videos(client, channel_id: str) -> dict[str, dict]:
    """Fetch R2-synced video rows with paths for remove job metadata."""
    results: dict[str, dict] = {}
    offset = 0
    page_size = CFG["db"]["page_size"]

    while True:
        resp = (
            client.table("videos")
            .select("youtube_id, media_path, thumbnail_path, subtitle_path, title")
            .eq("channel_id", channel_id)
            .filter("r2_synced_at", "not.is", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = resp.data or []
        for row in rows:
            results[row["youtube_id"]] = {
                "media_path": row.get("media_path"),
                "thumbnail_path": row.get("thumbnail_path"),
                "subtitle_path": row.get("subtitle_path"),
                "title": row.get("title", ""),
            }
        if len(rows) < page_size:
            break
        offset += page_size

    return results


def fetch_orphaned_channel_ids(client, curated_channel_ids: set[str]) -> set[str]:
    """Find channel_ids that have videos in DB but are not in the curated set."""
    resp = client.rpc("get_distinct_video_channel_ids", {}).execute()
    all_ids = {row["channel_id"] for row in (resp.data or []) if row.get("channel_id")}
    return all_ids - curated_channel_ids


# ─── Diff ─────────────────────────────────────────────────────────────────────

def compute_diff(
    desired_ids: set[str], existing_ids: set[str]
) -> tuple[set[str], set[str]]:
    """Compute download and remove sets from desired vs existing.

    Returns (to_download, to_remove).
    """
    to_download = desired_ids - existing_ids
    to_remove = existing_ids - desired_ids
    return to_download, to_remove


# ─── Queue management ─────────────────────────────────────────────────────────

def clear_channel_jobs(client, channel_id: str, dry_run: bool = False) -> int:
    """Delete ALL jobs for a channel regardless of status.

    Producer owns the queue — each run computes truth and replaces whatever
    was there. Consumer (future) will delete its own rows when done.
    """
    if dry_run:
        return 0

    resp = (
        client.table("sync_queue")
        .delete()
        .eq("channel_id", channel_id)
        .execute()
    )
    return len(resp.data or [])


def enqueue_jobs(
    client,
    jobs: list[dict],
    dry_run: bool = False,
) -> int:
    """Batch INSERT jobs into sync_queue with ON CONFLICT DO NOTHING.

    Uses the enqueue_sync_jobs RPC to respect the partial unique index.
    Each job dict: {video_id, channel_id, action, metadata}.
    Returns count of jobs enqueued (or would-be enqueued in dry_run).
    """
    if not jobs:
        return 0

    if dry_run:
        for job in jobs:
            print(f"    [DRY RUN] Would enqueue {job['action']}: {job['video_id']}")
        return len(jobs)

    batch_size = CFG["db"]["enqueue_batch_size"]
    enqueued = 0
    for i in range(0, len(jobs), batch_size):
        batch = jobs[i : i + batch_size]
        payload = [
            {
                "video_id": j["video_id"],
                "channel_id": j["channel_id"],
                "action": j["action"],
                "metadata": j.get("metadata", {}),
            }
            for j in batch
        ]
        resp = client.rpc("enqueue_sync_jobs", {"jobs": payload}).execute()
        enqueued += resp.data if isinstance(resp.data, int) else len(batch)

    return enqueued


# ─── process_channel ──────────────────────────────────────────────────────────

def update_source_tags(client, video_id: str, source_tags: list[str], dry_run: bool = False) -> None:
    """Update source_tags on a video in the DB."""
    if dry_run:
        return
    client.table("videos").update({"source_tags": source_tags}).eq("youtube_id", video_id).execute()


def update_full_refresh_timestamp(client, curated_ids: list[str], dry_run: bool = False) -> None:
    """Stamp last_full_refresh_at = NOW() on channels that just got a full run."""
    if dry_run or not curated_ids:
        return
    client.table("curated_channels").update(
        {"last_full_refresh_at": datetime.now(timezone.utc).isoformat()}
    ).in_("id", curated_ids).execute()


def process_channel(
    client,
    api_key: str,
    channel: dict,
    mode: str = "recent",
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Orchestrate full pipeline for a single channel.

    Modes:
      - 'recent': Daily run. Fetch playlist only, score recent candidates,
        preserve reserved (popular/rated) slots from prior full runs.
      - 'full': Weekly run. Fetch popular + rated + recent, deduplicate,
        score, apply source guarantees, full reconciliation.

    Returns summary dict with counts and quota usage.
    """
    channel_id = channel["channel_id"]
    title = channel.get("title", channel_id)
    target = channel.get("max_videos_override") or CFG["producer"]["max_videos_per_channel"]
    min_duration = channel.get("min_duration_override") or CFG["producer"]["min_duration_seconds"]
    weights = CFG["scoring"]["weights"]
    half_life = CFG["scoring"]["freshness_half_life_days"]
    source_cfg = CFG["sources"]

    date_override = channel.get("date_range_override")
    date_cutoff = parse_date_range(date_override)
    range_label = date_override or f"default {CFG['producer']['default_date_range']}"

    label = f"{title} ({channel_id})"

    def error_result(err: str, quota: int = 0) -> dict:
        return {
            "channel_id": channel_id,
            "title": title,
            "error": err,
            "mode": mode,
            "desired": 0,
            "existing": 0,
            "downloads": 0,
            "removals": 0,
            "reserved": 0,
            "quota_used": quota,
        }

    playlist_id = uploads_playlist_id(channel_id)
    quota_used = 0

    if mode == "recent":
        # ── RECENT MODE ──────────────────────────────────────────────
        # 1. Fetch reserved IDs (popular/rated from prior full run)
        reserved_ids = fetch_reserved_video_ids(client, channel_id)
        available_slots = max(target - len(reserved_ids), 0)

        # 2. Fetch recent candidates from playlist
        try:
            recent_candidates, total_fetched, q = fetch_desired_videos(
                api_key, playlist_id, available_slots, min_duration, date_cutoff,
            )
            quota_used += q
        except requests.exceptions.HTTPError as e:
            print(f"  {label}: ERROR fetching: {e}")
            return error_result(str(e))

        # 3. Score all recent candidates
        for v in recent_candidates:
            v["_score"] = score_video(v, weights, half_life)
            v["source_tags"] = ["recent"]

        # 4. Slice top by score
        recent_candidates.sort(key=lambda v: v.get("_score", 0), reverse=True)
        recent_candidates = recent_candidates[:available_slots]

        recent_ids = {v["video_id"] for v in recent_candidates}
        desired_ids = reserved_ids | recent_ids
        desired_lookup = {v["video_id"]: v for v in recent_candidates}

        # 5. Fetch existing, compute diff (never remove reserved)
        existing_videos = fetch_existing_videos(client, channel_id)
        existing_ids = set(existing_videos.keys())

        to_download = desired_ids - existing_ids
        to_remove = set()  # TEMP: pausig removals during initial seeding
        # to_remove = (existing_ids - desired_ids) - reserved_ids  # never remove reserved

        if verbose:
            score_range = ""
            if recent_candidates:
                scores = [v.get("_score", 0) for v in recent_candidates]
                score_range = f" (scores: {min(scores):.2f}–{max(scores):.2f})"
            print(f"    {label}: {len(reserved_ids)} reserved + {len(recent_ids)} recent{score_range}")
            for vid in sorted(to_download):
                meta = desired_lookup.get(vid, {})
                print(f"    + DOWNLOAD: {vid} — {meta.get('title', '?')}")
            for vid in sorted(to_remove):
                rm_meta = existing_videos.get(vid, {})
                print(f"    - REMOVE:   {vid} — {rm_meta.get('title', '?')}")

        # 6. Clear non-reserved jobs for this channel, then enqueue fresh
        if not dry_run:
            resp = (
                client.table("sync_queue")
                .select("id, video_id")
                .eq("channel_id", channel_id)
                .execute()
            )
            non_reserved = [r for r in (resp.data or []) if r["video_id"] not in reserved_ids]
            if non_reserved:
                client.table("sync_queue").delete().in_("id", [r["id"] for r in non_reserved]).execute()

        download_jobs = [
            {"video_id": vid, "channel_id": channel_id, "action": "download",
             "metadata": desired_lookup.get(vid, {})}
            for vid in to_download
        ]
        remove_jobs = [
            {"video_id": vid, "channel_id": channel_id, "action": "remove",
             "metadata": existing_videos.get(vid, {})}
            for vid in to_remove
        ]

        dl_count = enqueue_jobs(client, download_jobs, dry_run)
        rm_count = enqueue_jobs(client, remove_jobs, dry_run)

        # 7. Update source_tags on newly downloaded recent videos
        for vid in recent_ids:
            update_source_tags(client, vid, ["recent"], dry_run)

        print(
            f"  {label}: {len(reserved_ids)} reserved + {len(recent_ids)} recent = "
            f"{len(desired_ids)} desired, +{dl_count} dl, -{rm_count} rm, "
            f"{quota_used} quota ({range_label})"
        )

        return {
            "channel_id": channel_id,
            "title": title,
            "mode": mode,
            "desired": len(desired_ids),
            "existing": len(existing_ids),
            "reserved": len(reserved_ids),
            "downloads": dl_count,
            "removals": rm_count,
            "quota_used": quota_used,
        }

    else:
        # ── FULL MODE ────────────────────────────────────────────────
        # 1. Fetch popular via search.list(order=viewCount)
        popular_candidates: list[dict] = []
        rated_candidates: list[dict] = []

        try:
            popular_candidates, pop_quota = fetch_search_videos(
                api_key, channel_id, "viewCount",
                source_cfg.get("popular", {}).get("duration_floor", 60),
            )
            quota_used += pop_quota
        except Exception as e:
            print(f"    {label}: WARNING popular search failed: {e}")

        # 2. Fetch rated via search.list(order=rating)
        try:
            rated_candidates, rat_quota = fetch_search_videos(
                api_key, channel_id, "rating",
                source_cfg.get("rated", {}).get("duration_floor", 60),
            )
            quota_used += rat_quota
        except Exception as e:
            print(f"    {label}: WARNING rated search failed: {e}")

        # 3. Fetch recent via playlist
        try:
            recent_candidates, total_fetched, q = fetch_desired_videos(
                api_key, playlist_id, target, min_duration, date_cutoff,
            )
            quota_used += q
        except requests.exceptions.HTTPError as e:
            print(f"  {label}: ERROR fetching recent: {e}")
            return error_result(str(e), quota_used)

        # 4. Score all candidates
        for v in popular_candidates:
            v["_score"] = score_video(v, weights, half_life)
        for v in rated_candidates:
            v["_score"] = score_video(v, weights, half_life)
        for v in recent_candidates:
            v["_score"] = score_video(v, weights, half_life)

        # 5. Select desired set with source guarantees
        desired = select_desired_set(
            popular_candidates, rated_candidates, recent_candidates,
            target, source_cfg,
        )

        if not desired:
            print(f"  {label}: 0 videos found across all sources")
            return error_result("", quota_used)

        desired_ids = {v["video_id"] for v in desired}
        desired_lookup = {v["video_id"]: v for v in desired}

        # 6. Fetch existing, compute full diff
        existing_videos = fetch_existing_videos(client, channel_id)
        existing_ids = set(existing_videos.keys())
        to_download, _to_remove = compute_diff(desired_ids, existing_ids)
        to_remove = set()  # TEMP: pausing removals during initial seeding

        if verbose:
            pop_count = sum(1 for v in desired if "popular" in v.get("source_tags", []))
            rat_count = sum(1 for v in desired if "rated" in v.get("source_tags", []))
            rec_count = sum(1 for v in desired if "recent" in v.get("source_tags", []))
            scores = [v.get("_score", 0) for v in desired]
            print(
                f"    {label}: {pop_count} popular + {rat_count} rated + {rec_count} recent "
                f"= {len(desired)} desired (scores: {min(scores):.2f}–{max(scores):.2f})"
            )
            for vid in sorted(to_download):
                meta = desired_lookup.get(vid, {})
                tags = meta.get("source_tags", [])
                print(f"    + DOWNLOAD: {vid} [{','.join(tags)}] — {meta.get('title', '?')}")
            for vid in sorted(to_remove):
                rm_meta = existing_videos.get(vid, {})
                print(f"    - REMOVE:   {vid} — {rm_meta.get('title', '?')}")

        # 7. Clear ALL jobs (full reconciliation), then enqueue fresh
        cleared = clear_channel_jobs(client, channel_id, dry_run)

        download_jobs = [
            {"video_id": vid, "channel_id": channel_id, "action": "download",
             "metadata": desired_lookup.get(vid, {})}
            for vid in to_download
        ]
        remove_jobs = [
            {"video_id": vid, "channel_id": channel_id, "action": "remove",
             "metadata": existing_videos.get(vid, {})}
            for vid in to_remove
        ]

        dl_count = enqueue_jobs(client, download_jobs, dry_run)
        rm_count = enqueue_jobs(client, remove_jobs, dry_run)

        # 8. Update source_tags on all desired videos
        for v in desired:
            update_source_tags(client, v["video_id"], v.get("source_tags", []), dry_run)

        pop_count = sum(1 for v in desired if "popular" in v.get("source_tags", []))
        rat_count = sum(1 for v in desired if "rated" in v.get("source_tags", []))
        rec_count = sum(1 for v in desired if "recent" in v.get("source_tags", []))

        print(
            f"  {label}: {pop_count} popular + {rat_count} rated + {rec_count} recent = "
            f"{len(desired)} desired, +{dl_count} dl, -{rm_count} rm, "
            f"{quota_used} quota ({range_label})"
        )

        return {
            "channel_id": channel_id,
            "title": title,
            "mode": mode,
            "desired": len(desired),
            "existing": len(existing_ids),
            "reserved": 0,
            "downloads": dl_count,
            "removals": rm_count,
            "quota_used": quota_used,
        }


# ─── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Video Sync Producer — discover downloads, identify removals"
    )
    parser.add_argument(
        "--mode",
        choices=["recent", "full"],
        default="recent",
        help="Run mode: 'recent' (daily, playlist only) or 'full' (weekly, popular+rated+recent)",
    )
    parser.add_argument(
        "--channel",
        help="Run for a single channel ID only",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be enqueued, don't write",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show per-video decisions",
    )
    args = parser.parse_args()

    load_env()
    api_key = get_env("YOUTUBE_API_KEY")
    client = get_supabase_client()

    start_time = time.time()

    # Fetch curated channels
    print("Fetching curated channels...")
    channels = fetch_curated_channels(client)

    if not channels:
        print("No curated channels found.")
        return

    # Filter to single channel if requested
    if args.channel:
        channels = [c for c in channels if c["channel_id"] == args.channel]
        if not channels:
            print(f"Channel {args.channel} not found in curated channels.")
            return

    max_workers = CFG["api"]["max_workers"]
    warn_threshold = CFG["quota"]["warn_threshold"]

    # ── Rolling channel selection ─────────────────────────────────────────
    # --mode full: only a fraction get full (popular+rated+recent),
    # the rest get recent-only. Rotated by last_full_refresh_at.
    # --mode recent: all channels get recent.
    # --channel: single channel gets whatever mode was requested.

    channel_modes: dict[str, str] = {}  # channel_id → "full" or "recent"
    full_curated_ids: list[str] = []    # curated_channel IDs that got full mode

    if args.mode == "full" and not args.channel:
        pct = CFG["producer"]["full_refresh_percentage"]
        full_count = max(1, math.ceil(len(channels) * pct))

        # Sort by last_full_refresh_at ASC, NULLS FIRST (never-refreshed first)
        sorted_channels = sorted(
            channels,
            key=lambda c: c.get("last_full_refresh_at") or "",
        )
        full_set = {c["channel_id"] for c in sorted_channels[:full_count]}
        full_curated_ids = [c["curated_id"] for c in sorted_channels[:full_count]]

        for ch in channels:
            channel_modes[ch["channel_id"]] = "full" if ch["channel_id"] in full_set else "recent"

        full_names = [c["title"] for c in sorted_channels[:full_count]]
        print(f"Rolling refresh: {full_count} FULL + {len(channels) - full_count} RECENT")
        print(f"  Full channels: {', '.join(full_names)}")
    else:
        for ch in channels:
            channel_modes[ch["channel_id"]] = args.mode

    print(f"Processing {len(channels)} channel(s) with {max_workers} workers...")
    if args.dry_run:
        print("[DRY RUN MODE — no database writes]")

    # Process channels in parallel
    summaries: list[dict] = []
    total_quota = 0
    total_downloads = 0
    total_removals = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_channel = {
            executor.submit(
                process_channel, client, api_key, ch,
                channel_modes[ch["channel_id"]], args.dry_run, args.verbose
            ): ch
            for ch in channels
        }

        for future in as_completed(future_to_channel):
            ch = future_to_channel[future]
            try:
                summary = future.result()
                summaries.append(summary)
                total_quota += summary["quota_used"]
                total_downloads += summary["downloads"]
                total_removals += summary["removals"]

                if total_quota > warn_threshold:
                    print(f"  WARNING: Quota usage ({total_quota}) exceeds threshold ({warn_threshold})")
            except Exception as e:
                print(f"  ERROR processing {ch.get('title', ch['channel_id'])}: {e}")
                summaries.append({
                    "channel_id": ch["channel_id"],
                    "title": ch.get("title", ""),
                    "error": str(e),
                    "desired": 0,
                    "existing": 0,
                    "downloads": 0,
                    "removals": 0,
                    "quota_used": 0,
                })

    # Stamp last_full_refresh_at on channels that got full mode
    if full_curated_ids:
        update_full_refresh_timestamp(client, full_curated_ids, args.dry_run)

    # Orphaned channel cleanup
    curated_ids = {c["channel_id"] for c in channels}
    # TEMP: pausing removals during initial seeding
    # if not args.channel:
    #     print("\nChecking for orphaned channels...")
    #     try:
    #         orphaned = fetch_orphaned_channel_ids(client, curated_ids)
    #         if orphaned:
    #             print(f"  Found {len(orphaned)} orphaned channel(s)")
    #             for oc_id in orphaned:
    #                 orphan_vids = fetch_existing_video_ids(client, oc_id)
    #                 if orphan_vids:
    #                     remove_jobs = [
    #                         {
    #                             "video_id": vid,
    #                             "channel_id": oc_id,
    #                             "action": "remove",
    #                             "metadata": {},
    #                         }
    #                         for vid in orphan_vids
    #                     ]
    #                     rm_count = enqueue_jobs(client, remove_jobs, args.dry_run)
    #                     total_removals += rm_count
    #                     print(f"    {oc_id}: {rm_count} remove jobs")
    #         else:
    #             print("  No orphaned channels found")
    #     except Exception as e:
    #         print(f"  ERROR checking orphaned channels: {e}")

    # Summary
    elapsed = time.time() - start_time
    full_count = sum(1 for s in summaries if s.get("mode") == "full")
    recent_count = sum(1 for s in summaries if s.get("mode") == "recent")
    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Channels processed: {len(summaries)} ({full_count} full, {recent_count} recent)")
    print(f"  Total downloads enqueued: {total_downloads}")
    print(f"  Total removals enqueued: {total_removals}")
    print(f"  Total API quota used: {total_quota}")
    print(f"  Daily quota remaining: ~{CFG['quota']['daily_limit'] - total_quota}")
    print(f"  Runtime: {elapsed:.1f}s")

    errors = [s for s in summaries if s.get("error")]
    if errors:
        print(f"\n  Errors ({len(errors)}):")
        for s in errors:
            print(f"    {s['title']}: {s['error']}")


if __name__ == "__main__":
    main()
