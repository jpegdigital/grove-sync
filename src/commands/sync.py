"""SyncCommand — unified channel sync: fetch, calibrate, score, dedup, enqueue.

Single-pass pipeline per channel:
1. Paginate the full uploads playlist (one API pass)
2. Calibrate on the unfiltered set → upsert channel_calibration
3. Duration filter
4. Score all filtered videos (ESE)
5. Update scores on already-stored videos
6. Dedup against R2 → identify new videos
7. Enqueue download jobs to sync_queue
8. Stamp last_full_refresh_at
"""

from __future__ import annotations

import argparse
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.config import AppConfig
from src.scoring import VideoScorer
from src.services.db import SyncDatabase
from src.services.youtube import YouTubeClient, uploads_playlist_id


class SyncCommand:
    """Unified sync: fetch → calibrate → score → dedup → enqueue."""

    def __init__(
        self,
        config: AppConfig,
        db: SyncDatabase,
        scorer: VideoScorer,
        youtube: YouTubeClient,
    ):
        self.config = config
        self.db = db
        self.scorer = scorer
        self.yt = youtube

    def run(
        self,
        channel: str | None = None,
        all_channels: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Fetch channels, process in parallel, print summary."""
        start_time = time.time()

        self._reset(dry_run)

        print("Fetching curated channels...")
        channels = self.db.fetch_curated_channels()

        if not channels:
            print("No curated channels found.")
            return

        if channel:
            channels = [c for c in channels if c["channel_id"] == channel]
            if not channels:
                print(f"Channel {channel} not found in curated channels.")
                return
        elif not all_channels:
            channels = self._select_next_channel(channels)

        max_workers = self.config.api["max_workers"]
        warn_threshold = self.config.quota["warn_threshold"]

        print(f"Sync: {len(channels)} channel(s) with {max_workers} workers...")
        if dry_run:
            print("[DRY RUN MODE — no database writes]")

        summaries: list[dict] = []
        total_quota = 0
        total_downloads = 0
        refreshed_curated_ids: list[str] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_channel = {
                executor.submit(self.process_channel, ch, dry_run, verbose): ch
                for ch in channels
            }

            for future in as_completed(future_to_channel):
                ch = future_to_channel[future]
                try:
                    summary = future.result()
                    summaries.append(summary)
                    total_quota += summary["quota_used"]
                    total_downloads += summary["downloads"]
                    if not summary.get("error"):
                        refreshed_curated_ids.append(ch["curated_id"])

                    if total_quota > warn_threshold:
                        print(f"  WARNING: Quota usage ({total_quota}) exceeds threshold ({warn_threshold})")
                except Exception as e:
                    print(f"  ERROR processing {ch.get('title', ch['channel_id'])}: {e}")
                    summaries.append(self._error_result(ch["channel_id"], ch.get("title", ""), str(e), 0))

        if refreshed_curated_ids:
            self.db.update_full_refresh_timestamp(refreshed_curated_ids, dry_run)

        elapsed = time.time() - start_time
        self._print_summary(summaries, total_quota, elapsed)

    def _select_next_channel(self, channels: list[dict]) -> list[dict]:
        """Select the single channel with the oldest last_full_refresh_at."""
        selected = min(
            channels,
            key=lambda c: c.get("last_full_refresh_at") or "",
        )
        print(f"Next channel: {selected.get('title', selected['channel_id'])} (last synced: {selected.get('last_full_refresh_at') or 'never'})")
        return [selected]

    def process_channel(
        self,
        channel: dict,
        dry_run: bool,
        verbose: bool,
    ) -> dict:
        """Execute full pipeline for one channel."""
        channel_id = channel["channel_id"]
        title = channel.get("title", channel_id)
        sync_mode = channel.get("sync_mode", "sync")
        label = f"{title} ({channel_id})"

        alpha = channel["scoring_alpha"]
        min_dur = channel["min_duration_seconds"]
        max_dur = channel.get("max_duration_seconds") or None

        # Step 1: Paginate entire uploads playlist
        playlist_id = uploads_playlist_id(channel_id)
        early_stop = self.config.producer["early_stop_tolerance"]

        try:
            all_videos, total_fetched, quota_used = self.yt.fetch_playlist_videos(
                playlist_id, 100_000,
                datetime(1970, 1, 1, tzinfo=timezone.utc),
                early_stop,
            )
        except requests.exceptions.HTTPError as e:
            print(f"  {label}: ERROR fetching playlist: {e}")
            return self._error_result(channel_id, title, str(e), 0)

        # Step 2: Calibrate on full unfiltered set
        buckets = self._duration_buckets(all_videos)
        passing = self._passing_counts(all_videos)
        cadence = self._compute_cadence(all_videos)

        try:
            self.db.upsert_channel_calibration(
                channel_id=channel_id,
                total_videos_sampled=len(all_videos),
                cadence=cadence,
                passing=passing,
                duration_buckets=buckets,
            )
        except Exception as cal_err:
            print(f"  WARNING: Calibration upsert failed for {title}: {cal_err}")

        # Step 3 + 4: Duration filter + score
        scored = self.scorer.select_canonical(all_videos, alpha, min_dur, max_dur)

        # Step 5: Update scores on already-stored videos
        existing_r2 = self.db.fetch_existing_r2_with_bytes(channel_id)
        existing_ids = set(existing_r2.keys())

        scored_lookup = {v["video_id"]: v for v in scored}
        score_updates = {
            vid: scored_lookup[vid].get("score", 0)
            for vid in existing_ids
            if vid in scored_lookup
        }
        self.db.update_video_scores(score_updates, dry_run)

        # Step 6: Dedup
        to_download = [v for v in scored if v["video_id"] not in existing_ids]

        if verbose:
            print(f"    {label}: {len(all_videos)} fetched, {len(scored)} eligible, {len(existing_ids)} in R2, {len(to_download)} to download")
            for v in sorted(to_download, key=lambda x: x.get("score", 0), reverse=True)[:10]:
                print(f"      + {v['video_id']} score={v.get('score', 0):.2f} — {v.get('title', '?')}")
            if len(to_download) > 10:
                print(f"      ... and {len(to_download) - 10} more")

        # Step 7: Enqueue download jobs
        download_jobs = [
            {
                "video_id": v["video_id"],
                "channel_id": channel_id,
                "metadata": v,
                "score": v.get("score", 0),
                "published_at": v.get("published_at"),
            }
            for v in to_download
        ]
        self.db.replace_channel_jobs(channel_id, download_jobs, dry_run=dry_run)

        return {
            "channel_id": channel_id,
            "title": title,
            "sync_mode": sync_mode,
            "total_videos": len(all_videos),
            "eligible": len(scored),
            "existing": len(existing_ids),
            "downloads": len(to_download),
            "scores_updated": len(score_updates),
            "quota_used": quota_used,
            "cadence": cadence,
        }

    # ── Reset ─────────────────────────────────────────────────────────────

    def _reset(self, dry_run: bool) -> None:
        """Clear sync queue and staging directory for a clean run."""
        staging = self.config.project_root / "downloads" / "staging"

        if dry_run:
            print("[DRY RUN] Would clear sync queue and staging directory")
            return

        cleared = self.db.clear_sync_queue()
        if cleared:
            print(f"Cleared {cleared} jobs from sync queue")

        if staging.exists():
            shutil.rmtree(staging)
            staging.mkdir(parents=True, exist_ok=True)
            print("Cleared staging directory")

    # ── Output ────────────────────────────────────────────────────────────

    @staticmethod
    def _print_summary(summaries: list[dict], total_quota: int, elapsed: float) -> None:
        print(f"\n{'=' * 60}")
        print("SYNC SUMMARY")
        print(f"{'=' * 60}")

        for s in summaries:
            if s.get("error"):
                print(f"\n  {s['title']}: ERROR — {s['error']}")
                continue

            cadence = s.get("cadence", {})
            ppw = cadence.get("posts_per_week", 0)
            med_gap = cadence.get("median_gap_days")
            med_dur = cadence.get("median_duration_seconds")

            print(f"\n  {s['title']} [{s.get('sync_mode', 'sync')}]")
            print(f"    Discovered:  {s['total_videos']} videos, {s['eligible']} eligible")
            print(f"    Stored:      {s['existing']} in R2, {s['scores_updated']} scores refreshed")
            print(f"    Enqueued:    {s['downloads']} new downloads")
            if ppw:
                gap_str = f", {med_gap:.0f}d median gap" if med_gap else ""
                dur_str = f", {med_dur // 60:.0f}m median duration" if med_dur else ""
                print(f"    Cadence:     {ppw:.1f} posts/week{gap_str}{dur_str}")

        print(f"\n  Quota: {total_quota} used")
        print(f"  Runtime: {elapsed:.1f}s")

    # ── Pure helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _compute_cadence(videos: list[dict]) -> dict:
        """Compute posting frequency stats from sampled videos (60s+ only)."""
        qualified = []
        for v in videos:
            if v.get("duration_seconds", 0) < 60:
                continue
            pub_str = v.get("published_at", "")
            if not pub_str:
                continue
            try:
                pub = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            qualified.append({"pub": pub, "duration_seconds": v["duration_seconds"]})

        if not qualified:
            return {"avg_gap_days": None, "median_gap_days": None, "posts_per_week": 0}

        qualified.sort(key=lambda v: v["pub"], reverse=True)

        gaps = []
        for i in range(len(qualified) - 1):
            gap = (qualified[i]["pub"] - qualified[i + 1]["pub"]).total_seconds() / 86400
            gaps.append(gap)

        span_days = max((qualified[0]["pub"] - qualified[-1]["pub"]).total_seconds() / 86400, 1)
        posts_per_week = len(qualified) / span_days * 7

        durations = [v["duration_seconds"] for v in qualified]
        avg_dur = sum(durations) / len(durations)
        median_dur = sorted(durations)[len(durations) // 2]

        return {
            "posts_per_week": round(posts_per_week, 2),
            "avg_gap_days": round(sum(gaps) / len(gaps), 1) if gaps else None,
            "median_gap_days": round(sorted(gaps)[len(gaps) // 2], 1) if gaps else None,
            "avg_duration_seconds": round(avg_dur),
            "median_duration_seconds": median_dur,
        }

    @staticmethod
    def _duration_buckets(videos: list[dict]) -> dict:
        buckets = {
            "under_1m": 0,
            "1_5m": 0,
            "5_10m": 0,
            "10_20m": 0,
            "20_30m": 0,
            "30_60m": 0,
            "1_2h": 0,
            "over_2h": 0,
        }
        for v in videos:
            d = v.get("duration_seconds", 0)
            if d < 60:
                buckets["under_1m"] += 1
            elif d < 300:
                buckets["1_5m"] += 1
            elif d < 600:
                buckets["5_10m"] += 1
            elif d < 1200:
                buckets["10_20m"] += 1
            elif d < 1800:
                buckets["20_30m"] += 1
            elif d < 3600:
                buckets["30_60m"] += 1
            elif d < 7200:
                buckets["1_2h"] += 1
            else:
                buckets["over_2h"] += 1
        return buckets

    @staticmethod
    def _passing_counts(videos: list[dict]) -> dict:
        return {
            "min_60s": sum(1 for v in videos if v.get("duration_seconds", 0) >= 60),
            "min_300s": sum(1 for v in videos if v.get("duration_seconds", 0) >= 300),
            "min_300s_max_1800s": sum(1 for v in videos if 300 <= v.get("duration_seconds", 0) <= 1800),
            "min_300s_max_3600s": sum(1 for v in videos if 300 <= v.get("duration_seconds", 0) <= 3600),
            "min_60s_max_1800s": sum(1 for v in videos if 60 <= v.get("duration_seconds", 0) <= 1800),
            "min_60s_max_3600s": sum(1 for v in videos if 60 <= v.get("duration_seconds", 0) <= 3600),
        }

    @staticmethod
    def _error_result(channel_id: str, title: str, error: str, quota_used: int) -> dict:
        return {
            "channel_id": channel_id,
            "title": title,
            "error": error,
            "total_videos": 0,
            "eligible": 0,
            "existing": 0,
            "downloads": 0,
            "quota_used": quota_used,
        }


# ─── CLI entry point ─────────────────────────────────────────────────────────


def _bootstrap() -> tuple[AppConfig, SyncDatabase, VideoScorer, YouTubeClient]:
    """Create all shared service instances for the sync command."""
    config = AppConfig.load()
    config.validate_producer_env()

    from supabase import create_client

    client = create_client(
        config.get_env("NEXT_PUBLIC_SUPABASE_URL"),
        config.get_env("SUPABASE_SECRET_KEY"),
    )

    db = SyncDatabase(
        client,
        page_size=config.db["page_size"],
        enqueue_batch_size=config.db["enqueue_batch_size"],
    )
    yt = YouTubeClient(config.get_env("YOUTUBE_API_KEY"), config.api)
    scorer = VideoScorer()
    return config, db, scorer, yt


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unified sync — fetch, calibrate, score, dedup, enqueue downloads"
    )
    parser.add_argument("--channel", help="Single channel ID (skips rotation, full refresh)")
    parser.add_argument("--all", action="store_true", dest="all_channels",
                        help="Process all curated channels (skips rotation)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    parser.add_argument("--verbose", action="store_true", help="Detailed logging")
    args = parser.parse_args()

    config, db, scorer, yt = _bootstrap()
    cmd = SyncCommand(config, db, scorer, yt)
    cmd.run(args.channel, args.all_channels, args.dry_run, args.verbose)
