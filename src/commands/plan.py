"""PlanCommand — full-playlist discovery, scoring, and download enqueuing.

Paginates the entire uploads playlist for each channel, scores all videos,
deduplicates against R2, and enqueues download jobs for everything not already
stored. Budget decisions happen in sync-process with actual measured bytes.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from src.config import AppConfig
from src.scoring import VideoScorer
from src.services.db import SyncDatabase
from src.services.youtube import YouTubeClient, uploads_playlist_id


class PlanCommand:
    """Unified planning: discover, score, dedup against R2, enqueue downloads."""

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
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Fetch channels, process in parallel, print summary."""
        start_time = time.time()

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
        else:
            channels = self._select_rolling_channels(channels)

        max_workers = self.config.api["max_workers"]
        warn_threshold = self.config.quota["warn_threshold"]

        print(f"Sync plan: {len(channels)} channel(s) with {max_workers} workers...")
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
                    summaries.append({
                        "channel_id": ch["channel_id"],
                        "title": ch.get("title", ""),
                        "error": str(e),
                        "total_videos": 0,
                        "eligible": 0,
                        "existing": 0,
                        "downloads": 0,
                        "quota_used": 0,
                    })

        if refreshed_curated_ids:
            self.db.update_full_refresh_timestamp(refreshed_curated_ids, dry_run)

        elapsed = time.time() - start_time
        print(f"\n{'=' * 60}")
        print("SYNC PLAN SUMMARY")
        print(f"{'=' * 60}")
        print(f"  Channels processed: {len(summaries)}")
        print(f"  Downloads enqueued: {total_downloads}")
        print(f"  Total API quota used: {total_quota}")
        print(f"  Daily quota remaining: ~{self.config.quota['daily_limit'] - total_quota}")
        print(f"  Runtime: {elapsed:.1f}s")

        errors = [s for s in summaries if s.get("error")]
        if errors:
            print(f"\n  Errors ({len(errors)}):")
            for s in errors:
                print(f"    {s['title']}: {s['error']}")

    def _select_rolling_channels(self, channels: list[dict]) -> list[dict]:
        """Select channels due for refresh, rotated by last_full_refresh_at."""
        full_count = self.config.producer["channels_per_run"]

        sorted_channels = sorted(
            channels,
            key=lambda c: c.get("last_full_refresh_at") or "",
        )
        selected = sorted_channels[:full_count]

        full_names = [c.get("title", c["channel_id"]) for c in selected]
        print(f"Rolling refresh: {full_count}/{len(channels)} channels selected")
        print(f"  Channels: {', '.join(full_names)}")

        return selected

    def process_channel(
        self,
        channel: dict,
        dry_run: bool,
        verbose: bool,
    ) -> dict:
        """Paginate full uploads playlist → score → dedup against R2 → enqueue."""
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

        # Step 2: Score all candidates (duration filtering happens inside)
        scored = self.scorer.select_canonical(
            all_videos, alpha, min_dur, max_dur,
        )

        # Step 3: Dedup against R2
        existing_r2 = self.db.fetch_existing_r2_with_bytes(channel_id)
        existing_ids = set(existing_r2.keys())

        to_download = [v for v in scored if v["video_id"] not in existing_ids]

        if verbose:
            print(f"    {label}: {len(all_videos)} fetched, {len(scored)} eligible, {len(existing_ids)} in R2, {len(to_download)} to download")
            for v in sorted(to_download, key=lambda x: x.get("score", 0), reverse=True)[:10]:
                print(f"      + {v['video_id']} score={v.get('score', 0):.2f} — {v.get('title', '?')}")
            if len(to_download) > 10:
                print(f"      ... and {len(to_download) - 10} more")

        # Step 4: Build download jobs and enqueue
        download_jobs = []
        for v in to_download:
            download_jobs.append({
                "video_id": v["video_id"],
                "channel_id": channel_id,
                "metadata": v,
                "score": v.get("score", 0),
                "published_at": v.get("published_at"),
            })

        self.db.replace_channel_jobs(channel_id, download_jobs, dry_run=dry_run)

        # Step 5: Update scores on already-stored videos
        scored_lookup = {v["video_id"]: v for v in scored}
        score_updates = {
            vid: scored_lookup[vid].get("score", 0)
            for vid in existing_ids
            if vid in scored_lookup
        }
        self.db.update_video_scores(score_updates, dry_run)

        print(
            f"  {label} [{sync_mode}]: {len(all_videos)} videos, {len(scored)} eligible, "
            f"{len(existing_ids)} in R2, +{len(to_download)} dl, {quota_used} quota"
        )

        return {
            "channel_id": channel_id,
            "title": title,
            "total_videos": len(all_videos),
            "eligible": len(scored),
            "existing": len(existing_ids),
            "downloads": len(to_download),
            "quota_used": quota_used,
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
    """Create all shared service instances for the plan command."""
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
        description="Plan sync — paginate uploads, score, dedup against R2, enqueue downloads"
    )
    parser.add_argument("--channel", help="Single channel ID (skips rotation, full refresh)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    parser.add_argument("--verbose", action="store_true", help="Detailed logging")
    args = parser.parse_args()

    config, db, scorer, yt = _bootstrap()
    cmd = PlanCommand(config, db, scorer, yt)
    cmd.run(args.channel, args.dry_run, args.verbose)
