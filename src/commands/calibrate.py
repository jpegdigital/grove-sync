"""CalibrateCommand — lightweight YouTube API sampling for all curated channels.

Pages through each channel's entire uploads playlist, enriches with videos.list
for duration/stats, and writes per-channel JSON + a summary to ./tmp/calibration/.

No date clamping — samples the full library so calibration stats (posting frequency,
duration profile) reflect the complete picture.

Quota cost: ~2 units per page (1 playlistItems + 1 videos.list enrichment).
At 50 vids/page x 20 pages max = 1,000 videos per channel, ~40 quota per channel.
For 52 channels: ~2,080 quota total (well within 10,000 daily limit).
"""

from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from src.config import AppConfig
from src.scoring import VideoScorer
from src.services.db import SyncDatabase
from src.services.youtube import YouTubeClient, uploads_playlist_id


class CalibrateCommand:
    """Samples curated channels via YouTube API and writes calibration data."""

    def __init__(
        self,
        config: AppConfig,
        db: SyncDatabase,
        youtube: YouTubeClient,
        scorer: VideoScorer,
        output_dir: Path | None = None,
    ):
        self.config = config
        self.db = db
        self.yt = youtube
        self.scorer = scorer
        self.output_dir = output_dir or (config.project_root / "tmp" / "calibration")

    def run(
        self,
        max_pages: int = 20,
        channel: str | None = None,
        workers: int | None = None,
    ) -> None:
        """Main entry point — fetch channels, sample in parallel, write results."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        channels = self._fetch_channels()

        if channel:
            channels = [c for c in channels if c["channel_id"] == channel]
            if not channels:
                print(f"Channel {channel} not found in curated_channels")
                return

        max_workers = workers or self.config.api.get("max_workers", 8)
        print(f"Calibration run: {len(channels)} channels, max {max_pages} pages each")
        print(f"Output: {self.output_dir}\n")

        start_time = time.time()
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._sample_channel, ch, max_pages): ch for ch in channels}
            for future in as_completed(futures):
                ch = futures[future]
                try:
                    result = future.result()
                    results.append(result)

                    # Write per-channel JSON
                    safe_name = re.sub(r"[^\w\-]", "_", ch["title"])[:50]
                    filename = f"{safe_name}.json"
                    with open(self.output_dir / filename, "w", encoding="utf-8") as f:
                        json.dump(result, f, indent=2, ensure_ascii=False, default=str)

                    # Upsert to channel_calibration table
                    try:
                        self._upsert_calibration(result["summary"], result["videos"])
                    except Exception as db_err:
                        print(f"  WARNING: DB upsert failed for {ch['title']}: {db_err}")

                except Exception as e:
                    print(f"  ERROR {ch['title']}: {e}")
                    results.append({"summary": {"title": ch["title"], "error": str(e)}, "videos": []})

        self._print_summary(results, start_time)

    # ── Channel fetching (simplified — calibration only needs basic info) ─

    def _fetch_channels(self) -> list[dict]:
        """Fetch curated channels — uses the DB service's full fetch."""
        return self.db.fetch_curated_channels()

    # ── Channel sampling ─────────────────────────────────────────────────

    def _sample_channel(self, channel: dict, max_pages: int) -> dict:
        """Page through a channel's uploads playlist and enrich all videos."""
        channel_id = channel["channel_id"]
        title = channel.get("title", channel_id)
        playlist_id = uploads_playlist_id(channel_id)

        all_videos: list[dict] = []
        page_token = None
        pages_fetched = 0
        total_quota = 0

        while pages_fetched < max_pages:
            params = {
                "part": "snippet",
                "playlistId": playlist_id,
                "maxResults": self.yt.page_size,
                "key": self.yt.api_key,
            }
            if page_token:
                params["pageToken"] = page_token

            data = self.yt.api_get("https://www.googleapis.com/youtube/v3/playlistItems", params)
            total_quota += 1
            pages_fetched += 1

            raw_ids = []
            raw_items = []

            for item in data.get("items", []):
                snippet = item["snippet"]
                vid_id = snippet.get("resourceId", {}).get("videoId")
                if not vid_id:
                    continue
                raw_ids.append(vid_id)
                raw_items.append(
                    {
                        "video_id": vid_id,
                        "title": snippet.get("title", ""),
                        "published_at": snippet.get("publishedAt", ""),
                    }
                )

            if raw_ids:
                details, enrich_calls = self.yt.enrich_videos(raw_ids)
                total_quota += enrich_calls

                for v in raw_items:
                    enriched = {**v, **details.get(v["video_id"], {})}
                    all_videos.append(enriched)

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        # Score all videos
        alpha = channel.get("scoring_alpha", 0.3)
        for v in all_videos:
            v["score"] = round(self.scorer.score_video(v, alpha), 4)

        # Build summary stats
        durations = [v.get("duration_seconds", 0) for v in all_videos if v.get("duration_seconds")]

        summary = {
            "channel_id": channel_id,
            "title": title,
            "custom_url": channel.get("custom_url", ""),
            "sync_mode": channel.get("sync_mode", "sync"),
            "pages_fetched": pages_fetched,
            "playlist_exhausted": page_token is None,
            "quota_used": total_quota,
            "total_videos_sampled": len(all_videos),
            "duration_buckets": self._duration_buckets(all_videos),
            "passing_filter_counts": self._passing_counts(all_videos),
            "duration_stats": {
                "min": min(durations) if durations else 0,
                "max": max(durations) if durations else 0,
                "avg": round(sum(durations) / len(durations), 1) if durations else 0,
                "median": sorted(durations)[len(durations) // 2] if durations else 0,
                "total_hours": round(sum(durations) / 3600, 1) if durations else 0,
            },
        }

        print(f"  {title}: {len(all_videos)} videos sampled, {pages_fetched} pages, {total_quota} quota")
        return {"summary": summary, "videos": all_videos}

    # ── Calibration DB upsert ────────────────────────────────────────────

    def _upsert_calibration(self, summary: dict, videos: list[dict]) -> None:
        """Write calibration data to channel_calibration table."""
        channel_id = summary["channel_id"]
        cadence = self._compute_cadence(videos)
        passing = summary.get("passing_filter_counts", {})

        record = {
            "channel_id": channel_id,
            "calibrated_at": datetime.now(timezone.utc).isoformat(),
            "total_videos_sampled": summary.get("total_videos_sampled", 0),
            "videos_in_date_range": summary.get("total_videos_sampled", 0),
            "posts_per_week": cadence.get("posts_per_week", 0),
            "avg_gap_days": cadence.get("avg_gap_days"),
            "median_gap_days": cadence.get("median_gap_days"),
            "avg_duration_seconds": cadence.get("avg_duration_seconds"),
            "median_duration_seconds": cadence.get("median_duration_seconds"),
            "passing_min60": passing.get("min_60s", 0),
            "passing_min60_max3600": passing.get("min_60s_max_3600s", 0),
            "passing_min300": passing.get("min_300s", 0),
            "passing_min300_max3600": passing.get("min_300s_max_3600s", 0),
            "duration_buckets": summary.get("duration_buckets", {}),
        }
        self.db.client.table("channel_calibration").upsert(record).execute()

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

    # ── Summary output ───────────────────────────────────────────────────

    def _print_summary(self, results: list[dict], start_time: float) -> None:
        summaries = [r["summary"] for r in results if "error" not in r["summary"]]
        total_quota = sum(s.get("quota_used", 0) for s in summaries)
        total_videos = sum(s.get("total_videos_sampled", 0) for s in summaries)

        agg_passing: dict[str, int] = {}
        for s in summaries:
            for k, v in s.get("passing_filter_counts", {}).items():
                agg_passing[k] = agg_passing.get(k, 0) + v

        agg_buckets: dict[str, int] = {}
        for s in summaries:
            for k, v in s.get("duration_buckets", {}).items():
                agg_buckets[k] = agg_buckets.get(k, 0) + v

        per_channel_yield = []
        for s in summaries:
            pc = s.get("passing_filter_counts", {})
            per_channel_yield.append(
                {
                    "title": s["title"],
                    "channel_id": s["channel_id"],
                    "total_sampled": s.get("total_videos_sampled", 0),
                    **pc,
                }
            )
        per_channel_yield.sort(key=lambda x: x.get("min_60s", 0))

        overall = {
            "run_at": datetime.now(timezone.utc).isoformat(),
            "channels_sampled": len(summaries),
            "total_quota_used": total_quota,
            "total_videos_sampled": total_videos,
            "aggregate_duration_buckets": agg_buckets,
            "aggregate_passing_filters": agg_passing,
            "per_channel_yield": per_channel_yield,
            "elapsed_seconds": round(time.time() - start_time, 1),
        }

        with open(self.output_dir / "_summary.json", "w", encoding="utf-8") as f:
            json.dump(overall, f, indent=2, ensure_ascii=False, default=str)

        elapsed = time.time() - start_time
        print(f"\n{'=' * 60}")
        print("CALIBRATION SUMMARY")
        print(f"{'=' * 60}")
        print(f"  Channels: {len(summaries)}")
        print(f"  Total videos sampled: {total_videos}")
        print(f"  Quota used: {total_quota}")
        print("\n  Duration buckets (all videos):")
        for k, v in sorted(agg_buckets.items()):
            print(f"    {k:>12}: {v:>5}")
        print("\n  Filter pass counts:")
        for k, v in sorted(agg_passing.items()):
            print(f"    {k:>25}: {v:>5}")
        print("\n  Channels with fewest qualifying videos (min_60s):")
        for entry in per_channel_yield[:10]:
            print(f"    {entry['title']:>40}: {entry.get('min_60s', 0):>3} / {entry['total_sampled']:>4} sampled")
        print(f"\n  Runtime: {elapsed:.1f}s")
        print(f"  Output: {self.output_dir}")


# ─── CLI entry point ─────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibration run — sample all curated channels")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Max playlist pages per channel (default: 20, at 50/page = 1,000 videos)",
    )
    parser.add_argument("--channel", type=str, default=None, help="Sample a single channel ID only")
    parser.add_argument("--workers", type=int, default=None, help="Concurrent workers (default: from config)")
    args = parser.parse_args()

    # Bootstrap
    config = AppConfig.load()
    config.validate_producer_env()

    from supabase import create_client

    client = create_client(
        config.get_env("NEXT_PUBLIC_SUPABASE_URL"),
        config.get_env("SUPABASE_SECRET_KEY"),
    )

    db = SyncDatabase(client, page_size=config.db["page_size"])
    yt = YouTubeClient(config.get_env("YOUTUBE_API_KEY"), config.api)
    scorer = VideoScorer()

    cmd = CalibrateCommand(config, db, yt, scorer)
    cmd.run(args.max_pages, args.channel, args.workers)
