"""ProcessCommand — video-at-a-time budget loop.

Per-channel process: for each tier (fresh, catalog), download one video
at a time, make inline budget decisions, upload or evict immediately.

Fresh tier: ordered by published_at DESC, evicts oldest.
Catalog tier: ordered by score DESC, evicts lowest-scoring.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

from src.config import AppConfig
from src.models import DownloadResult
from src.services.db import SyncDatabase
from src.services.hls import HlsPipeline
from src.services.storage import R2Storage


class _TierFull(Exception):
    """Raised when a tier is full and no more jobs should be processed."""


def _fmt_mb(b: int) -> str:
    return f"{b / (1024 * 1024):.1f} MB"


def _fmt_gb(b: int) -> str:
    return f"{b / (1024 ** 3):.2f} GB"


def _fmt_pct(part: int, whole: int) -> str:
    if whole == 0:
        return "0%"
    return f"{part / whole * 100:.0f}%"


class ProcessCommand:
    """Processes download jobs from the sync queue with inline budget decisions."""

    def __init__(
        self,
        config: AppConfig,
        db: SyncDatabase,
        storage: R2Storage,
        hls: HlsPipeline,
        staging_dir: Path | None = None,
    ):
        self.config = config
        self.db = db
        self.storage = storage
        self.hls = hls
        self.staging_dir = staging_dir or (config.project_root / "downloads" / "staging")

    def run(
        self,
        limit: int | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Orchestrate per-channel video-at-a-time loops."""
        run_start = time.time()
        totals = {"downloaded": 0, "uploaded": 0, "evicted": 0, "skipped": 0}

        print(f"\n{'=' * 60}")
        print("SYNC PROCESS")
        print(f"{'=' * 60}")
        if dry_run:
            print("[DRY RUN MODE — no side effects]")
        if limit is not None:
            print(f"Download limit: {limit}")

        # Startup recovery: clean up from any previous crashed run
        self._recover_on_startup(dry_run)

        channels = self.db.fetch_curated_channels()
        print(f"Channels to process: {len(channels)}")

        remaining = limit

        for ch_cfg in channels:
            channel_id = ch_cfg["channel_id"]
            stats = self._process_channel(
                channel_id, ch_cfg, dry_run=dry_run, verbose=verbose,
                limit=remaining,
            )
            for key in totals:
                totals[key] += stats.get(key, 0)
            if remaining is not None:
                remaining -= stats.get("downloaded", 0)
                if remaining <= 0:
                    print(f"\nDownload limit ({limit}) reached, stopping.")
                    break

        self._print_summary(totals, run_start)

    # ── Per-channel processing ─────────────────────────────────────────

    def _process_channel(
        self,
        channel_id: str,
        ch_cfg: dict,
        dry_run: bool,
        verbose: bool,
        limit: int | None = None,
    ) -> dict:
        """Process a single channel: run fresh tier loop, then catalog tier loop."""
        budget_gb = ch_cfg.get("storage_budget_gb", 10.0)
        catalog_frac = ch_cfg.get("catalog_fraction", 0.6)
        is_archive = ch_cfg.get("sync_mode") == "archive"
        budget_bytes = int(budget_gb * 1024**3)
        title = ch_cfg.get("title", channel_id)

        stats = {"downloaded": 0, "uploaded": 0, "evicted": 0, "skipped": 0}

        # Channel header
        r2_all = self.db.fetch_existing_r2_with_bytes(channel_id)
        r2_total = sum((v.get("storage_bytes") or 0) for v in r2_all.values())
        mode_label = "archive" if is_archive else "sync"

        print(f"\n{'─' * 60}")
        print(f"Channel: {title} [{mode_label}]")
        print(f"  R2 now: {_fmt_gb(r2_total)} across {len(r2_all)} videos")
        print(f"  Budget: {budget_gb:.1f} GB total")
        if limit is not None:
            print(f"  Download limit remaining: {limit}")

        # Zero budget: skip entirely
        if budget_bytes == 0 and not is_archive:
            print(f"  Skipping: zero budget")
            self.db.delete_channel_pending_jobs(channel_id)
            return stats

        if is_archive:
            stats = self._process_archive(channel_id, dry_run, verbose, limit=limit)
            self.db.delete_channel_pending_jobs(channel_id)
            return stats

        fresh_budget = int(budget_bytes * (1 - catalog_frac))
        catalog_budget = int(budget_bytes * catalog_frac)

        fresh_pool = self._filter_r2_by_tier(r2_all, "fresh")
        catalog_pool = self._filter_r2_by_tier(r2_all, "catalog")
        fresh_used = sum((v.get("storage_bytes") or 0) for v in fresh_pool.values())
        catalog_used = sum((v.get("storage_bytes") or 0) for v in catalog_pool.values())
        unassigned = len(r2_all) - len(fresh_pool) - len(catalog_pool)

        print(f"  Fresh tier:   {_fmt_gb(fresh_used)} / {_fmt_gb(fresh_budget)} "
              f"({_fmt_pct(fresh_used, fresh_budget)}) — {len(fresh_pool)} videos")
        print(f"  Catalog tier: {_fmt_gb(catalog_used)} / {_fmt_gb(catalog_budget)} "
              f"({_fmt_pct(catalog_used, catalog_budget)}) — {len(catalog_pool)} videos")
        if unassigned > 0:
            print(f"  Unassigned:   {unassigned} videos (no sync_tier set)")

        # Fresh tier
        print(f"\n  --- Fresh tier (newest first, budget {_fmt_gb(fresh_budget)}) ---")
        fresh_stats = self._process_tier(
            channel_id, fresh_budget, tier="fresh", sort_key="published_at",
            dry_run=dry_run, verbose=verbose, limit=limit,
        )

        # Catalog tier — subtract fresh downloads from remaining limit
        catalog_limit = None
        if limit is not None:
            catalog_limit = limit - fresh_stats.get("downloaded", 0)
            if catalog_limit <= 0:
                catalog_limit = 0

        print(f"\n  --- Catalog tier (best score first, budget {_fmt_gb(catalog_budget)}) ---")
        catalog_stats = self._process_tier(
            channel_id, catalog_budget, tier="catalog", sort_key="score",
            dry_run=dry_run, verbose=verbose, limit=catalog_limit,
        )

        for key in stats:
            stats[key] = fresh_stats.get(key, 0) + catalog_stats.get(key, 0)

        # Channel summary
        print(f"\n  Channel done: "
              f"+{stats['uploaded']} uploaded, "
              f"-{stats['evicted']} evicted, "
              f"{stats['skipped']} skipped, "
              f"{stats['downloaded']} downloaded")

        # Wipe remaining pending jobs
        self.db.delete_channel_pending_jobs(channel_id)
        return stats

    def _process_archive(
        self, channel_id: str, dry_run: bool, verbose: bool,
        limit: int | None = None,
    ) -> dict:
        """Archive mode: download and upload everything, no budget checks."""
        max_attempts = self.config.consumer["max_attempts"]
        stats = {"downloaded": 0, "uploaded": 0, "evicted": 0, "skipped": 0}

        while True:
            if limit is not None and stats["downloaded"] >= limit:
                break

            job = self.db.claim_next_pending_job(channel_id, max_attempts)
            if job is None:
                break

            dl = self._download_one(job, max_attempts, dry_run, verbose)
            if dl is None:
                continue

            stats["downloaded"] += 1
            if self._upload_one(dl, "archive", dry_run, verbose):
                stats["uploaded"] += 1

        return stats

    def _process_tier(
        self,
        channel_id: str,
        tier_budget_bytes: int,
        tier: str,
        sort_key: str,
        dry_run: bool,
        verbose: bool,
        limit: int | None = None,
    ) -> dict:
        """Generic tier loop: download one at a time, inline budget decisions.

        For fresh tier: sort_key='published_at', compare against oldest date, evict oldest.
        For catalog tier: sort_key='score', compare against lowest score, evict lowest.
        """
        max_attempts = self.config.consumer["max_attempts"]
        stats = {"downloaded": 0, "uploaded": 0, "evicted": 0, "skipped": 0}
        order_label = "newest first" if sort_key == "published_at" else "best score first"
        value_label = "date" if sort_key == "published_at" else "score"

        consecutive_errors = 0
        max_consecutive_errors = 3

        while True:
            if limit is not None and stats["downloaded"] >= limit:
                print(f"  Download limit reached for this tier.")
                break

            if consecutive_errors >= max_consecutive_errors:
                print(f"  ERROR: {max_consecutive_errors} consecutive failures, stopping {tier} tier")
                break

            try:
                job = self.db.claim_next_pending_job(
                    channel_id, max_attempts, sort_key,
                )
            except Exception as e:
                print(f"  ERROR claiming job: {e}")
                consecutive_errors += 1
                continue

            if job is None:
                print(f"  No more pending jobs for {tier} tier.")
                break

            try:
                self._process_one_job(
                    job, channel_id, tier, tier_budget_bytes, sort_key,
                    value_label, max_attempts, stats, dry_run, verbose,
                )
                consecutive_errors = 0
            except _TierFull:
                break
            except Exception as e:
                print(f"  ERROR processing {job.get('video_id', '?')}: {e}")
                consecutive_errors += 1

        print(f"  Tier {tier} done: +{stats['uploaded']} uploaded, "
              f"-{stats['evicted']} evicted, {stats['skipped']} skipped")
        return stats

    def _process_one_job(
        self,
        job: dict,
        channel_id: str,
        tier: str,
        tier_budget_bytes: int,
        sort_key: str,
        value_label: str,
        max_attempts: int,
        stats: dict,
        dry_run: bool,
        verbose: bool,
    ) -> None:
        """Process a single job within a tier loop. Raises _TierFull to stop the tier."""
        # Re-query R2 state each iteration
        r2_all = self.db.fetch_existing_r2_with_bytes(channel_id)
        r2_pool = self._filter_r2_by_tier(r2_all, tier)
        total_bytes = sum((v.get("storage_bytes") or 0) for v in r2_pool.values())
        headroom = tier_budget_bytes - total_bytes

        # Download
        dl = self._download_one(job, max_attempts, dry_run, verbose)
        if dl is None:
            return  # download failed, retry logic handled inside

        stats["downloaded"] += 1
        job_bytes = dl.storage_bytes

        # Budget context for this decision
        print(f"  Budget check: {_fmt_gb(total_bytes)} used + {_fmt_mb(job_bytes)} new "
              f"/ {_fmt_gb(tier_budget_bytes)} budget "
              f"({_fmt_gb(headroom)} headroom)")

        # Oversized: single video exceeds entire tier budget
        if job_bytes > tier_budget_bytes:
            print(f"  SKIP: {_fmt_mb(job_bytes)} exceeds entire {tier} budget of {_fmt_gb(tier_budget_bytes)}")
            self._cleanup_staging(dl.staging_dir)
            self.db.complete_job(dl.job_id)
            stats["skipped"] += 1
            return

        fits = total_bytes + job_bytes <= tier_budget_bytes

        if sort_key == "published_at":
            threshold = min(
                (v["published_at"] for v in r2_pool.values() if v.get("published_at")),
                default=None,
            )
            job_value = dl.published_at
            is_better = threshold is None or job_value > threshold
            if threshold:
                if fits:
                    verdict = "newer, fits" if is_better else "older, fits"
                else:
                    verdict = "newer, will evict oldest" if is_better else "older, no room"
                print(f"  Comparison: this {value_label}={job_value} vs oldest in R2={threshold}"
                      f" -> {verdict}")
            else:
                print(f"  Comparison: no existing {tier} videos in R2, fits automatically")
        else:
            threshold = min(
                (v["score"] for v in r2_pool.values()),
                default=None,
            )
            job_value = dl.score
            is_better = threshold is None or job_value > threshold
            if threshold is not None:
                if fits:
                    verdict = "better, fits" if is_better else "worse, fits"
                else:
                    verdict = "better, will evict lowest" if is_better else "worse, no room"
                print(f"  Comparison: this {value_label}={job_value:.2f} vs lowest in R2={threshold:.2f}"
                      f" -> {verdict}")
            else:
                print(f"  Comparison: no existing {tier} videos in R2, fits automatically")

        if is_better:
            # Newer/higher-scoring — evict enough to fit
            while total_bytes + job_bytes > tier_budget_bytes:
                victim_id = self._find_eviction_victim(r2_pool, sort_key)
                if victim_id is None:
                    print(f"  No more victims to evict, still over budget")
                    break
                victim = r2_pool.pop(victim_id)
                victim_bytes = victim.get("storage_bytes") or 0
                victim_val = victim.get("published_at", "") if sort_key == "published_at" else victim.get("score", 0)
                print(f"  Evicting {victim_id} ({_fmt_mb(victim_bytes)}, {value_label}={victim_val}) to make room")
                if self._evict_one(victim_id, victim, dry_run, verbose):
                    total_bytes -= victim_bytes
                    stats["evicted"] += 1

            if total_bytes + job_bytes <= tier_budget_bytes:
                if self._upload_one(dl, tier, dry_run, verbose):
                    stats["uploaded"] += 1
            else:
                print(f"  SKIP: could not free enough space after evictions")
                self._cleanup_staging(dl.staging_dir)
                self.db.complete_job(dl.job_id)
                stats["skipped"] += 1
        else:
            # Older/lower-scoring — only if room without evicting
            if total_bytes + job_bytes <= tier_budget_bytes:
                print(f"  Fits within budget without eviction")
                if self._upload_one(dl, tier, dry_run, verbose):
                    stats["uploaded"] += 1
            else:
                print(f"  STOP: no room and not worth evicting for — tier full")
                self._cleanup_staging(dl.staging_dir)
                self.db.complete_job(dl.job_id)
                stats["skipped"] += 1
                raise _TierFull()

    @staticmethod
    def _filter_r2_by_tier(r2_all: dict[str, dict], tier: str) -> dict[str, dict]:
        """Split R2 state by sync_tier column."""
        return {
            vid: data for vid, data in r2_all.items()
            if data.get("sync_tier") == tier
        }

    @staticmethod
    def _find_eviction_victim(r2_pool: dict[str, dict], sort_key: str) -> str | None:
        """Find the worst candidate to evict from the pool.

        For fresh (sort_key='published_at'): evict oldest (min published_at).
        For catalog (sort_key='score'): evict lowest-scoring (min score).
        """
        if not r2_pool:
            return None

        if sort_key == "published_at":
            return min(r2_pool, key=lambda vid: r2_pool[vid].get("published_at") or "")
        else:
            return min(r2_pool, key=lambda vid: r2_pool[vid].get("score") or 0)

    # ── Single-video operations ────────────────────────────────────────

    def _download_one(
        self,
        job: dict,
        max_attempts: int,
        dry_run: bool,
        verbose: bool,
    ) -> DownloadResult | None:
        """Download, remux, and measure a single video. Returns None on failure."""
        video_id = job["video_id"]
        channel_id = job["channel_id"]
        metadata = job.get("metadata") or {}
        title = metadata.get("title", video_id)
        score = job.get("score", 0.0)
        attempts = job.get("attempts", 0)

        published_at = metadata.get("published_at", "")
        pub_short = published_at[:10] if published_at else "?"

        if dry_run:
            print(f"  [DRY RUN] Would download {video_id} \"{title}\"")
            return DownloadResult(
                video_id=video_id,
                channel_id=channel_id,
                score=float(score),
                storage_bytes=0,
                staging_dir=self.staging_dir / video_id,
                job_id=job["id"],
                published_at=published_at,
                info_data=metadata,
            )

        print(f"\n  [{video_id}] \"{title}\"")
        print(f"  published={pub_short}  score={score:.2f}  attempt={attempts + 1}/{max_attempts}")

        job_staging = self.staging_dir / video_id
        job_staging.mkdir(parents=True, exist_ok=True)

        try:
            min_tiers = self.hls.hls_cfg.get("min_tiers", 1)

            completed_tiers, sidecar_files = self.hls.download_video_tiers(
                video_id, job_staging, verbose,
            )

            if len(completed_tiers) < min_tiers:
                raise RuntimeError(
                    f"Only {len(completed_tiers)} tier(s), minimum {min_tiers} required"
                )

            info_data = {}
            if "info_json" in sidecar_files:
                parsed = self.hls.parse_info_json(sidecar_files["info_json"])
                if parsed:
                    info_data = parsed

            remuxed_tiers = self.hls.remux_to_hls(completed_tiers, job_staging, verbose)

            if len(remuxed_tiers) < min_tiers:
                raise RuntimeError(
                    f"Only {len(remuxed_tiers)} tier(s) remuxed, minimum {min_tiers} required"
                )

            storage_bytes = self._measure_staging_bytes(job_staging)

            self.db.update_job_storage_bytes(job["id"], storage_bytes)
            self.db.update_job_status(job["id"], "downloaded")

            sidecar_paths = [
                path for path in sidecar_files.values() if isinstance(path, Path)
            ]

            result = DownloadResult(
                video_id=video_id,
                channel_id=channel_id,
                score=float(score),
                storage_bytes=storage_bytes,
                staging_dir=job_staging,
                job_id=job["id"],
                published_at=published_at or info_data.get("published_at", ""),
                info_data=info_data,
                remuxed_tiers=remuxed_tiers,
                sidecar_files=sidecar_paths,
            )

            mb = storage_bytes / (1024 * 1024)
            print(f"  Downloaded: {video_id} ({mb:.1f} MB total)")

            return result

        except Exception as e:
            new_attempts = attempts + 1
            print(f"  FAILED: {video_id} (attempt {new_attempts}/{max_attempts}): {e}")
            try:
                if new_attempts >= max_attempts:
                    self.db.mark_job_failed_permanent(job["id"], str(e))
                    print(f"  Marked permanently failed.")
                else:
                    self.db.fail_job(job["id"], str(e))
            except Exception as db_err:
                print(f"  WARNING: could not update job status in DB: {db_err}")

            self._cleanup_staging(job_staging)
            return None

    def _upload_one(
        self,
        dl: DownloadResult,
        sync_tier: str,
        dry_run: bool,
        verbose: bool,
    ) -> bool:
        """Upload a single video to R2, record in DB, complete job."""
        if dry_run:
            print(f"  [DRY RUN] Would upload {dl.video_id}")
            return True

        print(f"  Uploading {dl.video_id} to R2...", end="", flush=True)

        try:
            playlist_tiers = []
            for tier in dl.remuxed_tiers:
                meta = self.hls.extract_tier_metadata(tier)
                playlist_tiers.append({
                    "label": tier["label"],
                    "bandwidth": meta["bandwidth"],
                    "resolution": meta["resolution"],
                    "codecs": meta["codecs"],
                })

            master_content = self.hls.generate_master_playlist(playlist_tiers)

            channel_handle = dl.info_data.get("handle", "unknown")
            if channel_handle == "unknown":
                channel_handle = self.db.resolve_channel_handle({
                    "channel_id": dl.channel_id,
                    "metadata": dl.info_data,
                })

            published_at = dl.info_data.get("published_at")

            sidecar_dict: dict[str, Path] = {}
            for p in dl.sidecar_files:
                if isinstance(p, Path) and p.exists():
                    if p.suffix in (".jpg", ".jpeg", ".webp", ".png"):
                        sidecar_dict["thumbnail"] = p
                    elif p.suffix == ".vtt":
                        sidecar_dict["subtitle"] = p

            r2_keys = self.storage.upload_hls_package(
                dl.staging_dir, dl.remuxed_tiers, sidecar_dict,
                channel_handle, published_at, dl.video_id,
                master_content, verbose,
            )

            self.db.upsert_video_record(
                dl.video_id, dl.channel_id, dl.info_data,
                r2_keys,
            )

            self.db.update_video_sync_result(
                dl.video_id, dl.storage_bytes, sync_tier, dl.score,
            )

            self.db.complete_job(dl.job_id)

            mb = dl.storage_bytes / (1024 * 1024)
            print(f" OK ({mb:.1f} MB, {sync_tier})")

            return True

        except Exception as e:
            print(f" FAILED")
            print(f"  FAILED uploading {dl.video_id}: {e}")
            try:
                self.db.fail_job(dl.job_id, str(e))
            except Exception as db_err:
                print(f"  WARNING: could not mark job failed in DB: {db_err}")
            return False

        finally:
            self._cleanup_staging(dl.staging_dir)

    def _evict_one(
        self,
        video_id: str,
        r2_data: dict,
        dry_run: bool,
        verbose: bool,
    ) -> bool:
        """Delete a single video from R2 and DB."""
        if dry_run:
            print(f"  [DRY RUN] Would evict {video_id}")
            return True

        print(f"    Deleting {video_id} from R2...", end="", flush=True)
        try:
            success, error = self.storage.delete_video_objects(r2_data)
            if not success:
                raise RuntimeError(f"R2 deletion failed: {error}")
            self.db.delete_video_record(video_id)
            print(f" done")
            return True
        except Exception as e:
            print(f" FAILED: {e}")
            return False

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _measure_staging_bytes(staging_dir: Path) -> int:
        """Sum all file sizes in a staging directory."""
        total = 0
        if staging_dir.exists():
            for f in staging_dir.rglob("*"):
                if f.is_file():
                    total += f.stat().st_size
        return total

    @staticmethod
    def _cleanup_staging(staging_dir: Path) -> None:
        """Remove a staging directory."""
        try:
            if staging_dir.exists():
                shutil.rmtree(staging_dir)
        except OSError as e:
            print(f"  Warning: staging cleanup failed: {e}")

    def _recover_on_startup(self, dry_run: bool) -> None:
        """Reset stale jobs and wipe staging from any previous crashed run."""
        # Reset processing/downloaded jobs back to pending
        if not dry_run:
            recovered = self.db.reset_incomplete_jobs()
            if recovered:
                print(f"Recovery: reset {recovered} stale job(s) back to pending")

        # Wipe staging directory
        if self.staging_dir.exists():
            orphans = list(self.staging_dir.iterdir())
            if orphans:
                print(f"Recovery: cleaning {len(orphans)} orphaned staging dir(s)")
                for d in orphans:
                    try:
                        if d.is_dir():
                            shutil.rmtree(d)
                        else:
                            d.unlink()
                    except OSError as e:
                        print(f"  Warning: could not remove {d.name}: {e}")

    @staticmethod
    def _print_summary(totals: dict, start_time: float) -> None:
        duration = time.time() - start_time
        minutes = int(duration // 60)
        seconds = int(duration % 60)

        print(f"\n{'=' * 40}")
        print("SYNC PROCESS SUMMARY")
        print(f"{'=' * 40}")
        print(f"  Downloaded: {totals['downloaded']}")
        print(f"  Uploaded:   {totals['uploaded']}")
        print(f"  Evicted:    {totals['evicted']}")
        print(f"  Skipped:    {totals['skipped']}")
        print(f"  Duration:   {minutes}m {seconds}s")


# ─── CLI entry point ─────────────────────────────────────────────────────────


def _check_ffmpeg() -> None:
    """Verify ffmpeg and ffprobe are on PATH."""
    for tool in ("ffmpeg", "ffprobe"):
        try:
            result = subprocess.run([tool, "-version"], capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                print(f"Error: {tool} found but returned non-zero exit code.")
                print("  Install ffmpeg: https://ffmpeg.org/download.html")
                sys.exit(2)
        except FileNotFoundError:
            print(f"Error: {tool} not found on PATH.")
            print(f"  {tool} is required for HLS pipeline (remux + codec probing).")
            print("  Install ffmpeg (includes ffprobe): https://ffmpeg.org/download.html")
            sys.exit(2)
        except subprocess.TimeoutExpired:
            print(f"Error: {tool} timed out during version check.")
            sys.exit(2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process sync queue — per-channel video-at-a-time budget loop"
    )
    parser.add_argument("--limit", type=int, default=None, help="Cap download jobs this run")
    parser.add_argument("--dry-run", action="store_true", help="Preview without side effects")
    parser.add_argument("--verbose", action="store_true", help="Detailed per-job output")
    args = parser.parse_args()

    config = AppConfig.load()
    config.validate_consumer_env()
    _check_ffmpeg()

    from supabase import create_client

    client = create_client(
        config.get_env("NEXT_PUBLIC_SUPABASE_URL"),
        config.get_env("SUPABASE_SECRET_KEY"),
    )

    db = SyncDatabase(client)
    storage = R2Storage.from_env()
    cookies_file = config.project_root / "config" / "cookies.txt"
    hls = HlsPipeline(config._consumer, cookies_file)

    cmd = ProcessCommand(config, db, storage, hls)
    cmd.run(args.limit, args.dry_run, args.verbose)
