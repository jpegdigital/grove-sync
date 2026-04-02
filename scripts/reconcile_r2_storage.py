"""Backfill storage_bytes on video records from actual R2 object sizes.

Lists objects under each channel's R2 prefix, sums bytes per video_id folder,
and updates the videos table where storage_bytes is NULL or differs.

Usage:
    uv run python scripts/reconcile_r2_storage.py
    uv run python scripts/reconcile_r2_storage.py --channel @handle
    uv run python scripts/reconcile_r2_storage.py --dry-run --verbose
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import AppConfig
from services.storage import R2Storage


def _build_supabase_client() -> object:
    from supabase import create_client

    url = AppConfig.get_env("NEXT_PUBLIC_SUPABASE_URL")
    key = AppConfig.get_env("SUPABASE_SECRET_KEY")
    return create_client(url, key)


def _fetch_channels(supabase) -> list[dict]:
    """Fetch curated channels with their handle (custom_url)."""
    resp = (
        supabase.table("curated_channels")
        .select("channel_id, channels(custom_url)")
        .execute()
    )
    results = []
    for row in resp.data or []:
        ch = row.get("channels")
        if not ch or not ch.get("custom_url"):
            continue
        handle = ch["custom_url"].lstrip("@")
        results.append({"channel_id": row["channel_id"], "handle": handle})
    return results


def _fetch_video_rows(supabase, channel_id: str) -> dict[str, dict]:
    """Fetch R2-synced videos for a channel: youtube_id -> {storage_bytes, media_path}."""
    results: dict[str, dict] = {}
    offset = 0
    page_size = 1000

    while True:
        resp = (
            supabase.table("videos")
            .select("youtube_id, storage_bytes, media_path")
            .eq("channel_id", channel_id)
            .filter("r2_synced_at", "not.is", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = resp.data or []
        for row in rows:
            results[row["youtube_id"]] = {
                "storage_bytes": row.get("storage_bytes"),
                "media_path": row.get("media_path"),
            }
        if len(rows) < page_size:
            break
        offset += page_size

    return results


def _list_r2_sizes_by_video(storage: R2Storage, prefix: str) -> dict[str, int]:
    """List all objects under prefix and sum sizes per video_id folder.

    R2 key structure: {handle}/{YYYY-MM}/{video_id}/{file}
    Returns: {video_id: total_bytes}
    """
    sizes: dict[str, int] = defaultdict(int)
    paginator = storage.client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            # Expected: handle / YYYY-MM / video_id / file
            if len(parts) >= 4:
                video_id = parts[2]
                sizes[video_id] += obj["Size"]

    return dict(sizes)


def reconcile_channel(
    supabase,
    storage: R2Storage,
    channel_id: str,
    handle: str,
    dry_run: bool,
    verbose: bool,
) -> dict[str, int]:
    """Reconcile one channel. Returns {updated, skipped, missing_in_r2, matched}."""
    stats = {"updated": 0, "skipped": 0, "missing_in_r2": 0, "matched": 0}

    video_rows = _fetch_video_rows(supabase, channel_id)
    if not video_rows:
        if verbose:
            print(f"  No R2-synced videos in DB for {handle}")
        return stats

    r2_prefix = f"{handle}/"
    r2_sizes = _list_r2_sizes_by_video(storage, r2_prefix)

    for video_id, row in video_rows.items():
        r2_bytes = r2_sizes.get(video_id)

        if r2_bytes is None:
            stats["missing_in_r2"] += 1
            if verbose:
                print(f"  {video_id}: not found in R2 under {r2_prefix}")
            continue

        db_bytes = row["storage_bytes"]

        if db_bytes == r2_bytes:
            stats["matched"] += 1
            continue

        old_label = f"{db_bytes:,}" if db_bytes is not None else "NULL"
        new_label = f"{r2_bytes:,}"

        if dry_run:
            print(f"  [DRY RUN] {video_id}: {old_label} -> {new_label} bytes")
        else:
            supabase.table("videos").update(
                {"storage_bytes": r2_bytes}
            ).eq("youtube_id", video_id).execute()
            if verbose:
                print(f"  {video_id}: {old_label} -> {new_label} bytes")

        stats["updated"] += 1

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill storage_bytes from R2 object sizes"
    )
    parser.add_argument(
        "--channel",
        help="Single channel handle (e.g. @handle) to reconcile",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    parser.add_argument("--verbose", action="store_true", help="Show per-video details")
    args = parser.parse_args()

    config = AppConfig.load()
    config.validate_consumer_env()

    supabase = _build_supabase_client()
    storage = R2Storage.from_env()

    channels = _fetch_channels(supabase)

    if args.channel:
        target = args.channel.lstrip("@")
        channels = [c for c in channels if c["handle"] == target]
        if not channels:
            print(f"Channel @{target} not found in curated_channels")
            sys.exit(1)

    totals = {"updated": 0, "skipped": 0, "missing_in_r2": 0, "matched": 0}

    for ch in channels:
        print(f"\n@{ch['handle']} ({ch['channel_id']})")
        stats = reconcile_channel(
            supabase, storage, ch["channel_id"], ch["handle"],
            args.dry_run, args.verbose,
        )
        for k in totals:
            totals[k] += stats[k]
        print(
            f"  matched={stats['matched']}  updated={stats['updated']}  "
            f"missing_in_r2={stats['missing_in_r2']}"
        )

    print(f"\nTotal: {totals['updated']} updated, {totals['matched']} matched, "
          f"{totals['missing_in_r2']} missing in R2")
    if args.dry_run:
        print("(dry run — no changes written)")


if __name__ == "__main__":
    main()
