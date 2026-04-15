"""Supabase database operations for sync queue and video metadata."""

from __future__ import annotations

from datetime import datetime, timezone


class SyncDatabase:
    """All Supabase operations for producer and consumer.

    Wraps the Supabase client and provides typed methods for each operation.
    The client is injected via constructor for testability.
    """

    def __init__(self, client, page_size: int = 1000, enqueue_batch_size: int = 100):
        self.client = client
        self.page_size = page_size
        self.enqueue_batch_size = enqueue_batch_size

    # ── Producer: channel fetching ───────────────────────────────────────

    def fetch_curated_channels(self) -> list[dict]:
        """Fetch all curated channels with sync config and calibration data."""
        resp = (
            self.client.table("curated_channels")
            .select(
                "id, channel_id, sync_mode, date_range_override, "
                "storage_budget_gb, catalog_fraction, scoring_alpha, "
                "min_duration_seconds, max_duration_seconds, "
                "last_full_refresh_at, "
                "channels(youtube_id, title, custom_url, subscriber_count)"
            )
            .order("display_order")
            .execute()
        )

        cal_resp = self.client.table("channel_calibration").select("channel_id, median_gap_days").execute()
        cal_map = {r["channel_id"]: r for r in (cal_resp.data or [])}

        results = []
        for row in resp.data or []:
            ch = row.get("channels")
            if not ch:
                continue
            cal = cal_map.get(row["channel_id"], {})
            results.append(
                {
                    "curated_id": row["id"],
                    "channel_id": row["channel_id"],
                    "title": ch.get("title", ""),
                    "custom_url": ch.get("custom_url", ""),
                    "subscriber_count": ch.get("subscriber_count", 0),
                    "sync_mode": row.get("sync_mode", "sync"),
                    "date_range_override": row.get("date_range_override"),
                    "last_full_refresh_at": row.get("last_full_refresh_at"),
                    "storage_budget_gb": float(row.get("storage_budget_gb", 10)),
                    "catalog_fraction": float(row.get("catalog_fraction", 0.6)),
                    "scoring_alpha": float(row.get("scoring_alpha", 0.3)),
                    "min_duration_seconds": row.get("min_duration_seconds", 60),
                    "max_duration_seconds": row.get("max_duration_seconds", 3600),
                    "median_gap_days": cal.get("median_gap_days"),
                }
            )
        return results

    def fetch_existing_videos(self, channel_id: str, tier: str | None = None) -> dict[str, dict]:
        """Fetch R2-synced video rows for a channel, optionally filtered by tier."""
        results: dict[str, dict] = {}
        offset = 0

        while True:
            query = (
                self.client.table("videos")
                .select("youtube_id, media_path, thumbnail_path, subtitle_path, title, duration_seconds")
                .eq("channel_id", channel_id)
                .filter("r2_synced_at", "not.is", "null")
            )
            if tier:
                query = query.eq("sync_tier", tier)
            resp = query.range(offset, offset + self.page_size - 1).execute()
            rows = resp.data or []
            for row in rows:
                results[row["youtube_id"]] = {
                    "media_path": row.get("media_path"),
                    "thumbnail_path": row.get("thumbnail_path"),
                    "subtitle_path": row.get("subtitle_path"),
                    "title": row.get("title", ""),
                    "duration_seconds": row.get("duration_seconds", 0),
                }
            if len(rows) < self.page_size:
                break
            offset += self.page_size

        return results

    # ── Producer: queue management ───────────────────────────────────────

    def update_video_tier(self, video_id: str, sync_tier: str, dry_run: bool = False) -> None:
        """Update sync_tier on a video."""
        if dry_run:
            return
        self.client.table("videos").update({"sync_tier": sync_tier}).eq(
            "youtube_id", video_id
        ).execute()

    def update_video_scores(self, scores_by_video_id: dict[str, float], dry_run: bool = False) -> None:
        """Update scores on existing video rows."""
        if dry_run or not scores_by_video_id:
            return

        for video_id, score in scores_by_video_id.items():
            self.client.table("videos").update(
                {"score": score}
            ).eq("youtube_id", video_id).execute()

    def update_full_refresh_timestamp(self, curated_ids: list[str], dry_run: bool = False) -> None:
        """Stamp last_full_refresh_at = NOW() on channels that just got a full run."""
        if dry_run or not curated_ids:
            return
        self.client.table("curated_channels").update(
            {"last_full_refresh_at": datetime.now(timezone.utc).isoformat()}
        ).in_("id", curated_ids).execute()

    # ── Producer: calibration ─────────────────────────────────────────────

    def upsert_channel_calibration(
        self,
        channel_id: str,
        total_videos_sampled: int,
        cadence: dict,
        passing: dict,
        duration_buckets: dict,
    ) -> None:
        """Write calibration data to channel_calibration table.

        Caller computes cadence and filter counts; this method only does the
        DB write.
        """
        record = {
            "channel_id": channel_id,
            "calibrated_at": datetime.now(timezone.utc).isoformat(),
            "total_videos_sampled": total_videos_sampled,
            "videos_in_date_range": total_videos_sampled,
            "posts_per_week": cadence.get("posts_per_week", 0),
            "avg_gap_days": cadence.get("avg_gap_days"),
            "median_gap_days": cadence.get("median_gap_days"),
            "avg_duration_seconds": cadence.get("avg_duration_seconds"),
            "median_duration_seconds": cadence.get("median_duration_seconds"),
            "passing_min60": passing.get("min_60s", 0),
            "passing_min60_max3600": passing.get("min_60s_max_3600s", 0),
            "passing_min300": passing.get("min_300s", 0),
            "passing_min300_max3600": passing.get("min_300s_max_3600s", 0),
            "duration_buckets": duration_buckets,
        }
        self.client.table("channel_calibration").upsert(record).execute()

    # ── Producer: unified pipeline ────────────────────────────────────────

    def replace_channel_jobs(
        self,
        channel_id: str,
        jobs: list[dict],
        dry_run: bool = False,
    ) -> None:
        """Replace pending jobs for a channel without disrupting in-flight work.

        Failed jobs are preserved for operator review. In-flight jobs are also
        preserved, and fresh pending jobs are not inserted for those video IDs.
        """
        if dry_run:
            for j in jobs:
                print(f"    [DRY RUN] Would enqueue: {j['video_id']}")
            return

        # Preserve in-flight work and avoid re-enqueueing duplicates for it.
        in_flight_resp = (
            self.client.table("sync_queue")
            .select("video_id")
            .eq("channel_id", channel_id)
            .neq("status", "pending")
            .neq("status", "failed")
            .execute()
        )
        in_flight_video_ids = {
            row["video_id"] for row in (in_flight_resp.data or [])
            if row.get("video_id")
        }

        # Re-plan only replaces pending jobs for the channel.
        self.client.table("sync_queue").delete().eq(
            "channel_id", channel_id
        ).eq("status", "pending").execute()

        filtered_jobs = [
            j for j in jobs
            if j["video_id"] not in in_flight_video_ids
        ]

        if filtered_jobs:
            payload = [
                {
                    "video_id": j["video_id"],
                    "channel_id": j["channel_id"],
                    "metadata": j.get("metadata", {}),
                    "score": j.get("score"),
                    "published_at": j.get("published_at"),
                    "status": "pending",
                }
                for j in filtered_jobs
            ]
            for i in range(0, len(payload), self.enqueue_batch_size):
                batch = payload[i : i + self.enqueue_batch_size]
                self.client.table("sync_queue").insert(batch).execute()

    def fetch_existing_r2_with_bytes(self, channel_id: str) -> dict[str, dict]:
        """Fetch R2-synced videos with storage_bytes for budget math.

        Returns dict of video_id -> {storage_bytes, published_at, score,
        sync_tier, duration_seconds, media_path}.
        """
        results: dict[str, dict] = {}
        offset = 0

        while True:
            resp = (
                self.client.table("videos")
                .select(
                    "youtube_id, storage_bytes, sync_tier, duration_seconds,"
                    " published_at, media_path, score"
                )
                .eq("channel_id", channel_id)
                .filter("r2_synced_at", "not.is", "null")
                .range(offset, offset + self.page_size - 1)
                .execute()
            )
            rows = resp.data or []
            for row in rows:
                results[row["youtube_id"]] = {
                    "storage_bytes": row.get("storage_bytes"),
                    "published_at": row.get("published_at") or "",
                    "score": row.get("score") or 0,
                    "sync_tier": row.get("sync_tier") or "",
                    "duration_seconds": row.get("duration_seconds", 0),
                    "media_path": row.get("media_path") or "",
                }
            if len(rows) < self.page_size:
                break
            offset += self.page_size

        return results

    # ── Consumer: startup recovery ────────────────────────────────────────

    def reset_incomplete_jobs(self) -> int:
        """Reset all processing/downloaded jobs back to pending.

        Safe when there is a single job runner — anything in-flight on
        startup is leftover from a previous crashed run.
        """
        count = 0
        for status in ("processing", "downloaded"):
            resp = (
                self.client.table("sync_queue")
                .select("id")
                .eq("status", status)
                .execute()
            )
            rows = resp.data or []
            if rows:
                job_ids = [r["id"] for r in rows]
                self.client.table("sync_queue").update(
                    {"status": "pending", "started_at": None}
                ).in_("id", job_ids).execute()
                count += len(rows)
        return count

    # ── Consumer: queue operations ───────────────────────────────────────

    def fail_job(self, job_id: str, error_message: str) -> None:
        """Mark a job as failed: reset to pending, increment attempts, record error.

        Uses an atomic Postgres function to avoid read-then-write races on the
        attempts counter.
        """
        self.client.rpc("fail_job_atomic", {
            "p_job_id": job_id,
            "p_error": error_message,
        }).execute()

    def complete_job(self, job_id: str) -> None:
        """Delete a completed job from the queue."""
        self.client.table("sync_queue").delete().eq("id", job_id).execute()

    def update_job_storage_bytes(self, job_id: str, storage_bytes: int) -> None:
        """Set storage_bytes on a job after download+measure."""
        self.client.table("sync_queue").update(
            {"storage_bytes": storage_bytes}
        ).eq("id", job_id).execute()

    def update_job_status(self, job_id: str, status: str) -> None:
        """Transition a job to a new status."""
        self.client.table("sync_queue").update(
            {"status": status}
        ).eq("id", job_id).execute()

    def mark_job_failed_permanent(self, job_id: str, error: str) -> None:
        """Mark a job as permanently failed (max attempts exceeded)."""
        self.client.table("sync_queue").update(
            {"status": "failed", "error": error[:1000]}
        ).eq("id", job_id).execute()

    def claim_next_pending_job(
        self,
        channel_id: str,
        max_attempts: int,
        sort_key: str = "published_at",
    ) -> dict | None:
        """Atomically claim a single pending job for a channel.

        Uses a Postgres function with FOR UPDATE SKIP LOCKED to guarantee
        exactly-once delivery even with concurrent consumers.

        Args:
            channel_id: Channel to claim from.
            max_attempts: Skip jobs with attempts >= this.
            sort_key: 'score' for catalog (highest first),
                      'published_at' for fresh (newest first).

        Returns the claimed job dict, or None if no eligible jobs remain.
        """
        resp = self.client.rpc("claim_next_job", {
            "p_channel_id": channel_id,
            "p_max_attempts": max_attempts,
            "p_sort_key": sort_key,
        }).execute()

        rows = resp.data or []
        if not rows:
            return None
        return rows[0]

    def clear_sync_queue(self) -> int:
        """Delete all jobs from the sync queue regardless of status."""
        resp = (
            self.client.table("sync_queue")
            .select("id", count="exact")
            .execute()
        )
        count = resp.count or 0
        if count > 0:
            self.client.table("sync_queue").delete().gte("created_at", "1970-01-01").execute()
        return count

    def delete_channel_pending_jobs(self, channel_id: str) -> None:
        """Delete all remaining pending jobs for a channel."""
        self.client.table("sync_queue").delete().eq(
            "channel_id", channel_id
        ).eq("status", "pending").execute()

    # ── Consumer: video records ──────────────────────────────────────────

    def upsert_video_record(
        self,
        video_id: str,
        channel_id: str,
        info_data: dict,
        r2_keys: dict[str, str],
    ) -> None:
        """Build row from info.json data + R2 paths, upsert on youtube_id conflict."""
        now_iso = datetime.now(timezone.utc).isoformat()

        row = {
            "youtube_id": video_id,
            "channel_id": channel_id,
            "title": info_data.get("title", "Untitled"),
            "description": info_data.get("description", ""),
            "thumbnail_url": info_data.get("thumbnail_url", ""),
            "published_at": info_data.get("published_at"),
            "duration_seconds": info_data.get("duration_seconds"),
            "view_count": info_data.get("view_count"),
            "like_count": info_data.get("like_count"),
            "comment_count": info_data.get("comment_count"),
            "tags": info_data.get("tags", []),
            "categories": info_data.get("categories", []),
            "chapters": info_data.get("chapters"),
            "width": info_data.get("width"),
            "height": info_data.get("height"),
            "fps": info_data.get("fps"),
            "language": info_data.get("language"),
            "webpage_url": info_data.get("webpage_url", ""),
            "handle": info_data.get("handle", ""),
            "media_path": r2_keys.get("master") or r2_keys.get("video"),
            "thumbnail_path": r2_keys.get("thumbnail"),
            "subtitle_path": r2_keys.get("subtitle"),
            "is_downloaded": True,
            "downloaded_at": now_iso,
            "r2_synced_at": now_iso,
            "info_json_synced_at": now_iso,
        }

        self.client.table("videos").upsert(row, on_conflict="youtube_id").execute()

    def update_video_sync_result(
        self,
        video_id: str,
        storage_bytes: int,
        sync_tier: str,
        score: float,
    ) -> None:
        """Update storage_bytes, sync_tier, and score after a successful upload."""
        self.client.table("videos").update(
            {"storage_bytes": storage_bytes, "sync_tier": sync_tier, "score": score}
        ).eq("youtube_id", video_id).execute()

    def delete_video_record(self, video_id: str) -> None:
        """Delete the video row from the database."""
        self.client.table("videos").delete().eq("youtube_id", video_id).execute()

    def resolve_channel_handle(self, job: dict) -> str:
        """Extract channel handle from job metadata, fallback to DB lookup."""
        metadata = job.get("metadata") or {}

        handle = metadata.get("handle") or metadata.get("channel_handle")
        if handle:
            return handle

        channel_id = job.get("channel_id")
        if channel_id:
            resp = self.client.table("channels").select("custom_url").eq("youtube_id", channel_id).limit(1).execute()
            if resp.data and resp.data[0].get("custom_url"):
                return resp.data[0]["custom_url"]

        return "unknown"
