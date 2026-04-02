"""Cloudflare R2 storage operations — upload, delete, key building."""

from __future__ import annotations

import concurrent.futures
import os
import threading
from datetime import datetime
from pathlib import Path


class R2Storage:
    """Cloudflare R2 storage client for HLS package upload and deletion.

    The boto3 S3 client and bucket name are injected via constructor.
    """

    # Content-Type mapping
    CONTENT_TYPES = {
        ".m3u8": "application/vnd.apple.mpegurl",
        ".m4s": "video/mp4",
        ".mp4": "video/mp4",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".png": "image/png",
        ".vtt": "text/vtt",
        ".json": "application/json",
    }

    # Cache-Control per type
    CACHE_CONTROLS = {
        ".m3u8": "public, max-age=3600",
        ".m4s": "public, max-age=31536000, immutable",
        ".mp4": "public, max-age=31536000, immutable",
    }

    def __init__(self, r2_client, bucket: str):
        self.client = r2_client
        self.bucket = bucket

    @classmethod
    def from_env(cls) -> R2Storage:
        """Create R2Storage from environment variables."""
        import boto3

        account_id = os.environ.get("R2_ACCOUNT_ID")
        access_key = os.environ.get("R2_ACCESS_KEY_ID")
        secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
        bucket = os.environ.get("R2_BUCKET_NAME", "")

        r2_client = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )
        return cls(r2_client, bucket)

    # ── Key building ─────────────────────────────────────────────────────

    @staticmethod
    def build_r2_key(
        channel_handle: str,
        published_at: str | None,
        video_id: str,
        relative_path: str,
    ) -> str:
        """Build R2 object key for HLS folder-per-video structure.

        Returns key like "handle/YYYY-MM/video_id/master.m3u8"
        """
        handle = channel_handle.lstrip("@")

        if published_at:
            try:
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                year = f"{dt.year:04d}"
                month = f"{dt.month:02d}"
            except (ValueError, AttributeError):
                year = "unknown"
                month = "00"
        else:
            year = "unknown"
            month = "00"

        return f"{handle}/{year}-{month}/{video_id}/{relative_path}"

    # ── Upload ───────────────────────────────────────────────────────────

    def upload_hls_package(
        self,
        staging_dir: Path,
        remuxed_tiers: list[dict],
        sidecar_files: dict[str, Path],
        channel_handle: str,
        published_at: str | None,
        video_id: str,
        master_content: str,
        verbose: bool = False,
    ) -> dict[str, str]:
        """Upload the complete HLS package to R2 using parallel threads.

        Returns dict of R2 keys: {'master': '...', 'thumbnail': '...', 'subtitle': '...'}.
        """
        from botocore.exceptions import ClientError

        uploaded_count = 0
        upload_lock = threading.Lock()

        def upload_one(local_path: Path, relative_path: str) -> str:
            nonlocal uploaded_count
            r2_key = self.build_r2_key(channel_handle, published_at, video_id, relative_path)
            suffix = local_path.suffix.lower()
            ct = self.CONTENT_TYPES.get(suffix, "application/octet-stream")
            cc = self.CACHE_CONTROLS.get(suffix, "public, max-age=86400")
            extra_args = {"ContentType": ct, "CacheControl": cc}

            try:
                self.client.upload_file(str(local_path), self.bucket, r2_key, ExtraArgs=extra_args)
                with upload_lock:
                    uploaded_count += 1
                return r2_key
            except (ClientError, OSError) as e:
                raise RuntimeError(f"Failed to upload {relative_path}: {e}")

        # Collect all files to upload
        upload_tasks: list[tuple[Path, str]] = []

        # 1. Write master.m3u8
        master_path = staging_dir / "hls" / "master.m3u8"
        master_path.parent.mkdir(parents=True, exist_ok=True)
        master_path.write_text(master_content, encoding="utf-8")
        upload_tasks.append((master_path, "master.m3u8"))

        # 2. Per-tier HLS files
        for tier in remuxed_tiers:
            label = tier["label"]
            hls_dir = tier["hls_dir"]
            for file_path in sorted(Path(hls_dir).iterdir()):
                if file_path.is_file():
                    upload_tasks.append((file_path, f"{label}/{file_path.name}"))

        # 3. Sidecars
        if "thumbnail" in sidecar_files:
            upload_tasks.append((sidecar_files["thumbnail"], "thumb.jpg"))
        if "subtitle" in sidecar_files:
            upload_tasks.append((sidecar_files["subtitle"], "subs.en.vtt"))

        if verbose:
            print(f"    Uploading {len(upload_tasks)} files to R2 (10 threads)...")

        # Upload all files in parallel
        errors = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_path = {
                executor.submit(upload_one, local_path, rel_path): rel_path for local_path, rel_path in upload_tasks
            }
            for future in concurrent.futures.as_completed(future_to_path):
                rel_path = future_to_path[future]
                try:
                    future.result()
                except Exception as e:
                    errors.append(str(e))
                    print(f"      FAILED {rel_path}: {e}")

        if errors:
            raise RuntimeError(f"Failed to upload {len(errors)} file(s): {errors[0]}")

        # Build return keys
        r2_keys: dict[str, str] = {}
        r2_keys["master"] = self.build_r2_key(channel_handle, published_at, video_id, "master.m3u8")
        if "thumbnail" in sidecar_files:
            r2_keys["thumbnail"] = self.build_r2_key(channel_handle, published_at, video_id, "thumb.jpg")
        if "subtitle" in sidecar_files:
            r2_keys["subtitle"] = self.build_r2_key(channel_handle, published_at, video_id, "subs.en.vtt")

        if verbose:
            print(f"    Uploaded {uploaded_count} files to R2")

        return r2_keys

    # ── Delete ───────────────────────────────────────────────────────────

    def delete_video_objects(self, job_metadata: dict) -> tuple[bool, str | None]:
        """Delete all R2 objects for a video by listing the HLS folder prefix."""
        from botocore.exceptions import ClientError

        media_path = job_metadata.get("media_path")
        if not media_path:
            return True, None

        prefix = media_path.rsplit("/", 1)[0] + "/"

        errors = []
        deleted_count = 0

        try:
            paginator = self.client.get_paginator("list_objects_v2")
            objects_to_delete = []

            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if not objects_to_delete:
                print(f"    No objects found under prefix: {prefix}")
                return True, None

            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                resp = self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": batch, "Quiet": True},
                )
                batch_errors = resp.get("Errors", [])
                if batch_errors:
                    for err in batch_errors:
                        errors.append(f"{err['Key']}: [{err.get('Code')}] {err.get('Message')}")
                deleted_count += len(batch) - len(batch_errors)

            print(f"    Deleted {deleted_count} R2 object(s) under {prefix}")

        except ClientError as e:
            errors.append(f"List/delete failed for prefix {prefix}: {e}")

        if errors:
            return False, "; ".join(errors)
        return True, None
