# CLI Contract: sync-process

**Entry point**: `src.commands.process:main`
**Replaces**: existing `sync-process` (rewrite with four-phase architecture)

## Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--limit` | integer | (config batch_size) | Cap the number of download jobs to process this run. |
| `--dry-run` | flag | false | Preview all phases without executing downloads, uploads, or deletions. |
| `--verbose` | flag | false | Detailed logging of each phase and job. |
| `--downloads-only` | flag | false | Skip purge phase; only download, reconcile, upload. |
| `--removals-only` | flag | false | Only run purge phase; skip downloads and uploads. |

## Behavior

### Phase 1: Purge (Step 4)

1. Claim all `action='remove'` jobs (they have `priority=10`, claimed first).
2. For each removal: delete R2 objects, delete video record, complete job.
3. Skipped if `--downloads-only`.

### Phase 2: Download + Measure (Step 5)

1. Claim all `action='download'` jobs up to `--limit`.
2. For each job:
   a. Download via yt-dlp to `downloads/staging/{video_id}/`.
   b. Remux to HLS via ffmpeg.
   c. Measure actual bytes: `sum(file sizes in staging dir)`.
   d. Update job: set `storage_bytes`, status → `downloaded`.
3. On failure: increment attempts, continue to next job. Mark `failed` if max_attempts exceeded.
4. Skipped if `--removals-only`.

### Phase 3: Reconcile (Step 6)

1. Load per-channel: existing R2 videos (with `storage_bytes`), all `downloaded` jobs, all `overflow` jobs.
2. Per channel, compute:
   - `existing_keep_gb`: sum of `storage_bytes` for R2 videos still in canonical set.
   - `new_gb`: sum of `storage_bytes` for downloaded candidates.
   - If over budget: trim lowest-scoring downloaded candidates. Mark `skipped`.
   - If under budget: claim overflow candidates one-by-one, download+measure, admit if fits.
3. Produce final admit/skip lists.
4. Skipped if `--removals-only`.

### Phase 4: Upload (Step 7)

1. For each admitted candidate:
   a. Upload HLS package to R2.
   b. Upsert video record with `storage_bytes`, `r2_synced_at`, `source_tags`.
   c. Complete job.
   d. Clean up local staging.
2. For each skipped candidate:
   a. Delete local staging.
   b. Mark job `skipped`.
3. Skipped if `--removals-only`.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (all phases completed, individual job failures don't affect exit code) |
| 1 | Unrecoverable error (missing env, ffmpeg not found, config parse failure) |

## Output (stdout)

- Phase headers: `[PURGE]`, `[DOWNLOAD]`, `[RECONCILE]`, `[UPLOAD]`.
- Per-job status: video_id, action, result (done/failed/skipped).
- Summary: removed, downloaded, admitted, skipped, uploaded, failed counts.
- With `--verbose`: byte counts, scores, budget math details.
- With `--dry-run`: prefixed with `[DRY RUN]`, no side effects.
