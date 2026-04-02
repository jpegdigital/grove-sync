# Sync Pipeline Architecture

> Supersedes: `tiered-sync.md` (the separate fresh/catalog tier model with
> date-based retention windows is replaced by the unified cache model below)

## Core Model: R2 as a Cache

The YouTube API is the source of truth. It returns the full universe of videos
for a channel. Scoring and budget rules produce a **canonical set** — the
subset of that universe that *should* be cached in R2 at any given time.

R2 is a materialization layer. The sync pipeline's job is to keep R2 in sync
with the canonical set: download what's missing, evict what shouldn't be there,
and never exceed the storage budget.

```
YouTube API            Full Universe            R2 (cache)
───────────            ─────────────            ──────────
All videos for   →     Scored + ranked     →    Materialized subset
a channel              (no budget filtering)    with actual bytes tracked
```

There is no date-based retention window. The storage budget is the only
constraint on how much content a channel keeps. Newer videos naturally displace
older ones because recency is a scoring signal — not because of a time cutoff.

**Key principle**: Budget decisions require actual measured bytes. The plan
phase builds the full universe without budget filtering. The process phase
downloads, measures, and *then* decides what fits using real data.

## Pipeline Overview

Two commands, seven steps, one queue.

```
sync plan          sync process
───────────        ─────────────────────────────────────────────
Step 1: Discover   Step 4: Purge (prior evictions)
Step 2: Score      Step 5: Download + Measure (to local disk)
Step 3: Dedup      Step 6: Reconcile (full pool budget math)
                   Step 7: Upload + Evict (to/from R2)
```

`sync plan` is fast (minutes), API-quota-bound, and runs daily for a rolling
subset of channels. `sync process` is slow (hours), I/O-bound, and runs after
plan completes. The sync queue decouples them.

## Step 1: Discover

Hit the YouTube API for each channel in this run's rotation. The current
implementation fetches the channel's uploads playlist and enriches those videos
with duration and engagement stats.

- **Playlist items** (`playlistItems.list` on the uploads playlist) — cheap,
  1 quota unit per 50 results. Produces the candidate set in reverse
  chronological order.
- **Video enrichment** (`videos.list`) — 1 quota unit per batch of up to 50
  video IDs. Adds `duration_seconds`, `view_count`, `like_count`, and
  `comment_count`.

There is helper code for search-based discovery and local JSON caching, but it
is not wired into the current `sync-plan` CLI path.

**Rolling channel rotation**: not every channel is hit every run. A fraction
of channels (e.g., 10%) get a full API refresh per daily run, rotated by
`last_full_refresh_at`. This keeps API quota usage predictable across 59+
channels. When a single channel is targeted via `--channel`, it always gets
a full refresh.

**Output**: playlist-backed candidate list per channel with enrichment data.

## Step 2: Score

Filter all candidates by the channel's duration bounds, then compute an
engagement-storage-efficiency score:

```
score = log10(likes * 0.7 + comments * 0.3 + 1) / estimated_gb(duration) ^ alpha
```

**No budget filtering happens here.** The full scored universe is the output.
Budget decisions are deferred to Step 6 (Reconcile) in `sync process`, where
actual measured bytes are available.

```
All candidates → filter by duration → score each → output full list
```

This is a deliberate design choice. `estimate_gb()` is unreliable (actual bytes
can vary 0.5x–2x from estimates depending on content type and source quality).
The only reliable budget enforcement uses real bytes from Step 5.

**Output**: all eligible candidates with a single `score` field.

## Step 3: Dedup Against R2

Query the `videos` table for everything currently materialized on R2
(`r2_synced_at IS NOT NULL`) for this channel.

```
candidates:    {V1, V2, V3, V7, V8, V10, V12}
in R2 already: {V2, V3, V8, V10}

to download:    {V1, V7, V12}    ← candidate, not in R2
already cached: {V2, V3, V8, V10} ← in both, no download needed
```

Enqueue **download jobs** for `to download` set. No removal jobs are created
by plan — eviction decisions are made by reconciliation in Step 6 using actual
bytes.

For `already cached` videos: update `score` on the video record if it changed,
but take no queue action.

**What about videos in R2 that aren't in the candidate set?** They stay in R2
until reconciliation decides to evict them. Plan doesn't have the byte data
to make that call.

## Step 4: Purge

*First phase of `sync process`.*

Claim and process all `action='remove'` jobs. These are eviction jobs created
by a *previous* reconciliation run (Step 6 from a prior process run).

For each removal:

1. Delete all R2 objects under the video's prefix
2. Delete the video record from the database
3. Complete the queue job

This runs before downloads to free R2 space. Removals are fast (API delete
calls) and independent — a failed removal doesn't block other work.

## Step 5: Download + Measure

*Second phase of `sync process`.*

Claim all `action='download'` jobs up to `--limit`. For each:

1. Download via yt-dlp to local staging (`downloads/staging/{video_id}/`)
2. Remux to HLS segments via ffmpeg (per configured tiers: 480p, 720p, etc.)
3. **Measure actual bytes**: `sum(f.stat().st_size for f in staging_dir.rglob('*') if f.is_file())`
4. Update the queue job record with `storage_bytes` (actual) and set
   `status='downloaded'`

On failure: log error, increment `attempts`, continue to the next job. A bad
download does not crash the run or block other downloads. Jobs exceeding
`max_attempts` (default 3) are marked permanently failed.

The `--limit` flag throttles how many downloads happen per run. This is a
practical bound on I/O and time, not a budget mechanism.

After this phase completes, every candidate on local disk has a known, exact
byte count. Nothing has been uploaded to R2 yet.

## Step 6: Reconcile

*Third phase of `sync process`. Pure logic, instant.*

This is where budget enforcement happens — and it considers **new downloads
and existing R2 content as a single pool**. Both can be kept or evicted.

Load the full picture for each channel:

```
existing_r2:  videos WHERE r2_synced_at IS NOT NULL AND channel_id = X
              each has storage_bytes (actual, from previous uploads)

downloaded:   local staging candidates with measured storage_bytes from Step 5
```

### Fresh tier reconciliation

Pool = new fresh downloads + existing fresh R2 content.
Sort by `published_at` descending (newest first).
Greedy keep: walk the list, accumulate bytes, keep until fresh budget full.

```
fresh_budget = storage_budget_gb * (1 - catalog_fraction)

All fresh content, newest first:
  V_new1 (Mar 25)  1.2 GB  → cumulative 1.2  → KEEP (new → upload)
  V_r2_3 (Mar 20)  0.8 GB  → cumulative 2.0  → KEEP (existing → stays)
  V_new2 (Mar 15)  0.9 GB  → cumulative 2.9  → KEEP (new → upload)
  V_r2_1 (Mar 10)  1.1 GB  → cumulative 4.0  → KEEP (existing → stays)
  V_r2_2 (Feb 20)  0.7 GB  → cumulative 4.7  → OVER BUDGET → EVICT
  V_new3 (Feb 10)  0.5 GB  →                 → OVER BUDGET → SKIP
```

Results:
- **Admitted** (new downloads to upload): V_new1, V_new2
- **Evicted** (existing R2 to delete): V_r2_2
- **Skipped** (new downloads to discard): V_new3

### Catalog tier reconciliation

Pool = new catalog downloads + existing catalog R2 content.
Sort by `score` descending (highest first).
Greedy keep: walk the list, accumulate bytes, keep until catalog budget full.

Same logic as fresh but ranked by score instead of recency.

### Backfill

If either tier is under budget after keeping everything, check pending download
jobs (queued but not yet downloaded). Return candidates that could fill the
remaining space, ordered by recency (fresh) or score (catalog). These are
downloaded one-by-one in future process runs.

### 6d. Produce the final output

After reconciliation, every piece of content is in one state:

- **Admitted**: new downloads → upload to R2
- **Skipped**: new downloads → discard local staging
- **Kept**: existing R2 → no action
- **Evicted**: existing R2 → delete from R2

## Step 7: Upload + Evict

*Final phase of `sync process`.*

### Uploads

For each admitted candidate:

1. Upload HLS package from local staging to R2
2. Upsert video record with `storage_bytes`, `r2_synced_at`, and video metadata
3. Complete the queue job
4. Clean up local staging

### Evictions

For each evicted existing video:

1. Delete all R2 objects under the video's prefix
2. Delete the video record from the database

### Cleanup

For each skipped candidate:

1. Delete local staging directory
2. Mark queue job as `skipped`

## Data Model Changes

### `videos` table

```sql
ALTER TABLE videos ADD COLUMN storage_bytes bigint;
```

Populated after upload with the actual measured byte count of all R2 objects
for this video (HLS segments + master playlist + sidecars). Used in Step 6
for budget math on existing R2 content.

### `sync_queue` table

```sql
ALTER TABLE sync_queue ADD COLUMN storage_bytes bigint;
ALTER TABLE sync_queue ADD COLUMN score numeric;
ALTER TABLE sync_queue ADD COLUMN estimated_bytes bigint;
ALTER TABLE sync_queue ADD COLUMN priority integer NOT NULL DEFAULT 0;

ALTER TABLE sync_queue DROP CONSTRAINT sync_queue_status_check;
ALTER TABLE sync_queue ADD CONSTRAINT sync_queue_status_check
  CHECK (status IN ('pending', 'processing', 'downloaded', 'uploading',
                    'done', 'failed', 'skipped', 'overflow'));

ALTER TABLE sync_queue DROP CONSTRAINT IF EXISTS sync_queue_action_check;
ALTER TABLE sync_queue ADD CONSTRAINT sync_queue_action_check
  CHECK (action IN ('download', 'remove', 'backfill'));
```

- `storage_bytes` — actual measured bytes, written after download+remux
  (Step 5). NULL until measured.
- `score` — video's score at plan time.
- `estimated_bytes` — rough estimate from `estimate_gb()`, for ordering only.
- `priority` — removal jobs get `priority=10` (processed first).
- Expanded status values track the job lifecycle: `pending` → `processing`
  (downloading) → `downloaded` → `uploading` → `done`. Terminal states:
  `failed`, `skipped`.

### Columns dropped (from `curated_channels`)

```sql
ALTER TABLE curated_channels DROP COLUMN retention_cycles;
```

`last_full_refresh_at` is retained — still used for rolling channel rotation.

### `channel_calibration` table

`median_gap_days` is no longer used for retention windows but the calibration
table remains useful for analytics and observability (posting frequency,
duration distribution). No schema changes needed.

## Scoring

Scoring is unchanged. The five signals and their weights remain:

| Signal | Range | Description |
|--------|-------|-------------|
| popularity | ~0-8 | `log10(views)` |
| engagement | ~0-5 | `(like_rate * 0.7 + comment_rate * 0.3) * 100` |
| freshness | 0-1 | `exp(-age_days * ln(2) / half_life_days)` |
| velocity | ~0-6 | `log10(views / age_days)` |
| reach | ~-2 to 4 | `log10(views / subscriber_count)` |

ESE scoring for catalog ranking:

```
ese_score = raw_score / storage_gb ^ alpha
```

`estimate_gb()` is used as input to ESE and for estimated_bytes in job
metadata. It is **never** used for budget enforcement — Steps 5-6 use
measured bytes exclusively.

## Commands

### `sync plan`

Replaces the old playlist-driven planning path. Discovers the uploads playlist,
scores everything eligible, deduplicates against R2, and enqueues downloads.

```bash
uv run sync-plan                         # daily run, rolling channels
uv run sync-plan --channel UC...         # single channel, full refresh
uv run sync-plan --dry-run --verbose     # preview without DB writes
```

### `sync process`

Replaces the current `sync-process`. Runs all four phases sequentially:
purge → download → reconcile → upload+evict.

```bash
uv run sync-process                      # process all pending work
uv run sync-process --limit 20           # cap downloads this run
uv run sync-process --dry-run --verbose  # preview
uv run sync-process --downloads-only     # skip purge phase
uv run sync-process --removals-only      # only purge
```

### `sync calibrate`

Unchanged. Samples channels for posting frequency and duration statistics.

## Queue Lifecycle

```
sync plan                                sync process
─────────                                ─────────────

Step 1-3:                                Step 4: Purge
Discover + Score                           (prior eviction jobs)
+ Dedup                                     │
    │                                        ▼ done
    │                                    Step 5: Download + Measure
    └─── action=download ──────────────── │
           (pending)                       │ measure storage_bytes
                                           │ status → downloaded
                                           ▼
                                        Step 6: Reconcile
                                        (new + existing = one pool)
                                           │
                               ┌───────────┼───────────┐
                               ▼           ▼           ▼
                            admitted     skipped     evicted
                               │           │        (existing R2)
                               ▼           ▼           │
                            Step 7:     discard        ▼
                            Upload      local       delete from
                               │        staging     R2 + DB
                               ▼
                             done
```

## Resilience

| Failure | Recovery |
|---------|----------|
| Bad download (yt-dlp error, throttle) | `attempts` incremented, job stays pending. Next run retries. Other jobs unaffected. Max attempts (default 3) → permanently failed. |
| Process killed mid-download | Local staging persists. Pending jobs re-claimed on restart. |
| Process killed mid-upload | Video is on local disk but not R2. Next plan run sees it's not materialized, re-queues. |
| Budget changed between plan and process | Reconciliation (Step 6) reads current budget from DB, not what plan assumed. |
| Actual bytes >> estimate | Step 6 handles it — new + existing pool is reconciled with real bytes. Over budget → evict oldest/lowest-scoring. |
| API quota exhausted mid-plan | Partially-planned channels get what they got. Next run picks up remaining channels via rotation. |
| Plan enqueues more downloads than will fit | Normal. Plan intentionally over-enqueues. Reconciliation decides what actually fits after measuring real bytes. |

## File Map

```
src/commands/
  plan.py          ← discovery + scoring + dedup + enqueue (playlist-only, no budget)
  process.py       ← REWRITE: four-phase (purge → download → reconcile → upload/evict)
  calibrate.py     ← UNCHANGED

src/scoring.py     ← ESE scoring + duration filtering for the plan phase

src/models.py      ← shared dataclasses

src/services/
  db.py            ← channel loading, existing-R2 lookup, queue replacement
  hls.py           ← UNCHANGED
  storage.py       ← UNCHANGED
  youtube.py       ← UNCHANGED
  video_fetcher.py ← cache-capable helper, not used by current sync-plan CLI

src/config.py      ← producer/consumer YAML loading and env validation

config/
  producer.yaml    ← rolling selection, API, quota, and DB batch settings
  consumer.yaml    ← UNCHANGED

DELETE:
  src/commands/fresh.py    ← absorbed into plan.py
  src/commands/catalog.py  ← absorbed into plan.py
```

## Archive Mode

Channels with `sync_mode='archive'` skip all budget enforcement. All
discovered videos are canonical. Reconciliation admits everything without
trimming or eviction. The storage budget is advisory — archive channels grow
indefinitely. The plan enqueues download jobs for new content and the process
uploads everything that was downloaded.
