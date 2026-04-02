# Data Model: Unified Sync Pipeline

**Branch**: `001-docs-architecture-sync` | **Date**: 2026-04-01

## Entity Changes

### videos table

**New columns**:

| Column | Type | Nullable | Default | Purpose |
|--------|------|----------|---------|---------|
| `storage_bytes` | bigint | yes | NULL | Actual measured byte count of all R2 objects for this video. Populated after upload (Step 7). Used in reconciliation (Step 6) for budget math on existing R2 content. |

**No other changes**. Existing columns (`youtube_id`, `channel_id`, `r2_synced_at`, `sync_tier`, `source_tags`, etc.) are retained.

### sync_queue table

**New columns**:

| Column | Type | Nullable | Default | Purpose |
|--------|------|----------|---------|---------|
| `storage_bytes` | bigint | yes | NULL | Actual measured bytes, written after download+remux (Step 5). NULL until measured. Used in reconciliation for budget math on downloaded candidates. |
| `score` | numeric | yes | NULL | Video's score at plan time. Used in reconciliation to determine trim order (lowest score trimmed first when over budget). |
| `estimated_bytes` | bigint | yes | NULL | Estimated bytes from `estimate_gb()` at plan time. Used for overflow candidate ordering; never for actual budget enforcement. |
| `priority` | integer | no | 0 | Job priority. Removal jobs get `priority=10` (processed first). Download jobs get `priority=0`. |

**Modified constraints**:

| Constraint | Old | New |
|------------|-----|-----|
| `sync_queue_status_check` | `status IN ('pending', 'processing', 'done', 'failed')` | `status IN ('pending', 'processing', 'downloaded', 'uploading', 'done', 'failed', 'skipped')` |
| `sync_queue_action_check` | `action IN ('download', 'remove')` | `action IN ('download', 'remove', 'backfill')` |

**Status lifecycle**:

```
Plan enqueues:
  action='remove'   → status='pending', priority=10
  action='download'  → status='pending', priority=0
  action='backfill'  → status='overflow' (not claimed until reconciliation)

Process phases:
  pending → processing (claimed)
  processing → done (removal completed)
  processing → downloaded (download+measure completed, awaiting reconciliation)
  downloaded → uploading (admitted by reconciliation)
  downloaded → skipped (trimmed by reconciliation)
  uploading → done (upload completed)
  processing → failed (error, attempts incremented; reset to pending if under max_attempts)
  overflow → processing (backfill candidate claimed during reconciliation)
```

### curated_channels table

**Dropped columns**:

| Column | Reason |
|--------|--------|
| `retention_cycles` | Date-based retention replaced by budget-only selection. No longer referenced by any code path. |

**Retained columns**: `last_full_refresh_at` (still used for rolling channel rotation).

### channel_calibration table

**No changes**. `median_gap_days` is no longer used for retention window calculation but remains useful for analytics and observability.

## Dataclass Changes

### SyncJob (src/models.py)

```
Current:
  video_id: str
  channel_id: str
  action: str
  metadata: dict
  id: str | None

Add fields:
  score: float = 0.0
  storage_bytes: int | None = None
  estimated_bytes: int | None = None
  priority: int = 0
  status: str = "pending"
```

### ChannelConfig (src/models.py)

```
Remove:
  retention_cycles: int = 30

Keep all other fields unchanged.
```

### Video (src/models.py)

```
Add fields:
  storage_bytes: int | None = None   # actual measured bytes after upload
  estimated_gb: float = 0.0          # estimated GB from estimate_gb()
```

## New Pure Types

### DownloadResult (src/models.py)

```
@dataclass
class DownloadResult:
    video_id: str
    channel_id: str
    score: float
    storage_bytes: int
    staging_dir: Path
    job_id: str
    info_data: dict
    remuxed_tiers: list
    sidecar_files: list[Path]
```

### ReconciliationResult (src/models.py)

```
@dataclass
class ReconciliationResult:
    admitted: list[DownloadResult]       # will be uploaded
    skipped: list[DownloadResult]        # will be discarded
    backfill_to_download: list[dict]     # overflow candidates to attempt
```

## State Transitions

### Sync Queue Job Lifecycle

```
                          ┌─────────────────────────────┐
                          │       sync plan              │
                          │  (creates/replaces jobs)     │
                          └──────┬──────────┬───────────┘
                                 │          │
                          remove jobs    download jobs    backfill jobs
                          priority=10    priority=0       status=overflow
                          status=pending status=pending
                                 │          │                  │
                    ┌────────────┘          │                  │
                    ▼                       ▼                  │
              ┌──────────┐           ┌──────────┐             │
              │ Step 4:  │           │ Step 5:  │             │
              │ Purge    │           │ Download │             │
              │          │           │ +Measure │             │
              └────┬─────┘           └────┬─────┘             │
                   │                      │                   │
                   ▼                      ▼                   │
                 done              downloaded                 │
                                       │                     │
                                       ▼                     │
                                 ┌──────────┐                │
                                 │ Step 6:  │◄───────────────┘
                                 │Reconcile │  (draws from overflow
                                 │          │   for backfill)
                                 └──┬───┬───┘
                                    │   │
                              admitted  skipped
                                    │       │
                                    ▼       ▼
                              ┌────────┐  discard
                              │ Step 7:│  local staging
                              │ Upload │  status=skipped
                              └───┬────┘
                                  │
                                  ▼
                                done
```

### Failure Paths

```
processing → fail_job() → pending (attempts < max_attempts)
processing → fail_job() → failed  (attempts >= max_attempts)

sync plan re-run:
  pending/processing/overflow → deleted (replaced by fresh jobs)
  failed → preserved (terminal; operator must manually address)
  done → already deleted (complete_job removes from queue)
```

## Migration SQL (ordered)

```sql
-- 1. Add storage_bytes to videos
ALTER TABLE videos ADD COLUMN storage_bytes bigint;

-- 2. Add new columns to sync_queue
ALTER TABLE sync_queue ADD COLUMN storage_bytes bigint;
ALTER TABLE sync_queue ADD COLUMN score numeric;
ALTER TABLE sync_queue ADD COLUMN estimated_bytes bigint;
ALTER TABLE sync_queue ADD COLUMN priority integer NOT NULL DEFAULT 0;

-- 3. Expand status constraint
ALTER TABLE sync_queue DROP CONSTRAINT sync_queue_status_check;
ALTER TABLE sync_queue ADD CONSTRAINT sync_queue_status_check
  CHECK (status IN ('pending', 'processing', 'downloaded', 'uploading',
                    'done', 'failed', 'skipped', 'overflow'));

-- 4. Expand action constraint (if exists)
ALTER TABLE sync_queue DROP CONSTRAINT IF EXISTS sync_queue_action_check;
ALTER TABLE sync_queue ADD CONSTRAINT sync_queue_action_check
  CHECK (action IN ('download', 'remove', 'backfill'));

-- 5. Drop retention_cycles (after old commands removed)
ALTER TABLE curated_channels DROP COLUMN retention_cycles;
```
