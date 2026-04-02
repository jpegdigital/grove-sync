# Research: Unified Sync Pipeline

**Branch**: `001-docs-architecture-sync` | **Date**: 2026-04-01

## R1: Overflow List Persistence Strategy

**Decision**: Store overflow candidates as sync_queue jobs with `action='backfill'` and `status='overflow'`.

**Rationale**: Reuses the existing sync_queue table and RPC infrastructure rather than introducing a new table. Overflow rows carry `video_id`, `channel_id`, `score`, and `estimated_bytes` in metadata. During reconciliation, `sync process` queries overflow rows ordered by score descending and processes them one-by-one. When `sync plan` re-runs, it replaces overflow rows along with other pending jobs (consistent with the job replacement clarification).

**Alternatives considered**:
- Separate `overflow_candidates` table: More normalized but adds a new table, new queries, and new cleanup logic for a transient dataset.
- JSON sidecar file on disk: Simple but fragile (no atomicity, no cross-machine visibility, cleanup burden).
- Store in sync_queue metadata as a JSON array on a sentinel row: Awkward to query and paginate.

## R2: Job Replacement Implementation

**Decision**: In the diff/enqueue step, delete all existing pending/processing jobs for the channel, then insert fresh jobs. This is a replace-all-for-channel approach rather than per-video upsert.

**Rationale**: The existing `clear_channel_jobs(channel_id)` method already deletes all jobs for a channel. The plan always computes the complete desired state for a channel, so replacing all jobs is simpler and correct. Per-video upsert would require conflict handling on `(channel_id, video_id)` and careful status checks, adding complexity for no benefit since plan always produces the full picture.

**Alternatives considered**:
- Per-video upsert with status check: More granular but complex; risks edge cases with partially-replaced job sets.
- Soft-cancel (set status='cancelled') then insert: Preserves audit trail but clutters the queue table and complicates claim queries.

## R3: Reconciliation as Pure Logic

**Decision**: Implement reconciliation (Step 6) as a pure function in `scoring.py` that takes existing R2 state + downloaded candidates + overflow candidates + budget, and returns `(admitted, skipped, backfill_to_download)` lists.

**Rationale**: Keeps the budget math testable without mocks. The process command orchestrates the I/O (reading DB state, calling the pure function, then acting on results). This follows the existing pattern where `scoring.py` is pure and commands handle I/O.

**Alternatives considered**:
- Reconciliation as a method on ProcessCommand: Mixes I/O orchestration with pure budget logic, harder to test.
- Separate reconciliation service: Overkill for a single pure function; violates YAGNI.

## R4: Database Migration Strategy

**Decision**: Use Supabase migrations (SQL files) to add columns and modify constraints. Apply before deploying new code.

**Rationale**: The project already uses Supabase. Schema changes are backward-compatible additions (new columns with defaults, expanded CHECK constraint). The old code won't break if new columns exist but are unused. Drop `retention_cycles` only after old entry points are removed.

**Migration order**:
1. Add `storage_bytes` (bigint, nullable) to `videos` and `sync_queue`
2. Add `score` (numeric, nullable) to `sync_queue`
3. Expand `sync_queue` status CHECK to include `downloaded`, `uploading`, `skipped`
4. Add `overflow` to action CHECK (or use existing `download` action with a status marker)
5. Drop `retention_cycles` from `curated_channels` (after old commands deleted)

## R5: Four-Phase Process Orchestration

**Decision**: The `ProcessCommand.run()` method calls four private methods sequentially: `_purge()`, `_download_all()`, `_reconcile()`, `_upload()`. Each phase operates on explicit data passed between them (no shared mutable state).

**Rationale**: Sequential phases are required by the architecture (downloads must complete before reconciliation, which must complete before upload). Explicit data passing between phases makes the flow testable and debuggable. Each phase can be tested independently by providing its input data.

**Data flow**:
```
_purge(removal_jobs) → count_removed
_download_all(download_jobs, limit) → downloaded_results: list[DownloadResult]
_reconcile(existing_r2, downloaded, overflow, budget) → (admitted, skipped, backfill_admitted)
_upload(admitted + backfill_admitted) → count_uploaded
```

## R6: Max Retry Threshold Enforcement

**Decision**: Add `max_attempts` to consumer config (default 3). The `claim_jobs` RPC already filters by `attempts < max_attempts`. Jobs that reach the threshold stay in the queue with `status='failed'` rather than being deleted, so operators can inspect and manually retry.

**Rationale**: The existing `claim_consumer_jobs` RPC already accepts a `max_attempts` parameter and filters on it. The only change needed is: when `sync plan` replaces jobs for a channel, it should not delete `failed` jobs (they're terminal). Operators can review failed jobs and either delete them or reset their attempt count for retry.

**Alternatives considered**:
- Auto-delete after max attempts: Loses audit trail of what failed and why.
- Exponential backoff between retries: Unnecessary — retries happen across separate `sync process` runs (hours/days apart), providing natural backoff.
