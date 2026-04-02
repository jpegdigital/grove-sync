# Feature Specification: Unified Sync Pipeline

> Historical design document: this feature spec captures the broader original
> proposal. The shipped `sync-plan` command currently fetches the uploads
> playlist, scores eligible videos, diffs against R2, and enqueues downloads.
> For current behavior, prefer `src/commands/plan.py` and
> `docs/architecture/sync-pipeline.md`.

**Feature Branch**: `001-docs-architecture-sync`  
**Created**: 2026-04-01  
**Status**: Draft  
**Input**: User description: "Implement the new unified sync pipeline architecture that replaces the separate fresh/catalog tier model with a single cache-based pipeline using R2 as a materialization layer."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Plan a Channel Sync (Priority: P1)

An operator runs `sync plan` to evaluate which videos should be cached for a channel. The system discovers all video candidates from YouTube (playlist items + search), scores them, builds the canonical set using fresh and catalog budget fractions, diffs against what's already in R2, and enqueues download and removal jobs to the sync queue.

**Why this priority**: Planning is the foundation of the entire pipeline. Without the ability to discover, score, and diff candidates, no downstream work (downloads, uploads, removals) can happen.

**Independent Test**: Can be fully tested by running `sync plan --channel UC... --dry-run --verbose` against a channel with known videos and verifying the candidate list, scores, canonical set, and diff output without writing to the database.

**Acceptance Scenarios**:

1. **Given** a channel with 100 videos on YouTube and a 5 GB storage budget, **When** the operator runs `sync plan --channel UC...`, **Then** the system fetches candidates from both playlist items and search APIs, scores all candidates, selects a canonical set that fits within the budget (fresh + catalog fractions), diffs against R2, and enqueues appropriate download and removal jobs.

2. **Given** a daily run with 59+ configured channels, **When** the operator runs `sync plan` without `--channel`, **Then** only the channels due for refresh (based on `last_full_refresh_at` rotation) are processed, keeping API quota usage predictable.

3. **Given** a channel where all canonical videos are already in R2, **When** the plan runs, **Then** no new jobs are enqueued, but scores and source tags on existing video records are updated if they changed.

4. **Given** a `--dry-run` flag, **When** the plan runs, **Then** the system logs all decisions (candidates found, scores, canonical set, diff results) but writes nothing to the database or queue.

---

### User Story 2 - Process Queued Downloads and Removals (Priority: P1)

An operator runs `sync process` to execute all pending work from the sync queue. The system processes removal jobs first (to free R2 space), then downloads all candidates to local staging and measures their actual byte counts, reconciles against the budget using real bytes, and finally uploads admitted videos to R2.

**Why this priority**: Processing is the other half of the pipeline — without it, plan output sits idle in the queue. The four-phase sequential design (purge, download, reconcile, upload) is critical for correct budget enforcement.

**Independent Test**: Can be tested by seeding the sync queue with known jobs and running `sync process --dry-run --verbose`, verifying that phases execute in correct order and budget reconciliation produces expected admit/skip decisions.

**Acceptance Scenarios**:

1. **Given** a queue with 3 removal jobs and 5 download jobs, **When** the operator runs `sync process`, **Then** all removals complete before any downloads begin, all downloads complete before reconciliation, and only admitted videos are uploaded.

2. **Given** downloaded candidates whose actual total bytes exceed the channel budget, **When** reconciliation runs, **Then** the lowest-scoring candidates are trimmed first (since they haven't been uploaded yet), and the final uploaded set fits within the budget.

3. **Given** downloaded candidates whose actual total bytes are under the channel budget, **When** reconciliation runs, **Then** the system attempts to backfill additional candidates (one at a time, download and measure, until the budget is full or the next candidate would exceed it).

4. **Given** a download failure for one video (yt-dlp error), **When** the process continues, **Then** the failed job's attempt count is incremented, it remains in the queue for the next run, and all other jobs proceed normally.

---

### User Story 3 - Budget-Only Content Selection (Priority: P2)

The system selects cached content based solely on storage budget — no date-based retention windows. The fresh budget fraction selects the newest videos by publish date, and the catalog budget fraction selects the highest-scoring videos by ESE score. The union of both sets forms the canonical set.

**Why this priority**: This is the core model change from the old architecture. It ensures content selection is driven by budget and scoring, not arbitrary time cutoffs, allowing the effective content window to be emergent based on video length and budget size.

**Independent Test**: Can be tested with unit tests that provide a list of scored video candidates and a budget, verifying that the canonical set is correctly computed as the union of fresh-selected and catalog-selected videos within their respective budget fractions.

**Acceptance Scenarios**:

1. **Given** a channel with short videos (avg 5 min) and a 5 GB budget, **When** the canonical set is computed, **Then** more videos fit and the effective time window extends further back in time.

2. **Given** a channel with long videos (avg 60 min) and a 5 GB budget, **When** the canonical set is computed, **Then** fewer videos fit and the effective time window is narrower.

3. **Given** a video that ranks highly in both recency and ESE score, **When** it appears in both fresh and catalog selections, **Then** it is counted once in the canonical set with merged source tags.

---

### User Story 4 - Archive Mode (Priority: P3)

Channels configured with `sync_mode='archive'` bypass removal logic entirely. All discovered videos are canonical. The budget is treated as advisory — archive channels grow without eviction.

**Why this priority**: Archive mode is a special case for high-value channels where content should never be evicted. It's important but serves a subset of channels.

**Independent Test**: Can be tested by configuring a channel as archive mode and running a plan, verifying that no removal jobs are enqueued regardless of budget.

**Acceptance Scenarios**:

1. **Given** an archive-mode channel with content exceeding the storage budget, **When** `sync plan` runs, **Then** download jobs are enqueued for new content but no removal jobs are enqueued for any existing content.

2. **Given** an archive-mode channel, **When** `sync process` runs, **Then** all downloaded videos are uploaded to R2 without budget-based trimming.

---

### Edge Cases

- What happens when the YouTube API quota is exhausted mid-plan? Partially-planned channels keep what they got; remaining channels are picked up via rotation on the next run.
- What happens when `sync process` is killed mid-download? Local staging persists on disk. Pending jobs are re-claimed on restart.
- What happens when `sync process` is killed mid-upload? The video is on local disk but not marked in R2. The next plan run sees it's not materialized and re-queues it.
- What happens when the storage budget changes between plan and process? Reconciliation reads the current budget from the database, not what plan assumed.
- What happens when actual bytes far exceed estimated bytes? Reconciliation handles it by trimming the lowest-scoring candidates from the download pool.
- What happens when a channel has zero videos discovered? The plan produces an empty canonical set, and any existing R2 content is enqueued for removal (unless archive mode).
- What happens when all download jobs fail? No videos reach the reconciliation phase; nothing is uploaded. All jobs stay in the queue with incremented attempt counts for retry.
- What happens when `sync plan` runs again before `sync process` clears the queue? Existing pending/processing jobs for the same video are cancelled and replaced with fresh jobs reflecting the new plan's scores and decisions.

## Clarifications

### Session 2026-04-01

- Q: How should `sync plan` handle pre-existing pending jobs for the same video? → A: Replace — cancel any existing pending job for that video and enqueue a fresh one reflecting current plan decisions.
- Q: Should there be a maximum retry attempt threshold for failed jobs? → A: Yes — configurable max attempts (default 3). Jobs exceeding the threshold are marked `failed` permanently and skipped in future runs.
- Q: Where should reconciliation get its backfill candidates? → A: Overflow list from plan — `sync plan` persists a ranked list of next-best candidates (beyond the canonical cutoff) for reconciliation to draw from during backfill.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a unified `sync plan` command that replaces the separate `sync-fresh` and `sync-catalog` commands, performing discovery, scoring, canonical set computation, and diff/enqueue in a single run.
- **FR-002**: System MUST discover video candidates from two YouTube API sources per channel: playlist items (uploads playlist) and search (by view count and rating), then merge and deduplicate by video ID.
- **FR-003**: System MUST tag each discovered video with its source(s): `recent`, `popular`, `rated`, or combinations thereof.
- **FR-004**: System MUST compute the canonical set as the union of a fresh selection (sorted by publish date, filling the fresh budget fraction) and a catalog selection (sorted by ESE score, filling the catalog budget fraction).
- **FR-005**: System MUST use `estimate_gb()` for rough budget partitioning in canonical set computation, never for actual budget enforcement.
- **FR-006**: System MUST diff the canonical set against currently materialized R2 content and enqueue removal jobs (higher priority) and download jobs to the sync queue. When a pending or processing job already exists for a video, the system MUST cancel it and enqueue a fresh job reflecting the current plan's decisions.
- **FR-007**: System MUST support rolling channel rotation, processing a configurable fraction of channels per daily run based on `last_full_refresh_at`, while `--channel` forces a full refresh for a single channel.
- **FR-008**: System MUST provide a unified `sync process` command that executes four sequential phases: purge, download+measure, reconcile, upload.
- **FR-009**: System MUST process all removal jobs before starting any download jobs to free R2 space first.
- **FR-010**: System MUST measure actual byte counts of all downloaded and remuxed content before any upload decisions are made.
- **FR-011**: System MUST reconcile the budget using actual measured bytes, trimming the lowest-scoring downloaded candidates first when over budget.
- **FR-012**: System MUST backfill additional candidates (one at a time, download and measure) when under budget, stopping when the next candidate would exceed the budget. The backfill candidate list MUST be persisted by `sync plan` as a ranked overflow list (candidates below the canonical cutoff, ordered by score descending) so that reconciliation can draw from it without re-querying APIs or re-scoring.
- **FR-013**: System MUST track `storage_bytes` on both video records and sync queue jobs for budget math using real measurements.
- **FR-014**: System MUST track `score` on sync queue jobs for reconciliation trim ordering.
- **FR-015**: System MUST support expanded sync queue statuses: `pending`, `processing`, `downloaded`, `uploading`, `done`, `failed`, `skipped`.
- **FR-016**: System MUST handle individual download failures gracefully — increment attempt count, leave job in queue for retry, continue processing other jobs. Jobs exceeding a configurable maximum attempt threshold (default 3) MUST be marked `failed` permanently and skipped in future processing runs.
- **FR-017**: System MUST support archive mode channels that skip all removal logic and treat budgets as advisory.
- **FR-018**: System MUST support `--dry-run` and `--verbose` flags for both `sync plan` and `sync process`.
- **FR-019**: System MUST support `--use-cache` for `sync plan` to skip API calls and use cached responses.
- **FR-020**: System MUST support `--limit` for `sync process` to cap the number of downloads per run.
- **FR-021**: System MUST remove the `retention_cycles` column from the `curated_channels` table, as date-based retention is no longer used.
- **FR-022**: System MUST add `storage_bytes` column to both `videos` and `sync_queue` tables.
- **FR-023**: System MUST add `score` column to the `sync_queue` table.
- **FR-024**: System MUST delete the old `fresh.py` and `catalog.py` command files, whose functionality is absorbed into the new `plan.py`.

### Key Entities

- **Video Candidate**: A YouTube video discovered during planning — carries video ID, channel ID, metadata, source tags, score, and estimated storage size. Becomes a video record after upload.
- **Canonical Set**: The union of fresh-selected and catalog-selected candidates for a channel — the target state that R2 should match.
- **Sync Queue Job**: A unit of work (download or remove) with action type, priority, status lifecycle, score, and measured storage bytes.
- **Channel Configuration**: Per-channel settings including storage budget (GB), catalog fraction, sync mode (normal/archive), and last full refresh timestamp.
- **Storage Budget**: The total GB a channel may consume in R2, partitioned into fresh and catalog fractions. The sole constraint on content selection.
- **Overflow List**: A ranked list of video candidates that scored below the canonical set cutoff, persisted during planning. Used by reconciliation for budget backfill without requiring API calls or re-scoring.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A single `sync plan` command produces the same or better content selection results as the previous two-command (`sync-fresh` + `sync-catalog`) workflow, measured by canonical set quality (higher average ESE score per GB stored).
- **SC-002**: Budget enforcement using actual measured bytes results in channel storage usage within 5% of the configured budget (no persistent overruns).
- **SC-003**: The four-phase `sync process` completes all pending work for a channel without manual intervention, with individual failures isolated (one bad download does not block others).
- **SC-004**: Rolling channel rotation keeps daily YouTube API quota usage within the configured warning threshold across 59+ channels.
- **SC-005**: Operator can preview all pipeline decisions using `--dry-run --verbose` without any side effects to the database, queue, or R2.
- **SC-006**: Archive-mode channels accumulate content without eviction, verified by zero removal jobs being created for archive channels across multiple plan runs.
- **SC-007**: The old `sync-fresh` and `sync-catalog` entry points are fully replaced — running the old commands produces an error or is removed from configuration.

## Assumptions

- The existing scoring algorithm (five signals: popularity, engagement, freshness, velocity, reach) and ESE formula are unchanged and remain correct for the new unified selection.
- The existing `HlsPipeline`, `R2Storage`, `YouTubeClient`, `VideoFetcher`, and `SyncDatabase` services are functionally correct and require only interface additions (not rewrites) to support the new pipeline.
- The `estimate_gb()` function provides sufficiently accurate estimates for relative ranking and rough budget partitioning, even though actual bytes may differ.
- The database schema can be altered (add/drop columns) with a migration applied before the new code is deployed.
- The `calibrate` command remains unchanged and continues to work independently of the pipeline changes.
- Local disk staging has sufficient space to hold all downloaded candidates for a run before uploads begin.
- The existing cookie-based YouTube authentication and yt-dlp configuration remain compatible with the new pipeline.
