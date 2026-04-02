# Tasks: Unified Sync Pipeline

> Historical implementation checklist: several completed items in this file
> reflect the original proposal rather than the current `sync-plan` code path.
> Treat it as planning history, not as the source of truth for live behavior.

**Input**: Design documents from `/specs/001-docs-architecture-sync/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Included (constitution mandates TDD for all non-trivial logic).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Database migrations and config changes that must land before any code changes.

- [x] T001 Apply Supabase migration: add `storage_bytes` (bigint) to `videos` table, add `storage_bytes` (bigint), `score` (numeric), `estimated_bytes` (bigint), `priority` (integer NOT NULL DEFAULT 0) to `sync_queue` table
- [x] T002 Apply Supabase migration: expand `sync_queue` status CHECK constraint to include `downloaded`, `uploading`, `skipped`, `overflow`; expand action CHECK to include `backfill`
- [x] T003 [P] Update config/producer.yaml: add `overflow_limit: 20` under `producer` section; remove any retention-related comments
- [x] T004 [P] Update config/consumer.yaml: add `max_attempts: 3` under `consumer` section

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Update shared models, scoring pure functions, and database methods that ALL user stories depend on.

**CRITICAL**: No user story work can begin until this phase is complete.

### Tests for Foundational

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [x] T005 [P] Test `select_canonical()` in tests/test_scoring_class.py: parameterized tests for fresh selection (newest-first, budget-limited), catalog selection (ESE-sorted, budget-limited), union with deduplication and source tag merging, overflow list generation (candidates below cutoff sorted by score)
- [x] T006 [P] Test `reconcile_budget()` in tests/test_reconciliation.py: parameterized tests for over-budget trimming (lowest score first), under-budget with no backfill candidates, under-budget with backfill (admits until budget full), exact-budget (no trim, no backfill), single-channel and multi-channel scenarios
- [x] T007 [P] Test new db methods in tests/test_services/test_db.py: `replace_channel_jobs()` (deletes non-failed pending/overflow jobs then inserts fresh), `enqueue_overflow()` (inserts backfill jobs with overflow status), `fetch_downloaded_jobs()` (returns jobs with status=downloaded), `fetch_overflow_jobs()` (returns overflow jobs ordered by score desc), `update_job_storage_bytes()`, `update_job_status()`, `fetch_existing_r2_with_bytes()` (returns videos with storage_bytes)

### Implementation for Foundational

- [x] T008 [P] Update dataclasses in src/models.py: add `storage_bytes: int | None = None` and `estimated_gb: float = 0.0` to `Video`; add `score: float = 0.0`, `storage_bytes: int | None = None`, `estimated_bytes: int | None = None`, `priority: int = 0`, `status: str = "pending"` to `SyncJob`; remove `retention_cycles` from `ChannelConfig`; add `DownloadResult` and `ReconciliationResult` dataclasses per data-model.md
- [x] T009 [P] Add `select_canonical()` to src/scoring.py: takes candidates list, fresh_budget_gb, catalog_budget_gb, alpha, min/max duration, subscriber_count. Returns `(canonical_set, overflow_list)` where canonical = union of fresh-selected (newest-first) + catalog-selected (ESE-sorted), overflow = next-best candidates below cutoff ordered by score desc, capped at `overflow_limit`
- [x] T010 [P] Add `reconcile_budget()` to src/scoring.py: pure function taking `existing_keep_bytes`, `downloaded: list[DownloadResult]`, `overflow: list[dict]`, `budget_gb`. Returns `ReconciliationResult(admitted, skipped, backfill_to_download)`. Over-budget: trims lowest-scoring downloaded candidates. Under-budget: returns overflow candidates to attempt as backfill (one at a time until budget full)
- [x] T011 Add new producer methods to src/services/db.py: `replace_channel_jobs(channel_id, jobs, overflow_jobs, dry_run)` — deletes all non-failed jobs for channel then batch-inserts new jobs and overflow jobs; `enqueue_overflow(overflow_jobs, dry_run)` — inserts backfill action jobs with overflow status; `fetch_existing_r2_with_bytes(channel_id)` — returns dict of video_id to {storage_bytes, score, source_tags} for R2-synced videos
- [x] T012 Add new consumer methods to src/services/db.py: `fetch_downloaded_jobs(channel_id)` — returns jobs with status=downloaded; `fetch_overflow_jobs(channel_id)` — returns overflow jobs ordered by score desc; `update_job_storage_bytes(job_id, storage_bytes)` — sets storage_bytes on job; `update_job_status(job_id, status)` — transitions job status; `mark_job_failed_permanent(job_id, error)` — sets status=failed for max-attempts-exceeded jobs
- [x] T013 [P] Update AppConfig defaults in src/config.py: add `overflow_limit` (default 20) to producer defaults; add `max_attempts` (default 3) to consumer defaults; remove `retention_cycles` from any default config references

**Checkpoint**: Foundation ready — all shared models, pure scoring functions, and db methods are implemented and tested. User story implementation can now begin.

---

## Phase 3: User Story 1 + User Story 3 — Plan a Channel Sync + Budget-Only Selection (Priority: P1) MVP

**Goal**: A single `sync plan` command discovers candidates from playlist + search APIs, scores them, computes the budget-driven canonical set (fresh + catalog fractions, no date-based retention), diffs against R2, and enqueues download/removal/overflow jobs — replacing the old `sync-fresh` and `sync-catalog` commands.

**Independent Test**: Run `sync plan --channel UC... --dry-run --verbose` against a channel and verify candidates discovered, scored, canonical set computed within budget, diff output correct, and no DB writes in dry-run mode.

### Tests for User Story 1

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [x] T014 [P] [US1] Unit tests for PlanCommand.process_channel() in tests/test_commands/test_plan.py: mock fetcher/db/scorer, verify discovery merges playlist + search candidates, deduplicates by video_id, tags sources correctly; verify canonical set computed via select_canonical(); verify diff produces correct download/removal/overflow job lists; verify job replacement (replace_channel_jobs called, not additive)
- [x] T015 [P] [US1] Integration tests for PlanCommand in tests/test_commands/test_plan.py: test rolling channel rotation (only fraction processed based on last_full_refresh_at); test --channel flag forces single-channel full refresh; test --dry-run produces no DB writes; test --use-cache skips API calls; test already-cached videos get score/tag updates but no new jobs

### Implementation for User Story 1

- [x] T016 [US1] Create src/commands/plan.py with PlanCommand class: constructor takes `(config, db, scorer, fetcher)` following existing DI pattern. Implement `run(channel_id, dry_run, verbose, use_cache)` orchestrating: channel rotation selection, per-channel processing, summary logging. Implement `_bootstrap()` function creating AppConfig, SyncDatabase, VideoScorer, VideoFetcher instances.
- [x] T017 [US1] Implement PlanCommand.process_channel() in src/commands/plan.py: Step 1 (Discover) — call `fetcher.fetch_playlist()` + `fetcher.fetch_search_pair()`, merge and deduplicate by video_id, tag sources (recent/popular/rated). Step 2 (Canonical) — call `scorer.select_canonical()` to get canonical set + overflow list. Step 3 (Diff) — call `db.fetch_existing_r2_with_bytes()`, compute must_download/must_evict/already_cached sets, build job dicts with score/estimated_bytes/priority, call `db.replace_channel_jobs()`.
- [x] T018 [US1] Implement rolling channel rotation in src/commands/plan.py: when no --channel flag, select channels where last_full_refresh_at is oldest (fraction = config full_refresh_percentage). All channels get playlist discovery; only rotation subset gets search API (popular+rated). After processing, call `db.update_full_refresh_timestamp()` on processed channels.
- [x] T019 [US1] Implement `main()` entry point in src/commands/plan.py: argparse with --channel, --dry-run, --verbose, --use-cache flags. Call _bootstrap() then PlanCommand.run(). Follow pattern from existing fresh.py main().
- [x] T020 [US1] Register `sync-plan` entry point in pyproject.toml: add `sync-plan = "src.commands.plan:main"` to `[project.scripts]`

**Checkpoint**: `sync plan` is fully functional. Run `uv run sync-plan --dry-run --verbose` to verify discovery, scoring, canonical set, diff, and enqueue logic. Budget-only selection (US3) is exercised through the canonical set computation — no date-based retention anywhere in the plan path.

---

## Phase 4: User Story 2 — Process Queued Downloads and Removals (Priority: P1)

**Goal**: A rewritten `sync process` command executes four sequential phases: purge (removals first to free space), download+measure (actual bytes), reconcile (budget math with real bytes, backfill from overflow), upload (admitted candidates to R2).

**Independent Test**: Seed sync_queue with known jobs, run `sync process --dry-run --verbose`, verify phases execute in order: purge first, then download, then reconcile (correct admit/skip decisions), then upload.

### Tests for User Story 2

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [x] T021 [P] [US2] Unit tests for ProcessCommand._purge() in tests/test_commands/test_process.py: mock db/storage, verify removal jobs claimed first (priority=10), R2 objects deleted, video records deleted, jobs completed
- [x] T022 [P] [US2] Unit tests for ProcessCommand._download_all() in tests/test_commands/test_process.py: mock hls/db, verify download+remux per job, storage_bytes measured and written to job, status transitions to downloaded, failure handling (attempts incremented, permanent failure at max_attempts)
- [x] T023 [P] [US2] Unit tests for ProcessCommand._reconcile() in tests/test_commands/test_process.py: mock db, verify calls reconcile_budget() from scoring.py with correct inputs, verify backfill candidates downloaded one-by-one, verify admitted/skipped lists correct
- [x] T024 [P] [US2] Unit tests for ProcessCommand._upload() in tests/test_commands/test_process.py: mock storage/db, verify upload per admitted candidate, storage_bytes written to video record, jobs completed, skipped candidates cleaned up
- [x] T025 [US2] Integration test for full four-phase flow in tests/test_commands/test_process.py: mock all services, seed queue with removals + downloads, verify phase ordering (purge before download before reconcile before upload), verify budget-correct final state

### Implementation for User Story 2

- [x] T026 [US2] Rewrite src/commands/process.py ProcessCommand class: update constructor to `(config, db, storage, hls, staging_dir)`. Implement `run(limit, dry_run, verbose, downloads_only, removals_only)` orchestrating four sequential phases with explicit data passing between them.
- [x] T027 [US2] Implement ProcessCommand._purge() in src/commands/process.py: claim all action=remove jobs (priority=10 claimed first), for each: delete R2 objects via storage, delete video record via db, complete job. Log count removed. Skip if --downloads-only.
- [x] T028 [US2] Implement ProcessCommand._download_all() in src/commands/process.py: claim action=download jobs up to --limit, for each: download via hls.download_video_tiers(), remux via hls.remux_to_hls(), measure actual bytes (sum file sizes in staging dir), update job storage_bytes and status=downloaded via db. On failure: increment attempts, mark failed if >= max_attempts. Return list[DownloadResult]. Skip if --removals-only.
- [x] T029 [US2] Implement ProcessCommand._reconcile() in src/commands/process.py: per channel, load existing R2 state via db.fetch_existing_r2_with_bytes(), load downloaded jobs, load overflow jobs. Call scoring.reconcile_budget(). For backfill candidates in result: download+measure one-by-one, add to admitted if fits budget. Mark skipped jobs status=skipped. Return final admitted list.
- [x] T030 [US2] Implement ProcessCommand._upload() in src/commands/process.py: for each admitted DownloadResult: generate master playlist via hls, upload HLS package to R2 via storage, upsert video record with storage_bytes + r2_synced_at via db, complete job, clean up local staging. For skipped: delete local staging, mark job skipped.
- [x] T031 [US2] Update main() in src/commands/process.py: update argparse (keep --limit, --dry-run, --verbose, --downloads-only, --removals-only), update bootstrap to pass max_attempts from config, update entry point wiring. Existing `sync-process` entry point in pyproject.toml already points here.

**Checkpoint**: `sync process` is fully functional with four-phase architecture. Run `uv run sync-process --dry-run --verbose` to verify purge, download, reconcile, upload phases execute correctly with budget math using actual bytes.

---

## Phase 5: User Story 4 — Archive Mode (Priority: P3)

**Goal**: Channels with `sync_mode='archive'` skip removal logic entirely. All discovered videos are canonical. Budget is advisory — no eviction, no trimming.

**Independent Test**: Configure a test channel as archive mode, run `sync plan --channel UC... --dry-run`, verify zero removal jobs and all candidates enqueued for download. Run `sync process --dry-run`, verify no budget-based trimming.

### Tests for User Story 4

- [x] T032 [P] [US4] Test archive mode in plan in tests/test_commands/test_plan.py: verify process_channel with is_archive=True produces zero removal jobs, all discovered videos in canonical set regardless of budget, overflow list empty
- [x] T033 [P] [US4] Test archive mode in process in tests/test_commands/test_process.py: verify _reconcile with archive channel admits all downloaded candidates without budget trimming, no backfill attempted

### Implementation for User Story 4

- [x] T034 [US4] Add archive mode handling to PlanCommand.process_channel() in src/commands/plan.py: when channel.is_archive, canonical set = all discovered candidates (no budget filtering), no removal jobs enqueued, overflow list empty
- [x] T035 [US4] Add archive mode handling to ProcessCommand._reconcile() in src/commands/process.py: when channel is archive, skip budget trimming — admit all downloaded candidates. Skip backfill (no overflow for archive channels).

**Checkpoint**: Archive mode channels accumulate content without eviction. Verified by zero removal jobs across plan runs for archive channels.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Clean up old commands, update entry points, final validation.

- [x] T036 Delete src/commands/fresh.py and src/commands/catalog.py (absorbed into plan.py)
- [x] T037 Remove `sync-fresh` and `sync-catalog` entry points from pyproject.toml `[project.scripts]`
- [x] T038 Apply Supabase migration: drop `retention_cycles` column from `curated_channels` table (safe now that old commands are deleted)
- [x] T039 Update tests: remove or update tests/test_producer.py references to FreshCommand/CatalogCommand, ensure all existing tests pass with new code
- [x] T040 [P] Run full test suite (`uv run pytest`) and fix any failures
- [x] T041 [P] Run linter and type checker (`uv run ruff check src/ tests/` and `uv run mypy`) and fix any issues
- [x] T042 Run quickstart.md validation: execute `uv run sync-plan --dry-run --verbose` and `uv run sync-process --dry-run --verbose` to verify end-to-end flow

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup (migrations must be applied first) — BLOCKS all user stories
- **US1+US3 (Phase 3)**: Depends on Foundational (models, scoring, db methods)
- **US2 (Phase 4)**: Depends on Foundational. Can run in parallel with Phase 3 if desired (different files: plan.py vs process.py)
- **US4 (Phase 5)**: Depends on Phase 3 + Phase 4 (adds behavior to both plan and process)
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **US1+US3 (Plan + Budget Selection)**: Can start after Foundational — no dependency on US2
- **US2 (Process)**: Can start after Foundational — no dependency on US1 (uses jobs already in queue)
- **US4 (Archive Mode)**: Depends on US1 and US2 (adds conditional behavior to both)

### Within Each User Story

- Tests MUST be written and FAIL before implementation (TDD per constitution)
- Models/scoring before command logic
- Core implementation before CLI wiring
- Story complete before moving to next priority

### Parallel Opportunities

- T003 + T004 (config files) can run in parallel
- T005 + T006 + T007 (foundational tests) can run in parallel
- T008 + T009 + T010 + T013 (foundational implementation across different files) can run in parallel
- T014 + T015 (US1 tests) can run in parallel
- T021 + T022 + T023 + T024 (US2 tests) can run in parallel
- **Phase 3 (US1) and Phase 4 (US2) can run in parallel** — they touch different command files
- T032 + T033 (US4 tests) can run in parallel
- T040 + T041 (lint + test) can run in parallel

---

## Parallel Example: Foundational Phase

```bash
# Launch all foundational tests together (TDD — write first, verify fail):
Task T005: "Test select_canonical() in tests/test_scoring_class.py"
Task T006: "Test reconcile_budget() in tests/test_reconciliation.py"
Task T007: "Test new db methods in tests/test_services/test_db.py"

# Launch foundational implementation across different files:
Task T008: "Update dataclasses in src/models.py"
Task T009: "Add select_canonical() to src/scoring.py"
Task T010: "Add reconcile_budget() to src/scoring.py"
Task T013: "Update AppConfig defaults in src/config.py"
```

## Parallel Example: US1 + US2 in Parallel

```bash
# After Foundational is complete, both can start simultaneously:

# Developer A: US1 (plan command)
Task T016: "Create src/commands/plan.py with PlanCommand"
Task T017: "Implement process_channel()"
...

# Developer B: US2 (process command)
Task T026: "Rewrite src/commands/process.py"
Task T027: "Implement _purge()"
...
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (migrations + config)
2. Complete Phase 2: Foundational (models, scoring, db)
3. Complete Phase 3: US1+US3 (plan command)
4. **STOP and VALIDATE**: Run `sync-plan --dry-run --verbose` end-to-end
5. Old commands still work — no breaking changes yet

### Incremental Delivery

1. Setup + Foundational -> Foundation ready
2. Add US1+US3 (Plan) -> Test independently -> `sync-plan` works (MVP!)
3. Add US2 (Process) -> Test independently -> `sync-process` works with new phases
4. Add US4 (Archive Mode) -> Test independently -> Archive channels handled
5. Polish -> Delete old commands, clean up, full validation
6. Each phase adds value without breaking previous phases

### Parallel Team Strategy

With two developers:

1. Both complete Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1+US3 (plan command) in Phase 3
   - Developer B: US2 (process command) in Phase 4
3. Both complete US4 (small additions to both files)
4. Both complete Polish

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- TDD is mandatory per project constitution — write tests first, verify they fail
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
