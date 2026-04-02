# Implementation Plan: Unified Sync Pipeline

> Historical design document: this spec describes the original unified-pipeline
> proposal. The current `sync-plan` implementation is narrower and playlist-only.
> For live behavior, prefer `src/commands/plan.py` and
> `docs/architecture/sync-pipeline.md`.

**Branch**: `001-docs-architecture-sync` | **Date**: 2026-04-01 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-docs-architecture-sync/spec.md`

## Summary

Replace the separate `sync-fresh` and `sync-catalog` commands with a unified two-command pipeline (`sync plan` + `sync process`). The plan command merges discovery from both playlist and search APIs, computes a budget-driven canonical set (fresh + catalog fractions), diffs against R2, and enqueues work. The process command executes four sequential phases: purge, download+measure, reconcile (with actual bytes), upload. Key changes: no date-based retention, budget-only selection, overflow list for backfill, job replacement on re-plan, configurable max retry attempts.

## Technical Context

**Language/Version**: Python >=3.10 (mypy targets 3.12)
**Primary Dependencies**: supabase, pyyaml, requests, boto3, yt-dlp, curl-cffi
**Storage**: Supabase PostgreSQL (metadata, queue) + Cloudflare R2 (HLS content via S3 API)
**Testing**: pytest with markers (unit, integration, safety, edge_cases), pytest-mock
**Target Platform**: Windows (dev), Linux server (deployment)
**Project Type**: CLI tool (entry points via pyproject.toml `[project.scripts]`)
**Performance Goals**: Process 59+ channels daily within YouTube API quota (10k units/day)
**Constraints**: YouTube API quota is the binding constraint; R2 storage budgets per-channel (typically 5-15 GB)
**Scale/Scope**: 59+ curated channels, ~250 candidate videos per channel, storage budgets of 5-50 GB per channel

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Progressive Complexity (WET/SOLID/YAGNI) | PASS | New `plan.py` consolidates two commands — justified by third consumer (unified model). No speculative abstractions. |
| II. Testing Discipline (TDD) | PASS | Plan includes tests for all new pure logic (canonical set, reconciliation, overflow). Parameterized tests for scoring/selection. |
| III. Fail Fast & Loud | PASS | Max retry threshold (3 attempts) surfaces permanently failing jobs. Plan replaces stale jobs instead of silently accumulating duplicates. |
| IV. Configuration as Data | PASS | Max attempts, overflow list size configurable in YAML. No magic numbers. |
| V. Code Style | PASS | Follows existing DI pattern (constructor injection). Composition via services. Snake_case modules, PascalCase classes. |
| VI. Anti-Patterns | PASS | No catch-all handlers, no god modules. `plan.py` is single-responsibility (planning). `process.py` is single-responsibility (execution). |

No violations to justify.

## Project Structure

### Documentation (this feature)

```text
specs/001-docs-architecture-sync/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── cli-plan.md      # sync-plan CLI contract
│   └── cli-process.md   # sync-process CLI contract
└── tasks.md             # Phase 2 output (via /speckit.tasks)
```

### Source Code (repository root)

```text
src/
├── config.py              # MODIFY: remove retention config, add overflow/max_attempts config
├── models.py              # MODIFY: add score/storage_bytes to SyncJob, remove retention_cycles from ChannelConfig
├── scoring.py             # MODIFY: add select_canonical() unifying fresh+catalog selection
├── commands/
│   ├── plan.py            # NEW: unified discovery + scoring + canonical set + diff + enqueue
│   ├── process.py         # REWRITE: four-phase (purge → download+measure → reconcile → upload)
│   ├── calibrate.py       # UNCHANGED
│   ├── fresh.py           # DELETE (absorbed into plan.py)
│   └── catalog.py         # DELETE (absorbed into plan.py)
└── services/
    ├── db.py              # MODIFY: add storage_bytes tracking, overflow persistence, job replacement
    ├── hls.py             # UNCHANGED
    ├── storage.py         # UNCHANGED
    ├── youtube.py         # UNCHANGED
    └── video_fetcher.py   # UNCHANGED

config/
├── producer.yaml          # MODIFY: add overflow_limit, max_attempts; remove retention references
└── consumer.yaml          # MODIFY: add max_attempts default

tests/
├── test_scoring_class.py  # MODIFY: add select_canonical tests
├── test_commands/
│   ├── test_plan.py       # NEW: plan command unit/integration tests
│   └── test_process.py    # NEW: rewritten process command tests
├── test_services/
│   └── test_db.py         # MODIFY: add overflow, job replacement, storage_bytes tests
└── test_reconciliation.py # NEW: budget reconciliation logic tests
```

**Structure Decision**: Single-project layout (existing). No new top-level directories. New files only where genuinely needed (`plan.py`, test files for new logic). Follows existing convention of commands/ + services/ separation.

## Complexity Tracking

No constitution violations to justify.
