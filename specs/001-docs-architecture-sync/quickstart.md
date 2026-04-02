# Quickstart: Unified Sync Pipeline

**Branch**: `001-docs-architecture-sync`

## Prerequisites

- Python >=3.10, uv installed
- ffmpeg + ffprobe on PATH
- `.env` with all required variables (YouTube API key, Supabase, R2 credentials)
- Database migrations applied (see data-model.md)

## New Commands

```bash
# Plan: fetch uploads playlist, score eligible videos, diff against R2, enqueue downloads
uv run sync-plan                         # daily run, rolling channels
uv run sync-plan --channel UC...         # single channel, full refresh
uv run sync-plan --dry-run --verbose     # preview without DB writes

# Process: purge → download → reconcile → upload
uv run sync-process                      # process all pending work
uv run sync-process --limit 20           # cap downloads this run
uv run sync-process --dry-run --verbose  # preview
uv run sync-process --downloads-only     # skip removals
uv run sync-process --removals-only      # only purge

# Calibrate: unchanged
uv run sync-calibrate
```

## What Changed from Old Commands

| Old | New | Notes |
|-----|-----|-------|
| `sync-fresh` | `sync-plan` | Uploads-playlist discovery and queue replacement happen here |
| `sync-catalog` | no direct CLI equivalent | Search-based catalog discovery is not wired into the current `sync-plan` command |
| `sync-process` | `sync-process` | Same name, rewritten with 4-phase architecture |
| `sync-calibrate` | `sync-calibrate` | Unchanged |

## Key Differences

1. **No date-based retention**: Content selection is purely budget-driven. The effective time window is emergent.
2. **Actual bytes for budget**: Downloads are measured before upload decisions. No overruns from bad estimates.
3. **Playlist-only planning**: The current `sync-plan` implementation fetches the uploads playlist; search-driven catalog expansion is not part of the live CLI path.
4. **Job replacement**: Re-running `sync-plan` replaces stale pending jobs instead of duplicating.
5. **Max retry threshold**: Jobs failing 3+ times are marked permanently failed (configurable).

## Running Tests

```bash
uv run pytest                            # full suite
uv run pytest -m unit                    # unit tests only
uv run pytest tests/test_commands/test_plan.py  # plan command tests
uv run pytest tests/test_commands/test_process.py  # process command tests
```

## Typical Daily Workflow

```bash
# 1. Plan (rotates through channels, ~5-10 minutes)
uv run sync-plan

# 2. Process (downloads, reconciles, uploads, ~1-4 hours)
uv run sync-process

# 3. Check for permanently failed jobs (optional)
# Query sync_queue WHERE status = 'failed'
```
