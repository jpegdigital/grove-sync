# Tiered Content Sync Architecture

> **SUPERSEDED** by [`sync-pipeline.md`](./sync-pipeline.md). The separate
> fresh/catalog tier model with date-based retention windows has been replaced
> by a unified cache model with budget-only selection and full-pool
> reconciliation using actual measured bytes. This document is retained for
> historical reference only.

## Overview

Grove Sync uses a **two-tier system** to manage video content per channel:

- **Catalog tier** — Stable, high-value content selected by ESE scoring. Only modified by `full` mode runs (weekly).
- **Fresh tier** — Recent uploads, budget-driven with a retention ceiling. Only modified by `recent` mode runs (daily).

Each tier has its own storage budget and selection logic. The tiers are independent: recent runs never touch catalog videos, and full runs never touch fresh videos. This prevents the historical problem where daily runs evicted videos placed by weekly full runs.

**Archive channels** (`sync_mode = 'archive'`) are exempt from tiers, duration caps, and removal. All content is kept indefinitely.

## Data Model

### `curated_channels` — Per-Channel Sync Config

Every sync tunable is a column on `curated_channels` with a DB DEFAULT, so new rows get sensible values automatically. The producer reads these directly — no YAML fallback.

| Column | Type | Default | Description |
|--------|------|---------|-------------|
| `storage_budget_gb` | NUMERIC(6,2) | 10.0 | Total storage budget for this channel (catalog + fresh) |
| `catalog_fraction` | NUMERIC(3,2) | 0.60 | Fraction of budget allocated to catalog tier (rest goes to fresh) |
| `scoring_alpha` | NUMERIC(3,2) | 0.30 | ESE alpha — controls duration sensitivity in scoring |
| `retention_cycles` | INT | 30 | Fresh tier max retention = `median_gap_days * retention_cycles` |
| `min_duration_seconds` | INT | 60 | Minimum video duration (seconds) |
| `max_duration_seconds` | INT | 3600 | Maximum video duration (seconds). 0 = no cap |
| `sync_mode` | TEXT | 'sync' | One of: `sync`, `archive` |
| `date_range_override` | TEXT | NULL | Custom date window (e.g. `"today-2years"`, `"all"`) |

**Budget split example** (defaults): 10 GB total, 60% catalog = 6 GB catalog + 4 GB fresh.

### `videos.sync_tier` — Tier Assignment

Each synced video has a `sync_tier` column:

| Value | Meaning |
|-------|---------|
| `'catalog'` | Selected by full-mode ESE scoring. Stable across runs. |
| `'fresh'` | Selected by recent-mode retention window. Rotates with new uploads. |
| `NULL` | Legacy pre-migration video (treated as untiered). |

Indexed: `(channel_id, sync_tier) WHERE r2_synced_at IS NOT NULL`

### `channel_calibration` — Posting Frequency Data

Populated by `scripts/calibration.py`. The producer reads `median_gap_days` to compute fresh retention windows.

| Column | Type | Description |
|--------|------|-------------|
| `channel_id` | TEXT (PK) | FK to `channels.youtube_id` |
| `median_gap_days` | NUMERIC(6,1) | Median days between uploads |
| `posts_per_week` | NUMERIC(6,2) | Posting frequency |
| `avg_duration_seconds` | INT | Average video duration |
| `median_duration_seconds` | INT | Median video duration |
| `total_videos_sampled` | INT | Total videos analyzed |
| `videos_in_date_range` | INT | Videos within configured date range |
| `passing_min60` | INT | Videos passing 60s min filter |
| `passing_min60_max3600` | INT | Videos passing 60s-3600s filter |
| `passing_min300` | INT | Videos passing 300s min filter |
| `passing_min300_max3600` | INT | Videos passing 300s-3600s filter |
| `duration_buckets` | JSONB | Duration distribution histogram |

## Scoring

### ESE (Engagement-Storage Efficiency)

```
ese_score = raw_score / storage_gb ^ alpha
```

- `raw_score` = weighted combination of popularity (log10 views), engagement (like/comment rate), and freshness (exponential decay with 90-day half-life)
- `storage_gb` = estimated HLS storage: `duration_seconds * 3.9 Mbps / 8 / 1024`
- `alpha` = 0.3 by default (per-channel override via `scoring_alpha`)

**Why alpha = 0.3?** Analysis showed 0.0 -> 0.3 is a massive jump (+64% engagement captured at same storage), beyond 0.3 it plateaus. This is the inflection point that moderately favors storage-efficient content without over-rewarding shorts.

### Global Scoring Weights (YAML config)

These remain in `config/producer.yaml` because they're truly global:

```yaml
scoring:
  weights:
    popularity: 0.35   # log10(views)
    engagement: 0.35   # (like_rate*0.7 + comment_rate*0.3)*100
    freshness: 0.30    # exp(-age_days * ln(2) / half_life)
  freshness_half_life_days: 90
```

## Selection Logic

### Catalog Selection (full mode)

1. Fetch popular candidates (`search.list order=viewCount`)
2. Fetch rated candidates (`search.list order=rating`)
3. Deduplicate across sources, merge `source_tags`
4. Apply `min_duration_seconds` / `max_duration_seconds` filter
5. Rank all by ESE score (`raw_score / gb^alpha`)
6. Greedy knapsack fill up to `catalog_budget` GB (`storage_budget_gb * catalog_fraction`)
7. Diff against existing catalog-tier videos only
8. Enqueue downloads/removals for catalog tier
9. Set `sync_tier='catalog'` and `source_tags` on selected videos

### Fresh Selection (recent mode)

1. Read `median_gap_days` from `channel_calibration` (defaults to 1.0 if uncalibrated)
2. Compute `max_retention_days = median_gap_days * retention_cycles`
3. Fetch recent candidates from uploads playlist
4. Exclude any videos already in catalog tier
5. Apply `min_duration_seconds` / `max_duration_seconds` filter
6. Filter out videos older than `max_retention_days`
7. Sort newest-first, fill up to `fresh_budget` GB (`storage_budget_gb * (1 - catalog_fraction)`)
8. Diff against existing fresh-tier videos only
9. Enqueue downloads/removals for fresh tier
10. Set `sync_tier='fresh'` and `source_tags=['recent']`

**Budget is the primary driver.** If videos are short, more fit in the budget and retention naturally extends. `max_retention_days` is a ceiling, not a target.

### Archive Mode

No tiers, no max_duration, no removal. All videos get `sync_tier='catalog'`. The `max_duration_seconds` is set to 0 (no cap) and `retention_cycles` to 9999.

## Storage Estimation

All budgets are in GB. Estimated HLS storage per video:

```
BITRATE_MBPS = 3.9  (480p: 1.2 + 720p: 2.5 + ~5% overhead)
estimate_gb(duration_seconds) = duration_seconds * 3.9 / 8 / 1024
```

| Duration | Estimated Storage |
|----------|------------------|
| 3 min | ~0.09 GB |
| 10 min | ~0.29 GB |
| 30 min | ~0.86 GB |
| 60 min | ~1.72 GB |
| 120 min | ~3.44 GB |

## Consumer Behavior

The consumer (`src/sync_consumer.py`) processes queue jobs regardless of tier. When processing a download job:

- Extracts `sync_tier` from job `metadata`
- Passes it to `upsert_video_record()` which writes `sync_tier` to the `videos` table
- Deletion uses `delete_video_record()` (full row DELETE, not nulling fields)

No consumer changes are needed for tier support — it just passes through what the producer enqueued.

## Config Split

| What | Where | Why |
|------|-------|-----|
| Per-channel tunables (budget, duration, retention, alpha) | `curated_channels` table (DB) | Per-channel overrides, DB defaults for new rows |
| Calibration data (posting frequency, duration profile) | `channel_calibration` table (DB) | Computed by calibration script |
| Global API settings (page size, workers, retries) | `config/producer.yaml` | Same for all channels |
| Global quota limits | `config/producer.yaml` | Same for all channels |
| Global scoring weights | `config/producer.yaml` | Same for all channels |
| Search source config | `config/producer.yaml` | Same for all channels |

## Key Functions (sync_producer.py)

| Function | Line | Purpose |
|----------|------|---------|
| `estimate_gb()` | 330 | Estimate HLS storage for a duration |
| `ese_score()` | 335 | Compute ESE: `raw_score / gb^alpha` |
| `select_catalog()` | 349 | Greedy knapsack by ESE for catalog tier |
| `select_fresh()` | 375 | Budget-driven newest-first for fresh tier |
| `passes_duration_filter()` | 510 | Min/max duration check |
| `fetch_curated_channels()` | 695 | Load all per-channel config from DB |
| `fetch_existing_videos()` | 745 | Tier-scoped query for existing synced videos |
| `update_video_tier()` | 860 | Write sync_tier + source_tags to videos table |
| `process_channel()` | 879 | Main orchestrator — routes to catalog or fresh flow |
