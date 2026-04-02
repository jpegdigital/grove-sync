# Channel Configuration — UI Guide

This document describes the per-channel sync settings stored in `curated_channels` and how a UI should present them for editing.

## Editable Fields

All per-channel sync config lives in the `curated_channels` table. Each field has a DB default so channels work without manual configuration.

### Core Settings

| Field | Column | Type | Default | UI Control | Notes |
|-------|--------|------|---------|------------|-------|
| Sync Mode | `sync_mode` | TEXT | `'sync'` | Dropdown: `sync` / `archive` | Archive disables removal, duration caps, and tiers |
| Storage Budget | `storage_budget_gb` | NUMERIC(6,2) | 10.0 | Number input (GB), step 0.5, min 1.0 | Total budget split between catalog + fresh |
| Date Range | `date_range_override` | TEXT | NULL | Text input or preset dropdown | e.g. `"today-6months"`, `"today-2years"`, `"all"`. NULL = global default |

### Tier Budget Split

| Field | Column | Type | Default | UI Control | Notes |
|-------|--------|------|---------|------------|-------|
| Catalog Fraction | `catalog_fraction` | NUMERIC(3,2) | 0.60 | Slider 0.0–1.0, or split visualization | Portion of budget for catalog (stable). Fresh gets the remainder. |

**Derived display values** (computed, not stored):
- Catalog budget: `storage_budget_gb * catalog_fraction` GB
- Fresh budget: `storage_budget_gb * (1 - catalog_fraction)` GB

Consider showing a stacked bar or split visualization so the user can see the budget allocation at a glance.

### Scoring

| Field | Column | Type | Default | UI Control | Notes |
|-------|--------|------|---------|------------|-------|
| Scoring Alpha | `scoring_alpha` | NUMERIC(3,2) | 0.30 | Slider 0.0–1.0, step 0.05 | Controls duration sensitivity in catalog selection |

**Alpha explainer for UI tooltip/help:**
- 0.0 = Pure engagement (ignores storage cost, favors long popular videos)
- 0.3 = Balanced (recommended — good engagement per GB)
- 0.5 = Square root normalization (strongly favors shorter content)
- 1.0 = Pure value-per-GB (over-rewards shorts)

### Duration Filters

| Field | Column | Type | Default | UI Control | Notes |
|-------|--------|------|---------|------------|-------|
| Min Duration | `min_duration_seconds` | INT | 60 | Number input (seconds), or formatted as mm:ss | Videos shorter than this are excluded |
| Max Duration | `max_duration_seconds` | INT | 3600 | Number input (seconds), or formatted as hh:mm:ss | Videos longer than this are excluded. 0 = no cap |

**Display format:** Show as human-readable duration (e.g. "1 min", "60 min", "No limit") with the raw seconds as the stored value.

## Read-Only / Computed Fields

These come from other tables and should be displayed but not directly editable in the channel config UI.

### From `channel_calibration`

| Field | Column | Source Table | Description |
|-------|--------|-------------|-------------|
| Posting Frequency | `posts_per_week` | `channel_calibration` | How often this channel uploads |
| Median Gap | `median_gap_days` | `channel_calibration` | Median days between uploads |
| Avg Duration | `avg_duration_seconds` | `channel_calibration` | Average video duration |
| Median Duration | `median_duration_seconds` | `channel_calibration` | Median video duration |
| Videos Sampled | `total_videos_sampled` | `channel_calibration` | Total videos analyzed by calibration |
| Last Calibrated | `calibrated_at` | `channel_calibration` | When calibration last ran |

**Join:** `channel_calibration.channel_id = curated_channels.channel_id`

### From `videos` (aggregated)

These should be computed on-the-fly or cached:

| Metric | Query | Description |
|--------|-------|-------------|
| Total Videos | `COUNT(*) WHERE channel_id=X AND r2_synced_at IS NOT NULL` | Current synced video count |
| Total Storage | `SUM(storage_bytes) WHERE channel_id=X AND r2_synced_at IS NOT NULL` | Actual storage used (bytes) |

Show as a usage summary: "Using X.X / Y.Y GB (Z videos)". The `storage_bytes` column contains actual measured bytes from the download+upload pipeline — no estimation needed.

## Archive Mode Behavior

When `sync_mode = 'archive'`:
- `max_duration_seconds` should display as "No limit" (DB value is 0)
- `storage_budget_gb` is typically set higher (e.g. 50 GB) but is advisory only
- Removal and eviction are disabled — all synced content is kept
- Budget enforcement is skipped — all downloaded content is uploaded
- The catalog/fresh split is irrelevant (everything is kept regardless)

Consider graying out or hiding tier-split controls when archive mode is selected.

## Supabase Queries

### Fetch channel config for edit form

```sql
SELECT
  cc.id,
  cc.channel_id,
  cc.sync_mode,
  cc.date_range_override,
  cc.storage_budget_gb,
  cc.catalog_fraction,
  cc.scoring_alpha,
  cc.min_duration_seconds,
  cc.max_duration_seconds,
  cc.display_order,
  c.title,
  c.custom_url,
  cal.median_gap_days,
  cal.posts_per_week,
  cal.avg_duration_seconds,
  cal.median_duration_seconds,
  cal.total_videos_sampled,
  cal.calibrated_at
FROM curated_channels cc
JOIN channels c ON c.youtube_id = cc.channel_id
LEFT JOIN channel_calibration cal ON cal.channel_id = cc.channel_id
WHERE cc.id = :id
```

### Update channel config

```sql
UPDATE curated_channels
SET
  sync_mode = :sync_mode,
  storage_budget_gb = :storage_budget_gb,
  catalog_fraction = :catalog_fraction,
  scoring_alpha = :scoring_alpha,
  min_duration_seconds = :min_duration_seconds,
  max_duration_seconds = :max_duration_seconds,
  date_range_override = :date_range_override
WHERE id = :id
```

### Fetch storage usage stats

```sql
SELECT
  COUNT(*) as video_count,
  COALESCE(SUM(storage_bytes), 0) as total_storage_bytes
FROM videos
WHERE channel_id = :channel_id
  AND r2_synced_at IS NOT NULL
```

## UI Layout Suggestion

```
┌─────────────────────────────────────────────────┐
│ Channel: Disney Junior (@disneyjunior)          │
│ Calibrated: 2026-03-28 · Posts 5.2/week · Avg 8m│
├─────────────────────────────────────────────────┤
│ Mode:  [sync ▼]                                 │
│                                                 │
│ Storage Budget: [10.0] GB                       │
│ ┌─catalog 6.0 GB──┬──fresh 4.0 GB─┐            │
│ │████████████████████████ 6.3 used │            │
│ └──────────────────────────────────┘            │
│ Catalog/Fresh Split: [====●=====] 60%           │
│                                                 │
│ Duration: [60]s min  —  [3600]s max             │
│ Scoring Alpha: [====●=====] 0.30                │
│                                                 │
│ Date Range: [today-6months] (override)          │
│                                                 │
│ ── Current Content ──                           │
│ Total: 60 videos (6.3 / 10.0 GB)               │
└─────────────────────────────────────────────────┘
```
