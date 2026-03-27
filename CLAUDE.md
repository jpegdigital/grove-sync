# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Grove Sync is a two-stage YouTube video pipeline that discovers, downloads, transcodes, and hosts curated channel content as HLS streams on Cloudflare R2.

## Architecture

**Producer** (`src/sync_producer.py`) — Discovers videos from curated YouTube channels and enqueues sync jobs into Supabase.

- Two run modes: `recent` (daily, playlist-only, ~326 quota/52 channels) and `full` (weekly, popular+rated+recent sources, ~5,500 quota/52 channels)
- Scores videos using weighted popularity (log10 views) + engagement (like/comment rate) + freshness (exponential decay with configurable half-life)
- Rolling refresh: in `full` mode, only a configurable fraction of channels get full treatment per run (rotated by `last_full_refresh_at`)
- Computes diff against existing `synced_videos` table, enqueues `download` and `remove` actions into `sync_queue`

**Consumer** (`src/sync_consumer.py`) — Claims jobs from `sync_queue`, downloads via yt-dlp, transcodes to multi-bitrate HLS via ffmpeg, uploads to R2, and upserts metadata.

- Downloads each video at multiple quality tiers (480p, 720p by default), remuxes to fMP4 HLS segments
- Uploads HLS packages (master playlist + per-tier playlists + segments + sidecars) to Cloudflare R2
- Handles both `download` and `remove` actions with job locking and retry logic
- R2 key structure: `{handle}/{YYYY}-{MM}/{video_id}.{ext}`

**Shared infrastructure:**
- Supabase tables: `curated_channels`, `channels`, `synced_videos`, `sync_queue`
- Config: YAML files in `config/` with hardcoded defaults in each script (config file values override defaults)
- Environment: `.env` file loaded by a simple custom parser (no dotenv dependency)

## Commands

```bash
# Install dependencies (uses uv)
uv sync

# Run producer (discovers videos, enqueues jobs)
uv run sync-producer --mode recent          # daily: playlist only
uv run sync-producer --mode full            # weekly: all sources
uv run sync-producer --channel UC... --verbose  # single channel debug
uv run sync-producer --dry-run              # preview without DB writes

# Run consumer (processes download/remove jobs)
uv run sync-consumer
uv run sync-consumer --limit 10 --verbose
uv run sync-consumer --dry-run
uv run sync-consumer --downloads-only
uv run sync-consumer --removals-only

# Run tests
uv run pytest
```

## Key Dependencies

- **yt-dlp**: Video downloading with cookie auth (`config/cookies.txt`) and remote JS solver for bot detection
- **ffmpeg**: Required on PATH for HLS remuxing (consumer validates at startup)
- **boto3**: Cloudflare R2 uploads via S3-compatible API
- **supabase**: Database client for job queue and metadata

## Environment Variables

Defined in `.env` (gitignored): `YOUTUBE_API_KEY`, `NEXT_PUBLIC_SUPABASE_URL`, `SUPABASE_SECRET_KEY`, `DATABASE_URL`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`, `NEXT_PUBLIC_R2_PUBLIC_URL`.

## Conventions

- All config tunables live in `config/producer.yaml` and `config/consumer.yaml` — no magic numbers in code
- Both scripts use `PROJECT_ROOT = Path(__file__).resolve().parent.parent` for path resolution
- Windows UTF-8 console fix is applied at module load in both scripts
- YouTube API quota is tracked and logged; producer warns when cumulative usage exceeds `quota.warn_threshold`
