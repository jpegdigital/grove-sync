## Project Overview

Grove Sync is a two-stage YouTube video pipeline that discovers, downloads, transcodes, and hosts curated channel content as HLS streams on Cloudflare R2.

## Architecture

**Commands** (`src/commands/`) — Each command is a class with constructor-injected dependencies:

- `sync.py` — `SyncCommand`: Unified per-channel pipeline that fetches the full uploads playlist, calibrates channel stats (cadence, duration distribution), scores eligible videos with ESE, updates scores on existing R2 content, deduplicates against R2, and enqueues download jobs. Replaces the former separate `calibrate` and `plan` commands.
- `process.py` — `ProcessCommand`: Per-channel video-at-a-time budget loop. For each tier (fresh, catalog), downloads one video at a time, makes inline budget decisions (upload, evict existing, or skip), then cleans up remaining pending jobs.

**Services** (`src/services/`) — Reusable infrastructure, injected into commands:

- `youtube.py` — `YouTubeClient`: YouTube Data API v3 with retry, batching, quota tracking.
- `video_fetcher.py` — `VideoFetcher`: Cache-capable fetching helper kept for experimentation; the current `sync-plan` CLI uses `YouTubeClient` directly.
- `db.py` — `SyncDatabase`: Supabase operations (queue, metadata, channel config).
- `hls.py` — `HlsPipeline`: yt-dlp download, ffmpeg remux, master playlist generation.
- `storage.py` — `R2Storage`: Cloudflare R2 uploads/deletions via S3-compatible API.

**Shared** (`src/`) — Cross-cutting concerns:

- `config.py` — `AppConfig`: Env loading, YAML config with deep merge, typed section accessors.
- `scoring.py` — `VideoScorer`: Duration filtering plus ESE scoring based on likes, comments, and estimated storage. `select_canonical()` returns the full scored universe for the plan phase. Pure functions, no I/O.
- `models.py` — Dataclasses for `Video`, `ChannelConfig`, `SyncJob`, `DownloadResult`, `ChannelResult`.

**Infrastructure:**
- Supabase tables: `curated_channels`, `channels`, `videos`, `sync_queue`, `channel_calibration`, `creators`, `profiles`, `user_subscriptions`, `watch_sessions`
- Config: YAML files in `config/` with hardcoded defaults, deep-merged at load time
- Environment: `.env` file loaded by custom parser (no dotenv dependency)
- R2 key structure: `{handle}/{YYYY}-{MM}/{video_id}.{ext}`

## Commands

```bash
# Install dependencies (uses uv)
uv sync

# Sync: fetch, calibrate, score, dedup, enqueue downloads
uv run sync                              # rolling channels (default batch)
uv run sync --channel UC...              # single channel, full refresh
uv run sync --all                        # all curated channels
uv run sync --dry-run --verbose          # preview without DB writes

# Process: per-channel download → budget decisions → upload/evict
uv run sync-process                      # process all pending work
uv run sync-process --channel UC...      # single channel
uv run sync-process --limit 20           # cap downloads this run
uv run sync-process --dry-run --verbose  # preview

# Run tests
uv run pytest
```

## Key Dependencies

- **yt-dlp**: Video downloading with cookie auth (`config/cookies.txt`) and remote JS solver for bot detection
- **ffmpeg**: Required on PATH for HLS remuxing (consumer validates at startup)
- **boto3**: Cloudflare R2 uploads via S3-compatible API
- **supabase**: Database client for job queue and metadata

## Environment Variables

Defined in `.env` (gitignored): `YOUTUBE_API_KEY`, `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`, `SUPABASE_SECRET_KEY`, `DATABASE_URL`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`, `NEXT_PUBLIC_R2_PUBLIC_URL`.

## Conventions

- **File naming**: Always `snake_case.py` for modules (e.g., `video_fetcher.py`, not `VideoFetcher.py`). Classes inside use `PascalCase` as normal Python convention.
- **Dependency injection**: All command and service classes receive dependencies via constructor arguments. Single service instances are created in bootstrap code and injected.
