# CLI Contract: sync-plan

**Entry point**: `src.commands.plan:main`
**Replaces**: `sync-fresh`, `sync-catalog`

## Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--channel` | string | (none) | Single channel ID to process. Skips rotation, forces full refresh. |
| `--dry-run` | flag | false | Preview all decisions without writing to database or queue. |
| `--verbose` | flag | false | Detailed logging of fetched, eligible, already-stored, and queued videos. |

## Behavior

1. Load config, validate producer env, bootstrap services.
2. Determine channel rotation (all channels if `--channel`, otherwise rolling fraction).
3. For each channel:
   a. **Discover**: Fetch the channel's uploads playlist from YouTube and enrich videos with duration and engagement stats.
   b. **Score**: Filter by configured duration bounds and assign an ESE score to every eligible video.
   c. **Diff**: Compare eligible videos against videos already materialized in R2.
   d. **Enqueue**: Replace all existing non-failed jobs for the channel with fresh pending download jobs for videos missing from R2.
   e. **Update**: Update scores on already-cached videos that are still eligible.
4. Update `last_full_refresh_at` on processed channels.
5. Log summary: channels processed, downloads enqueued, API quota used.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (including dry-run) |
| 1 | Unrecoverable error (missing env, config parse failure) |

## Output (stdout)

- Per-channel summary: total playlist videos, eligible videos, already-stored videos, downloads queued.
- With `--verbose`: top queued downloads by score for each channel.
- With `--dry-run`: prefixed with `[DRY RUN]`, no DB writes.
