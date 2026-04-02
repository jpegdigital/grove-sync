# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Grove Sync is a two-stage YouTube video pipeline that discovers, downloads, transcodes, and hosts curated channel content as HLS streams on Cloudflare R2.

## Architecture

**Commands** (`src/commands/`) — Each command is a class with constructor-injected dependencies:

- `plan.py` — `PlanCommand`: Fetches each channel's uploads playlist, enriches videos via YouTube, filters by duration, scores eligible videos with ESE, deduplicates against R2, and enqueues download jobs for videos not already stored. No budget filtering happens here.
- `process.py` — `ProcessCommand`: Per-channel video-at-a-time budget loop. For each tier (fresh, catalog), downloads one video at a time, makes inline budget decisions (upload, evict existing, or skip), then cleans up remaining pending jobs.
- `calibrate.py` — `CalibrateCommand`: Channel sampling for storage budget calibration.

**Services** (`src/services/`) — Reusable infrastructure, injected into commands:

- `youtube.py` — `YouTubeClient`: YouTube Data API v3 with retry, batching, quota tracking.
- `video_fetcher.py` — `VideoFetcher`: Cache-capable fetching helper kept for experimentation; the current `sync-plan` CLI uses `YouTubeClient` directly.
- `db.py` — `SyncDatabase`: Supabase operations (queue, metadata, channel config).
- `hls.py` — `HlsPipeline`: yt-dlp download, ffmpeg remux, master playlist generation.
- `storage.py` — `R2Storage`: Cloudflare R2 uploads/deletions via S3-compatible API.

**Shared** (`src/`) — Cross-cutting concerns:

- `config.py` — `AppConfig`: Env loading, YAML config with deep merge, typed section accessors.
- `scoring.py` — `VideoScorer`: Duration filtering plus ESE scoring based on likes, comments, and estimated storage. `select_canonical()` returns the full scored universe for the plan phase. Pure functions, no I/O.
- `models.py` — Dataclasses for `Video`, `ChannelConfig`, `SyncJob`, `ChannelResult`, `DownloadResult`, `ReconciliationResult` (includes `evictions` field for existing R2 content to remove).

**Infrastructure:**
- Supabase tables: `curated_channels`, `channels`, `synced_videos`, `sync_queue`
- Config: YAML files in `config/` with hardcoded defaults, deep-merged at load time
- Environment: `.env` file loaded by custom parser (no dotenv dependency)
- R2 key structure: `{handle}/{YYYY}-{MM}/{video_id}.{ext}`

## Commands

```bash
# Install dependencies (uses uv)
uv sync

# Plan: fetch uploads playlist, score eligible videos, diff against R2, enqueue
uv run sync-plan                         # daily run, rolling channels
uv run sync-plan --channel UC...         # single channel, full refresh
uv run sync-plan --dry-run --verbose     # preview without DB writes

# Process: per-channel download → budget decisions → upload/evict
uv run sync-process                      # process all pending work
uv run sync-process --limit 20           # cap downloads this run
uv run sync-process --dry-run --verbose  # preview

# Calibrate channel storage budgets
uv run sync-calibrate
uv run sync-calibrate --channel UC... --max-pages 5

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

- **File naming**: Always `snake_case.py` for modules (e.g., `video_fetcher.py`, not `VideoFetcher.py`). Classes inside use `PascalCase` as normal Python convention.
- **Dependency injection**: All command and service classes receive dependencies via constructor arguments. Single service instances are created in bootstrap code and injected.
- All config tunables live in `config/producer.yaml` and `config/consumer.yaml` — no magic numbers in code
- `PROJECT_ROOT = Path(__file__).resolve().parent.parent` for path resolution
- Windows UTF-8 console fix is applied at module load
- YouTube API quota is tracked and logged; producer warns when cumulative usage exceeds `quota.warn_threshold`
- **Playlist-only plan path**: `sync-plan` currently paginates the uploads playlist directly through `YouTubeClient`. Search-based discovery and `VideoFetcher` cache mode are not active in the CLI path.
- **ESE score**: `VideoScorer` uses `log10(likes * 0.7 + comments * 0.3 + 1) / estimated_gb(duration) ^ alpha` after duration filtering.
- **Plan = full universe**: `select_canonical()` scores and returns ALL eligible candidates with no budget filtering. Budget decisions require actual bytes and are deferred entirely to process reconciliation.
- **Reconciliation lives in process**: `sync-process` owns budget enforcement after download measurement. Keep plan logic focused on discovery, scoring, deduplication, and queue replacement.
- **No overflow list at plan time**: Plan enqueues downloads for everything not in R2. There's no budget cutoff to produce overflow. Backfill candidates are identified by reconciliation from pending download jobs.
- **Per-channel process**: `sync-process` iterates channels, runs fresh tier then catalog tier. Each tier downloads one video at a time with inline budget decisions — evictions happen immediately when needed to make room.
- **Job replacement**: `sync-plan` re-runs replace all non-failed pending jobs for a channel with fresh download jobs. Failed jobs are preserved for operator review.
- **Max retry threshold**: Configurable `max_attempts` (default 3). Jobs exceeding this are marked permanently failed.
- **Archive mode**: Channels with `sync_mode='archive'` skip all budget enforcement; reconciliation admits everything without trimming or eviction.
- **Bootstrap pattern**: `_bootstrap()` in `plan.py` creates all shared service instances (`AppConfig`, `SyncDatabase`, `VideoScorer`, `YouTubeClient`) and returns them for injection into commands.

# PradoTube Constitution

## Core Acronyms

| Principle | Meaning |
|-----------|---------|
| **SOLID** | Single Responsibility, Open/Closed, Liskov Substitution, Interface Segregation, Dependency Inversion |
| **DI** | Dependency Injection |
| **IoC** | Inversion of Control |
| **DRY** | Don't Repeat Yourself |
| **WET** | Write Everything Twice |
| **SLAP** | Single Level of Abstraction Principle |
| **KISS** | Keep It Simple, Stupid |
| **AHA** | Avoid Hasty Abstraction |
| **YAGNI** | You Ain't Gonna Need It |

## Core Principles

### I. Progressive Complexity

Code MUST earn its abstractions through demonstrated need, following a
phased approach:

1. **WET phase** — Write Everything Twice. Inline and duplicate freely
   until patterns emerge from real usage. Three similar blocks of code
   are better than a premature abstraction.
2. **SOLID phase** — When a third instance of a pattern appears,
   extract. Apply single-responsibility, open/closed, and dependency
   inversion only at this point.
3. **YAGNI phase** — Speculatively adding capability for hypothetical
   future requirements is forbidden. Every abstraction MUST have a
   concrete, current consumer.

Rules:

- Ship a thin vertical slice before broadening scope.
- If a feature can be a single-file script, it MUST remain so until
  complexity forces extraction.
- Delete dead code immediately; do not comment it out.
- No speculative abstractions, no premature generalization. A working
  script that prints output beats a polished framework that isn't
  wired up yet.

### II. Testing Discipline (NON-NEGOTIABLE)

TDD is mandatory for all non-trivial logic. The Red-Green-Refactor
cycle MUST be followed.

- **Write the test first.** Confirm it fails. Then implement.
- Tests MUST be runnable with the project's standard test command.
- Unit tests cover pure logic (parsing, transforming, filtering).
- Integration tests cover API interactions using recorded fixtures or
  mocks — never hit live APIs in CI.
- A feature is not done until its tests pass.

#### Parameterized Testing Convention

Use the test framework's parameterized/table-driven test facility for
any function with more than two meaningful input variations. Structure
test cases using the **give/want** convention with descriptive IDs:

```
// Pseudocode — adapt to your language's test framework
for each (give, want, id) in [
    ("input_a", "expected_a", "descriptive-case-a"),
    ("input_b", "expected_b", "descriptive-case-b"),
]:
    assert function(give) == want  // labeled with id
```

Examples by ecosystem:

- **pytest**: `@pytest.mark.parametrize("give, want", [...], ids=[...])`
- **Jest/Vitest**: `it.each([...])("case: %s", (give, want) => ...)`
- **Go**: table-driven tests with `t.Run(name, ...)`
- **JUnit**: `@ParameterizedTest` with `@MethodSource`

#### Coverage Strategy

- Happy path + at least one sad path per public function.
- Edge cases (empty input, null/nil/undefined, boundary values) MUST
  be covered for data-transforming functions.
- Mocks MUST be scoped as narrowly as possible — mock the boundary,
  not the internals.

### III. Fail Fast & Loud

Errors MUST surface immediately with actionable messages. Silent
failures are forbidden.

- Missing environment variables MUST raise on startup, not deep in a
  call stack.
- API errors MUST be caught, logged with context (URL, status code,
  response body), and re-raised or cause a non-zero exit.
- Never swallow errors with catch-all handlers (bare `catch`,
  `except Exception`, empty `rescue`, etc.).
- Use specific error/exception types. Catch only what you can
  meaningfully handle.

### IV. Configuration as Data

Runtime knobs MUST live in declarative configuration, not scattered
through code.

- Environment variables for secrets and deployment-specific values.
  Use the ecosystem's standard env-loading mechanism.
- Project manifest files for tool configuration and metadata (e.g.,
  `package.json`, `pyproject.toml`, `Cargo.toml`, `go.mod`).
- No magic constants buried in logic — extract to module-level
  constants, config objects, or dedicated config files.
- Feature flags (if ever needed) MUST be data-driven, not
  `if`-branches in business logic.

### V. Code Style

Write code that is idiomatic, explicit, and composable.

- **Idiomatic** — Follow the target language's community conventions
  and standard library idioms. Use built-in facilities over
  hand-rolled equivalents.
- **Explicit over implicit** — No metaprogramming tricks, no dynamic
  property injection, no catch-all parameter forwarding unless the
  API genuinely requires it.
- **Composition over inheritance** — Prefer functions, interfaces,
  and protocols over class hierarchies. Inheritance depth MUST NOT
  exceed 2 levels.
- **Colocation & single responsibility** — Each file MUST have one
  clear job. If you cannot summarize what a file does in one
  sentence, split it. Shared utilities go in a dedicated location
  only when two or more modules genuinely need them. No god-modules,
  no catch-all utility files.
- **Type safety** — Use the strongest type system available. Prefer
  structured types (interfaces, structs, typed records) for complex
  data over untyped maps/dictionaries. Use type annotations on all
  public function signatures when the language supports them.

### VI. Anti-Patterns (Banned)

| Pattern | Why Banned | Remedy |
|---------|-----------|--------|
| Catch-all error handlers | Hides bugs, masks real errors | Catch specific types |
| `TODO` without issue link | TODOs rot; no accountability | File an issue or fix now |
| God module / catch-all utils | Violates single responsibility | Split by domain |
| Deep inheritance (>2 levels) | Cognitive overhead, fragile coupling | Composition / interfaces |
| Magic strings / numbers in logic | Ungreppable, error-prone | Named constants or config |
| Wildcard / glob imports | Pollutes namespace, breaks tooling | Explicit imports only |
| Mutable shared default state | Shared state bugs across calls | Immutable defaults or fresh init |

## Development Workflow

1. **Branch per feature** — work on a descriptive branch, merge to
   `main` via PR.
2. **Write test first** — even a minimal assertion that the function
   exists and returns the expected type.
3. **Implement until green** — smallest change to make the test pass.
4. **Refactor** — clean up only what you just touched.
5. **Commit granularly** — one logical change per commit with a clear
   message.
6. **Run full suite before push** — the project's test command MUST
   pass.
7. **Lint before push** — the project's lint and format checks MUST
   pass.

### Error Handling Standards

- Use specific error/exception types; define custom types when the
  domain requires it.
- Every error handler MUST either handle, log-and-reraise, or
  translate the error — never silently swallow.
- External API calls MUST have timeouts and structured error responses.

### Idempotency & Retries

- Operations that touch external services SHOULD be idempotent where
  feasible.
- Retries (if added) MUST use exponential backoff with jitter.
- Non-idempotent side effects MUST be clearly documented.

### Audit Trail

- All external API calls SHOULD be logged at DEBUG level with request
  context (URL, method, relevant params — never secrets).
- State-changing operations SHOULD produce a log entry that allows
  reconstruction of what happened.

## Governance

This constitution is the authoritative source of engineering standards
for PradoTube. It supersedes all other conventions, defaults, and
ad-hoc practices.

- **Amendments** require documentation of the change, rationale, and
  version bump. Use semantic versioning (MAJOR for principle
  removals/redefinitions, MINOR for additions, PATCH for
  clarifications).
- **Compliance** — All PRs and code reviews MUST verify adherence to
  these principles. The plan template's Constitution Check gate
  enforces this at design time.
- **Runtime guidance** — Use `CLAUDE.md` for tool-specific development
  guidance that complements (but never contradicts) this constitution.

**Version**: 1.0.0 | **Ratified**: 2026-03-22 | **Last Amended**: 2026-03-22
