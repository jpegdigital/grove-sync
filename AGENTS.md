# Repository Guidelines

## Project Structure & Module Organization
Core application code lives in `src/`. Use `src/commands/` for CLI entrypoints (`plan.py`, `process.py`, `calibrate.py`), `src/services/` for external integrations (YouTube, Supabase, R2, HLS), and top-level `src/*.py` for shared models, scoring, and config loading. Tests mirror that layout in `tests/`, especially `tests/test_commands/` and `tests/test_services/`. Runtime configuration lives in `config/`, architecture notes in `docs/architecture/`, implementation specs in `specs/`, and one-off operational helpers in `scripts/`.

## Build, Test, and Development Commands
Install and sync dependencies with `uv sync`. Common development commands:

- `uv run sync-plan --dry-run --verbose` checks discovery and queueing logic without writes.
- `uv run sync-process --limit 20` processes a bounded batch of pending work.
- `uv run sync-calibrate --channel UC... --max-pages 5` samples a single channel for storage tuning.
- `uv run pytest` runs the full test suite.
- `uv run pytest -m unit` runs only fast unit coverage.
- `uv run ruff check .` and `uv run ruff format .` lint and format the codebase.
- `uv run mypy src` runs static type checks on application modules.

## Coding Style & Naming Conventions
Target Python 3.10+ and keep formatting compatible with Ruff: 4-space indentation, double quotes, and a 120-character line limit. Use `snake_case.py` for modules and functions, `PascalCase` for classes, and descriptive command/service names such as `video_fetcher.py` or `PlanCommand`. Prefer constructor injection for services and keep tunable runtime values in `config/producer.yaml` or `config/consumer.yaml` instead of scattering constants in code.

## Testing Guidelines
Use `pytest` with the existing markers: `unit`, `integration`, `safety`, and `edge_cases`. Name files `test_*.py` and mirror the production module they cover, for example `tests/test_services/test_hls.py`. Add or update tests with every non-trivial behavior change; for scoring, reconciliation, and queue logic, favor narrow assertions over broad end-to-end fixtures.

## Commit & Pull Request Guidelines
Recent history uses imperative, sentence-case subjects like `Update consumer configuration...` and `Refactor bitrate extraction...`. Keep commit titles specific to the changed subsystem and avoid bundling unrelated work. PRs should include a short behavior summary, linked issue or spec when relevant, the exact validation commands you ran, and sample CLI output when a pipeline command changes. Include screenshots only for documentation or UI updates.

## Security & Configuration Tips
Do not commit `.env`, `config/cookies.txt`, downloaded media, or cache artifacts. `ffmpeg` and `ffprobe` must be available on `PATH`, and secrets for YouTube, Supabase, and R2 should stay in local environment configuration only.
