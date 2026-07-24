# Contributing to dbt-altertable

## Development Setup

1. Fork and clone the repository
2. Install dependencies: `uv sync --extra dev`
3. Run tests: `uv run pytest`

## Making Changes

1. Create a branch from `main`
2. Make your changes
3. Add or update tests
4. Run the full check suite: `uv run ruff check . && uv run ty check && uv run pytest`
5. Commit using [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, etc.)
6. Open a pull request

## Code Style

This project uses `ruff` for linting and `ruff format` for formatting. Run `uv run ruff format --check . && uv run ruff check .` before committing.

## Tests

- Unit tests are required for all new functionality
- Integration tests run in CI when credentials are available
- Run tests locally: `uv run pytest`

## Pull Requests

- Keep PRs focused on a single change
- Update `CHANGELOG.md` under `[Unreleased]`
- Ensure CI passes before requesting review
