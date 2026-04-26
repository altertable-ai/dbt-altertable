# dbt-altertable

[![CI](https://github.com/altertable-ai/dbt-altertable/actions/workflows/ci.yml/badge.svg)](https://github.com/altertable-ai/dbt-altertable/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/dbt-altertable.svg)](https://pypi.org/project/dbt-altertable/)
[![Python versions](https://img.shields.io/pypi/pyversions/dbt-altertable.svg)](https://pypi.org/project/dbt-altertable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A [dbt](https://www.getdbt.com/) adapter for [Altertable](https://altertable.ai/), backed by Arrow Flight SQL.

## Requirements

- Python **3.10+**
- dbt-core `>=1.8,<2.0`

## Installation

```bash
pip install dbt-altertable
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add dbt-altertable
```

## Configuration

Add a profile to `~/.dbt/profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: altertable
      username: your_username
      password: your_password
      database: your_database
      schema: your_schema
      host: flight.altertable.ai  # optional, this is the default
      port: 443                    # optional, this is the default
      tls: true                    # optional, this is the default
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `username` | yes | — | Altertable username |
| `password` | yes | — | Altertable password |
| `database` | yes | — | Target catalog name |
| `schema` | yes | — | Target schema name |
| `host` | no | `flight.altertable.ai` | Flight SQL endpoint host |
| `port` | no | `443` | Flight SQL endpoint port |
| `tls` | no | `true` | Use TLS for the Flight SQL connection |

## SQL dialect

dbt models should use **DuckDB-compatible SQL**. Altertable executes queries via DuckDB, so all DuckDB SQL features and functions are available — see the [DuckDB SQL reference](https://duckdb.org/docs/sql/introduction).

## Persisting model and column descriptions

When you enable [persist_docs](https://docs.getdbt.com/referen  ce/resource-configs/persist_docs), dbt writes model and column `description` values to the warehouse using DuckDB’s `COMMENT ON TABLE` / `COMMENT ON COLUMN` syntax (one statement per column so Arrow Flight SQL accepts each round-trip).

Enable it in `dbt_project.yml` or on a model:

```yaml
models:
  my_project:
    +persist_docs:
      relation: true
      columns: true
```

After a successful `dbt run`, descriptions show up on `duckdb_tables()` / `duckdb_columns()` (and therefore in `dbt docs generate` / catalog metadata).

## Development

This project is managed with [uv](https://docs.astral.sh/uv/) and [hatchling](https://hatch.pypa.io/).

```bash
git clone https://github.com/altertable-ai/dbt-altertable.git
cd dbt-altertable
uv sync --extra dev
```

Common tasks (see `Makefile`):

```bash
make lint        # ruff format + ruff check --fix
make typecheck   # ty check
make test        # pytest
make build       # uv build (wheel + sdist)
```

### Integration tests

Flight SQL integration tests (for example `persist_docs`) run against **[altertable-mock](https://github.com/altertable-ai/altertable-mock)** in CI: the workflow builds the mock from `main`, starts it with `--user dbt_ci:dbt_ci_secret`, then runs `pytest tests/integration`. The default matrix job runs unit tests only (`pytest --ignore=tests/integration`).

**Locally — Testcontainers** (requires Docker; installs [testcontainers](https://testcontainers.com/) via the `integration` extra):

```bash
ALTERTABLE_USE_TESTCONTAINERS=1 make test
```

`make test` detects that flag, runs `uv sync --extra dev --extra integration`, sets `TESTCONTAINERS_RYUK_DISABLED=true` by default (Ryuk often hangs on Docker Desktop), then runs the full pytest suite.

Or run only integration tests:

```bash
uv sync --extra dev --extra integration
export TESTCONTAINERS_RYUK_DISABLED=${TESTCONTAINERS_RYUK_DISABLED:-true}
ALTERTABLE_USE_TESTCONTAINERS=1 uv run pytest tests/integration -v
```

Optional overrides: `ALTERTABLE_MOCK_IMAGE` (default `ghcr.io/altertable-ai/altertable-mock:latest`), `ALTERTABLE_MOCK_BOOT_USER` (default `dbt_ci:dbt_ci_secret`, passed as `--user` to the mock), `ALTERTABLE_MOCK_WAIT_TIMEOUT` (seconds to wait for `Starting Flight SQL server` in logs, default `180`).

If `dbt run` fails against the published image (for example catalog or `information_schema` errors), build the mock from [source](https://github.com/altertable-ai/altertable-mock) and point tests at it: `docker build -t altertable-mock:local .` then `export ALTERTABLE_MOCK_IMAGE=altertable-mock:local` — CI builds from `main` for the same reason.

**Real Altertable** — set the same `ALTERTABLE_TEST_*` variables to your Flight endpoint, catalog, and schema; do not set `ALTERTABLE_USE_TESTCONTAINERS`.

Optional pre-commit hooks:

```bash
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
```

## Releases

Releases are managed via [release-please](https://github.com/googleapis/release-please) — every push to `main` updates a rolling release PR. Merging it bumps the version, updates `CHANGELOG.md`, tags the release, and triggers PyPI publishing via trusted publishing.

## Credits

This adapter draws on the design of [dbt-duckdb](https://github.com/duckdb/dbt-duckdb).

## License

MIT — see [LICENSE](LICENSE).
