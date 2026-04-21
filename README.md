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
