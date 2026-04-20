# altertable_analytics

dbt project answering Altertable's internal operational questions:

- **Ingestion health** — are connections + databases + sync tasks operating normally?
- **Discovery lifecycle** — how many discoveries are created vs. approved vs. rejected? Time-to-approval?

Reads the Rails Postgres primary database via `dbt-duckdb` + `ATTACH` federation. DuckDB-dialect SQL so models port unchanged to the future `dbt-altertable` adapter.

## Status

**v1 scaffold** — PR 1. No models yet; parse-only CI. See `docs/superpowers/specs/2026-04-20-altertable-analytics-dbt-project-design.md` in the `dbt-altertable` repo for the full design.

## Local development

```bash
# Install tooling
uv sync
bun install

# Parse the project (no warehouse connection needed)
uv run dbt deps
uv run dbt parse --profiles-dir . --target dev

# Once you have a dev Postgres DSN
export APP_PG_DSN_DEV="postgres://readonly:...@dev-replica/app_dev"
uv run dbt build --target dev --profiles-dir .
```

See `profiles.yml.example` — copy to `profiles.yml` (gitignored) and fill in DSN.

## Layout

```
altertable_analytics/
  dbt_project.yml         # project config + flags
  packages.yml            # dbt packages (dbt-utils, dbt-expectations, elementary, ...)
  profiles.yml.example    # copy to profiles.yml for local dev
  Dockerfile              # k8s CronJob image (mirrors hndata)
  package.json            # bun entrypoint: `bun run dbt:run`
  pyproject.toml          # uv-managed Python deps (dbt-core, dbt-duckdb, elementary, ...)
  macros/                 # project-level macros (generate_schema_name, ...)
  models/                 # staging/, intermediate/, marts/ (populated from PR 2 onwards)
  seeds/                  # enum label CSVs (PR 3)
  snapshots/              # SCD2 snapshots (PR 3)
  semantic_models/        # MetricFlow semantic models (PR 5)
  metrics/                # MetricFlow metrics (PR 5)
  tests/                  # singular tests
  unit_tests/             # dbt 1.8+ unit tests
```

## Scheduling

Runs daily at 06:45 UTC via a Kubernetes CronJob in `eu-production` (mirrors the `hndata` pattern). See `backend/infra/helm/charts/altertable-analytics/` for the deployment manifest (added in PR 7).

## Related specs

- `docs/superpowers/specs/2026-04-20-altertable-analytics-dbt-project-design.md` (this project)
- `docs/superpowers/plans/2026-04-20-altertable-analytics-delivery-plan.md` (PR sequencing)
- `docs/superpowers/references/2026-04-20-dbt-adapter-reference-guide.md` (dbt patterns + package recommendations)
- `docs/superpowers/specs/2026-04-17-dbt-altertable-adapter-design.md` (the future adapter this project will swap to)

All specs live in the `altertable-ai/dbt-altertable` repo's `docs/superpowers/` directory.
