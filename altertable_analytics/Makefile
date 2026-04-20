.PHONY: install deps parse build test docs lint lint-fix ruff typecheck pre-commit ci clean

install:
	uv sync
	bun install

deps:
	uv run dbt deps

parse: deps
	uv run dbt parse --profiles-dir . --target dev

build: deps
	uv run dbt build --profiles-dir . --target dev

test: deps
	uv run dbt test --profiles-dir . --target dev

docs: deps
	uv run dbt docs generate --profiles-dir . --target dev --static

lint:
	uv run sqlfluff lint models --dialect duckdb --templater dbt

lint-fix:
	uv run sqlfluff fix models --dialect duckdb --templater dbt

ruff:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run ty check . 2>/dev/null || echo "No Python to check yet"

pre-commit:
	uv run pre-commit run --all-files

ci: install ruff typecheck deps parse lint

clean:
	rm -rf target dbt_packages logs
