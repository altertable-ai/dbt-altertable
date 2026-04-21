.PHONY: install lint format typecheck test build clean

install:
	uv sync --extra dev

lint:
	uv run ruff format .
	uv run ruff check --fix .

format:
	uv run ruff format .

typecheck:
	uv run ty check

test:
	uv run pytest

build:
	uv build

clean:
	rm -rf dist build *.egg-info .ruff_cache .pytest_cache .ty_cache
