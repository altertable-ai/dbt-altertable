from __future__ import annotations

import json

import pytest

from tests.integration._helpers import DbtProject

MODEL = "integ_catalog_model"

SCHEMA_YML = f"""\
version: 2
models:
  - name: {MODEL}
    columns:
      - name: id
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - not_null
"""


@pytest.mark.altertable_integration
def test_docs_generate_catalog_and_schema_tests(dbt_project: DbtProject) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(
        MODEL, "{{ config(materialized='table') }}\nselect 1 as id, 'ok' as status\n"
    )
    dbt_project.write_models_yml("_schema", SCHEMA_YML)

    dbt_project.run("run", "--select", MODEL)
    dbt_project.run("test", "--select", MODEL)
    dbt_project.run("docs", "generate")

    catalog_path = dbt_project.base / "target" / "catalog.json"
    assert catalog_path.is_file(), "dbt docs generate should write target/catalog.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    nodes = catalog.get("nodes") or {}
    key = f"model.{dbt_project.name}.{MODEL}"
    assert key in nodes, f"Expected catalog node {key}, got keys sample: {list(nodes)[:5]}"
    entry = nodes[key]
    assert entry.get("metadata", {}).get("type") == "BASE TABLE"
    cols = entry.get("columns") or {}
    assert "id" in cols and "status" in cols
