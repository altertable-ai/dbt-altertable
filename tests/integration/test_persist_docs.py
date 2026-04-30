from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject, sql_string_literal

MODEL = "pd_integ"
VIEW_MODEL = "pd_view"

MODELS_YML = f"""\
version: 2

models:
  - name: {MODEL}
    description: "Integration relation doc"
    columns:
      - name: id
        description: "Integration column id"
      - name: msg
        description: "Integration column msg"
"""

VIEW_MODELS_YML = f"""\
version: 2

models:
  - name: {VIEW_MODEL}
    description: "View relation doc"
    columns:
      - name: id
        description: "Column id doc"
      - name: msg
        description: "Column msg doc"
"""


@pytest.mark.altertable_integration
def test_persist_docs_comments_roundtrip(dbt_project: DbtProject, flight_client: Any) -> None:
    dbt_project.write_project_yml(
        models={"+materialized": "table", "+persist_docs": {"relation": True, "columns": True}},
    )
    dbt_project.write_model(
        MODEL, "{{ config(materialized='table') }}\n\nselect 1 as id, 'x' as msg\n"
    )
    dbt_project.write_models_yml("_persist_docs_models", MODELS_YML)

    dbt_project.run("run", "--select", MODEL)

    db, schema = dbt_project.db, dbt_project.schema
    q_table = (
        "select comment from duckdb_tables() "
        f"where lower(database_name) = lower({sql_string_literal(db)}) "
        f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
        f"and lower(table_name) = lower({sql_string_literal(MODEL)}) "
        "limit 1"
    )
    tbl = flight_client.query(q_table).read_all()
    assert tbl.num_rows == 1, f"Expected one row from duckdb_tables for {MODEL}, got {tbl.num_rows}"
    assert tbl.to_pylist()[0]["comment"] == "Integration relation doc"

    q_cols = (
        "select column_name, comment from duckdb_columns() "
        f"where lower(database_name) = lower({sql_string_literal(db)}) "
        f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
        f"and lower(table_name) = lower({sql_string_literal(MODEL)}) "
        "order by column_index"
    )
    by_name = {
        row["column_name"]: row["comment"]
        for row in flight_client.query(q_cols).read_all().to_pylist()
    }
    assert by_name.get("id") == "Integration column id"
    assert by_name.get("msg") == "Integration column msg"


@pytest.mark.altertable_integration
def test_persist_docs_column_comments_on_view_fail_clearly(dbt_project: DbtProject) -> None:
    dbt_project.write_project_yml(
        models={"+persist_docs": {"relation": True, "columns": True}},
    )
    dbt_project.write_model(
        VIEW_MODEL, "{{ config(materialized='view') }}\n\nselect 1 as id, 'x' as msg\n"
    )
    dbt_project.write_models_yml("_persist_docs_view_models", VIEW_MODELS_YML)

    proc = dbt_project.run("run", "--select", VIEW_MODEL, check=False)

    assert proc.returncode != 0, "expected dbt run to fail when persisting column docs on a view"
    combined = f"{proc.stdout}\n{proc.stderr}"
    assert "COMMENT ON COLUMN for views" in combined
    assert "persist_docs" in combined.lower()
