"""persist_docs round-trip against Arrow Flight SQL (altertable-mock or a real endpoint)."""

from __future__ import annotations

import contextlib
import os
import uuid
from pathlib import Path

import pytest

from tests.integration._helpers import (
    flight_client_ctx,
    quoted_ident,
    run_dbt,
    skip_if_no_flight_target,
    sql_string_literal,
    write_profiles,
)

PROFILE = "persist_docs_integration"


def _write_project(tmp: Path, model_name: str) -> None:
    (tmp / "models").mkdir(parents=True)
    model_sql = "{{ config(materialized='table') }}\n\nselect 1 as id, 'x' as msg\n"
    (tmp / "models" / f"{model_name}.sql").write_text(
        model_sql,
        encoding="utf-8",
    )
    (tmp / "models" / "_persist_docs_models.yml").write_text(
        f"""\
version: 2

models:
  - name: {model_name}
    description: "Integration relation doc"
    columns:
      - name: id
        description: "Integration column id"
      - name: msg
        description: "Integration column msg"
""",
        encoding="utf-8",
    )
    (tmp / "dbt_project.yml").write_text(
        """\
name: persist_docs_integration
version: "1.0.0"
config-version: 2
profile: persist_docs_integration

model-paths: ["models"]

models:
  persist_docs_integration:
    +materialized: table
    +persist_docs:
      relation: true
      columns: true
""",
        encoding="utf-8",
    )


@pytest.mark.altertable_integration
def test_persist_docs_comments_roundtrip(tmp_path: Path) -> None:
    """Run dbt and verify COMMENT ON via duckdb_* catalog."""
    skip_if_no_flight_target()

    model_name = f"pd_integ_{uuid.uuid4().hex[:10]}"
    _write_project(tmp_path, model_name)
    write_profiles(tmp_path, PROFILE)

    db = os.environ["ALTERTABLE_TEST_DATABASE"].strip()
    schema = os.environ["ALTERTABLE_TEST_SCHEMA"].strip()

    proc = run_dbt(
        [
            "run",
            "--project-dir",
            str(tmp_path),
            "--profiles-dir",
            str(tmp_path),
            "--select",
            model_name,
        ],
        tmp_path,
    )
    assert proc.returncode == 0, f"dbt run failed:\n{proc.stdout}\n{proc.stderr}"

    with flight_client_ctx() as client:
        q_table = (
            "select comment from duckdb_tables() "
            f"where lower(database_name) = lower({sql_string_literal(db)}) "
            f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
            f"and lower(table_name) = lower({sql_string_literal(model_name)}) "
            "limit 1"
        )
        tbl = client.query(q_table).read_all()
        assert tbl.num_rows == 1, (
            f"Expected one row from duckdb_tables for {model_name}, got {tbl.num_rows}"
        )
        row = tbl.to_pylist()[0]
        rel_comment = row["comment"]
        assert rel_comment == "Integration relation doc"

        q_cols = (
            "select column_name, comment from duckdb_columns() "
            f"where lower(database_name) = lower({sql_string_literal(db)}) "
            f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
            f"and lower(table_name) = lower({sql_string_literal(model_name)}) "
            "order by column_index"
        )
        ctbl = client.query(q_cols).read_all()
        by_name = {row["column_name"]: row["comment"] for row in ctbl.to_pylist()}
        assert by_name.get("id") == "Integration column id"
        assert by_name.get("msg") == "Integration column msg"

        drop_sql = f"drop table if exists {quoted_ident(db, schema, model_name)}"
        with contextlib.suppress(Exception):
            client.query(drop_sql).read_all()
