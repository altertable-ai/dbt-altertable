"""Table, view, and incremental materializations against altertable-mock."""

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

PROFILE = "integration"


def _base_project(name: str) -> str:
    return f"""\
name: {name}
version: "1.0.0"
config-version: 2
profile: {PROFILE}

model-paths: ["models"]

models:
  {name}:
    +materialized: table
"""


@pytest.mark.altertable_integration
def test_view_and_incremental_append(tmp_path: Path) -> None:
    """Exercises create_view_as, incremental append, and duckdb_* for views/tables."""
    skip_if_no_flight_target()

    proj = f"integ_mat_{uuid.uuid4().hex[:8]}"
    base = tmp_path / proj
    base.mkdir()
    models = base / "models"
    models.mkdir()

    base_model = "integ_base"
    (models / f"{base_model}.sql").write_text(
        "{{ config(materialized='table') }}\nselect 1 as id, 'base' as label\n",
        encoding="utf-8",
    )
    view_name = "integ_vw"
    (models / f"{view_name}.sql").write_text(
        f"{{{{ config(materialized='view') }}}}\nselect * from {{{{ ref('{base_model}') }}}}\n",
        encoding="utf-8",
    )
    inc_name = "integ_inc"
    (models / f"{inc_name}.sql").write_text(
        """\
{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}
{% if not is_incremental() %}
select 1 as id, 'first_run' as phase
{% else %}
select 2 as id, 'second_run' as phase
{% endif %}
""",
        encoding="utf-8",
    )

    (base / "dbt_project.yml").write_text(_base_project(proj), encoding="utf-8")
    write_profiles(base, PROFILE)

    db = os.environ["ALTERTABLE_TEST_DATABASE"].strip()
    schema = os.environ["ALTERTABLE_TEST_SCHEMA"].strip()

    proc = run_dbt(
        ["run", "--project-dir", str(base), "--profiles-dir", str(base), "--select", inc_name],
        base,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr

    proc2 = run_dbt(
        ["run", "--project-dir", str(base), "--profiles-dir", str(base), "--select", inc_name],
        base,
    )
    assert proc2.returncode == 0, proc2.stdout + proc2.stderr

    proc3 = run_dbt(
        [
            "run",
            "--project-dir",
            str(base),
            "--profiles-dir",
            str(base),
            "--select",
            f"{base_model} {view_name}",
        ],
        base,
    )
    assert proc3.returncode == 0, proc3.stdout + proc3.stderr

    with flight_client_ctx() as client:
        q = (
            "select count(*) as c from duckdb_tables() "
            f"where lower(database_name) = lower({sql_string_literal(db)}) "
            f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
            f"and lower(table_name) = lower({sql_string_literal(inc_name)})"
        )
        assert client.query(q).read_all().to_pylist()[0]["c"] >= 1

        qv = (
            "select count(*) as c from duckdb_views() "
            f"where lower(database_name) = lower({sql_string_literal(db)}) "
            f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
            f"and lower(view_name) = lower({sql_string_literal(view_name)})"
        )
        assert client.query(qv).read_all().to_pylist()[0]["c"] == 1

        qrows = f"select id, phase from {quoted_ident(db, schema, inc_name)} order by id"
        rows = client.query(qrows).read_all().to_pylist()
        assert {r["id"] for r in rows} == {1, 2}
        by_id = {r["id"]: r["phase"] for r in rows}
        assert by_id[1] == "first_run"
        assert by_id[2] == "second_run"

        drop_v = f"drop view if exists {quoted_ident(db, schema, view_name)}"
        with contextlib.suppress(Exception):
            client.query(drop_v).read_all()
        for rel in (base_model, inc_name):
            drop_sql = f"drop table if exists {quoted_ident(db, schema, rel)}"
            with contextlib.suppress(Exception):
                client.query(drop_sql).read_all()
