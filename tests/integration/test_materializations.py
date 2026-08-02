from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject, count_in_catalog, sql_string_literal

BASE_MODEL = "integ_base"
VIEW_NAME = "integ_vw"
INC_NAME = "integ_inc"
DELETE_INSERT_NAME = "integ_delete_insert"

INC_SQL = """\
{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}
{% if not is_incremental() %}
select 1 as id, 'first_run' as phase
{% else %}
select 2 as id, 'second_run' as phase
{% endif %}
"""

DELETE_INSERT_SQL = """\
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
) }}
{% if not is_incremental() %}
select 1 as id, 'first_run' as phase
union all
select 3 as id, 'first_run' as phase
{% else %}
select 1 as id, 'second_run_a' as phase
union all
select 1 as id, 'second_run_b' as phase
union all
select 2 as id, 'second_run' as phase
{% endif %}
"""


def count_scratch_relations(
    client: Any,
    dbt_project: DbtProject,
    model_name: str,
) -> int:
    prefix = sql_string_literal(f"{model_name}__dbt_tmp%")
    query = (
        "select count(*) as c from duckdb_tables() "
        f"where database_name = {sql_string_literal(dbt_project.db)} "
        f"and schema_name = {sql_string_literal(dbt_project.schema)} "
        f"and table_name like {prefix}"
    )
    return int(client.query(query).read_all().to_pylist()[0]["c"])


@pytest.mark.altertable_integration
def test_view_and_incremental_append(dbt_project: DbtProject, flight_client: Any) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(
        BASE_MODEL,
        "{{ config(materialized='table') }}\nselect 1 as id, 'base' as label\n",
    )
    dbt_project.write_model(
        VIEW_NAME,
        f"{{{{ config(materialized='view') }}}}\nselect * from {{{{ ref('{BASE_MODEL}') }}}}\n",
    )
    dbt_project.write_model(INC_NAME, INC_SQL)

    dbt_project.run("run", "--select", INC_NAME)
    dbt_project.run("run", "--select", INC_NAME)
    dbt_project.run("run", "--select", f"{BASE_MODEL} {VIEW_NAME}")

    db, schema = dbt_project.db, dbt_project.schema
    assert (
        count_in_catalog(flight_client, kind="table", database=db, schema=schema, name=INC_NAME)
        >= 1
    )
    assert (
        count_in_catalog(flight_client, kind="view", database=db, schema=schema, name=VIEW_NAME)
        == 1
    )

    rows = (
        flight_client.query(f"select id, phase from {dbt_project.qualify(INC_NAME)} order by id")
        .read_all()
        .to_pylist()
    )
    assert {r["id"]: r["phase"] for r in rows} == {1: "first_run", 2: "second_run"}
    assert count_scratch_relations(flight_client, dbt_project, INC_NAME) == 0


@pytest.mark.altertable_integration
def test_incremental_default_replaces_rows_on_multiple_runs(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(DELETE_INSERT_NAME, DELETE_INSERT_SQL)

    dbt_project.run("run", "--select", DELETE_INSERT_NAME)
    dbt_project.run("run", "--select", DELETE_INSERT_NAME)

    rows = (
        flight_client.query(
            f"select id, phase from {dbt_project.qualify(DELETE_INSERT_NAME)} order by id, phase"
        )
        .read_all()
        .to_pylist()
    )
    assert rows == [
        {"id": 1, "phase": "second_run_a"},
        {"id": 1, "phase": "second_run_b"},
        {"id": 2, "phase": "second_run"},
        {"id": 3, "phase": "first_run"},
    ]
    assert count_scratch_relations(flight_client, dbt_project, DELETE_INSERT_NAME) == 0


@pytest.mark.altertable_integration
def test_incremental_default_rolls_back_delete_when_insert_fails(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(DELETE_INSERT_NAME, DELETE_INSERT_SQL)
    dbt_project.run("run", "--select", DELETE_INSERT_NAME)
    flight_client.query(
        f"create unique index target_id on {dbt_project.qualify(DELETE_INSERT_NAME)} (id)"
    ).read_all()

    failed_run = dbt_project.run("run", "--select", DELETE_INSERT_NAME, check=False)

    assert failed_run.returncode != 0
    rows = (
        flight_client.query(
            f"select id, phase from {dbt_project.qualify(DELETE_INSERT_NAME)} order by id"
        )
        .read_all()
        .to_pylist()
    )
    assert rows == [
        {"id": 1, "phase": "first_run"},
        {"id": 3, "phase": "first_run"},
    ]
    assert count_scratch_relations(flight_client, dbt_project, DELETE_INSERT_NAME) == 0
