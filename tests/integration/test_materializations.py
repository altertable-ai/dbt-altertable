from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject, count_in_catalog

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

DELETE_INSERT_NULL_PHASE_SQL = """\
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
) }}
select 1 as id, cast(null as varchar) as phase
"""


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


@pytest.mark.altertable_integration
def test_incremental_default_rolls_back_delete_when_insert_fails(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(DELETE_INSERT_NAME, DELETE_INSERT_SQL)
    dbt_project.run("run", "--select", DELETE_INSERT_NAME)
    flight_client.query(
        f"alter table {dbt_project.qualify(DELETE_INSERT_NAME)} alter column phase set not null"
    ).read_all()
    dbt_project.write_model(DELETE_INSERT_NAME, DELETE_INSERT_NULL_PHASE_SQL)

    failed_run = dbt_project.run("run", "--select", DELETE_INSERT_NAME, check=False)

    assert failed_run.returncode != 0
    assert "NOT NULL constraint failed" in failed_run.stdout, failed_run.stdout
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


MERGE_NAME = "integ_merge"

MERGE_SQL = """\
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
) }}
{% if not is_incremental() %}
select 1 as id, 'a' as val
union all
select 2 as id, 'b' as val
{% else %}
select 2 as id, 'b2' as val
union all
select 3 as id, 'c' as val
{% endif %}
"""


@pytest.mark.altertable_integration
def test_incremental_merge_upserts_on_unique_key(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(MERGE_NAME, MERGE_SQL)

    dbt_project.run("run", "--select", MERGE_NAME)
    dbt_project.run("run", "--select", MERGE_NAME)

    rows = (
        flight_client.query(f"select id, val from {dbt_project.qualify(MERGE_NAME)} order by id")
        .read_all()
        .to_pylist()
    )
    assert rows == [
        {"id": 1, "val": "a"},
        {"id": 2, "val": "b2"},
        {"id": 3, "val": "c"},
    ]


@pytest.mark.altertable_integration
def test_incremental_merge_rejects_unsupported_returning(
    dbt_project: DbtProject,
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(
        MERGE_NAME,
        MERGE_SQL.replace(
            "unique_key='id',", "unique_key='id',\n    merge_returning_columns=['id'],"
        ),
    )
    dbt_project.run("run", "--select", MERGE_NAME)

    failed_run = dbt_project.run("run", "--select", MERGE_NAME, check=False)

    assert failed_run.returncode != 0
    assert "Altertable MERGE restrictions" in failed_run.stdout
    assert "DuckLake" not in failed_run.stdout, failed_run.stdout


EVENTS_NAME = "integ_events"
MICROBATCH_NAME = "integ_microbatch"

EVENTS_SQL = """\
{{ config(materialized='table', event_time='event_at') }}
select 1 as id, timestamp '2026-01-01 10:00:00' as event_at
union all
select 2 as id, timestamp '2026-01-02 10:00:00' as event_at
union all
select 3 as id, timestamp '2026-01-05 10:00:00' as event_at
"""

MICROBATCH_SQL = f"""\
{{{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_at',
    begin='2026-01-01',
    batch_size='day',
) }}}}
select id, event_at from {{{{ ref('{EVENTS_NAME}') }}}}
"""


@pytest.mark.altertable_integration
def test_incremental_microbatch_replaces_each_batch(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(EVENTS_NAME, EVENTS_SQL)
    dbt_project.write_model(MICROBATCH_NAME, MICROBATCH_SQL)
    dbt_project.run("run", "--select", EVENTS_NAME)

    window = ("--event-time-start", "2026-01-01", "--event-time-end", "2026-01-03")
    dbt_project.run("run", "--select", MICROBATCH_NAME, *window)
    dbt_project.run("run", "--select", MICROBATCH_NAME, *window)

    rows = (
        flight_client.query(f"select id from {dbt_project.qualify(MICROBATCH_NAME)} order by id")
        .read_all()
        .to_pylist()
    )
    assert rows == [{"id": 1}, {"id": 2}]


PARTITIONED_TABLE = "integ_partitioned_table"
PARTITIONED_INC = "integ_partitioned_inc"

PARTITIONED_SQL = """\
{{{{ config(
    materialized='{materialized}',
    partitioned_by=['category'],
    sorted_by=['id'],
) }}}}
select 2 as id, 'b' as category
union all
select 1 as id, 'a' as category
"""


@pytest.mark.altertable_integration
def test_partitioned_and_sorted_tables_build_and_rebuild(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(PARTITIONED_TABLE, PARTITIONED_SQL.format(materialized="table"))
    dbt_project.write_model(PARTITIONED_INC, PARTITIONED_SQL.format(materialized="incremental"))
    selection = ("--select", f"{PARTITIONED_TABLE} {PARTITIONED_INC}")

    dbt_project.run("run", *selection)
    dbt_project.run("run", *selection)
    dbt_project.run("run", *selection, "--full-refresh")

    for name in (PARTITIONED_TABLE, PARTITIONED_INC):
        rows = (
            flight_client.query(f"select id, category from {dbt_project.qualify(name)} order by id")
            .read_all()
            .to_pylist()
        )
        assert rows == [{"id": 1, "category": "a"}, {"id": 2, "category": "b"}]
        assert _current_layout(flight_client, dbt_project, name) == {
            "partitioned_by": ["category"],
            "sorted_by": ["id"],
        }


def _current_layout(client: Any, dbt_project: DbtProject, table: str) -> dict[str, list[str]]:
    meta = f"__ducklake_metadata_{dbt_project.db}"
    current_table = f"""
        select t.table_id
        from {meta}.ducklake_table t
        join {meta}.ducklake_schema s using (schema_id)
        where t.table_name = '{table}' and s.schema_name = '{dbt_project.schema}'
          and t.end_snapshot is null and s.end_snapshot is null
    """
    partitioned_by = client.query(f"""
        select c.column_name
        from {meta}.ducklake_partition_info p
        join {meta}.ducklake_partition_column pc using (partition_id, table_id)
        join {meta}.ducklake_column c on c.table_id = pc.table_id and c.column_id = pc.column_id
        where p.table_id = ({current_table}) and p.end_snapshot is null and c.end_snapshot is null
        order by pc.partition_key_index
    """).read_all()
    sorted_by = client.query(f"""
        select e.expression
        from {meta}.ducklake_sort_info i
        join {meta}.ducklake_sort_expression e using (sort_id, table_id)
        where i.table_id = ({current_table}) and i.end_snapshot is null
        order by e.sort_key_index
    """).read_all()
    return {
        "partitioned_by": partitioned_by.column("column_name").to_pylist(),
        "sorted_by": sorted_by.column("expression").to_pylist(),
    }


SCHEMA_CHANGE_NAME = "integ_schema_change"

SCHEMA_CHANGE_SQL = """\
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
) }}
{% if not is_incremental() %}
select 1 as id, 'dropped' as old_col
{% else %}
select 2 as id, 'added' as new_col
{% endif %}
"""


@pytest.mark.altertable_integration
def test_incremental_sync_all_columns_adds_and_removes_columns(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(SCHEMA_CHANGE_NAME, SCHEMA_CHANGE_SQL)

    dbt_project.run("run", "--select", SCHEMA_CHANGE_NAME)
    dbt_project.run("run", "--select", SCHEMA_CHANGE_NAME)

    rows = (
        flight_client.query(f"select * from {dbt_project.qualify(SCHEMA_CHANGE_NAME)} order by id")
        .read_all()
        .to_pylist()
    )
    assert rows == [{"id": 1, "new_col": None}, {"id": 2, "new_col": "added"}]
