from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject, sql_string_literal

MODEL = "integ_utils"
UNIT_MODEL = "integ_unit"

UTILS_SQL = """\
{{ config(materialized='table') }}
with series as (
    {{ dbt.generate_series(4) }}
)
select
    s.generated_number,
    {{ dbt.dateadd('day', 1, "date '2024-01-01'") }} as d_add,
    {{ dbt.datediff("timestamp '2024-01-01'", "timestamp '2024-01-15'", 'day') }} as d_diff_days,
    {{ dbt.datediff("timestamp '2024-01-01'", "timestamp '2024-01-10'", 'week') }} as d_diff_weeks,
    {{ dbt.last_day("date '2024-01-15'", 'month') }} as last_m,
    {{ dbt.last_day("date '2024-02-15'", 'quarter') }} as last_q,
    {{ dbt.split_part("'a|b|c'", "'|'", 2) }} as part2,
    av.any_x
from series s
cross join (
    select {{ dbt.any_value('x') }} as any_x from (values (10), (20), (30)) as t(x)
) av
"""

SCOPED_SCRATCH_SQL = """\
{% macro assert_scoped_scratch_relation() %}
  {% set base_relation = api.Relation.create(
      database=target.database,
      schema=target.schema,
      identifier='integ_scratch_source',
      type='table'
  ) %}
  {% set scratch_relation = make_temp_relation(base_relation) %}
  {% if scratch_relation.database != target.database or scratch_relation.schema != target.schema %}
    {% do exceptions.raise_compiler_error('scratch relation must retain the target scope') %}
  {% endif %}

  {% do run_query(get_create_table_as_sql(True, scratch_relation, 'select 1 as id')) %}
  {% set columns = adapter.get_columns_in_relation(scratch_relation) %}
  {% if columns | map(attribute='name') | list != ['id'] %}
    {% do exceptions.raise_compiler_error('scratch relation columns are not query-visible') %}
  {% endif %}
  {% do adapter.drop_relation(scratch_relation) %}
  {% do adapter.commit() %}
{% endmacro %}
"""

UNIT_TEST_YAML = f"""\
version: 2

unit_tests:
  - name: scratch_relation_cleanup
    model: {UNIT_MODEL}
    given: []
    expect:
      rows:
        - {{id: 1}}
"""


@pytest.mark.altertable_integration
def test_dispatch_macros_date_split_any_value(dbt_project: DbtProject) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(MODEL, UTILS_SQL)

    dbt_project.run("run", "--select", MODEL)


@pytest.mark.altertable_integration
def test_scratch_relation_is_scoped_and_query_visible(dbt_project: DbtProject) -> None:
    dbt_project.write_project_yml()
    macros_dir = dbt_project.base / "macros"
    macros_dir.mkdir()
    (macros_dir / "assert_scoped_scratch_relation.sql").write_text(
        SCOPED_SCRATCH_SQL,
        encoding="utf-8",
    )

    dbt_project.run("run-operation", "assert_scoped_scratch_relation")


@pytest.mark.altertable_integration
def test_unit_test_cleans_up_scoped_scratch_relation(
    dbt_project: DbtProject,
    flight_client: Any,
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(UNIT_MODEL, "select 1 as id")
    dbt_project.write_models_yml(UNIT_MODEL, UNIT_TEST_YAML)

    dbt_project.run("test", "--select", "test_type:unit")

    query = (
        "select count(*) as c from duckdb_tables() "
        f"where database_name = {sql_string_literal(dbt_project.db)} "
        f"and schema_name = {sql_string_literal(dbt_project.schema)} "
        "and table_name like '%__dbt_tmp%'"
    )
    scratch_count = flight_client.query(query).read_all().to_pylist()[0]["c"]
    assert scratch_count == 0
