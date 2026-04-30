from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject, count_in_catalog

BASE_MODEL = "integ_base"
VIEW_NAME = "integ_vw"
INC_NAME = "integ_inc"

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
