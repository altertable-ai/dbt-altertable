from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject, sql_string_literal


@pytest.mark.altertable_integration
@pytest.mark.parametrize("materialized", ["table", "incremental"])
@pytest.mark.parametrize("layout", ["partitioned_by", "sorted_by"])
def test_table_layout_accepts_a_trailing_sql_comment(
    dbt_project: DbtProject, flight_client: Any, materialized: str, layout: str
) -> None:
    catalog = flight_client.query(
        "select type from duckdb_databases() "
        f"where database_name = {sql_string_literal(dbt_project.db)}"
    ).read_all()
    if catalog.to_pylist() != [{"type": "ducklake"}]:
        pytest.skip("Table layout settings require a DuckLake catalog")

    dbt_project.write_project_yml()
    dbt_project.write_model(
        "trailing_comment",
        "{{ config(materialized='" + materialized + "', " + layout + "=['id']) }}\n"
        "select 1 as id -- trailing comment without a newline",
    )

    dbt_project.run("run", "--select", "trailing_comment")

    rows = flight_client.query(
        f"select id from {dbt_project.qualify('trailing_comment')}"
    ).read_all()
    assert rows.to_pylist() == [{"id": 1}]
