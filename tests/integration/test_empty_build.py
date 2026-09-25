from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject

LEFT = "integ_left"
RIGHT = "integ_right"
JOINED = "integ_joined"

JOINED_SQL = """\
select src.*, dim.category
from {{ ref('integ_left') }} src
join {{ ref('integ_right') }} dim using (id)
"""


@pytest.mark.altertable_integration
def test_build_empty_keeps_user_alias_on_ref(dbt_project: DbtProject, flight_client: Any) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(LEFT, "select 1 as id, 10 as quantity\n")
    dbt_project.write_model(RIGHT, "select 1 as id, 'standard' as category\n")
    dbt_project.write_model(JOINED, JOINED_SQL)

    dbt_project.run("build", "--empty")

    compiled = (
        dbt_project.base / "target" / "compiled" / dbt_project.name / "models" / f"{JOINED}.sql"
    ).read_text(encoding="utf-8")
    assert "_dbt_limit_subq_" not in compiled
    assert "where false limit 0) src" in compiled
    assert "where false limit 0) dim" in compiled

    rows = flight_client.query(
        f"select id, quantity, category from {dbt_project.qualify(JOINED)}"
    ).read_all()
    assert rows.num_rows == 0
    assert rows.column_names == ["id", "quantity", "category"]
