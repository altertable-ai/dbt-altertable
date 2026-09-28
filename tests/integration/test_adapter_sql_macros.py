from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest

from tests.integration._helpers import DbtProject

SERIES_MODEL = "integ_series"
SCALARS_MODEL = "integ_scalars"

SERIES_SQL = """\
{{ config(materialized='table') }}
{{ dbt.generate_series(4) }}
"""

SCALARS_SQL = """\
{{ config(materialized='table') }}
select
    cast({{ dbt.dateadd('day', 1, "date '2024-01-01'") }} as date) as d_add,
    {{ dbt.datediff("timestamp '2024-01-01'", "timestamp '2024-01-15'", 'day') }} as d_diff_days,
    {{ dbt.datediff("timestamp '2024-01-01'", "timestamp '2024-01-10'", 'week') }} as d_diff_weeks,
    cast({{ dbt.last_day("date '2024-01-15'", 'month') }} as date) as last_m,
    cast({{ dbt.last_day("date '2024-02-15'", 'quarter') }} as date) as last_q,
    {{ dbt.split_part("'a|b|c'", "'|'", 2) }} as part2,
    (select {{ dbt.any_value('x') }} from (values (10), (20), (30)) as t(x)) as any_x,
    (
        select {{ dbt.listagg('cast(x as varchar)', "','", 'order by x') }}
        from (values (30), (10), (20)) as t(x)
    ) as agg,
    {{ dbt.current_timestamp() }} as now_ts,
    {{ snapshot_string_as_time('2026-01-02 03:04:05') }} as snap_ts
"""


@pytest.mark.altertable_integration
def test_cross_database_macros_return_expected_values(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(SERIES_MODEL, SERIES_SQL)
    dbt_project.write_model(SCALARS_MODEL, SCALARS_SQL)
    selection = ("--select", f"{SERIES_MODEL} {SCALARS_MODEL}")

    dbt_project.run("run", *selection)
    dbt_project.run("run", *selection)

    series = (
        flight_client.query(
            f"select generated_number from {dbt_project.qualify(SERIES_MODEL)} order by 1"
        )
        .read_all()
        .column("generated_number")
        .to_pylist()
    )
    assert series == [1, 2, 3, 4]

    (row,) = (
        flight_client.query(f"select * from {dbt_project.qualify(SCALARS_MODEL)}")
        .read_all()
        .to_pylist()
    )
    assert isinstance(row.pop("now_ts"), datetime)
    assert row.pop("any_x") in (10, 20, 30)
    assert row == {
        "d_add": date(2024, 1, 2),
        "d_diff_days": 14,
        "d_diff_weeks": 1,
        "last_m": date(2024, 1, 31),
        "last_q": date(2024, 3, 31),
        "part2": "b",
        "agg": "10,20,30",
        "snap_ts": datetime(2026, 1, 2, 3, 4, 5),
    }
