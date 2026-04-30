from __future__ import annotations

import pytest

from tests.integration._helpers import DbtProject

MODEL = "integ_utils"

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


@pytest.mark.altertable_integration
def test_dispatch_macros_date_split_any_value(dbt_project: DbtProject) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(MODEL, UTILS_SQL)

    dbt_project.run("run", "--select", MODEL)
