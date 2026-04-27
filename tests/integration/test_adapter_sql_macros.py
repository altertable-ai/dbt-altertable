"""Compile and run models that exercise adapter-dispatched SQL macros.

DuckDB-specific macro implementations.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from tests.integration._helpers import run_dbt, skip_if_no_flight_target, write_profiles

PROFILE = "integration"


@pytest.mark.altertable_integration
def test_dispatch_macros_date_split_any_value(tmp_path: Path) -> None:
    """Runs SQL using dateadd, datediff, last_day, split_part, any_value, generate_series."""
    skip_if_no_flight_target()

    proj = f"integ_macros_{uuid.uuid4().hex[:8]}"
    base = tmp_path / proj
    base.mkdir()
    models = base / "models"
    models.mkdir()

    (models / "integ_utils.sql").write_text(
        """\
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
""",
        encoding="utf-8",
    )

    (base / "dbt_project.yml").write_text(
        f"""\
name: {proj}
version: "1.0.0"
config-version: 2
profile: {PROFILE}

model-paths: ["models"]

models:
  {proj}:
    +materialized: table
""",
        encoding="utf-8",
    )
    write_profiles(base, PROFILE)

    proc = run_dbt(
        [
            "run",
            "--project-dir",
            str(base),
            "--profiles-dir",
            str(base),
            "--select",
            "integ_utils",
        ],
        base,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
