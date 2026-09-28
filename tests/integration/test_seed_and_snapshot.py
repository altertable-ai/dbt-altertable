from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject, count_in_catalog

SEED_NAME = "integ_seed_rows"
SNAP_NAME = "integ_snap_capture"

SNAPSHOT_SQL = f"""\
{{% snapshot {SNAP_NAME} %}}
{{{{
  config(
    unique_key='id',
    strategy='check',
    check_cols='all',
  )
}}}}
select * from {{{{ ref('{SEED_NAME}') }}}}
{{% endsnapshot %}}
"""


@pytest.mark.altertable_integration
def test_seed_fast_false_and_check_strategy_snapshot(
    dbt_project: DbtProject,
    flight_client: Any,
) -> None:
    dbt_project.write_seed(SEED_NAME, "id,label\n1,alpha\n2,beta\n")
    dbt_project.write_snapshot("integ_snapshot", SNAPSHOT_SQL)
    dbt_project.write_project_yml(
        seed_paths=["seeds"],
        snapshot_paths=["snapshots"],
        seeds={"+fast": False},
    )

    dbt_project.run("seed", "--select", SEED_NAME)
    dbt_project.run("snapshot")

    db, schema = dbt_project.db, dbt_project.schema
    assert (
        count_in_catalog(flight_client, kind="table", database=db, schema=schema, name=SEED_NAME)
        >= 1
    )
    assert (
        count_in_catalog(flight_client, kind="table", database=db, schema=schema, name=SNAP_NAME)
        >= 1
    )

    dbt_project.write_seed(SEED_NAME, "id,label\n1,alpha_v2\n2,beta\n3,gamma\n")
    dbt_project.run("seed", "--select", SEED_NAME)
    dbt_project.run("snapshot")

    rows = (
        flight_client.query(
            f"select id, label, dbt_valid_to is null as is_current "
            f"from {dbt_project.qualify(SNAP_NAME)} order by id, dbt_valid_from"
        )
        .read_all()
        .to_pylist()
    )
    assert rows == [
        {"id": 1, "label": "alpha", "is_current": False},
        {"id": 1, "label": "alpha_v2", "is_current": True},
        {"id": 2, "label": "beta", "is_current": True},
        {"id": 3, "label": "gamma", "is_current": True},
    ]
