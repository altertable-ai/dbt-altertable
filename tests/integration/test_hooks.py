from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject

MODEL_NAME = "integ_post_hook"


def model_rows(dbt_project: DbtProject, flight_client: Any) -> list[dict[str, Any]]:
    return (
        flight_client.query(f"select id, phase from {dbt_project.qualify(MODEL_NAME)} order by id")
        .read_all()
        .to_pylist()
    )


@pytest.mark.altertable_integration
def test_post_hook_after_commit_runs_outside_transaction(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(
        MODEL_NAME,
        """\
{{ config(
    post_hook=after_commit(
        "insert into {{ this }} select 2 as id, 'post_hook' as phase"
    ),
) }}
select 1 as id, 'model' as phase
""",
    )

    dbt_project.run("run", "--select", MODEL_NAME)

    assert model_rows(dbt_project, flight_client) == [
        {"id": 1, "phase": "model"},
        {"id": 2, "phase": "post_hook"},
    ]


@pytest.mark.altertable_integration
def test_post_hook_list_runs_inside_transaction(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(
        MODEL_NAME,
        """\
{{ config(
    post_hook=[
        "insert into {{ this }} select 2 as id, 'list_hook' as phase"
    ],
) }}
select 1 as id, 'model' as phase
""",
    )

    dbt_project.run("run", "--select", MODEL_NAME)

    assert model_rows(dbt_project, flight_client) == [
        {"id": 1, "phase": "model"},
        {"id": 2, "phase": "list_hook"},
    ]


@pytest.mark.altertable_integration
def test_post_hook_dictionary_runs_outside_transaction(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(
        MODEL_NAME,
        """\
{{ config(
    post_hook={
        "sql": "insert into {{ this }} select 2 as id, 'dictionary_hook' as phase",
        "transaction": false,
    },
) }}
select 1 as id, 'model' as phase
""",
    )

    dbt_project.run("run", "--select", MODEL_NAME)

    assert model_rows(dbt_project, flight_client) == [
        {"id": 1, "phase": "model"},
        {"id": 2, "phase": "dictionary_hook"},
    ]
