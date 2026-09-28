from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject

MODEL = "integ_unsupported"

CONTRACT_YML = f"""\
version: 2
models:
  - name: {MODEL}
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        constraints:
          - type: not_null
{{extra_constraint}}
      - name: label
        data_type: varchar
"""


def _assert_fails_with(dbt_project: DbtProject, message: str) -> None:
    failed_run = dbt_project.run("run", "--select", MODEL, check=False)
    assert failed_run.returncode != 0
    assert message in failed_run.stdout, failed_run.stdout
    assert "DuckLake" not in failed_run.stdout, failed_run.stdout


@pytest.mark.altertable_integration
@pytest.mark.parametrize(
    ("model_sql", "message"),
    [
        (
            "{{ config(materialized='table', indexes=[{'columns': ['id']}]) }}\nselect 1 as id\n",
            "Indexes are not supported on Altertable",
        ),
        (
            "{{ config(materialized='table', grants={'select': ['someone']}) }}\nselect 1 as id\n",
            "Grants are not supported on Altertable",
        ),
        (
            "{{ config(materialized='external') }}\nselect 1 as id\n",
            "The 'external' materialization is not supported on Altertable",
        ),
    ],
    ids=["indexes", "grants", "external"],
)
def test_unsupported_sql_model_configs_fail_clearly(
    dbt_project: DbtProject, model_sql: str, message: str
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(MODEL, model_sql)

    _assert_fails_with(dbt_project, message)


@pytest.mark.altertable_integration
def test_python_models_fail_clearly(dbt_project: DbtProject) -> None:
    dbt_project.write_project_yml()
    (dbt_project.models_dir / f"{MODEL}.py").write_text(
        "def model(dbt, session):\n    dbt.config(materialized='table')\n    return None\n",
        encoding="utf-8",
    )

    _assert_fails_with(dbt_project, "Python models are not supported on Altertable")


@pytest.mark.altertable_integration
@pytest.mark.parametrize("constraint", ["primary_key", "unique"])
def test_contract_with_unsupported_constraint_fails_clearly(
    dbt_project: DbtProject, constraint: str
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(
        MODEL, "{{ config(materialized='table') }}\nselect 1 as id, 'a' as label\n"
    )
    dbt_project.write_models_yml(
        "_schema",
        CONTRACT_YML.format(extra_constraint=f"          - type: {constraint}"),
    )

    _assert_fails_with(dbt_project, f"{constraint} constraints are not supported on Altertable")


@pytest.mark.altertable_integration
def test_contract_with_not_null_builds_and_enforces(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(
        MODEL, "{{ config(materialized='table') }}\nselect 1 as id, 'a' as label\n"
    )
    dbt_project.write_models_yml("_schema", CONTRACT_YML.format(extra_constraint=""))

    dbt_project.run("run", "--select", MODEL)

    rows = flight_client.query(f"select id, label from {dbt_project.qualify(MODEL)}").read_all()
    assert rows.to_pylist() == [{"id": 1, "label": "a"}]
    with pytest.raises(Exception, match="NOT NULL constraint failed"):
        flight_client.query(
            f"insert into {dbt_project.qualify(MODEL)} values (null, 'b')"
        ).read_all()
