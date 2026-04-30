from __future__ import annotations

import pytest

from tests.integration._helpers import DbtProject

MODEL = "integ_parse_model"


@pytest.mark.altertable_integration
def test_parse_and_compile_select(dbt_project: DbtProject) -> None:
    dbt_project.write_project_yml(models={"+materialized": "view"})
    dbt_project.write_model(MODEL, "{{ config(materialized='view') }}\nselect 1 as n\n")

    dbt_project.run("parse")
    dbt_project.run("compile", "--select", MODEL)

    compiled = (
        dbt_project.base / "target" / "compiled" / dbt_project.name / "models" / f"{MODEL}.sql"
    )
    assert compiled.is_file()
    assert "select 1" in compiled.read_text(encoding="utf-8").lower()
