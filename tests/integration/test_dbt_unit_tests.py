from __future__ import annotations

import pytest

from tests.integration._helpers import DbtProject

SOURCE_MODEL = "integ_unit_source"
UNIT_MODEL = "integ_unit_model"

SOURCE_SQL = """\
{{ config(materialized='table') }}
select 1 as id, 'a' as label
"""

UNIT_MODEL_SQL = f"""\
{{{{ config(materialized='table') }}}}
select id, upper(label) as label
from {{{{ ref('{SOURCE_MODEL}') }}}}
"""

UNIT_TESTS_YML = f"""\
version: 2
unit_tests:
  - name: test_label_is_uppercased
    model: {UNIT_MODEL}
    given:
      - input: ref('{SOURCE_MODEL}')
        rows:
          - {{id: 1, label: 'a'}}
          - {{id: 2, label: 'b'}}
    expect:
      rows:
        - {{id: 1, label: 'A'}}
        - {{id: 2, label: 'B'}}
"""


@pytest.mark.altertable_integration
def test_dbt_unit_tests_run_against_the_server(dbt_project: DbtProject) -> None:
    """
    dbt *unit tests* (the ``unit_tests:`` YAML block, not pytest) build the model
    through the ``unit`` materialization: ``run_query()`` stages the ``given``
    rows in a temp relation, ``get_columns_in_relation`` reads it back, then the
    model SQL runs against the fixture and is compared to ``expect``. No other
    integration test reaches that materialization.
    """
    dbt_project.write_project_yml(models={"+materialized": "table"})
    dbt_project.write_model(SOURCE_MODEL, SOURCE_SQL)
    dbt_project.write_model(UNIT_MODEL, UNIT_MODEL_SQL)
    dbt_project.write_models_yml("_unit_tests", UNIT_TESTS_YML)

    dbt_project.run("run", "--select", SOURCE_MODEL)
    proc = dbt_project.run("test", "--select", UNIT_MODEL)

    # ``run(check=True)`` already fails on a failing unit test; this catches the
    # quieter failure where the selector matches nothing and dbt still exits 0.
    assert "test_label_is_uppercased" in proc.stdout
    assert "PASS=1" in proc.stdout, proc.stdout
