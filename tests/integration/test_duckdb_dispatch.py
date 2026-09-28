from __future__ import annotations

from typing import Any

import pytest
import yaml

from tests.integration._helpers import DbtProject

MODEL = "integ_dispatch"
PACKAGE = "dispatch_pkg"

PROJECT_MACROS = """\
{% macro project_impl() %}{{ return(adapter.dispatch('project_impl')()) }}{% endmacro %}
{% macro default__project_impl() %}'default'{% endmacro %}
{% macro duckdb__project_impl() %}'duckdb'{% endmacro %}

{% macro overridden_impl() %}{{ return(adapter.dispatch('overridden_impl')()) }}{% endmacro %}
{% macro duckdb__overridden_impl() %}'duckdb'{% endmacro %}
{% macro altertable__overridden_impl() %}'altertable'{% endmacro %}
"""

PACKAGE_MACROS = f"""\
{{% macro package_impl() %}}
  {{{{ return(adapter.dispatch('package_impl', '{PACKAGE}')()) }}}}
{{% endmacro %}}
{{% macro default__package_impl() %}}'default'{{% endmacro %}}
{{% macro duckdb__package_impl() %}}'duckdb'{{% endmacro %}}
"""

MODEL_SQL = f"""\
select
    {{{{ project_impl() }}}} as project_impl,
    {{{{ {PACKAGE}.package_impl() }}}} as package_impl,
    {{{{ overridden_impl() }}}} as overridden_impl
"""


def _write_local_package(dbt_project: DbtProject) -> None:
    pkg = dbt_project.base / PACKAGE
    (pkg / "macros").mkdir(parents=True)
    (pkg / "dbt_project.yml").write_text(
        yaml.safe_dump(
            {"name": PACKAGE, "version": "1.0.0", "config-version": 2, "macro-paths": ["macros"]}
        ),
        encoding="utf-8",
    )
    (pkg / "macros" / "package_impl.sql").write_text(PACKAGE_MACROS, encoding="utf-8")
    (dbt_project.base / "packages.yml").write_text(
        yaml.safe_dump({"packages": [{"local": PACKAGE}]}), encoding="utf-8"
    )


@pytest.mark.altertable_integration
def test_dispatch_falls_back_to_duckdb_implementations(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml(models={"+materialized": "table"})
    macros_dir = dbt_project.base / "macros"
    macros_dir.mkdir()
    (macros_dir / "impls.sql").write_text(PROJECT_MACROS, encoding="utf-8")
    _write_local_package(dbt_project)
    dbt_project.write_model(MODEL, MODEL_SQL)

    dbt_project.run("deps")
    dbt_project.run("run", "--select", MODEL)

    rows = flight_client.query(f"select * from {dbt_project.qualify(MODEL)}").read_all().to_pylist()
    assert rows == [
        {"project_impl": "duckdb", "package_impl": "duckdb", "overridden_impl": "altertable"}
    ]
