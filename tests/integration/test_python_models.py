from __future__ import annotations

from typing import Any

import pytest

from tests.integration._helpers import DbtProject, count_in_catalog

UPSTREAM = "integ_py_upstream"
PY_TABLE = "integ_py_table"
PY_INC = "integ_py_inc"

TABLE_MODEL = """\
import pyarrow.compute as pc


def model(dbt, session):
    upstream = dbt.ref("integ_py_upstream")
    return upstream.append_column("doubled", pc.multiply(upstream["id"], 2))
"""

INCREMENTAL_MODEL = """\
import pyarrow as pa


def model(dbt, session):
    dbt.config(materialized="incremental", unique_key="id")
    if dbt.is_incremental:
        return pa.table({"id": [2, 3], "phase": ["second_run", "second_run"]})
    return dbt.ref("integ_py_upstream").append_column("phase", pa.array(["first_run"] * 2))
"""

EMPTY_MODEL = """\
import pyarrow as pa


def model(dbt, session):
    return pa.table({"id": pa.array([], pa.int64())})
"""

FAILING_MODEL = """\
def model(dbt, session):
    upstream = f"{dbt.this.database}.{dbt.this.schema}.integ_py_upstream"
    session.cursor().execute(f"update {upstream} set id = 99")
    raise ValueError("model failed after writing")
    return None
"""

FIRST_RUN_INCREMENTAL_MODEL = """\
import pyarrow as pa


def model(dbt, session):
    dbt.config(materialized="incremental")
    return pa.table({"id": [1]})
"""

LARGE_MODEL = """\
import pyarrow as pa


def model(dbt, session):
    return pa.table({"payload": ["x" * 8000] * 1000 + [""] * 7000})
"""

PANDAS_MODEL = """\
import pandas as pd


def model(dbt, session):
    df = pd.DataFrame({"id": [1, 2], "country": pd.Categorical(["fr", "de"])}, index=["a", "b"])
    return df[df["id"] > 1]
"""

EVENT_LOOP_MODEL = """\
import asyncio

import pyarrow as pa


async def answer():
    return 42


def model(dbt, session):
    return pa.table({"id": [asyncio.get_event_loop().run_until_complete(answer())]})
"""


def _write_python_model(dbt_project: DbtProject, name: str, code: str) -> None:
    (dbt_project.models_dir / f"{name}.py").write_text(code, encoding="utf-8")


def _rows(flight_client: Any, dbt_project: DbtProject, name: str) -> list[dict[str, Any]]:
    return (
        flight_client.query(f"select * from {dbt_project.qualify(name)} order by all")
        .read_all()
        .to_pylist()
    )


def _table_exists(flight_client: Any, dbt_project: DbtProject, name: str) -> bool:
    db, schema = dbt_project.db, dbt_project.schema
    return count_in_catalog(flight_client, kind="table", database=db, schema=schema, name=name) > 0


@pytest.fixture
def upstream(dbt_project: DbtProject) -> None:
    dbt_project.write_project_yml()
    dbt_project.write_model(
        UPSTREAM, "{{ config(materialized='table') }}\nselect 1 as id union all select 2 as id\n"
    )
    dbt_project.run("run", "--select", UPSTREAM)


@pytest.mark.altertable_integration
@pytest.mark.usefixtures("upstream")
def test_python_table_model_reads_a_ref_and_rebuilds(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    _write_python_model(dbt_project, PY_TABLE, TABLE_MODEL)

    dbt_project.run("run", "--select", PY_TABLE)
    dbt_project.run("run", "--select", PY_TABLE)

    assert _rows(flight_client, dbt_project, PY_TABLE) == [
        {"id": 1, "doubled": 2},
        {"id": 2, "doubled": 4},
    ]


@pytest.mark.altertable_integration
@pytest.mark.usefixtures("upstream")
@pytest.mark.parametrize("strategy", ["delete+insert", "merge"])
def test_python_incremental_model_upserts_on_the_second_run(
    dbt_project: DbtProject, flight_client: Any, strategy: str
) -> None:
    dbt_project.write_project_yml(models={"+incremental_strategy": strategy})
    _write_python_model(dbt_project, PY_INC, INCREMENTAL_MODEL)

    dbt_project.run("run", "--select", PY_INC)
    dbt_project.run("run", "--select", PY_INC)

    assert _rows(flight_client, dbt_project, PY_INC) == [
        {"id": 1, "phase": "first_run"},
        {"id": 2, "phase": "second_run"},
        {"id": 3, "phase": "second_run"},
    ]


@pytest.mark.altertable_integration
def test_python_model_with_no_rows_creates_an_empty_table(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    _write_python_model(dbt_project, PY_TABLE, EMPTY_MODEL)

    dbt_project.run("run", "--select", PY_TABLE)

    assert _table_exists(flight_client, dbt_project, PY_TABLE)
    assert _rows(flight_client, dbt_project, PY_TABLE) == []


@pytest.mark.altertable_integration
@pytest.mark.usefixtures("upstream")
def test_python_model_error_rolls_back_its_session_writes_and_keeps_the_previous_table(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    _write_python_model(dbt_project, PY_TABLE, TABLE_MODEL)
    dbt_project.run("run", "--select", PY_TABLE)
    _write_python_model(dbt_project, PY_TABLE, FAILING_MODEL)

    failed_run = dbt_project.run("run", "--select", PY_TABLE, check=False)

    assert failed_run.returncode != 0
    assert "model failed after writing" in failed_run.stdout, failed_run.stdout
    assert _rows(flight_client, dbt_project, UPSTREAM) == [{"id": 1}, {"id": 2}]
    assert _rows(flight_client, dbt_project, PY_TABLE) == [
        {"id": 1, "doubled": 2},
        {"id": 2, "doubled": 4},
    ]


@pytest.mark.altertable_integration
def test_python_model_first_run_is_rolled_back_when_a_post_hook_fails(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml(models={"+post-hook": "select * from integ_no_such_table"})
    _write_python_model(dbt_project, PY_INC, FIRST_RUN_INCREMENTAL_MODEL)

    failed_run = dbt_project.run("run", "--select", PY_INC, check=False)

    assert failed_run.returncode != 0
    assert "integ_no_such_table" in failed_run.stdout, failed_run.stdout
    assert not _table_exists(flight_client, dbt_project, PY_INC)


@pytest.mark.altertable_integration
def test_python_model_result_larger_than_a_grpc_message_is_written(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    _write_python_model(dbt_project, PY_TABLE, LARGE_MODEL)

    dbt_project.run("run", "--select", PY_TABLE)

    count = flight_client.query(f"select count(*) as n from {dbt_project.qualify(PY_TABLE)}")
    assert count.read_all().to_pylist() == [{"n": 8000}]


@pytest.mark.altertable_integration
def test_python_model_returning_pandas_writes_plain_columns(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    _write_python_model(dbt_project, PY_TABLE, PANDAS_MODEL)

    dbt_project.run("run", "--select", PY_TABLE)

    assert _rows(flight_client, dbt_project, PY_TABLE) == [{"id": 2, "country": "de"}]


@pytest.mark.altertable_integration
def test_python_model_can_use_the_thread_event_loop(
    dbt_project: DbtProject, flight_client: Any
) -> None:
    dbt_project.write_project_yml()
    _write_python_model(dbt_project, PY_TABLE, EVENT_LOOP_MODEL)

    dbt_project.run("run", "--select", PY_TABLE)

    assert _rows(flight_client, dbt_project, PY_TABLE) == [{"id": 42}]
