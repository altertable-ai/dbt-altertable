from __future__ import annotations

import contextlib
import json
import uuid
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
import yaml
from dbt.adapters.capability import Capability

from dbt.adapters.altertable.impl import AltertableAdapter
from tests.integration._helpers import DbtProject, quoted_ident, sql_string_literal

SOURCE_TABLE = "orders"
NEVER_LOADED_YEAR = 1

pytestmark = pytest.mark.skipif(
    not AltertableAdapter.supports(Capability.TableLastModifiedMetadata),
    reason="Metadata freshness awaits the HTTP GET /tables integration.",
)


def write_sources(
    project: DbtProject, sources: dict[str, tuple[str, str]], table: str = SOURCE_TABLE
) -> None:
    project.write_models_yml(
        "sources",
        yaml.safe_dump(
            {
                "version": 2,
                "sources": [
                    {
                        "name": name,
                        "database": database,
                        "schema": schema,
                        "config": {"freshness": {"warn_after": {"count": 1, "period": "day"}}},
                        "tables": [{"name": table}],
                    }
                    for name, (database, schema) in sources.items()
                ],
            },
            sort_keys=False,
        ),
    )


def run_source_freshness(project: DbtProject) -> tuple[dict[str, dict[str, Any]], str]:
    proc = project.run("source", "freshness", check=False)
    sources_json = json.loads((project.base / "target" / "sources.json").read_text())
    results = {result["unique_id"].split(".")[-2]: result for result in sources_json["results"]}
    return results, proc.stdout


def loaded_at(result: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(result["max_loaded_at"].replace("Z", "+00:00"))


def max_loaded_at(project: DbtProject, source_name: str = "landing") -> datetime:
    results, output = run_source_freshness(project)
    assert source_name in results, output
    return loaded_at(results[source_name])


def server_now(client: Any) -> datetime:
    return client.query("select now() as now").read_all().to_pylist()[0]["now"]


def execute(client: Any, sql: str) -> None:
    client.query(sql).read_all()


def create_source_table(client: Any, project: DbtProject, *, inlined: bool) -> str:
    table = project.qualify(SOURCE_TABLE)
    execute(client, f"create table {table} (id bigint)")
    if inlined:
        execute(
            client,
            f"call {quoted_ident(project.db)}.set_option('data_inlining_row_limit', 10, "
            f"schema => {sql_string_literal(project.schema)}, "
            f"table_name => {sql_string_literal(SOURCE_TABLE)})",
        )
    execute(client, f"insert into {table} values (1), (2)")
    return table


@pytest.fixture
def ducklake_source(dbt_project: DbtProject) -> DbtProject:
    dbt_project.write_project_yml()
    write_sources(dbt_project, {"landing": (dbt_project.db, dbt_project.schema)})
    return dbt_project


@pytest.fixture
def other_schema(dbt_project: DbtProject, flight_client: Any) -> Iterator[str]:
    schema = f"{dbt_project.schema}_other"
    execute(flight_client, f"create schema {quoted_ident(dbt_project.db, schema)}")
    try:
        yield schema
    finally:
        with contextlib.suppress(Exception):
            execute(flight_client, f"drop schema {quoted_ident(dbt_project.db, schema)} cascade")


@pytest.fixture
def plain_catalog(flight_client: Any) -> Iterator[str]:
    catalog = f"plain_{uuid.uuid4().hex[:8]}"
    try:
        execute(flight_client, f"attach ':memory:' as {quoted_ident(catalog)}")
    except Exception as error:
        pytest.skip(f"Target does not allow attaching a non-DuckLake catalog: {error}")
    try:
        yield catalog
    finally:
        with contextlib.suppress(Exception):
            execute(flight_client, f"detach {quoted_ident(catalog)}")


@pytest.mark.altertable_integration
def test_source_without_loaded_at_field_reads_its_last_change_from_ducklake(
    ducklake_source: DbtProject,
    flight_client: Any,
) -> None:
    execute(flight_client, f"create table {ducklake_source.qualify(SOURCE_TABLE)} (id bigint)")
    before_insert = server_now(flight_client)
    execute(flight_client, f"insert into {ducklake_source.qualify(SOURCE_TABLE)} values (1), (2)")
    after_insert = server_now(flight_client)

    source_loaded_at = max_loaded_at(ducklake_source)

    assert before_insert <= source_loaded_at <= after_insert


@pytest.mark.altertable_integration
@pytest.mark.parametrize(
    ("inlined", "change_sql"),
    [
        (False, "insert into {table} values (3)"),
        (False, "delete from {table} where id = 1"),
        (True, "insert into {table} values (3)"),
        (True, "delete from {table} where id = 1"),
    ],
    ids=["inserted_into_table", "deleted_from_table", "inlined_insert", "inlined_delete"],
)
def test_each_kind_of_data_change_advances_source_freshness(
    ducklake_source: DbtProject,
    flight_client: Any,
    inlined: bool,
    change_sql: str,
) -> None:
    table = create_source_table(flight_client, ducklake_source, inlined=inlined)
    loaded_at_before_change = max_loaded_at(ducklake_source)

    execute(flight_client, change_sql.format(table=table))

    assert max_loaded_at(ducklake_source) > loaded_at_before_change


@pytest.mark.altertable_integration
def test_flushing_inlined_inserts_is_not_a_data_change(
    ducklake_source: DbtProject,
    flight_client: Any,
) -> None:
    create_source_table(flight_client, ducklake_source, inlined=True)
    loaded_at_before_flush = max_loaded_at(ducklake_source)

    execute(
        flight_client,
        f"call ducklake_flush_inlined_data({sql_string_literal(ducklake_source.db)}, "
        f"schema_name => {sql_string_literal(ducklake_source.schema)}, "
        f"table_name => {sql_string_literal(SOURCE_TABLE)})",
    )

    assert max_loaded_at(ducklake_source) == loaded_at_before_flush


@pytest.mark.altertable_integration
def test_a_change_to_a_same_named_table_in_another_schema_is_ignored(
    ducklake_source: DbtProject,
    flight_client: Any,
    other_schema: str,
) -> None:
    create_source_table(flight_client, ducklake_source, inlined=False)
    same_named_table = quoted_ident(ducklake_source.db, other_schema, SOURCE_TABLE)
    execute(flight_client, f"create table {same_named_table} (id bigint)")
    loaded_at_before_change = max_loaded_at(ducklake_source)

    execute(flight_client, f"insert into {same_named_table} values (1)")

    assert max_loaded_at(ducklake_source) == loaded_at_before_change


@pytest.mark.altertable_integration
def test_an_empty_source_table_reports_its_creation_time(
    ducklake_source: DbtProject,
    flight_client: Any,
) -> None:
    before_creation = server_now(flight_client)
    execute(flight_client, f"create table {ducklake_source.qualify(SOURCE_TABLE)} (id bigint)")
    after_creation = server_now(flight_client)

    source_loaded_at = max_loaded_at(ducklake_source)

    assert before_creation <= source_loaded_at <= after_creation


@pytest.mark.altertable_integration
def test_a_source_outside_ducklake_reads_as_never_loaded_without_breaking_the_batch(
    dbt_project: DbtProject,
    flight_client: Any,
    plain_catalog: str,
) -> None:
    create_source_table(flight_client, dbt_project, inlined=False)
    execute(flight_client, f"create schema {quoted_ident(plain_catalog, 'landing')}")
    execute(
        flight_client,
        f"create table {quoted_ident(plain_catalog, 'landing', SOURCE_TABLE)} (id bigint)",
    )
    dbt_project.write_project_yml()
    write_sources(
        dbt_project,
        {"landing": (dbt_project.db, dbt_project.schema), "federated": (plain_catalog, "landing")},
    )

    results, output = run_source_freshness(dbt_project)

    assert "could not be computed in batch" not in output, output
    assert loaded_at(results["landing"]).year > NEVER_LOADED_YEAR
    assert loaded_at(results["federated"]).year == NEVER_LOADED_YEAR


@pytest.mark.altertable_integration
def test_renaming_a_source_table_does_not_advance_freshness(
    ducklake_source: DbtProject,
    flight_client: Any,
) -> None:
    create_source_table(flight_client, ducklake_source, inlined=False)
    loaded_at_before_rename = max_loaded_at(ducklake_source)
    renamed_table = f"{SOURCE_TABLE}_renamed"

    execute(
        flight_client,
        f"alter table {ducklake_source.qualify(SOURCE_TABLE)} "
        f"rename to {quoted_ident(renamed_table)}",
    )
    write_sources(
        ducklake_source, {"landing": (ducklake_source.db, ducklake_source.schema)}, renamed_table
    )

    assert max_loaded_at(ducklake_source) == loaded_at_before_rename


@pytest.mark.altertable_integration
def test_a_table_name_that_looks_like_a_change_entry_does_not_advance_another_table(
    ducklake_source: DbtProject,
    flight_client: Any,
) -> None:
    create_source_table(flight_client, ducklake_source, inlined=False)
    metadata_catalog = quoted_ident("__ducklake_metadata_" + ducklake_source.db)
    source_table_id = (
        flight_client.query(
            f"select table_id from {metadata_catalog}.ducklake_table "
            f"where table_name = {sql_string_literal(SOURCE_TABLE)} and end_snapshot is null"
        )
        .read_all()
        .to_pylist()[0]["table_id"]
    )
    loaded_at_before_lookalike = max_loaded_at(ducklake_source)
    lookalike_name = f"x,inserted_into_table:{source_table_id}"
    lookalike_table = quoted_ident(ducklake_source.db, ducklake_source.schema, lookalike_name)

    execute(
        flight_client,
        f"create table {lookalike_table} (id bigint)",
    )

    assert max_loaded_at(ducklake_source) == loaded_at_before_lookalike
