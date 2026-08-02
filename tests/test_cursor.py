from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from dbt.adapters.altertable.connections import (
    AltertableConnectionManager,
    AltertableCursor,
)


@pytest.fixture
def sample_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


@pytest.fixture
def cursor_with_result(sample_table: pa.Table) -> AltertableCursor:
    cursor = AltertableCursor(MagicMock())
    cursor._table = sample_table
    return cursor


def test_fresh_cursor_exposes_pep249_sentinels_before_any_execute():
    cursor = AltertableCursor(MagicMock())
    assert cursor.description is None
    assert cursor.rowcount == -1
    assert cursor.fetchone() is None
    assert cursor.fetchmany() == []
    assert cursor.fetchall() == []


def test_description_derives_column_names_and_seven_tuple_shape_from_arrow_schema(
    cursor_with_result: AltertableCursor,
):
    description = cursor_with_result.description
    assert description is not None
    assert [column[0] for column in description] == ["id", "name"]
    assert all(len(column) == 7 for column in description)


def test_rowcount_equals_arrow_table_num_rows(cursor_with_result: AltertableCursor):
    assert cursor_with_result.rowcount == 3


def test_fetchone_walks_rows_in_arrow_order_then_returns_none_once_exhausted(
    cursor_with_result: AltertableCursor,
):
    assert cursor_with_result.fetchone() == (1, "a")
    assert cursor_with_result.fetchone() == (2, "b")
    assert cursor_with_result.fetchone() == (3, "c")
    assert cursor_with_result.fetchone() is None


def test_fetchmany_advances_position_across_calls_and_returns_empty_when_exhausted(
    cursor_with_result: AltertableCursor,
):
    assert cursor_with_result.fetchmany(2) == [(1, "a"), (2, "b")]
    assert cursor_with_result.fetchmany(2) == [(3, "c")]
    assert cursor_with_result.fetchmany(2) == []


def test_fetchall_after_partial_consumption_returns_only_remaining_rows(
    cursor_with_result: AltertableCursor,
):
    cursor_with_result.fetchone()
    assert cursor_with_result.fetchall() == [(2, "b"), (3, "c")]
    assert cursor_with_result.fetchall() == []


def test_close_releases_arrow_table_and_restores_pep249_sentinels(
    cursor_with_result: AltertableCursor,
):
    cursor_with_result.close()
    assert cursor_with_result.table is None
    assert cursor_with_result.description is None
    assert cursor_with_result.rowcount == -1
    assert cursor_with_result.fetchone() is None


@pytest.mark.parametrize(
    "limit,expected_rows",
    [
        (None, [(1, "a"), (2, "b"), (3, "c")]),
        (2, [(1, "a"), (2, "b")]),
        (10, [(1, "a"), (2, "b"), (3, "c")]),
    ],
    ids=["no_limit", "limit_below_row_count", "limit_above_row_count"],
)
def test_get_result_from_cursor_returns_agate_table_honoring_limit(
    cursor_with_result: AltertableCursor,
    limit: int | None,
    expected_rows: list[tuple],
):
    result = AltertableConnectionManager.get_result_from_cursor(cursor_with_result, limit)

    assert tuple(result.column_names) == ("id", "name")
    assert [tuple(row) for row in result.rows] == expected_rows


def test_get_result_from_cursor_returns_empty_agate_table_when_cursor_has_no_result():
    cursor = AltertableCursor(MagicMock())

    result = AltertableConnectionManager.get_result_from_cursor(cursor, None)

    assert len(result.rows) == 0


@pytest.mark.parametrize(
    "sql",
    [
        """\
create temporary table "unit_test__dbt_tmp20260802110054347458"
as (
    select * from (select 1 as item_id) as __dbt_sbq
    where false
    limit 0
)
/* {"app": "dbt"} */;
""",
        "create or replace view new_view as select 1 as id",
        "/* dbt */ alter table old_table rename to new_table",
        "/* dbt */ comment on table new_table is 'model description'",
        "insert into new_table values (1)",
        "with new_rows as (select 1 as id) insert into new_table select * from new_rows",
    ],
    ids=["create", "create_or_replace", "alter", "comment", "insert", "with_insert"],
)
def test_non_row_statements_use_flight_update_with_transaction(sql: str) -> None:
    client = MagicMock()
    transaction = MagicMock()
    cursor = AltertableCursor(client, transaction)

    cursor.execute(sql)

    client.execute.assert_called_once_with(sql, transaction=transaction)
    client.query.assert_not_called()
    assert cursor.table is None


@pytest.mark.parametrize(
    "sql",
    [
        "select 1 as value",
        "with value as (select 1 as n) select n from value",
        "show tables",
        "describe new_table",
        "pragma show_tables",
        "explain select 1",
    ],
    ids=["select", "with", "show", "describe", "pragma", "explain"],
)
def test_row_returning_statements_stay_on_flight_query(sql: str) -> None:
    client = MagicMock()
    transaction = MagicMock()
    reader = MagicMock()
    reader.read_all.return_value = pa.table({"value": [1]})
    client.query.return_value = reader
    cursor = AltertableCursor(client, transaction)

    cursor.execute(sql)

    client.query.assert_called_once_with(sql, transaction=transaction)
    client.execute.assert_not_called()
    assert cursor.table is not None
    assert cursor.table.to_pylist() == [{"value": 1}]


def test_large_row_returning_statement_does_not_require_a_full_sql_parse() -> None:
    sql = (
        "with values_to_check as (select 1 where 1 in ("
        + ",".join(["1"] * 10_001)
        + ")) select * from values_to_check"
    )
    client = MagicMock()
    reader = MagicMock()
    reader.read_all.return_value = pa.table({"value": [1]})
    client.query.return_value = reader
    cursor = AltertableCursor(client)

    cursor.execute(sql)

    client.query.assert_called_once_with(sql, transaction=None)
    client.execute.assert_not_called()


def test_successful_create_tracks_only_generated_scoped_scratch_relation() -> None:
    invocation_id = "0123456789abcdef0123456789abcdef"
    sql = (
        'create table "memory"."analytics".'
        f'"orders""archive__dbt_tmp{invocation_id}" as (select 1 as id)'
    )
    client = MagicMock()
    scratch_relations: set[tuple[str, ...]] = set()
    cursor = AltertableCursor(client, scratch_relations=scratch_relations)

    cursor.execute(sql)

    assert scratch_relations == {("memory", "analytics", f'orders"archive__dbt_tmp{invocation_id}')}


def test_regular_create_without_generated_suffix_is_not_tracked() -> None:
    client = MagicMock()
    scratch_relations: set[tuple[str, ...]] = set()
    cursor = AltertableCursor(client, scratch_relations=scratch_relations)

    cursor.execute('create table "memory"."analytics"."orders" as (select 1 as id)')

    assert scratch_relations == set()


def test_successful_drop_untracks_generated_scoped_scratch_relation() -> None:
    invocation_id = "0123456789abcdef0123456789abcdef"
    relation = ("memory", "analytics", f"orders__dbt_tmp{invocation_id}")
    client = MagicMock()
    scratch_relations = {relation}
    cursor = AltertableCursor(client, scratch_relations=scratch_relations)

    cursor.execute(f'drop table if exists "memory"."analytics"."orders__dbt_tmp{invocation_id}"')

    assert scratch_relations == set()


def test_transactional_drop_keeps_scratch_relation_tracked_until_transaction_ends() -> None:
    invocation_id = "0123456789abcdef0123456789abcdef"
    relation = ("memory", "analytics", f"orders__dbt_tmp{invocation_id}")
    client = MagicMock()
    transaction = MagicMock()
    scratch_relations = {relation}
    cursor = AltertableCursor(client, transaction, scratch_relations)

    cursor.execute(f'drop table if exists "memory"."analytics"."orders__dbt_tmp{invocation_id}"')

    assert scratch_relations == {relation}
