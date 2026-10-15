from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from dbt_common.events import get_event_manager

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


@pytest.mark.parametrize("bindings", [None, ["sensitive-value"], {"token": "sensitive-value"}])
def test_execute_does_not_emit_sql_or_bindings(bindings, monkeypatch, capsys):
    sql = "select ? as sensitive_query" if bindings is not None else "select 1 as sensitive_query"
    table = pa.table({"result": [1]})
    client = MagicMock()
    client.query.return_value.read_all.return_value = table
    statement = client.prepare.return_value.__enter__.return_value
    statement.query.return_value.read_all.return_value = table
    cursor = AltertableCursor(client)
    messages: list[str] = []
    monkeypatch.setattr(
        get_event_manager(), "callbacks", [lambda event: messages.append(event.info.msg)]
    )

    cursor.execute(sql, bindings)

    assert cursor.fetchall() == [(1,)]
    assert sql not in "\n".join(messages)
    assert "sensitive-value" not in "\n".join(messages)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


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


def test_fetch_methods_preserve_duplicate_column_values_in_arrow_order() -> None:
    number_column = pa.chunked_array([[1, None], [3, 4, 5]])
    text_column = pa.chunked_array([["a"], ["b", "c", None, "e"]])
    cursor = AltertableCursor(MagicMock())
    cursor._table = pa.table([number_column, text_column], names=["value", "value"])

    first_row = cursor.fetchone()
    next_rows = cursor.fetchmany(2)
    remaining_rows = cursor.fetchall()

    assert cursor.description is not None
    assert [column[0] for column in cursor.description] == ["value", "value"]
    assert first_row == (1, "a")
    assert next_rows == [(None, "b"), (3, "c")]
    assert remaining_rows == [(4, None), (5, "e")]
    assert cursor.fetchone() is None
    assert cursor.fetchmany() == []
    assert cursor.fetchall() == []


def test_fetch_methods_preserve_rows_with_no_columns(sample_table: pa.Table) -> None:
    cursor = AltertableCursor(MagicMock())
    cursor._table = sample_table.select([])

    first_row = cursor.fetchone()
    next_rows = cursor.fetchmany()
    remaining_rows = cursor.fetchall()

    assert first_row == ()
    assert next_rows == [()]
    assert remaining_rows == [()]
    assert cursor.fetchone() is None


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
        (0, []),
        (1, [(1, "a")]),
        (2, [(1, "a"), (2, "b")]),
        (10, [(1, "a"), (2, "b"), (3, "c")]),
    ],
    ids=["no_limit", "zero_limit", "one_row_limit", "two_row_limit", "limit_above_row_count"],
)
def test_get_result_from_cursor_honors_limit_without_advancing_cursor(
    cursor_with_result: AltertableCursor,
    limit: int | None,
    expected_rows: list[tuple],
):
    cursor_with_result.fetchone()

    result = AltertableConnectionManager.get_result_from_cursor(cursor_with_result, limit)
    next_row = cursor_with_result.fetchone()

    assert tuple(result.column_names) == ("id", "name")
    assert [tuple(row) for row in result.rows] == expected_rows
    assert next_row == (2, "b")


def test_get_result_from_cursor_returns_empty_agate_table_when_cursor_has_no_result():
    cursor = AltertableCursor(MagicMock())

    result = AltertableConnectionManager.get_result_from_cursor(cursor, None)

    assert len(result.rows) == 0


@pytest.mark.parametrize(
    "limit,expected_rows",
    [
        (None, [(1, "001", True), (2, "null", False)]),
        (0, []),
        (1, [(1, "001", True)]),
        (10, [(1, "001", True), (2, "null", False)]),
    ],
    ids=["no_limit", "zero_limit", "limit_below_row_count", "limit_above_row_count"],
)
def test_get_result_from_cursor_preserves_duplicate_column_values_and_limit(
    limit: int | None,
    expected_rows: list[tuple],
) -> None:
    cursor = AltertableCursor(MagicMock())
    cursor._table = pa.table(
        [[1, 2], ["001", "null"], [True, False]], names=["value", "value", "value_2"]
    )

    result = AltertableConnectionManager.get_result_from_cursor(cursor, limit)

    assert tuple(result.column_names) == ("value", "value_2", "value_2_2")
    assert [tuple(row) for row in result.rows] == expected_rows
