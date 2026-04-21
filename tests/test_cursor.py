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
