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


def _session_lost_error() -> Exception:
    return Exception(
        'Flight returned internal error, with message: {"message":{"value":"Session not found"}}'
    )


def test_execute_reconnects_and_retries_once_when_worker_dropped_the_session(
    monkeypatch: pytest.MonkeyPatch, sample_table: pa.Table
):
    stale = MagicMock()
    stale.query.side_effect = _session_lost_error()
    healthy = MagicMock()
    healthy.query.return_value.read_all.return_value = sample_table
    monkeypatch.setattr(AltertableConnectionManager, "reconnect", lambda _stale: healthy)

    cursor = AltertableCursor(stale)
    cursor.execute("insert into t values (1)")

    assert cursor._client is healthy
    assert cursor.table is sample_table
    stale.query.assert_called_once()
    healthy.query.assert_called_once()


def test_execute_retries_only_the_failed_statement_of_a_batch(
    monkeypatch: pytest.MonkeyPatch, sample_table: pa.Table
):
    stale = MagicMock()
    delete_reader = MagicMock()
    delete_reader.read_all.return_value = pa.table({"x": [1]})
    stale.query.side_effect = [delete_reader, _session_lost_error()]
    healthy = MagicMock()
    healthy.query.return_value.read_all.return_value = sample_table
    monkeypatch.setattr(AltertableConnectionManager, "reconnect", lambda _stale: healthy)

    cursor = AltertableCursor(stale)
    cursor.execute("DELETE FROM t WHERE id = 1; INSERT INTO t SELECT 1;")

    # the DELETE ran once on the old session and must not be re-run
    assert stale.query.call_count == 2
    healthy.query.assert_called_once()
    assert "INSERT INTO t" in healthy.query.call_args.args[0]
    assert cursor.table is sample_table


def test_execute_reraises_non_session_errors_without_reconnecting(
    monkeypatch: pytest.MonkeyPatch,
):
    reconnects = []
    monkeypatch.setattr(
        AltertableConnectionManager, "reconnect", lambda stale: reconnects.append(stale)
    )
    client = MagicMock()
    client.query.side_effect = ValueError("syntax error near FROM")

    cursor = AltertableCursor(client)
    with pytest.raises(ValueError, match="syntax error near FROM"):
        cursor.execute("select bogus")

    assert reconnects == []


def test_reconnect_rebuilds_shared_client_once_for_a_dead_client(
    monkeypatch: pytest.MonkeyPatch,
):
    dead, fresh = MagicMock(), MagicMock()
    builds = []
    monkeypatch.setattr(AltertableConnectionManager, "_shared_client", dead)
    monkeypatch.setattr(AltertableConnectionManager, "_shared_credentials", MagicMock())
    monkeypatch.setattr(
        AltertableConnectionManager,
        "_build_client",
        lambda _creds: (builds.append(1), fresh)[1],
    )

    assert AltertableConnectionManager.reconnect(dead) is fresh
    assert AltertableConnectionManager._shared_client is fresh
    # a straggler thread still holding the dead client shares the rebuilt one, no second handshake
    assert AltertableConnectionManager.reconnect(dead) is fresh
    assert len(builds) == 1
    # other threads may still be mid-statement on the dead client; closing its
    # channel would turn their retryable session-lost errors into fatal ones
    dead.close.assert_not_called()
