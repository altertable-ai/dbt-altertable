"""
Server-side contract tests for Flight transactions.

``tests/test_transaction_routing.py`` pins the *adapter* side of this contract
against a fake. These talk to whatever Flight SQL implementation the integration
env points at and assert the server really provides what that fake assumes:

1. A transaction runs on the session's own connection, so session state is shared
   with it.
2. While a transaction is active, every statement must carry its transaction id.
3. ``BEGIN`` while a transaction is active, or a statement carrying a finished
   transaction id, is rejected.

If any of these turn red, the fake in the unit tests is no longer describing the
server and the adapter's routing assumptions need revisiting.
"""

from __future__ import annotations

from typing import Any

import pytest

TEMP_TABLE = "affinity_tmp"


def _pylist(result: Any) -> list[dict[str, Any]]:
    return result.read_all().to_pylist()


@pytest.mark.altertable_integration
def test_temp_relation_created_on_the_session_is_readable_inside_a_transaction(
    flight_client: Any,
) -> None:
    """
    The ``incremental`` and ``unit`` materializations both stage data in a temp
    relation through ``run_query()`` (``auto_begin=false``) and then read it back
    from statements that do open a transaction.
    """
    flight_client.query(f"create temporary table {TEMP_TABLE} as select 1 as id, 'a' as label")

    transaction = flight_client.begin_transaction()
    try:
        rows = _pylist(
            flight_client.query(f"select id, label from {TEMP_TABLE}", transaction=transaction)
        )
    finally:
        flight_client.commit_transaction(transaction)

    assert rows == [{"id": 1, "label": "a"}]


@pytest.mark.altertable_integration
def test_temp_relation_is_visible_to_duckdb_columns_inside_a_transaction(
    flight_client: Any,
) -> None:
    """
    ``altertable__get_columns_in_relation`` filters ``duckdb_columns()`` by name,
    so a temp relation the transaction cannot see yields zero rows rather than an
    error — the ``unit`` materialization then fails downstream on an empty column
    map. Assert the rows are there.
    """
    flight_client.query(f"create temporary table {TEMP_TABLE} as select 1 as id, 'a' as label")

    transaction = flight_client.begin_transaction()
    try:
        columns = _pylist(
            flight_client.query(
                "select column_name from duckdb_columns() "
                f"where table_name = '{TEMP_TABLE}' order by column_index",
                transaction=transaction,
            )
        )
    finally:
        flight_client.commit_transaction(transaction)

    assert [c["column_name"] for c in columns] == ["id", "label"]


@pytest.mark.altertable_integration
def test_temp_relation_created_inside_a_transaction_survives_the_commit(
    flight_client: Any,
) -> None:
    """dbt drops the temp relation after ``adapter.commit()``, outside any transaction."""
    transaction = flight_client.begin_transaction()
    try:
        flight_client.query(
            f"create temporary table {TEMP_TABLE} as select 1 as id",
            transaction=transaction,
        )
    finally:
        flight_client.commit_transaction(transaction)

    assert _pylist(flight_client.query(f"select id from {TEMP_TABLE}")) == [{"id": 1}]


@pytest.mark.altertable_integration
def test_a_statement_that_bypasses_an_open_transaction_is_rejected(
    flight_client: Any,
) -> None:
    """
    This is what makes the routing in ``AltertableConnection.cursor()`` load
    bearing: a leaked bare statement is a hard server error, not a silent
    divergence.
    """
    transaction = flight_client.begin_transaction()
    try:
        with pytest.raises(Exception, match="a transaction is already active"):
            flight_client.query("select 1 as x").read_all()
    finally:
        flight_client.commit_transaction(transaction)


@pytest.mark.altertable_integration
def test_a_second_transaction_on_one_session_is_rejected(flight_client: Any) -> None:
    """Session and transaction are 1:1, so ``begin()`` has to reuse the open one."""
    transaction = flight_client.begin_transaction()
    try:
        with pytest.raises(Exception, match="a transaction is already active"):
            flight_client.begin_transaction()
    finally:
        flight_client.commit_transaction(transaction)


@pytest.mark.altertable_integration
def test_a_finished_transaction_id_is_rejected(flight_client: Any) -> None:
    """A stale handle after ``commit()`` would surface here rather than silently working."""
    transaction = flight_client.begin_transaction()
    flight_client.commit_transaction(transaction)

    with pytest.raises(Exception, match="not found"):
        flight_client.query("select 1 as x", transaction=transaction).read_all()
