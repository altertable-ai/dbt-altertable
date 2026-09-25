"""
Transaction routing, exercised against a real Flight server.

A session and its transaction are 1:1: the transaction runs on the session's own
connection, and while it is open every statement must carry its id — the server
rejects a bare one outright. So ``auto_begin=False`` has to keep using an open
transaction rather than bypass it, and that routing lives in
``AltertableConnection.cursor()``.

These drive ``AltertableConnectionManager`` rather than the Flight client, so an
adapter regression fails here and not only a server one. The last two go through
the client directly: the adapter has no way to emit the statements they assert on.
"""

from __future__ import annotations

from collections.abc import Iterator
from multiprocessing import get_context
from typing import Any
from unittest.mock import MagicMock

import pytest
from dbt.adapters.contracts.connection import Connection, ConnectionState

from dbt.adapters.altertable.connections import (
    AltertableConnection,
    AltertableConnectionManager,
)

# Unqualified on purpose: altertable__make_temp_relation strips database and
# schema, so this is the shape the server actually receives.
TEMP_RELATION = "affinity_model__dbt_tmp160142387245"

CREATE_TEMP_SQL = f"create temporary table \"{TEMP_RELATION}\" as (select 1 as id, 'a' as label)"
SELECT_TEMP_SQL = f'select id, label from "{TEMP_RELATION}"'
DROP_TEMP_SQL = f'drop table if exists "{TEMP_RELATION}"'
COLUMNS_IN_TEMP_SQL = (
    "select column_name from duckdb_columns() "
    f"where table_name = '{TEMP_RELATION}' order by column_index"
)


@pytest.fixture
def manager(flight_client: Any) -> Iterator[AltertableConnectionManager]:
    """``profile`` and ``credentials`` are stubs — these paths reach the server
    through ``connection.handle`` and never consult them."""
    connection_manager = AltertableConnectionManager(
        profile=MagicMock(), mp_context=get_context("spawn")
    )
    connection_manager.set_thread_connection(
        Connection(
            type="altertable",
            name="test.transaction_affinity",
            state=ConnectionState.OPEN,
            handle=AltertableConnection(flight_client),
            credentials=MagicMock(),
        )
    )
    try:
        yield connection_manager
    finally:
        connection_manager.rollback_if_open()
        connection_manager.clear_thread_connection()


@pytest.mark.altertable_integration
def test_temp_relation_staged_outside_a_transaction_is_readable_inside_one(
    manager: AltertableConnectionManager,
) -> None:
    manager.add_query(CREATE_TEMP_SQL, auto_begin=False)
    assert manager.get_thread_connection().transaction_open is False

    _, cursor = manager.add_query(SELECT_TEMP_SQL, auto_begin=True)

    assert manager.get_thread_connection().transaction_open is True
    assert cursor.fetchall() == [(1, "a")]
    manager.commit()


@pytest.mark.altertable_integration
def test_staged_temp_relation_is_visible_to_get_columns_in_relation(
    manager: AltertableConnectionManager,
) -> None:
    """``get_columns_in_relation`` filters ``duckdb_columns()`` by name, so an
    unreachable relation yields zero rows instead of raising. Assert on the rows,
    or the failure stays silent until the caller trips over an empty column map."""
    manager.add_query(CREATE_TEMP_SQL, auto_begin=False)

    _, cursor = manager.add_query(COLUMNS_IN_TEMP_SQL, auto_begin=True)

    assert [row[0] for row in cursor.fetchall()] == ["id", "label"]
    manager.commit()


@pytest.mark.altertable_integration
def test_temp_relation_staged_inside_a_transaction_survives_the_commit(
    manager: AltertableConnectionManager,
) -> None:
    """dbt reads and drops the staged relation after ``adapter.commit()``."""
    manager.add_query(CREATE_TEMP_SQL, auto_begin=True)
    manager.commit()

    _, cursor = manager.add_query(SELECT_TEMP_SQL, auto_begin=False)

    assert cursor.fetchall() == [(1, "a")]


@pytest.mark.altertable_integration
def test_statements_are_routed_through_an_open_transaction_whatever_auto_begin_says(
    manager: AltertableConnectionManager,
) -> None:
    """``auto_begin=False`` means "do not *open* a transaction", never "do not use
    the open one"."""
    manager.add_query("select 1 as x", auto_begin=True)

    _, cursor = manager.add_query("select 2 as x", auto_begin=False)

    assert cursor.fetchall() == [(2,)]
    assert manager.get_thread_connection().transaction_open is True
    manager.commit()


@pytest.mark.altertable_integration
def test_statements_after_commit_run_on_the_bare_session(
    manager: AltertableConnectionManager,
) -> None:
    """A handle left behind by ``commit()`` would answer ``Transaction not found``."""
    manager.add_query(CREATE_TEMP_SQL, auto_begin=True)
    manager.commit()

    manager.add_query(DROP_TEMP_SQL, auto_begin=False)

    assert manager.get_thread_connection().transaction_open is False


@pytest.mark.altertable_integration
def test_server_rejects_a_statement_that_bypasses_an_open_transaction(
    flight_client: Any,
) -> None:
    """The server-side half of the routing invariant: a leak is refused, not tolerated."""
    transaction = flight_client.begin_transaction()
    try:
        with pytest.raises(Exception, match="a transaction is already active"):
            flight_client.query("select 1 as x").read_all()
    finally:
        flight_client.commit_transaction(transaction)


@pytest.mark.altertable_integration
def test_server_rejects_a_second_transaction_on_one_session(flight_client: Any) -> None:
    """Why ``begin()`` has to reuse the open transaction rather than nest."""
    transaction = flight_client.begin_transaction()
    try:
        with pytest.raises(Exception, match="a transaction is already active"):
            flight_client.begin_transaction()
    finally:
        flight_client.commit_transaction(transaction)
