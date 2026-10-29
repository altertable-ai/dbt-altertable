from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from altertable_flightsql import ComputeSize
from altertable_flightsql.client import IngestTableMode
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.altertable.connections import (
    AltertableConnection,
    AltertableConnectionManager,
    _normalize_flight_sql_scalar,
    _split_to_message_size,
)
from dbt.adapters.altertable.credentials import AltertableCredentials


@pytest.fixture
def base_creds_kwargs():
    return dict(
        username="test-user",
        password="test-secret",
        database="altertable_test",
        schema="analytics",
        host="flight.test.local",
        port=15002,
        tls=False,
    )


@pytest.fixture
def mock_client_cls(mocker):
    cls_mock = mocker.patch("dbt.adapters.altertable.connections.altertable_flightsql.Client")
    cls_mock.return_value = MagicMock()
    return cls_mock


def test_connection_diagnostics_exclude_password(base_creds_kwargs):
    creds = AltertableCredentials(**base_creds_kwargs)

    diagnostics = dict(creds.connection_info())

    assert "password" not in diagnostics
    assert creds.password not in repr(diagnostics)
    assert diagnostics["host"] == creds.host


def test_connect_client_keeps_flight_session_unscoped(mock_client_cls, base_creds_kwargs):
    creds = AltertableCredentials(**base_creds_kwargs)

    AltertableConnectionManager._connect_client(creds)

    mock_client_cls.assert_called_once_with(
        username=base_creds_kwargs["username"],
        password=base_creds_kwargs["password"],
        catalog=None,
        schema=None,
        host=base_creds_kwargs["host"],
        port=base_creds_kwargs["port"],
        tls=base_creds_kwargs["tls"],
        compute_size=None,
    )
    mock_client_cls.return_value.query.assert_not_called()


def test_connect_client_passes_compute_size(mock_client_cls, base_creds_kwargs):
    creds = AltertableCredentials.from_dict(
        {"type": "altertable", **base_creds_kwargs, "compute_size": "XL"}
    )

    AltertableConnectionManager._connect_client(creds)

    assert mock_client_cls.call_args.kwargs["compute_size"] is ComputeSize.XL


def test_compute_size_accepts_multi_x_sizes(base_creds_kwargs):
    creds = AltertableCredentials.from_dict(
        {"type": "altertable", **base_creds_kwargs, "compute_size": "2XL"}
    )

    assert creds.compute_size is ComputeSize.XXL


def test_connect_client_creates_client_for_each_dbt_connection(mock_client_cls, base_creds_kwargs):
    creds = AltertableCredentials(**base_creds_kwargs)
    first_client = MagicMock()
    second_client = MagicMock()
    mock_client_cls.side_effect = [first_client, second_client]

    first_connection = AltertableConnectionManager._connect_client(creds)
    second_connection = AltertableConnectionManager._connect_client(creds)

    assert mock_client_cls.call_count == 2
    assert first_connection._client is first_client
    assert second_connection._client is second_client


def test_connect_client_factory_opens_a_new_session_with_same_kwargs(
    mock_client_cls, base_creds_kwargs
):
    creds = AltertableCredentials(**base_creds_kwargs)
    first_client = MagicMock()
    second_client = MagicMock()
    mock_client_cls.side_effect = [first_client, second_client]

    connection = AltertableConnectionManager._connect_client(creds)
    reopened = connection._connect()

    assert reopened is second_client
    assert mock_client_cls.call_args_list[0] == mock_client_cls.call_args_list[1]


SESSION_EXPIRED = pa.ArrowInvalid(
    "gRPC returned precondition failed error, with message: Session expired. Open a new session."
)


def test_begin_reopens_expired_session_and_retries():
    expired_client = MagicMock()
    expired_client.begin_transaction.side_effect = SESSION_EXPIRED
    new_client = MagicMock()
    conn = AltertableConnection(expired_client, connect=MagicMock(return_value=new_client))

    conn.begin()

    expired_client.close.assert_called_once_with()
    assert conn._client is new_client
    assert conn._transaction is new_client.begin_transaction.return_value


def test_reconnect_logs_at_info_so_warn_error_does_not_abort_the_retry(mocker):
    warning = mocker.patch("dbt.adapters.altertable.connections.logger.warning")
    info = mocker.patch("dbt.adapters.altertable.connections.logger.info")
    expired_client = MagicMock()
    expired_client.begin_transaction.side_effect = SESSION_EXPIRED
    conn = AltertableConnection(expired_client, connect=MagicMock(return_value=MagicMock()))

    conn.begin()

    warning.assert_not_called()
    info.assert_called_once_with("Flight session expired; opening a new session")


def test_begin_reopens_session_even_if_closing_the_expired_one_fails():
    expired_client = MagicMock()
    expired_client.begin_transaction.side_effect = SESSION_EXPIRED
    expired_client.close.side_effect = SESSION_EXPIRED
    new_client = MagicMock()
    conn = AltertableConnection(expired_client, connect=MagicMock(return_value=new_client))

    conn.begin()

    assert conn._transaction is new_client.begin_transaction.return_value


def test_begin_does_not_reconnect_on_other_errors():
    client = MagicMock()
    client.begin_transaction.side_effect = pa.ArrowInvalid("something else")
    connect = MagicMock()
    conn = AltertableConnection(client, connect=connect)

    with pytest.raises(pa.ArrowInvalid, match="something else"):
        conn.begin()

    connect.assert_not_called()


def test_begin_retries_expired_session_only_once():
    expired_client = MagicMock()
    expired_client.begin_transaction.side_effect = SESSION_EXPIRED
    still_expired_client = MagicMock()
    still_expired_client.begin_transaction.side_effect = SESSION_EXPIRED
    connect = MagicMock(return_value=still_expired_client)
    conn = AltertableConnection(expired_client, connect=connect)

    with pytest.raises(pa.ArrowInvalid, match="Session expired"):
        conn.begin()

    connect.assert_called_once_with()
    assert conn._transaction is None


def test_begin_without_connect_factory_raises_expired_session():
    client = MagicMock()
    client.begin_transaction.side_effect = SESSION_EXPIRED
    conn = AltertableConnection(client)

    with pytest.raises(pa.ArrowInvalid, match="Session expired"):
        conn.begin()


def test_rollback_on_expired_session_clears_transaction_and_reconnects():
    expired_client = MagicMock()
    expired_client.rollback_transaction.side_effect = SESSION_EXPIRED
    new_client = MagicMock()
    conn = AltertableConnection(expired_client, connect=MagicMock(return_value=new_client))
    conn._transaction = MagicMock()

    conn.rollback()

    assert conn._transaction is None
    assert conn._client is new_client


def test_rollback_failure_still_clears_transaction():
    client = MagicMock()
    client.rollback_transaction.side_effect = pa.ArrowInvalid("rollback failed")
    connect = MagicMock()
    conn = AltertableConnection(client, connect=connect)
    conn._transaction = MagicMock()

    with pytest.raises(pa.ArrowInvalid, match="rollback failed"):
        conn.rollback()

    assert conn._transaction is None
    connect.assert_not_called()

    conn.begin()
    assert conn._transaction is client.begin_transaction.return_value


def test_cursor_reconnect_updates_connection_client():
    expired_client = MagicMock()
    expired_client.query.side_effect = SESSION_EXPIRED
    new_client = MagicMock()
    new_client.query.return_value.read_all.return_value = pa.table({"x": [1]})
    conn = AltertableConnection(expired_client, connect=MagicMock(return_value=new_client))

    conn.cursor().execute("select 1")

    assert conn._client is new_client


def test_altertable_connection_close_closes_underlying_client():
    mock_client = MagicMock()
    conn = AltertableConnection(mock_client)

    conn.close()

    mock_client.close.assert_called_once_with()


def test_altertable_connection_close_does_not_interrupt_dbt_cleanup(mocker):
    mock_client = MagicMock()
    mock_client.close.side_effect = RuntimeError("close session failed")
    warning = mocker.patch("dbt.adapters.altertable.connections.logger.warning")
    conn = AltertableConnection(mock_client)

    conn.close()

    warning.assert_called_once_with("Failed to close Flight session: close session failed")


def test_exception_handler_logs_exception_and_rolls_back(mocker):
    manager = MagicMock(spec=AltertableConnectionManager)
    log_exception = mocker.patch("dbt.adapters.altertable.connections.logger.exception")

    with (
        pytest.raises(DbtRuntimeError, match="query failed") as exc_info,
        AltertableConnectionManager.exception_handler(manager, "select broken"),
    ):
        raise ValueError("query failed")

    log_exception.assert_called_once_with("Error executing SQL (ValueError): query failed")
    manager.rollback_if_open.assert_called_once_with()
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_data_type_code_to_name_maps_arrow_types() -> None:
    assert AltertableConnectionManager.data_type_code_to_name(pa.int64()) == "BIGINT"
    assert AltertableConnectionManager.data_type_code_to_name(pa.uint16()) == "INTEGER"
    assert AltertableConnectionManager.data_type_code_to_name(pa.uint32()) == "BIGINT"
    assert AltertableConnectionManager.data_type_code_to_name(pa.uint64()) == "UBIGINT"
    assert (
        AltertableConnectionManager.data_type_code_to_name(pa.decimal128(18, 4)) == "DECIMAL(18, 4)"
    )
    assert AltertableConnectionManager.data_type_code_to_name(pa.string()) == "VARCHAR"


def test_normalize_flight_sql_scalar_preserves_fractional_decimal() -> None:
    value = Decimal("12345678901234567890.123456789")

    assert _normalize_flight_sql_scalar(value) == value


def test_normalize_flight_sql_scalar_converts_integral_decimal() -> None:
    assert _normalize_flight_sql_scalar(Decimal("42")) == 42


def test_normalize_flight_sql_scalar_preserves_datetime_microseconds() -> None:
    value = datetime(2024, 1, 1, 0, 0, 0, 123456)

    assert _normalize_flight_sql_scalar(value) == "2024-01-01 00:00:00.123456"


def test_catalog_alias_maps_to_database():
    creds = AltertableCredentials.from_dict(
        {
            "type": "altertable",
            "username": "test-user",
            "password": "test-secret",
            "catalog": "altertable_test",
            "schema": "analytics",
            "host": "flight.test.local",
            "port": 15002,
            "tls": False,
        }
    )

    assert creds.database == "altertable_test"


def test_split_to_message_size_keeps_every_row_and_bounds_each_batch():
    table = pa.table({"payload": ["x" * 800] * 10 + [""] * 70})

    parts = [p for batch in table.to_batches() for p in _split_to_message_size(batch, 1000)]

    assert pa.Table.from_batches(parts, schema=table.schema) == table
    assert all(part.nbytes <= 1000 or part.num_rows == 1 for part in parts)


def test_ingest_joins_the_open_transaction():
    client = MagicMock()
    conn = AltertableConnection(client)
    conn.begin()
    table = pa.table({"id": [1]})

    conn.ingest(table, "memory", "main", "orders")

    client.ingest.assert_called_once_with(
        table_name="orders",
        schema_name="main",
        catalog_name="memory",
        schema=table.schema,
        mode=IngestTableMode.CREATE,
        transaction=client.begin_transaction.return_value,
    )


def test_ingest_of_an_empty_table_still_sends_a_batch():
    client = MagicMock()
    writer = client.ingest.return_value.__enter__.return_value
    table = pa.table({"id": pa.array([], pa.int64())})

    AltertableConnection(client).ingest(table, "memory", "main", "orders")

    (batch,) = writer.write_batch.call_args.args
    assert batch.schema == table.schema
    assert batch.num_rows == 0
