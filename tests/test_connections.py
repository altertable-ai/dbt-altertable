from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from dbt.adapters.altertable.connections import (
    AltertableConnection,
    AltertableConnectionManager,
    AltertableCursor,
    _normalize_flight_sql_scalar,
    _split_sql_into_statements,
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
    AltertableConnectionManager._shared_client = None
    AltertableConnectionManager._shared_credentials_key = None
    cls_mock = mocker.patch("dbt.adapters.altertable.connections.altertable_flightsql.Client")
    cls_mock.return_value = MagicMock()
    return cls_mock


def test_connect_client_passes_database_and_schema_as_catalog_and_schema(
    mock_client_cls, base_creds_kwargs
):
    creds = AltertableCredentials(**base_creds_kwargs)

    AltertableConnectionManager._connect_client(creds)

    mock_client_cls.assert_called_once_with(
        username=base_creds_kwargs["username"],
        password=base_creds_kwargs["password"],
        catalog=base_creds_kwargs["database"],
        schema=base_creds_kwargs["schema"],
        host=base_creds_kwargs["host"],
        port=base_creds_kwargs["port"],
        tls=base_creds_kwargs["tls"],
    )
    mock_client_cls.return_value.query.assert_not_called()


def test_connect_client_reuses_shared_client_for_same_credentials(
    mock_client_cls, base_creds_kwargs
):
    creds = AltertableCredentials(**base_creds_kwargs)

    conn1 = AltertableConnectionManager._connect_client(creds)
    conn2 = AltertableConnectionManager._connect_client(creds)

    assert mock_client_cls.call_count == 1
    assert conn1._client is conn2._client


def test_connect_client_creates_new_client_when_credentials_change(
    mock_client_cls, base_creds_kwargs
):
    creds1 = AltertableCredentials(**base_creds_kwargs)
    creds2 = AltertableCredentials(**{**base_creds_kwargs, "database": "other_catalog"})

    AltertableConnectionManager._connect_client(creds1)
    AltertableConnectionManager._connect_client(creds2)

    assert mock_client_cls.call_count == 2


def test_altertable_connection_close_does_not_close_underlying_client():
    mock_client = MagicMock()
    conn = AltertableConnection(mock_client)

    conn.close()

    mock_client.close.assert_not_called()


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


def test_split_sql_returns_single_statement_unchanged() -> None:
    assert _split_sql_into_statements("SELECT 1") == ["SELECT 1"]


def test_split_sql_drops_empty_trailing_segment() -> None:
    assert _split_sql_into_statements("SELECT 1;") == ["SELECT 1;"]


def test_split_sql_separates_multiple_statements() -> None:
    statements = _split_sql_into_statements("SELECT 1; SELECT 2;")

    assert len(statements) == 2
    assert "SELECT 1" in statements[0]
    assert "SELECT 2" in statements[1]


def test_split_sql_filters_transaction_noops() -> None:
    sql = "BEGIN TRANSACTION; INSERT INTO t SELECT * FROM s; COMMIT;"

    statements = _split_sql_into_statements(sql)

    assert len(statements) == 1
    assert "INSERT INTO t" in statements[0]


def test_split_sql_preserves_semicolons_in_string_literals() -> None:
    sql = "INSERT INTO t VALUES ('a;b'); SELECT 1;"

    statements = _split_sql_into_statements(sql)

    assert len(statements) == 2
    assert "'a;b'" in statements[0]


def test_split_sql_ignores_empty_input() -> None:
    assert _split_sql_into_statements("") == []
    assert _split_sql_into_statements(";") == []
    assert _split_sql_into_statements("   \n  ") == []


def test_cursor_execute_runs_each_statement_through_client() -> None:
    client = MagicMock()
    reader = MagicMock()
    reader.read_all.return_value = pa.table({"a": [1]})
    client.query.return_value = reader

    cursor = AltertableCursor(client)
    cursor.execute("BEGIN TRANSACTION; DELETE FROM t WHERE id = 1; INSERT INTO t SELECT 1; COMMIT;")

    assert client.query.call_count == 2
    sent = [call.args[0] for call in client.query.call_args_list]
    assert any("DELETE FROM t" in s for s in sent)
    assert any("INSERT INTO t" in s for s in sent)
    assert not any("BEGIN" in s.upper().split()[0:1] for s in sent if s.strip())


def test_cursor_execute_keeps_only_last_result_table() -> None:
    client = MagicMock()
    first_reader = MagicMock()
    first_reader.read_all.return_value = pa.table({"x": [1]})
    last_reader = MagicMock()
    last_reader.read_all.return_value = pa.table({"x": [99]})
    client.query.side_effect = [first_reader, last_reader]

    cursor = AltertableCursor(client)
    cursor.execute("DELETE FROM t WHERE id = 1; SELECT 99 AS x;")

    assert cursor.table is not None
    assert cursor.table.to_pylist() == [{"x": 99}]


def test_cursor_execute_with_bindings_skips_splitting() -> None:
    client = MagicMock()
    stmt = MagicMock()
    stmt.__enter__ = MagicMock(return_value=stmt)
    stmt.__exit__ = MagicMock(return_value=False)
    reader = MagicMock()
    reader.read_all.return_value = pa.table({"a": [1]})
    stmt.query.return_value = reader
    client.prepare.return_value = stmt

    cursor = AltertableCursor(client)
    cursor.execute("INSERT INTO t VALUES (?)", bindings=[42])

    client.prepare.assert_called_once_with("INSERT INTO t VALUES (?)")
    client.query.assert_not_called()


def test_cursor_execute_with_only_transaction_noops_runs_nothing() -> None:
    client = MagicMock()

    cursor = AltertableCursor(client)
    result = cursor.execute("BEGIN; COMMIT;")

    client.query.assert_not_called()
    assert result.table is None
