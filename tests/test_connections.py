from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from dbt.adapters.altertable.connections import (
    AltertableConnection,
    AltertableConnectionManager,
    _normalize_flight_sql_scalar,
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
    )
    mock_client_cls.return_value.query.assert_not_called()


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


def test_rollback_drops_tracked_scratch_relations_after_ending_transaction():
    mock_client = MagicMock()
    transaction = MagicMock()
    mock_client.begin_transaction.return_value = transaction
    conn = AltertableConnection(mock_client)
    conn.begin()
    conn._scratch_relations.add(
        (
            "memory",
            "analytics",
            'orders"archive__dbt_tmp0123456789abcdef0123456789abcdef',
        )
    )

    conn.rollback()

    mock_client.rollback_transaction.assert_called_once_with(transaction)
    mock_client.execute.assert_called_once_with(
        'drop table if exists "memory"."analytics".'
        '"orders""archive__dbt_tmp0123456789abcdef0123456789abcdef"'
    )
    assert conn._scratch_relations == set()


def test_commit_keeps_tracked_scratch_relations_for_post_commit_cleanup():
    mock_client = MagicMock()
    transaction = MagicMock()
    mock_client.begin_transaction.return_value = transaction
    conn = AltertableConnection(mock_client)
    conn.begin()
    conn._scratch_relations.add(
        ("memory", "analytics", "orders__dbt_tmp0123456789abcdef0123456789abcdef")
    )

    conn.commit()

    mock_client.commit_transaction.assert_called_once_with(transaction)
    mock_client.execute.assert_not_called()
    assert conn._scratch_relations == {
        ("memory", "analytics", "orders__dbt_tmp0123456789abcdef0123456789abcdef")
    }


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
