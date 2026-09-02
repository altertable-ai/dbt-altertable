from datetime import datetime
from decimal import Decimal
from multiprocessing import get_context
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.flight as flight
import pytest
from dbt.adapters.contracts.connection import ConnectionState
from dbt_common.exceptions import DbtRuntimeError

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


# What pyarrow raises when the backend no longer knows the session bound to the bearer
# token (`api_sessions` row gone: worker replaced during a deploy, bulk delete, TTL).
# The gRPC code differs per backend path (Internal, FailedPrecondition, Unauthenticated),
# so the exception class differs too; only the message identifies the condition.
_SESSION_EXPIRED_ERRORS = [
    flight.FlightInternalError(
        "Flight returned internal error, with message: Session configuration not found or "
        "expired. gRPC client debug context: UNKNOWN:Error received from peer "
        'ipv4:138.199.133.83:443 {grpc_message:"Session configuration not found or expired", '
        "grpc_status:13}"
    ),
    flight.FlightUnavailableError(
        "Flight returned unavailable error, with message: Session expired. Open a new session. "
        "gRPC client debug context: UNKNOWN:Error received from peer ipv4:138.199.133.83:443 "
        '{grpc_message:"Session expired. Open a new session.", grpc_status:9}'
    ),
]


@pytest.fixture
def manager(mock_client_cls, base_creds_kwargs):
    profile = SimpleNamespace(credentials=AltertableCredentials(**base_creds_kwargs))
    return AltertableConnectionManager(profile, get_context("spawn"))


@pytest.mark.parametrize(
    "session_expired", _SESSION_EXPIRED_ERRORS, ids=["internal", "unavailable"]
)
def test_session_expiry_fails_connection_and_next_node_opens_new_session(
    manager, mock_client_cls, session_expired
):
    dead_client, fresh_client = MagicMock(), MagicMock()
    mock_client_cls.side_effect = [dead_client, fresh_client]
    dead_client.query.side_effect = session_expired

    connection = manager.set_connection_name("model.first")
    with pytest.raises(DbtRuntimeError, match="Session"):
        manager.add_query("select 1")

    # The failing node loses its statement, but the dead session must not survive it.
    assert connection.state == ConnectionState.FAIL
    dead_client.close.assert_called_once_with()

    # dbt re-installs LazyHandle(open) on a non-open connection, so the next node
    # handshakes again: a second Client, hence a second server-minted session id.
    connection = manager.set_connection_name("model.second")
    assert connection.handle._client is fresh_client
    assert connection.state == ConnectionState.OPEN
    assert mock_client_cls.call_count == 2


def test_other_errors_keep_the_session(manager, mock_client_cls):
    client = mock_client_cls.return_value
    client.query.side_effect = flight.FlightInternalError(
        "Flight returned internal error, with message: Catalog Error: Table with name nope "
        "does not exist! gRPC client debug context: {grpc_status:13}"
    )

    connection = manager.set_connection_name("model.first")
    with pytest.raises(DbtRuntimeError, match="Catalog Error"):
        manager.add_query("select * from nope")

    assert connection.state == ConnectionState.OPEN
    client.close.assert_not_called()
    assert manager.set_connection_name("model.second").handle._client is client
    assert mock_client_cls.call_count == 1
