from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.altertable.connections import (
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


def test_connect_client_opens_session_without_binding_a_catalog(mock_client_cls, base_creds_kwargs):
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


def test_exception_handler_includes_failing_sql_in_raised_error() -> None:
    manager = AltertableConnectionManager.__new__(AltertableConnectionManager)
    failing_sql = "select count(*) from prod_db.staging.users where job_title not in ('a', 'b')"

    with pytest.raises(DbtRuntimeError) as exc_info, manager.exception_handler(failing_sql):
        raise RuntimeError("Flight returned internal error, with message: Internal error")

    error_message = str(exc_info.value)
    assert "Flight returned internal error" in error_message
    assert failing_sql in error_message


def test_exception_handler_truncates_very_long_sql_in_raised_error() -> None:
    manager = AltertableConnectionManager.__new__(AltertableConnectionManager)
    long_sql = "select * from foo where x in (" + ",".join(str(i) for i in range(1000)) + ")"
    assert len(long_sql) > 2000

    with pytest.raises(DbtRuntimeError) as exc_info, manager.exception_handler(long_sql):
        raise RuntimeError("Flight error")

    error_message = str(exc_info.value)
    assert "(truncated)" in error_message
    assert len(error_message) < len(long_sql) + 200
