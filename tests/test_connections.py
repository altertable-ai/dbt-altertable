import unittest.mock
from unittest.mock import MagicMock

import pytest

from dbt.adapters.altertable.connections import AltertableConnectionManager
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


def test_connect_client_binds_schema_at_session_open_by_default(mock_client_cls, base_creds_kwargs):
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


def test_connect_client_creates_target_schema_when_auto_create_enabled(
    mock_client_cls, base_creds_kwargs
):
    creds = AltertableCredentials(auto_create_schema=True, **base_creds_kwargs)

    AltertableConnectionManager._connect_client(creds)

    database = base_creds_kwargs["database"]
    schema = base_creds_kwargs["schema"]
    common_kwargs = dict(
        username=base_creds_kwargs["username"],
        password=base_creds_kwargs["password"],
        catalog=database,
        host=base_creds_kwargs["host"],
        port=base_creds_kwargs["port"],
        tls=base_creds_kwargs["tls"],
    )
    assert mock_client_cls.call_count == 2
    bootstrap_call, real_call = mock_client_cls.call_args_list
    assert bootstrap_call == unittest.mock.call(**common_kwargs, schema=None)
    assert real_call == unittest.mock.call(**common_kwargs, schema=schema)

    issued_sql = [c.args[0] for c in mock_client_cls.return_value.query.call_args_list]
    assert issued_sql == [f'CREATE SCHEMA IF NOT EXISTS "{database}"."{schema}"']
