"""Pytest configuration for integration tests."""

import os
from unittest.mock import MagicMock

import adbc_driver_flightsql.dbapi
import pytest

from dbt.adapters.altertable.connections import AltertableCredentials
from dbt.adapters.altertable.impl import AltertableAdapter
from dbt.adapters.contracts.connection import Connection, ConnectionState


def pytest_configure(config):
    required_vars = [
        "ALTERTABLE_HOST",
        "ALTERTABLE_PORT",
        "ALTERTABLE_USERNAME",
        "ALTERTABLE_PASSWORD",
    ]

    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        pytest.skip(
            f"Skipping integration tests: missing environment variables: {', '.join(missing_vars)}",
            allow_module_level=True,
        )


@pytest.fixture(scope="session")
def integration_credentials():
    return {
        "username": os.environ["ALTERTABLE_USERNAME"],
        "password": os.environ["ALTERTABLE_PASSWORD"],
        "host": os.environ["ALTERTABLE_HOST"],
        "port": int(os.environ["ALTERTABLE_PORT"]),
        "database": os.environ.get("ALTERTABLE_DATABASE", "test_db"),
        "schema": os.environ.get("ALTERTABLE_SCHEMA", "dbt_test_schema"),
        "tls": os.environ.get("ALTERTABLE_TLS", "true").lower() == "true",
    }


@pytest.fixture(scope="session")
def raw_connection(integration_credentials):
    """Raw database connection for setup/teardown operations."""
    creds = AltertableCredentials(**integration_credentials)
    uri = creds.adbc_uri()
    options = creds.adbc_options()

    conn = adbc_driver_flightsql.dbapi.connect(uri, autocommit=True, **options)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def adapter(integration_credentials, raw_connection):
    """Create a dbt adapter instance with a real connection."""
    config = MagicMock()
    config.credentials = AltertableCredentials(**integration_credentials)
    config.quoting = {
        "database": True,
        "schema": True,
        "identifier": True,
    }

    adapter = AltertableAdapter(config)

    with raw_connection.cursor() as cursor:
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {integration_credentials['schema']}")
        except Exception:
            pass

    yield adapter

    if hasattr(adapter, "connections"):
        adapter.connections.cleanup_all()


@pytest.fixture
def test_tables(raw_connection, integration_credentials):
    """Create test tables in the test schema and return their metadata."""
    schema = integration_credentials["schema"]

    tables_info = {
        "test_table_1": ["id", "name", "value"],
        "test_table_2": ["user_id", "email", "created_at"],
    }

    with raw_connection.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.test_table_1 (
                id INTEGER,
                name VARCHAR,
                value DOUBLE
            )
        """
        )

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.test_table_2 (
                user_id INTEGER,
                email VARCHAR,
                created_at TIMESTAMP
            )
        """
        )

        cursor.execute(
            f"""
            INSERT INTO {schema}.test_table_1 VALUES 
                (1, 'Alice', 100.0),
                (2, 'Bob', 200.0)
        """
        )

    yield tables_info

    with raw_connection.cursor() as cursor:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.test_table_1")
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.test_table_2")
        except Exception:
            pass
