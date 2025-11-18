"""Unit tests for AltertableCredentials and AltertableConnectionManager."""

from unittest.mock import MagicMock, patch

import adbc_driver_manager

from dbt.adapters.altertable.connections import (
    AltertableConnectionManager,
    AltertableCredentials,
)
from dbt.adapters.contracts.connection import Connection, ConnectionState


class TestAltertableCredentials:
    """Test AltertableCredentials dataclass."""

    def test_credentials_type(self, sample_credentials):
        """Test that credentials return correct type."""
        creds = AltertableCredentials(**sample_credentials)
        assert creds.type == "altertable"

    def test_credentials_unique_field(self, sample_credentials):
        """Test that unique_field is 'host'."""
        creds = AltertableCredentials(**sample_credentials)
        assert creds.unique_field == "host"

    def test_default_host(self, minimal_credentials):
        """Test default host value."""
        creds = AltertableCredentials(**minimal_credentials)
        assert creds.host == "flight.altertable.ai"

    def test_default_port(self, minimal_credentials):
        """Test default port value."""
        creds = AltertableCredentials(**minimal_credentials)
        assert creds.port == 443

    def test_default_tls(self, minimal_credentials):
        """Test default tls value."""
        creds = AltertableCredentials(**minimal_credentials)
        assert creds.tls is True

    def test_adbc_uri_with_tls(self, minimal_credentials):
        """Test URI generation with TLS enabled."""
        creds = AltertableCredentials(**minimal_credentials)
        uri = creds.adbc_uri()
        assert uri == "grpc+tls://flight.altertable.ai:443"

    def test_adbc_uri_without_tls(self, sample_credentials):
        """Test URI generation with TLS disabled."""
        creds = AltertableCredentials(**sample_credentials)
        uri = creds.adbc_uri()
        assert uri == "grpc://test.altertable.ai:15002"

    def test_adbc_options(self, sample_credentials):
        """Test ADBC options generation."""
        creds = AltertableCredentials(**sample_credentials)
        options = creds.adbc_options()

        assert "db_kwargs" in options
        assert "conn_kwargs" in options

        # Check db_kwargs contain username and password
        assert adbc_driver_manager.DatabaseOptions.USERNAME.value in options["db_kwargs"]
        assert (
            options["db_kwargs"][adbc_driver_manager.DatabaseOptions.USERNAME.value] == "test_user"
        )
        assert adbc_driver_manager.DatabaseOptions.PASSWORD.value in options["db_kwargs"]
        assert (
            options["db_kwargs"][adbc_driver_manager.DatabaseOptions.PASSWORD.value]
            == "test_password"
        )

        # Check conn_kwargs contain catalog and schema
        assert adbc_driver_manager.ConnectionOptions.CURRENT_CATALOG.value in options["conn_kwargs"]
        assert (
            options["conn_kwargs"][adbc_driver_manager.ConnectionOptions.CURRENT_CATALOG.value]
            == "test_db"
        )
        assert (
            adbc_driver_manager.ConnectionOptions.CURRENT_DB_SCHEMA.value in options["conn_kwargs"]
        )
        assert (
            options["conn_kwargs"][adbc_driver_manager.ConnectionOptions.CURRENT_DB_SCHEMA.value]
            == "test_schema"
        )

    def test_custom_port(self, sample_credentials):
        """Test custom port value."""
        creds = AltertableCredentials(**sample_credentials)
        assert creds.port == 15002

    def test_custom_host(self, sample_credentials):
        """Test custom host value."""
        creds = AltertableCredentials(**sample_credentials)
        assert creds.host == "test.altertable.ai"


class TestAltertableConnectionManager:
    """Test AltertableConnectionManager."""

    @patch("dbt.adapters.altertable.connections.adbc_driver_flightsql.dbapi.connect")
    def test_open_connection_success(self, mock_connect, sample_credentials):
        """Test successful connection opening."""
        # Setup
        mock_connect.return_value = MagicMock()

        connection = Connection(
            type="altertable",
            name="test_connection",
            state=ConnectionState.CLOSED,
            credentials=AltertableCredentials(**sample_credentials),
        )

        # Execute
        AltertableConnectionManager.open(connection)

        # Verify
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args

        assert call_args[0][0] == "grpc://test.altertable.ai:15002"
        assert call_args[1]["autocommit"] is True

    @patch("dbt.adapters.altertable.connections.adbc_driver_flightsql.dbapi.connect")
    def test_open_already_open_connection(self, mock_connect, sample_credentials):
        """Test opening an already open connection returns without reconnecting."""
        mock_connect.return_value = MagicMock()

        connection = Connection(
            type="altertable",
            name="test_connection",
            state=ConnectionState.OPEN,
            credentials=AltertableCredentials(**sample_credentials),
        )

        result = AltertableConnectionManager.open(connection)

        # Should not attempt to connect
        mock_connect.assert_not_called()
        assert result.state == ConnectionState.OPEN
