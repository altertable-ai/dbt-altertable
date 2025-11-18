"""Unit tests for AltertableAdapter."""

from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.altertable.connections import (
    AltertableConnectionManager,
    AltertableCredentials,
)
from dbt.adapters.altertable.impl import AltertableAdapter
from dbt.adapters.contracts.connection import Connection


class TestAltertableAdapter:
    @pytest.fixture
    def adapter(self, sample_credentials):
        config = MagicMock()
        config.credentials = AltertableCredentials(**sample_credentials)

        with patch.object(AltertableAdapter, "__init__", lambda x, y: None):
            adapter = AltertableAdapter(config)
            adapter.config = config
            adapter.connections = MagicMock(spec=AltertableConnectionManager)
            return adapter

    def test_date_function(self):
        assert AltertableAdapter.date_function() == "now()"

    def test_adapter_type(self, adapter):
        assert adapter.config.credentials.type == "altertable"
