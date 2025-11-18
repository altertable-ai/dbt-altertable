"""Integration tests for dbt adapter operations."""

import pytest


class TestAdapterOperations:
    def test_list_tables(self, adapter, test_tables, integration_credentials):
        """Test listing tables in a schema using the adapter."""
        schema = integration_credentials["schema"]
        database = integration_credentials["database"]

        with adapter.connection_named("test"):
            tables = adapter.list_relations(database=database, schema=schema)

        table_names = [table.name for table in tables]
        for table_name in test_tables.keys():
            assert table_name in table_names

    def test_list_columns(self, adapter, test_tables, integration_credentials):
        """Test listing columns from a table using the adapter."""
        schema = integration_credentials["schema"]
        database = integration_credentials["database"]
        table_name = next(iter(test_tables.keys()))
        expected_columns = test_tables[table_name]

        with adapter.connection_named("test"):
            columns = adapter.get_columns_in_relation(
                relation=adapter.Relation.create(
                    database=database, schema=schema, identifier=table_name
                )
            )

        column_names = [col.name for col in columns]
        for expected_col in expected_columns:
            assert expected_col in column_names
        assert len(columns) == len(expected_columns)
