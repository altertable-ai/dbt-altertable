from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest
from dbt.context.providers import ParseDatabaseWrapper
from dbt_common.contracts.constraints import ColumnLevelConstraint, ConstraintType
from dbt_common.exceptions import CompilationError
from packaging.version import Version

from dbt.adapters.altertable.connections import AltertableConnection
from dbt.adapters.altertable.impl import AltertableAdapter
from dbt.adapters.altertable.relation import AltertableRelation

STRATEGIES = ["append", "delete+insert", "merge", "microbatch"]


def _adapter() -> AltertableAdapter:
    return AltertableAdapter.__new__(AltertableAdapter)


def test_get_seed_file_path_returns_root_joined_with_original_file_path() -> None:
    adapter = _adapter()
    seed_node = {
        "root_path": "/projects/altertable_dbt",
        "original_file_path": "seeds/lookup.csv",
    }
    expected_path = os.path.join("/projects/altertable_dbt", "seeds/lookup.csv")

    resolved_path = adapter.get_seed_file_path(seed_node)

    assert resolved_path == expected_path


def test_valid_incremental_strategies_includes_merge_and_microbatch() -> None:
    assert _adapter().valid_incremental_strategies() == STRATEGIES


def test_valid_incremental_strategies_survives_dbt_appending_default() -> None:
    adapter = _adapter()

    adapter.valid_incremental_strategies().append("default")

    assert adapter.valid_incremental_strategies() == STRATEGIES


def test_adapter_is_never_motherduck_and_keeps_transactions_enabled() -> None:
    adapter = _adapter()

    assert adapter.is_motherduck() is False
    assert adapter.disable_transactions() is False


def test_ducklake_catalog_names_are_loaded_once_per_connection() -> None:
    adapter = _adapter()
    adapter.connections = MagicMock()
    thread_connection = adapter.connections.get_thread_connection.return_value
    first_connection = AltertableConnection(MagicMock())
    thread_connection.handle = first_connection
    cursor = MagicMock()
    cursor.fetchall.return_value = [("customer's catalog",)]
    adapter.connections.add_select_query.return_value = (None, cursor)
    relation = AltertableRelation.create(database="Customer's Catalog", schema="main")

    assert adapter.is_ducklake(relation) is True
    assert adapter.is_ducklake(relation) is True
    assert adapter.is_ducklake(AltertableRelation.create(database="other")) is False
    assert adapter.connections.add_select_query.call_count == 1

    thread_connection.handle = AltertableConnection(MagicMock())
    cursor.fetchall.return_value = []

    assert adapter.is_ducklake(relation) is False
    assert adapter.is_ducklake(relation) is False
    assert adapter.connections.add_select_query.call_count == 2

    thread_connection.handle = first_connection
    assert adapter.is_ducklake(relation) is True
    assert adapter.connections.add_select_query.call_count == 2


def test_failed_catalog_lookup_is_not_cached() -> None:
    adapter = _adapter()
    adapter.connections = MagicMock()
    connection = AltertableConnection(MagicMock())
    adapter.connections.get_thread_connection.return_value.handle = connection
    cursor = MagicMock()
    cursor.fetchall.return_value = [("reporting",)]
    adapter.connections.add_select_query.side_effect = [
        RuntimeError("metadata unavailable"),
        (None, cursor),
    ]
    relation = AltertableRelation.create(database="reporting")

    with pytest.raises(RuntimeError, match="metadata unavailable"):
        adapter.is_ducklake(relation)

    assert connection.ducklake_catalog_names is None
    assert adapter.is_ducklake(relation) is True
    assert adapter.connections.add_select_query.call_count == 2


@pytest.mark.parametrize("relation", [None, AltertableRelation.create(identifier="temporary")])
def test_is_ducklake_without_a_catalog_does_not_query(relation) -> None:
    adapter = _adapter()
    adapter.connections = MagicMock()

    assert adapter.is_ducklake(relation) is False
    assert adapter.connections.mock_calls == []


def test_catalog_checks_do_not_query_during_parsing() -> None:
    adapter = _adapter()
    adapter.config = MagicMock()
    adapter.connections = MagicMock()
    wrapper = ParseDatabaseWrapper(adapter, MagicMock())
    relation = AltertableRelation.create(database="reporting")

    assert wrapper.is_ducklake(relation) is False
    assert wrapper.use_ducklake_table_workarounds(relation) is False
    assert adapter.connections.mock_calls == []


@pytest.mark.parametrize(
    ("server_version", "catalog_type", "expected"),
    [("1.5.2", "ducklake", True), ("1.5.3", "ducklake", False), ("1.5.2", "bigquery", False)],
)
def test_table_workarounds_follow_catalog_type_and_server_version(
    server_version: str, catalog_type: str, expected: bool
) -> None:
    adapter = _adapter()
    adapter.__dict__["server_duckdb_version"] = Version(server_version)
    adapter.connections = MagicMock()
    connection = AltertableConnection(MagicMock())
    connection.ducklake_catalog_names = {"reporting"} if catalog_type == "ducklake" else set()
    adapter.connections.get_thread_connection.return_value.handle = connection
    relation = AltertableRelation.create(database="reporting")

    assert adapter.use_ducklake_table_workarounds(relation) is expected


def test_server_duckdb_version_reads_select_version() -> None:
    adapter = _adapter()
    cursor = MagicMock()
    cursor.fetchone.return_value = ("v1.5.2",)
    adapter.connections = MagicMock()
    adapter.connections.add_select_query.return_value = (None, cursor)

    assert adapter.server_duckdb_version == Version("1.5.2")
    adapter.connections.add_select_query.assert_called_once_with("select version()")


def test_parse_index_raises_because_indexes_are_unsupported() -> None:
    with pytest.raises(CompilationError, match="Indexes are not supported"):
        _adapter().parse_index({"columns": ["id"]})


def test_warn_once_logs_each_message_once(monkeypatch: pytest.MonkeyPatch) -> None:
    warning = MagicMock()
    monkeypatch.setattr("dbt.adapters.altertable.impl.logger.warning", warning)
    monkeypatch.setattr(AltertableAdapter, "_warned_messages", set())
    adapter = _adapter()

    adapter.warn_once("careful")
    adapter.warn_once("careful")

    warning.assert_called_once_with("careful")


@pytest.mark.parametrize(
    "constraint_type",
    [
        ConstraintType.primary_key,
        ConstraintType.unique,
        ConstraintType.foreign_key,
        ConstraintType.check,
    ],
)
def test_unsupported_constraints_raise(constraint_type: ConstraintType) -> None:
    constraint = ColumnLevelConstraint(type=constraint_type, expression="x > 0")

    with pytest.raises(CompilationError, match="not supported on Altertable"):
        AltertableAdapter.process_parsed_constraint(constraint, lambda c: "rendered")


def test_not_null_and_custom_constraints_render() -> None:
    render = lambda c: c.type.value  # noqa: E731

    assert (
        AltertableAdapter.process_parsed_constraint(
            ColumnLevelConstraint(type=ConstraintType.not_null), render
        )
        == "not_null"
    )
    assert (
        AltertableAdapter.process_parsed_constraint(
            ColumnLevelConstraint(type=ConstraintType.custom, expression="default 1"), render
        )
        == "custom"
    )
