from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest
from dbt_common.contracts.constraints import ColumnLevelConstraint, ConstraintType
from dbt_common.exceptions import CompilationError
from packaging.version import Version

from dbt.adapters.altertable.impl import AltertableAdapter

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


def test_every_relation_takes_the_lakehouse_paths_and_is_never_motherduck() -> None:
    adapter = _adapter()

    assert adapter.is_ducklake(None) is True
    assert adapter.is_motherduck() is False
    assert adapter.disable_transactions() is False


@pytest.mark.parametrize(
    ("server_version", "expected"),
    [("v1.5.2", True), ("v1.5.3", False), ("v1.6.0", False)],
)
def test_table_workarounds_follow_server_version(server_version: str, expected: bool) -> None:
    adapter = _adapter()
    adapter.__dict__["server_duckdb_version"] = Version(server_version.lstrip("v"))

    assert adapter.use_ducklake_table_workarounds(None) is expected


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
    ("model", "expected"),
    [
        ({}, ""),
        ({"batch": {"event_time_start": "2026-01-02 03:04:05+00:00"}}, "20260102_0304050000"),
    ],
)
def test_batch_id_for_model_derives_suffix_from_batch_start(model: dict, expected: str) -> None:
    assert _adapter().batch_id_for_model(model) == expected


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
