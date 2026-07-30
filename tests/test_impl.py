from __future__ import annotations

import os

from dbt.adapters.altertable.impl import AltertableAdapter


def test_get_seed_file_path_returns_root_joined_with_original_file_path() -> None:
    adapter = AltertableAdapter.__new__(AltertableAdapter)
    seed_node = {
        "root_path": "/projects/altertable_dbt",
        "original_file_path": "seeds/lookup.csv",
    }
    expected_path = os.path.join("/projects/altertable_dbt", "seeds/lookup.csv")

    resolved_path = adapter.get_seed_file_path(seed_node)

    assert resolved_path == expected_path


def test_valid_incremental_strategies_declares_append_and_delete_insert() -> None:
    adapter = AltertableAdapter.__new__(AltertableAdapter)

    assert adapter.valid_incremental_strategies() == ["append", "delete+insert"]


def test_valid_incremental_strategies_survives_dbt_appending_default() -> None:
    adapter = AltertableAdapter.__new__(AltertableAdapter)

    adapter.valid_incremental_strategies().append("default")

    assert adapter.valid_incremental_strategies() == ["append", "delete+insert"]
