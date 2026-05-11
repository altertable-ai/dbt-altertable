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
