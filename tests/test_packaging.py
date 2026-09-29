from __future__ import annotations

from pathlib import Path

from dbt.adapters.factory import AdapterContainer

from dbt.adapters.altertable import Plugin


def test_adapter_plugin_resolves_under_canonical_namespace() -> None:
    assert Plugin.adapter.__name__ == "AltertableAdapter"
    assert Plugin.credentials.__name__ == "AltertableCredentials"


def test_plugin_include_path_contains_dbt_project_and_macros() -> None:
    include_dir = Path(Plugin.include_path)
    assert (include_dir / "dbt_project.yml").is_file()
    assert (include_dir / "macros" / "adapters.sql").is_file()


def test_plugin_declares_duckdb_as_parent() -> None:
    assert Plugin.dependencies == ["duckdb"]


def test_dispatch_searches_altertable_then_duckdb() -> None:
    container = AdapterContainer()
    container.load_plugin("altertable")

    assert container.get_adapter_type_names("altertable") == ["altertable", "duckdb"]
    assert container.get_adapter_package_names("altertable") == [
        "dbt_altertable",
        "dbt_duckdb",
        "dbt",
    ]
