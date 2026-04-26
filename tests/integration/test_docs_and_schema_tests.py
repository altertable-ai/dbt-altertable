"""dbt docs generate (catalog) and built-in schema tests."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from tests.integration._helpers import run_dbt, skip_if_no_flight_target, write_profiles

PROFILE = "integration"


@pytest.mark.altertable_integration
def test_docs_generate_catalog_and_schema_tests(tmp_path: Path) -> None:
    """Runs altertable__get_catalog via ``dbt docs generate`` and generic data tests."""
    skip_if_no_flight_target()

    proj = f"integ_docs_{uuid.uuid4().hex[:8]}"
    base = tmp_path / proj
    base.mkdir()
    models = base / "models"
    models.mkdir()

    model_name = "integ_catalog_model"
    (models / f"{model_name}.sql").write_text(
        "{{ config(materialized='table') }}\nselect 1 as id, 'ok' as status\n",
        encoding="utf-8",
    )
    (models / "_schema.yml").write_text(
        f"""\
version: 2
models:
  - name: {model_name}
    columns:
      - name: id
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - not_null
""",
        encoding="utf-8",
    )

    (base / "dbt_project.yml").write_text(
        f"""\
name: {proj}
version: "1.0.0"
config-version: 2
profile: {PROFILE}

model-paths: ["models"]

models:
  {proj}:
    +materialized: table
""",
        encoding="utf-8",
    )
    write_profiles(base, PROFILE)

    proc_run = run_dbt(
        ["run", "--project-dir", str(base), "--profiles-dir", str(base), "--select", model_name],
        base,
    )
    assert proc_run.returncode == 0, proc_run.stdout + proc_run.stderr

    proc_test = run_dbt(
        ["test", "--project-dir", str(base), "--profiles-dir", str(base), "--select", model_name],
        base,
    )
    assert proc_test.returncode == 0, proc_test.stdout + proc_test.stderr

    proc_docs = run_dbt(
        ["docs", "generate", "--project-dir", str(base), "--profiles-dir", str(base)],
        base,
    )
    assert proc_docs.returncode == 0, proc_docs.stdout + proc_docs.stderr

    catalog_path = base / "target" / "catalog.json"
    assert catalog_path.is_file(), "dbt docs generate should write target/catalog.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    nodes = catalog.get("nodes") or {}
    key = f"model.{proj}.{model_name}"
    assert key in nodes, f"Expected catalog node {key}, got keys sample: {list(nodes)[:5]}"
    entry = nodes[key]
    assert entry.get("metadata", {}).get("type") == "BASE TABLE"
    cols = entry.get("columns") or {}
    assert "id" in cols and "status" in cols
