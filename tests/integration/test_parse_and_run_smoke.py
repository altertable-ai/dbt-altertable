"""Lightweight ``dbt parse`` / ``dbt compile`` smoke tests (no database writes)."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from tests.integration._helpers import run_dbt, skip_if_no_flight_target, write_profiles

PROFILE = "integration"


@pytest.mark.altertable_integration
def test_parse_and_compile_select(tmp_path: Path) -> None:
    """Ensures the project graph resolves and SQL compiles against the adapter."""
    skip_if_no_flight_target()

    proj = f"integ_parse_{uuid.uuid4().hex[:8]}"
    base = tmp_path / proj
    base.mkdir()
    models = base / "models"
    models.mkdir()
    (models / "integ_parse_model.sql").write_text(
        "{{ config(materialized='view') }}\nselect 1 as n\n",
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
    +materialized: view
""",
        encoding="utf-8",
    )
    write_profiles(base, PROFILE)

    proc_parse = run_dbt(
        ["parse", "--project-dir", str(base), "--profiles-dir", str(base)],
        base,
    )
    assert proc_parse.returncode == 0, proc_parse.stdout + proc_parse.stderr

    proc_compile = run_dbt(
        [
            "compile",
            "--project-dir",
            str(base),
            "--profiles-dir",
            str(base),
            "--select",
            "integ_parse_model",
        ],
        base,
    )
    assert proc_compile.returncode == 0, proc_compile.stdout + proc_compile.stderr
    compiled = base / "target" / "compiled" / proj / "models" / "integ_parse_model.sql"
    assert compiled.is_file()
    text = compiled.read_text(encoding="utf-8")
    assert "select 1" in text.lower()
