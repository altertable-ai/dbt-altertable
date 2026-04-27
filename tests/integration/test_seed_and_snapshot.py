"""Seeds (parameterized insert path) and snapshots exercising altertable-specific macros."""

from __future__ import annotations

import contextlib
import os
import uuid
from pathlib import Path

import pytest

from tests.integration._helpers import (
    flight_client_ctx,
    quoted_ident,
    run_dbt,
    skip_if_no_flight_target,
    sql_string_literal,
    write_profiles,
)

PROFILE = "integration"


@pytest.mark.altertable_integration
def test_seed_fast_false_and_check_strategy_snapshot(tmp_path: Path) -> None:
    """COPY FROM a local path runs on the Flight server; ``fast: false`` uses INSERT + bindings."""
    skip_if_no_flight_target()

    proj = f"integ_seed_{uuid.uuid4().hex[:8]}"
    base = tmp_path / proj
    base.mkdir()
    (base / "models").mkdir()
    seeds = base / "seeds"
    seeds.mkdir()
    snaps = base / "snapshots"
    snaps.mkdir()

    seed_name = "integ_seed_rows"
    (seeds / f"{seed_name}.csv").write_text(
        "id,label\n1,alpha\n2,beta\n",
        encoding="utf-8",
    )

    (snaps / "integ_snapshot.sql").write_text(
        f"""\
{{% snapshot integ_snap_capture %}}
{{{{
  config(
    unique_key='id',
    strategy='check',
    check_cols='all',
  )
}}}}
select * from {{{{ ref('{seed_name}') }}}}
{{% endsnapshot %}}
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
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]

seeds:
  {proj}:
    +fast: false
""",
        encoding="utf-8",
    )
    write_profiles(base, PROFILE)

    proc_seed = run_dbt(
        ["seed", "--project-dir", str(base), "--profiles-dir", str(base), "--select", seed_name],
        base,
    )
    assert proc_seed.returncode == 0, proc_seed.stdout + proc_seed.stderr

    proc_snap = run_dbt(
        ["snapshot", "--project-dir", str(base), "--profiles-dir", str(base)],
        base,
    )
    assert proc_snap.returncode == 0, proc_snap.stdout + proc_snap.stderr

    db = os.environ["ALTERTABLE_TEST_DATABASE"].strip()
    schema = os.environ["ALTERTABLE_TEST_SCHEMA"].strip()
    snap_tbl = "integ_snap_capture"

    with flight_client_ctx() as client:
        q = (
            "select count(*) as c from duckdb_tables() "
            f"where lower(database_name) = lower({sql_string_literal(db)}) "
            f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
            f"and lower(table_name) = lower({sql_string_literal(seed_name)})"
        )
        assert client.query(q).read_all().to_pylist()[0]["c"] >= 1

        q2 = (
            "select count(*) as c from duckdb_tables() "
            f"where lower(database_name) = lower({sql_string_literal(db)}) "
            f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
            f"and lower(table_name) = lower({sql_string_literal(snap_tbl)})"
        )
        assert client.query(q2).read_all().to_pylist()[0]["c"] >= 1

        n = (
            client.query(f"select count(*) as c from {quoted_ident(db, schema, snap_tbl)}")
            .read_all()
            .to_pylist()[0]["c"]
        )
        assert int(n) >= 2

        for rel in (snap_tbl, seed_name):
            drop_sql = f"drop table if exists {quoted_ident(db, schema, rel)}"
            with contextlib.suppress(Exception):
                client.query(drop_sql).read_all()
