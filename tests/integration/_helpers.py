from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pytest
import yaml

# profiles.yml target name produced by ``write_profiles`` for standard Flight tests.
INTEGRATION_PROFILE = "integration"

REQUIRED_ENV = (
    "ALTERTABLE_TEST_USERNAME",
    "ALTERTABLE_TEST_PASSWORD",
    "ALTERTABLE_TEST_DATABASE",
)


def missing_integration_env() -> list[str]:
    return [k for k in REQUIRED_ENV if not os.environ.get(k, "").strip()]


def skip_if_missing_integration_env() -> None:
    missing = missing_integration_env()
    if missing:
        pytest.skip(
            "Integration tests need ALTERTABLE_TEST_* credentials and a database. "
            "In CI this is provided by altertable-mock. Locally either:\n"
            "  - export ALTERTABLE_TEST_* (see README), or\n"
            "  - ALTERTABLE_USE_TESTCONTAINERS=1 uv run pytest tests/integration "
            "(requires Docker and: uv sync --extra dev --extra integration)"
        )


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def grpc_host(raw: str) -> str:
    """Use IPv4 for Flight/gRPC when Docker only mapped the port on 127.0.0.1 (common on macOS)."""
    h = raw.strip()
    if h.lower() in ("localhost", "::1", "0.0.0.0"):
        return "127.0.0.1"
    return h


def write_profiles(tmp: Path, profile_name: str, schema: str) -> None:
    profile = {
        profile_name: {
            "target": "test",
            "outputs": {
                "test": {
                    "type": "altertable",
                    "host": grpc_host(os.environ.get("ALTERTABLE_TEST_HOST", "127.0.0.1")),
                    "port": int(os.environ.get("ALTERTABLE_TEST_PORT", "15002").strip()),
                    "tls": env_bool("ALTERTABLE_TEST_TLS", False),
                    "username": os.environ["ALTERTABLE_TEST_USERNAME"].strip(),
                    "password": os.environ["ALTERTABLE_TEST_PASSWORD"],
                    "database": os.environ["ALTERTABLE_TEST_DATABASE"].strip(),
                    "schema": schema,
                },
            },
        },
    }
    (tmp / "profiles.yml").write_text(
        yaml.safe_dump(profile, sort_keys=False),
        encoding="utf-8",
    )


def dbt_invocation() -> list[str]:
    """Prefer the venv ``dbt`` shim, else ``python -m dbt.cli.main``."""
    bin_dir = Path(sys.executable).resolve().parent
    shim = bin_dir / "dbt"
    if shim.is_file():
        return [str(shim)]
    which = shutil.which("dbt")
    if which:
        return [which]
    return [sys.executable, "-m", "dbt.cli.main"]


def run_dbt(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*dbt_invocation(), *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


@contextlib.contextmanager
def flight_client_ctx() -> Iterator[Any]:
    import altertable_flightsql

    client = altertable_flightsql.Client(
        username=os.environ["ALTERTABLE_TEST_USERNAME"].strip(),
        password=os.environ["ALTERTABLE_TEST_PASSWORD"],
        catalog=None,
        schema=None,
        host=grpc_host(os.environ.get("ALTERTABLE_TEST_HOST", "127.0.0.1")),
        port=int(os.environ.get("ALTERTABLE_TEST_PORT", "15002").strip()),
        tls=env_bool("ALTERTABLE_TEST_TLS", False),
    )
    try:
        yield client
    finally:
        with contextlib.suppress(Exception):
            client.close()


def quoted_ident(*parts: str) -> str:
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts)


@dataclass
class DbtProject:
    """Per-test scratch dbt project tied to a tmp_path and an isolated schema."""

    base: Path
    name: str
    schema: str
    profile: str = INTEGRATION_PROFILE

    @property
    def db(self) -> str:
        return os.environ["ALTERTABLE_TEST_DATABASE"].strip()

    @property
    def models_dir(self) -> Path:
        return self._ensure_dir("models")

    @property
    def seeds_dir(self) -> Path:
        return self._ensure_dir("seeds")

    @property
    def snapshots_dir(self) -> Path:
        return self._ensure_dir("snapshots")

    def _ensure_dir(self, name: str) -> Path:
        d = self.base / name
        d.mkdir(exist_ok=True)
        return d

    def write_project_yml(
        self,
        *,
        models: dict[str, Any] | None = None,
        seeds: dict[str, Any] | None = None,
        seed_paths: list[str] | None = None,
        snapshot_paths: list[str] | None = None,
    ) -> None:
        cfg: dict[str, Any] = {
            "name": self.name,
            "version": "1.0.0",
            "config-version": 2,
            "profile": self.profile,
            "model-paths": ["models"],
        }
        if seed_paths is not None:
            cfg["seed-paths"] = seed_paths
        if snapshot_paths is not None:
            cfg["snapshot-paths"] = snapshot_paths
        if models is not None:
            cfg["models"] = {self.name: models}
        if seeds is not None:
            cfg["seeds"] = {self.name: seeds}
        (self.base / "dbt_project.yml").write_text(
            yaml.safe_dump(cfg, sort_keys=False),
            encoding="utf-8",
        )

    def write_model(self, name: str, sql: str) -> None:
        (self.models_dir / f"{name}.sql").write_text(sql, encoding="utf-8")

    def write_seed(self, name: str, csv: str) -> None:
        (self.seeds_dir / f"{name}.csv").write_text(csv, encoding="utf-8")

    def write_snapshot(self, name: str, sql: str) -> None:
        (self.snapshots_dir / f"{name}.sql").write_text(sql, encoding="utf-8")

    def write_models_yml(self, name: str, body: str) -> None:
        (self.models_dir / f"{name}.yml").write_text(body, encoding="utf-8")

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        proc = run_dbt(
            [*args, "--project-dir", str(self.base), "--profiles-dir", str(self.base)],
            self.base,
        )
        if check and proc.returncode != 0:
            raise AssertionError(
                f"dbt {' '.join(args)} failed (rc={proc.returncode}):\n{proc.stdout}\n{proc.stderr}"
            )
        return proc

    def qualify(self, name: str) -> str:
        """Fully-qualified, double-quoted identifier ``"db"."schema"."name"``."""
        return quoted_ident(self.db, self.schema, name)


def count_in_catalog(
    client: Any,
    *,
    kind: Literal["table", "view"],
    database: str,
    schema: str,
    name: str,
) -> int:
    catalog = "duckdb_tables()" if kind == "table" else "duckdb_views()"
    name_col = "table_name" if kind == "table" else "view_name"
    q = (
        f"select count(*) as c from {catalog} "
        f"where lower(database_name) = lower({sql_string_literal(database)}) "
        f"and lower(schema_name) = lower({sql_string_literal(schema)}) "
        f"and lower({name_col}) = lower({sql_string_literal(name)})"
    )
    return int(client.query(q).read_all().to_pylist()[0]["c"])
