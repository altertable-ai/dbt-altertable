"""Shared helpers for Flight SQL integration tests (altertable-mock or real endpoint)."""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

REQUIRED_ENV = (
    "ALTERTABLE_TEST_USERNAME",
    "ALTERTABLE_TEST_PASSWORD",
    "ALTERTABLE_TEST_DATABASE",
    "ALTERTABLE_TEST_SCHEMA",
)


def missing_integration_env() -> list[str]:
    return [k for k in REQUIRED_ENV if not os.environ.get(k, "").strip()]


def skip_if_missing_integration_env() -> None:
    missing = missing_integration_env()
    if missing:
        pytest.skip(
            "Integration tests need ALTERTABLE_TEST_* credentials and catalog/schema. "
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


def write_profiles(tmp: Path, profile_name: str) -> None:
    host = grpc_host(os.environ.get("ALTERTABLE_TEST_HOST", "127.0.0.1"))
    port = int(os.environ.get("ALTERTABLE_TEST_PORT", "15002").strip())
    tls = env_bool("ALTERTABLE_TEST_TLS", False)
    user = os.environ["ALTERTABLE_TEST_USERNAME"].strip()
    password = os.environ["ALTERTABLE_TEST_PASSWORD"]
    database = os.environ["ALTERTABLE_TEST_DATABASE"].strip()
    schema = os.environ["ALTERTABLE_TEST_SCHEMA"].strip()
    escaped = password.replace("\\", "\\\\").replace('"', '\\"')
    user_escaped = user.replace("\\", "\\\\").replace('"', '\\"')
    (tmp / "profiles.yml").write_text(
        f"""\
{profile_name}:
  target: test
  outputs:
    test:
      type: altertable
      host: "{host}"
      port: {port}
      tls: {str(tls).lower()}
      username: "{user_escaped}"
      password: "{escaped}"
      database: "{database}"
      schema: "{schema}"
""",
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


def flight_client() -> Any:
    import altertable_flightsql

    return altertable_flightsql.Client(
        username=os.environ["ALTERTABLE_TEST_USERNAME"].strip(),
        password=os.environ["ALTERTABLE_TEST_PASSWORD"],
        catalog=None,
        schema=None,
        host=grpc_host(os.environ.get("ALTERTABLE_TEST_HOST", "127.0.0.1")),
        port=int(os.environ.get("ALTERTABLE_TEST_PORT", "15002").strip()),
        tls=env_bool("ALTERTABLE_TEST_TLS", False),
    )


@contextlib.contextmanager
def flight_client_ctx() -> Iterator[Any]:
    client = flight_client()
    try:
        yield client
    finally:
        with contextlib.suppress(Exception):
            client.close()


def quoted_ident(*parts: str) -> str:
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts)
