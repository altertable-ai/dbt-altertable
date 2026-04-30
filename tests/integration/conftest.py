from __future__ import annotations

import contextlib
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.integration._helpers import (
    INTEGRATION_PROFILE,
    DbtProject,
    env_bool,
    flight_client_ctx,
    quoted_ident,
    skip_if_missing_integration_env,
    write_profiles,
)


@pytest.fixture(scope="session", autouse=True)
def _maybe_start_testcontainers() -> Iterator[None]:
    """When ALTERTABLE_USE_TESTCONTAINERS=1, run altertable-mock via testcontainers (local dev)."""
    if env_bool("CI", False) or not env_bool("ALTERTABLE_USE_TESTCONTAINERS", False):
        yield
        return

    try:
        from testcontainers.core.config import testcontainers_config
        from testcontainers.core.container import DockerContainer
        from testcontainers.core.wait_strategies import LogMessageWaitStrategy
    except ImportError:
        pytest.exit(
            "ALTERTABLE_USE_TESTCONTAINERS=1 requires testcontainers. "
            "Install with: uv sync --extra dev --extra integration"
        )

    # Ryuk often hangs or misbehaves on Docker Desktop (macOS). Disable unless the user opted in.
    if "TESTCONTAINERS_RYUK_DISABLED" not in os.environ:
        os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    testcontainers_config.ryuk_disabled = True

    image = os.environ.get("ALTERTABLE_MOCK_IMAGE", "").strip() or (
        "ghcr.io/altertable-ai/altertable-mock:latest"
    )
    user = os.environ.get("ALTERTABLE_MOCK_BOOT_USER", "dbt_ci:dbt_ci_secret").strip()
    user_name, _, user_secret = user.partition(":")

    wait_s = int(os.environ.get("ALTERTABLE_MOCK_WAIT_TIMEOUT", "180"))
    container = (
        DockerContainer(image)
        .with_exposed_ports(15002)
        .with_command(["--user", user])
        .waiting_for(
            LogMessageWaitStrategy("Starting Flight SQL server").with_startup_timeout(wait_s),
        )
    )
    try:
        container.start()
        time.sleep(0.5)

        host = container.get_container_host_ip()
        if host.strip().lower() in ("localhost", "::1", "0.0.0.0"):
            host = "127.0.0.1"
        port = int(container.get_exposed_port(15002))
        os.environ.setdefault("ALTERTABLE_TEST_HOST", host)
        os.environ.setdefault("ALTERTABLE_TEST_PORT", str(port))
        os.environ.setdefault("ALTERTABLE_TEST_TLS", "false")
        os.environ.setdefault("ALTERTABLE_TEST_USERNAME", user_name)
        os.environ.setdefault("ALTERTABLE_TEST_PASSWORD", user_secret)
        os.environ.setdefault("ALTERTABLE_TEST_DATABASE", "memory")

        yield
    finally:
        with contextlib.suppress(Exception):
            container.stop()


@pytest.fixture(autouse=True)
def _skip_if_missing_integration_env(
    request: pytest.FixtureRequest,
    _maybe_start_testcontainers: None,
) -> None:
    """Skip @altertable_integration tests when Flight SQL credentials are not set."""
    if request.node.get_closest_marker("altertable_integration"):
        skip_if_missing_integration_env()


@pytest.fixture
def flight_client() -> Iterator[Any]:
    with flight_client_ctx() as client:
        yield client


@pytest.fixture
def dbt_project(tmp_path: Path, flight_client: Any) -> Iterator[DbtProject]:
    """Each test gets a unique schema; teardown drops it CASCADE so cleanup is automatic."""
    name = f"integ_{uuid.uuid4().hex[:8]}"
    schema = f"test_{uuid.uuid4().hex[:10]}"
    db = os.environ["ALTERTABLE_TEST_DATABASE"].strip()
    base = tmp_path / name
    base.mkdir()

    flight_client.query(f"create schema if not exists {quoted_ident(db, schema)}").read_all()
    write_profiles(base, INTEGRATION_PROFILE, schema=schema)

    try:
        yield DbtProject(base=base, name=name, schema=schema)
    finally:
        with contextlib.suppress(Exception):
            flight_client.query(
                f"drop schema if exists {quoted_ident(db, schema)} cascade"
            ).read_all()
