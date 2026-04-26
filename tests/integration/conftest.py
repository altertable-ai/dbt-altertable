"""Session fixtures for Flight SQL integration tests (altertable-mock or a real endpoint)."""

from __future__ import annotations

import contextlib
import os
import time
from collections.abc import Generator

import pytest


@pytest.fixture(scope="session", autouse=True)
def _maybe_start_testcontainers() -> Generator[None, None, None]:
    """When ALTERTABLE_USE_TESTCONTAINERS=1, run altertable-mock via testcontainers (local dev)."""
    if os.environ.get("CI", "").strip().lower() in ("1", "true", "yes"):
        yield
        return

    if os.environ.get("ALTERTABLE_USE_TESTCONTAINERS", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
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
        # Log wait ensures gRPC is up; brief settle for port mapping on Docker Desktop.
        time.sleep(0.5)

        host = container.get_container_host_ip()
        # Docker Desktop publishes mapped ports on IPv4; gRPC may resolve "localhost" to ::1 and fail.
        if host.strip().lower() in ("localhost", "::1", "0.0.0.0"):
            host = "127.0.0.1"
        port = int(container.get_exposed_port(15002))
        os.environ.setdefault("ALTERTABLE_TEST_HOST", host)
        os.environ.setdefault("ALTERTABLE_TEST_PORT", str(port))
        os.environ.setdefault("ALTERTABLE_TEST_TLS", "false")
        os.environ.setdefault("ALTERTABLE_TEST_USERNAME", user_name)
        os.environ.setdefault("ALTERTABLE_TEST_PASSWORD", user_secret)
        os.environ.setdefault("ALTERTABLE_TEST_DATABASE", "memory")
        os.environ.setdefault("ALTERTABLE_TEST_SCHEMA", "main")

        yield
    finally:
        with contextlib.suppress(Exception):
            container.stop()
