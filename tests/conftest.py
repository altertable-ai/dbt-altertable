"""Shared pytest fixtures for dbt-altertable tests."""

import pytest


@pytest.fixture
def sample_credentials():
    return {
        "username": "test_user",
        "password": "test_password",
        "database": "test_db",
        "schema": "test_schema",
        "host": "test.altertable.ai",
        "port": 15002,
        "tls": False,
    }


@pytest.fixture
def minimal_credentials():
    return {
        "username": "test_user",
        "password": "test_password",
        "database": "test_db",
        "schema": "test_schema",
    }
