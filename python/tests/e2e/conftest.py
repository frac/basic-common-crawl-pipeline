import pytest


def pytest_configure(config):
    """Configure pytest markers for e2e tests."""
    config.addinivalue_line(
        "markers", "docker: marks tests as requiring docker services"
    )
