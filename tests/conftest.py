"""Shared test fixtures for the Kameraposti integration test suite."""

from __future__ import annotations

import pytest

pytest_plugins = "pytest_homeassistant_custom_component"


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):  # noqa: ARG001
    """Make custom_components/kameraposti discoverable in every test."""
    yield
