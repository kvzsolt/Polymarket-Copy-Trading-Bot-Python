"""Release-gate marker autouse (Issue #75).

Every test under ``tests/release_gate/`` is tagged with the ``release_gate``
marker automatically so the CI ``release-gate`` job picks them up via
``pytest -m release_gate`` without each test file needing its own decorator.
"""
from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):
    """Auto-tag every item under ``tests/release_gate/`` with the marker."""
    marker = pytest.mark.release_gate
    for item in items:
        if "/release_gate/" in item.nodeid.replace("\\", "/"):
            item.add_marker(marker)
