"""Obsolete — coordinator scope-lock moved to the call sites in issue #71.

This module originally asserted that ``ReconciliationService`` accepted a
``position_coordinator`` kwarg and acquired the per-scope lock around its
mutation block. Issue #71 (``feat(issue-71): make reconciliation sole owner
of terminal settlement``) intentionally rewrote the service to remove that
parameter so the reconciler is pure: the lock is now acquired by the
callers (``orchestrators/trade_pipeline.py`` and ``services/riskMonitor.py``)
before they invoke the reconciler.

The current coordinator-scope-lock contract is covered by:

* ``tests/test_position_action_coordinator.py`` — primitive serialization.
* ``tests/test_position_action_coordinator_integration.py`` — caller-side
  scope-lock acquisition (trade_pipeline + riskMonitor).
* ``tests/release_gate/test_coordinator_serialization.py`` — release-gate
  assertion of same-scope serialization.

Skipping this module rather than deleting it preserves the discovery trail
for anyone tracing the issue-67 → issue-71 architectural shift.
"""
from __future__ import annotations

import pytest

pytest.skip(
    "Coordinator scope-lock was moved to callers in issue #71; "
    "see module docstring and the integration tests listed there.",
    allow_module_level=True,
)
