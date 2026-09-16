"""Matrix-coverage meta-test (Issue #75).

The release-gate matrix lives in the GitHub issue body. This test makes that
matrix executable: it imports every gate-test module so an accidental rename,
move, or deletion breaks the gate immediately rather than silently shrinking
coverage.
"""

from __future__ import annotations

import importlib

REQUIRED_MODULES = (
    "tests.release_gate.test_position_key_uniqueness",
    "tests.release_gate.test_coordinator_serialization",
    "tests.release_gate.test_cache_ttl_invalidation",
    "tests.release_gate.test_wss_heartbeat",
    "tests.release_gate.test_wss_reconnect_resync",
    "tests.release_gate.test_sizing_metadata_driven",
    "tests.release_gate.test_effective_idempotency",
    "tests.release_gate.test_reservation_lifecycle",
    "tests.release_gate.test_reconciler_terminal_ownership",
    "tests.release_gate.test_startup_recovery",
    "tests.release_gate.test_manual_drift_reconciliation",
    "tests.release_gate.test_typed_config_fail_closed",
    "tests.release_gate.test_clob_gateway_smoke",
    "tests.release_gate.test_concurrency_stress",
    "tests.release_gate.test_e2e_dry_run_buy",
    "tests.release_gate.test_e2e_dry_run_sell",
    "tests.release_gate.test_protective_exit_integration",
    "tests.release_gate.test_live_execution_invariants",
)


def test_every_matrix_module_is_importable() -> None:
    missing: list[str] = []
    for name in REQUIRED_MODULES:
        try:
            importlib.import_module(name)
        except Exception as exc:
            missing.append(f"{name}: {exc!r}")
    assert not missing, (
        "release-gate matrix incomplete; the following modules failed to import:\n  - "
        + "\n  - ".join(missing)
    )


def test_matrix_module_has_at_least_one_test() -> None:
    empty: list[str] = []
    for name in REQUIRED_MODULES:
        mod = importlib.import_module(name)
        funcs = [
            attr for attr in dir(mod) if attr.startswith("test_") and callable(getattr(mod, attr))
        ]
        if not funcs:
            empty.append(name)
    assert not empty, f"matrix modules with no tests: {empty}"
