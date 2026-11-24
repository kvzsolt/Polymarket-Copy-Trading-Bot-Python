"""Multi-copier addon §7: aggregate readiness must not conceal the degraded account.

"Aggregate health must report degraded accounts truthfully; one global
boolean must not conceal which account is blocked." The aggregate booleans
(``wss_user_ok`` / ``wss_market_ok`` / ``market_data_ready`` / ``drift_ok``)
already fail when any account fails, but the readiness snapshot carried no
per-account breakdown, so an operator (or the /health payload) could see
that *something* was degraded without being able to see *which* account.
These tests pin the per-account truth in the snapshot while keeping the
aggregates' fail-closed semantics unchanged.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pmcopy.services.observability.health_server import ReadinessTracker
from pmcopy.services.market_data.market_readiness import MarketReadinessGroup
from pmcopy.services.accounts.user_readiness import UserReadinessGroup


def test_user_stream_snapshot_names_the_degraded_account() -> None:
    readiness = ReadinessTracker()
    group = UserReadinessGroup(readiness, ("primary", "secondary"))
    group.scope("primary").update(wss_user_ok=True)
    group.scope("secondary").update(wss_user_ok=False)

    snapshot = readiness.snapshot()
    # Aggregate still fails closed...
    assert snapshot["wss_user_ok"] is False
    # ...and the snapshot names the account that is blocked.
    assert snapshot["wss_user_accounts"] == {"primary": True, "secondary": False}


def test_user_stream_account_map_flips_back_when_account_recovers() -> None:
    readiness = ReadinessTracker()
    group = UserReadinessGroup(readiness, ("primary", "secondary"))
    group.scope("primary").update(wss_user_ok=True)
    group.scope("secondary").update(wss_user_ok=False)
    group.scope("secondary").update(wss_user_ok=True)

    snapshot = readiness.snapshot()
    assert snapshot["wss_user_ok"] is True
    assert snapshot["wss_user_accounts"] == {"primary": True, "secondary": True}


def test_market_runtime_snapshot_names_the_degraded_account() -> None:
    readiness = ReadinessTracker()
    group = MarketReadinessGroup(readiness, ("primary", "secondary"))
    group.scope("primary").update(wss_market_ok=True, market_data_ready=True)
    group.scope("secondary").update(wss_market_ok=True, market_data_ready=False)

    snapshot = readiness.snapshot()
    assert snapshot["market_data_ready"] is False
    assert snapshot["wss_market_accounts"] == {"primary": True, "secondary": True}
    assert snapshot["market_data_accounts"] == {"primary": True, "secondary": False}


def test_drift_snapshot_names_the_degraded_account() -> None:
    readiness = ReadinessTracker()
    readiness.register_drift_account("primary")
    readiness.register_drift_account("secondary")
    readiness.update_account_drift("primary", True)
    readiness.update_account_drift("secondary", False)

    snapshot = readiness.snapshot()
    assert snapshot["drift_ok"] is False
    assert snapshot["account_drift"] == {"primary": True, "secondary": False}


def test_account_maps_absent_when_no_accounts_registered() -> None:
    """No fabricated detail: an unwired process reports no account rows."""
    snapshot = ReadinessTracker().snapshot()
    assert snapshot.get("wss_user_accounts") in (None, {})
    assert snapshot.get("wss_market_accounts") in (None, {})
    assert snapshot.get("market_data_accounts") in (None, {})
    assert snapshot.get("account_drift") in (None, {})


def test_strict_readiness_unchanged_by_per_account_maps() -> None:
    """The per-account maps are observability, not new gates."""
    readiness = ReadinessTracker()
    readiness.update(
        db_ok=True,
        api_ok=True,
        gateway_ok=True,
        wss_market_ok=True,
        wss_user_ok=True,
        reconciler_ok=True,
        market_data_ready=True,
        drift_ok=True,
        circuit_breaker_state="closed",
    )
    ok, failed = readiness.is_ready(strict=True)
    assert ok is True, f"strict readiness failed: {failed}"
