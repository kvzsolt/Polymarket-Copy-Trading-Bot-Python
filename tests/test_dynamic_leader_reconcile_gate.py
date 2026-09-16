"""SHP-11: dynamic-leader discovery must drop wallets whose downstream
contexts include any user_id that failed startup reconciliation.

The cold-start path in ``app.main`` already refuses to flip
``reconciler_ok`` when ``reconcile_failures`` is non-empty; this regression
covers the second caller (``_refresh_contexts_for_new_leaders`` →
``startup_reconcile_unconfirmed``) which previously logged and started
monitors anyway, exposing affected users to double-fills against stale
position state.
"""

from __future__ import annotations

from types import SimpleNamespace

from app import _wallets_blocked_by_reconcile_failures


def _ctx(user_id: str) -> SimpleNamespace:
    return SimpleNamespace(user_id=user_id)


def test_no_failures_blocks_nothing() -> None:
    assert (
        _wallets_blocked_by_reconcile_failures(
            ["leader_a", "leader_b"],
            {"leader_a": [_ctx("u1")], "leader_b": [_ctx("u2")]},
            [],
        )
        == []
    )


def test_failed_user_blocks_its_leader() -> None:
    blocked = _wallets_blocked_by_reconcile_failures(
        ["leader_a", "leader_b"],
        {"leader_a": [_ctx("u1")], "leader_b": [_ctx("u2")]},
        ["u1"],
    )
    assert blocked == ["leader_a"]


def test_shared_leader_blocks_when_any_user_failed() -> None:
    blocked = _wallets_blocked_by_reconcile_failures(
        ["leader_a"],
        {"leader_a": [_ctx("u1"), _ctx("u2")]},
        ["u2"],
    )
    assert blocked == ["leader_a"]


def test_unknown_wallet_does_not_crash() -> None:
    blocked = _wallets_blocked_by_reconcile_failures(
        ["leader_a"],
        {},
        ["u1"],
    )
    assert blocked == []


# ---------------------------------------------------------------------------
# P0-4 / B4-01: the COLD-START path must apply the same gate. Recording
# reconciler_ok=False alone is not enough — no monitor reads it — so the
# affected wallets must actually be removed from the monitored set before
# the leader monitors spawn.
# ---------------------------------------------------------------------------

from app import _cold_start_monitored_after_reconcile  # noqa: E402


def test_cold_start_no_failures_returns_all() -> None:
    leaders = ["leader_a", "leader_b"]
    out = _cold_start_monitored_after_reconcile(
        leaders,
        {"leader_a": [_ctx("u1")], "leader_b": [_ctx("u2")]},
        [],
    )
    assert out == leaders


def test_cold_start_drops_wallet_of_failed_user() -> None:
    out = _cold_start_monitored_after_reconcile(
        ["leader_a", "leader_b"],
        {"leader_a": [_ctx("u1")], "leader_b": [_ctx("u2")]},
        ["u1"],
    )
    assert out == ["leader_b"]  # leader_a blocked, leader_b still monitored


def test_cold_start_all_blocked_yields_empty() -> None:
    out = _cold_start_monitored_after_reconcile(
        ["leader_a"],
        {"leader_a": [_ctx("u1")]},
        ["u1"],
    )
    assert out == []  # fail closed: no leader monitors spawn
