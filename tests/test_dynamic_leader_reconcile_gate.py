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
    assert _wallets_blocked_by_reconcile_failures(
        ["leader_a", "leader_b"],
        {"leader_a": [_ctx("u1")], "leader_b": [_ctx("u2")]},
        [],
    ) == []


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
        ["leader_a"], {}, ["u1"],
    )
    assert blocked == []
