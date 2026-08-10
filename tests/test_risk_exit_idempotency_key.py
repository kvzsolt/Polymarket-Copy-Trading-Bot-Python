"""C3 — risk-exit idempotency key must be stable across retries.

The audit found the risk-exit ``signal_id`` used ``time.time_ns()``
(``services/risk/risk_monitor.py:1637``), so every tick produced a fresh
idempotency key. With ``reservations_enabled: false`` the submission lock
released on ``submitted_unconfirmed``, and the next tick re-submitted a
duplicate protective SELL against the same held shares before reconciliation
converged the prior in-flight order.

Fix: the key is now ``scope + reason + position-epoch`` (stable for a given
position until it is re-entered), not a wall-clock nanosecond timestamp.
"""

from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from services.risk.risk_monitor import _risk_exit_signal_id


def _pos(
    *,
    condition_id: str = "cond-1",
    token_id: str = "tok-1",
    leader_id: str = "leader-1",
    avg_entry_price: Decimal = Decimal("0.50"),
    size_tokens: Decimal = Decimal("10"),
) -> SimpleNamespace:
    return SimpleNamespace(
        condition_id=condition_id,
        token_id=token_id,
        leader_id=leader_id,
        avg_entry_price=avg_entry_price,
        size_tokens=size_tokens,
    )


def _tracked(entry_time: float) -> SimpleNamespace:
    return SimpleNamespace(entry_time=entry_time)


def test_signal_id_is_stable_across_consecutive_calls_for_same_position():
    pos = _pos()
    pm = MagicMock()
    pm.get_position.return_value = _tracked(entry_time=1_700_000_000.0)

    key1 = _risk_exit_signal_id(pos, reason="near_max_partial_trim", position_manager=pm)
    key2 = _risk_exit_signal_id(pos, reason="near_max_partial_trim", position_manager=pm)

    assert key1 == key2
    assert "near_max_partial_trim" in key1
    assert "cond-1" in key1


def test_signal_id_changes_when_position_is_reentered():
    """A re-entry resets the in-memory tracked position's entry_time, which must
    produce a fresh epoch so the new position's trim is not dedup-suppressed
    by the prior position's one-shot trim."""
    pos = _pos()
    pm = MagicMock()
    pm.get_position.return_value = _tracked(entry_time=1_700_000_000.0)
    key_first = _risk_exit_signal_id(pos, reason="near_max_partial_trim", position_manager=pm)

    pm.get_position.return_value = _tracked(entry_time=1_700_000_500.0)
    key_reentered = _risk_exit_signal_id(pos, reason="near_max_partial_trim", position_manager=pm)

    assert key_first != key_reentered


def test_signal_id_differs_by_reason_for_same_position():
    pos = _pos()
    pm = MagicMock()
    pm.get_position.return_value = _tracked(entry_time=1_700_000_000.0)

    key_trim = _risk_exit_signal_id(pos, reason="near_max_partial_trim", position_manager=pm)
    key_full = _risk_exit_signal_id(pos, reason="near_max_full_exit", position_manager=pm)

    assert key_trim != key_full


def test_signal_id_differs_by_scope():
    pm = MagicMock()
    pm.get_position.return_value = _tracked(entry_time=1_700_000_000.0)

    key_a = _risk_exit_signal_id(
        _pos(condition_id="cond-a"), reason="risk_exit", position_manager=pm
    )
    key_b = _risk_exit_signal_id(
        _pos(condition_id="cond-b"), reason="risk_exit", position_manager=pm
    )

    assert key_a != key_b


def test_signal_id_falls_back_to_stable_hash_when_tracked_position_absent():
    """When the in-memory tracked position is unavailable (e.g. fresh boot
    before rehydrate), the key must still be stable for the same DB position —
    derived from immutable DB fields, never from the wall clock."""
    pos = _pos()
    pm = MagicMock()
    pm.get_position.return_value = None

    key1 = _risk_exit_signal_id(pos, reason="stop_loss", position_manager=pm)
    key2 = _risk_exit_signal_id(pos, reason="stop_loss", position_manager=pm)

    assert key1 == key2
    assert "stop_loss" in key1


def test_signal_id_never_contains_time_ns():
    """The key must not embed a wall-clock nanosecond timestamp — that was the
    root cause of the duplicate-submission storm."""
    pos = _pos()
    pm = MagicMock()
    pm.get_position.return_value = _tracked(entry_time=time.time())

    key = _risk_exit_signal_id(pos, reason="risk_exit", position_manager=pm)

    # A stable key repeats; a time_ns key would not.
    again = _risk_exit_signal_id(pos, reason="risk_exit", position_manager=pm)
    assert key == again
