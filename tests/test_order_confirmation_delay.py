"""PR 5: confirmation delay policy.

Covers OrderConfirmationConfig.resolve_initial_delay and
ReconciliationService._should_skip_for_delay / _is_past_ambiguous_timeout.
"""
from __future__ import annotations

import time

import pytest

from config.app_config import OrderConfirmationConfig
from services.execution.reconciliation_service import ReconciliationService


def _cfg(**overrides) -> OrderConfirmationConfig:
    defaults = dict(
        initial_delay_seconds=2.0,
        poll_interval_seconds=3.0,
        max_wait_seconds=90.0,
        delayed_extra_seconds=2.0,
        token_delay_overrides={},
        condition_delay_overrides={},
        auto_release_ambiguous=True,
        auto_release_grace_seconds=300.0,
        auto_release_require_all_probes_clean=True,
        auto_release_max_probe_errors=0,
    )
    defaults.update(overrides)
    return OrderConfirmationConfig(**defaults)


def _record(*, age_seconds: float = 0.0, status: str = "submitted_unconfirmed") -> dict:
    return {
        "order_id": "ord-1",
        "created_at": time.time() - age_seconds,
        "status": status,
        "asset": "0xtok",
        "condition_id": "cond-1",
        "response": {"size_matched": "0"},
    }


def _svc() -> ReconciliationService:
    return ReconciliationService(
        orders_repo=None,
        fills_repo=None,
        positions_repo=None,
    )


def test_resolve_default_delay_with_no_overrides():
    cfg = _cfg()
    assert cfg.resolve_initial_delay(token_id="t1", condition_id="c1") == 2.0


def test_resolve_delayed_status_adds_extra():
    cfg = _cfg()
    assert cfg.resolve_initial_delay(
        token_id="t1", condition_id="c1", venue_status="delayed"
    ) == 4.0


def test_token_override_beats_condition_and_default():
    cfg = _cfg(
        token_delay_overrides={"t1": 10.0},
        condition_delay_overrides={"c1": 5.0},
    )
    assert cfg.resolve_initial_delay(token_id="t1", condition_id="c1") == 10.0


def test_condition_override_beats_default():
    cfg = _cfg(condition_delay_overrides={"c1": 5.0})
    assert cfg.resolve_initial_delay(token_id="t1", condition_id="c1") == 5.0


def test_should_skip_for_delay_returns_true_before_delay():
    svc = _svc()
    cfg = _cfg(initial_delay_seconds=60.0)
    record = _record(age_seconds=1.0)
    assert svc._should_skip_for_delay(record, order_confirmation_cfg=cfg) is True


def test_should_skip_for_delay_returns_false_after_delay():
    svc = _svc()
    cfg = _cfg(initial_delay_seconds=0.1)
    record = _record(age_seconds=5.0)
    assert svc._should_skip_for_delay(record, order_confirmation_cfg=cfg) is False


def test_should_skip_for_delay_returns_false_when_matched_already():
    svc = _svc()
    cfg = _cfg(initial_delay_seconds=60.0)
    record = _record(age_seconds=1.0)
    record["response"] = {"size_matched": "5"}  # already matched something
    assert svc._should_skip_for_delay(record, order_confirmation_cfg=cfg) is False


def test_should_skip_returns_false_when_cfg_is_none():
    svc = _svc()
    record = _record(age_seconds=1.0)
    assert svc._should_skip_for_delay(record, order_confirmation_cfg=None) is False


def test_is_past_ambiguous_timeout_after_max_wait():
    svc = _svc()
    cfg = _cfg(max_wait_seconds=10.0)
    record = _record(age_seconds=15.0)
    assert svc._is_past_ambiguous_timeout(record, order_confirmation_cfg=cfg) is True


def test_is_past_ambiguous_timeout_before_max_wait():
    svc = _svc()
    cfg = _cfg(max_wait_seconds=120.0)
    record = _record(age_seconds=10.0)
    assert svc._is_past_ambiguous_timeout(record, order_confirmation_cfg=cfg) is False
