"""Runtime wiring contracts for bounded no-order-id ambiguous recovery.

The recovery service is only useful when production actually builds and
schedules it.  These contracts pin that wiring: it is skipped (never weakened)
when prerequisites are missing, it reuses the configured grace/scan thresholds,
and the supervised loop survives a failing scan so rows keep blocking.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

from config.app_config import AmbiguousRecoveryConfig
from orchestrators.runtime_bootstrap import (
    _build_ambiguous_recovery_service,
    ambiguous_recovery_loop,
)
from services.execution.ambiguous_recovery import AmbiguousSubmissionRecoveryService

pytestmark = pytest.mark.asyncio

_WALLET = "0x" + "22" * 20

# Module-level sentinel so ``_build`` can distinguish "use a stand-in gateway"
# from an explicit ``gateway=None`` (the un-wired case) without a call in the
# argument default.
_DEFAULT_GATEWAY = object()


class _FakePool:
    """Marker pool; the builder only stores it into repositories."""


class _FakeOrdersRepo:
    def __init__(self) -> None:
        self.listed = 0
        self.list_scopes: list[tuple[str, int]] = []

    async def list_no_id_ambiguous_submissions(self, *, user_id: str, limit: int):
        self.listed += 1
        self.list_scopes.append((user_id, limit))
        return ()

    async def record_ambiguous_scan_evidence(self, **kwargs):  # pragma: no cover
        raise AssertionError("no evidence should be recorded for an empty batch")

    async def attach_recovered_venue_order_id(self, **kwargs):  # pragma: no cover
        raise AssertionError("no identity should be attached for an empty batch")

    async def mark_ambiguous_submission_confirmed_absent(self, **kwargs):  # pragma: no cover
        raise AssertionError("no absence should be marked for an empty batch")


class _FakePositionsRepo:
    async def get_position(self, user_id, condition_id, *, token_id=None):  # pragma: no cover
        return None


def _config(**overrides) -> SimpleNamespace:
    recovery = AmbiguousRecoveryConfig(**overrides)
    return SimpleNamespace(reconciliation=SimpleNamespace(ambiguous_recovery=recovery))


def _build(*, funder=_WALLET, gateway=_DEFAULT_GATEWAY, config=None, orders_repo=None):
    return _build_ambiguous_recovery_service(
        user_id="user-1",
        funder_address=funder,
        gateway=object() if gateway is _DEFAULT_GATEWAY else gateway,
        orders_repo=orders_repo if orders_repo is not None else _FakeOrdersRepo(),
        positions_repo=_FakePositionsRepo(),
        pool=_FakePool(),
        config=config if config is not None else _config(),
    )


async def test_missing_wallet_or_gateway_skips_recovery_without_weakening_blocks() -> None:
    assert _build(funder=None) is None
    assert _build(funder="") is None
    assert _build(gateway=None) is None


async def test_disabled_recovery_is_not_wired() -> None:
    assert _build(config=_config(enabled=False)) is None


async def test_wired_service_reuses_the_configured_grace_and_scan_thresholds() -> None:
    config = _config(
        grace_seconds=42.0,
        required_absence_scans=7,
        batch_size=12,
        evidence_history_limit=4,
    )
    orders_repo = _FakeOrdersRepo()

    service = _build(config=config, orders_repo=orders_repo)

    assert isinstance(service, AmbiguousSubmissionRecoveryService)
    assert service._grace_period == timedelta(seconds=42.0)
    assert service._required_absence_scans == 7
    assert service._batch_size == 12
    assert service._evidence_history_limit == 4
    # The repository port is the real orders repo, so an empty batch is a no-op.
    assert await service.recover_once() == ()
    assert orders_repo.listed == 1


async def test_default_config_matches_the_service_defaults() -> None:
    service = _build()

    assert isinstance(service, AmbiguousSubmissionRecoveryService)
    assert service._grace_period == timedelta(minutes=2)
    assert service._required_absence_scans == 3
    assert service._batch_size == 100
    assert service._evidence_history_limit == 8


async def test_wired_service_scopes_recovery_to_its_own_account_only() -> None:
    """The per-user service must carry its account identity end to end.

    Audit A01: each follower's recovery loop may only list, scan, and mutate
    its OWN ambiguous submissions. The built service owns the bootstrap
    user's identity, and the evidence source resolves exactly that user to
    the funder wallet — any other account is unserved and fails closed.
    """
    orders_repo = _FakeOrdersRepo()

    service = _build(orders_repo=orders_repo)

    assert isinstance(service, AmbiguousSubmissionRecoveryService)
    assert service._account_user_id == "user-1"
    assert service._account_wallet == _WALLET
    assert service._evidence_source.wallet_for_user("user-1") == _WALLET
    # Foreign accounts resolve to no wallet on this per-user evidence source.
    assert service._evidence_source.wallet_for_user("user-2") is None

    assert await service.recover_once() == ()
    assert orders_repo.list_scopes == [("user-1", 100)]


async def test_supervised_loop_survives_a_failing_scan_and_keeps_sweeping() -> None:
    calls = {"count": 0}

    class _Service:
        async def recover_once(self):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("venue transport down")
            return ()

    task = asyncio.create_task(
        ambiguous_recovery_loop({"user-1": _Service()}, interval_seconds=0.01)
    )
    try:
        await asyncio.sleep(1.2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert calls["count"] >= 2


async def test_supervised_loop_skips_services_without_recover_once() -> None:
    class _NotAService:
        pass

    task = asyncio.create_task(
        ambiguous_recovery_loop({"user-1": _NotAService()}, interval_seconds=0.01)
    )
    try:
        await asyncio.sleep(1.1)
        assert not task.done()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
