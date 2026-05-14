"""PR7: PositionDriftService behaviour."""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from config.app_config import PositionDriftConfig
from domain.position_drift import PositionDriftAction, PositionDriftStatus
from services.position_drift_service import PositionDriftService


def _cfg(**overrides) -> PositionDriftConfig:
    base = dict(
        check_on_startup=True,
        fail_closed=True,
        allow_auto_prune_zero=True,
        allow_external_position_tracking=False,
        pause_scope_on_unresolved=True,
        max_size_epsilon=Decimal("0.000001"),
    )
    base.update(overrides)
    return PositionDriftConfig(**base)


def _pos(token_id: str, condition_id: str, size: str) -> SimpleNamespace:
    return SimpleNamespace(
        condition_id=condition_id,
        token_id=token_id,
        size_tokens=Decimal(size),
    )


def _make_svc(
    *,
    local_positions=None,
    venue_positions=None,
    active_reservations=None,
    config=None,
):
    positions_repo = MagicMock()
    positions_repo.list_open_positions = MagicMock(return_value=local_positions or [])
    positions_repo.delete_position = MagicMock()
    reservations_repo = MagicMock()
    reservations_repo.list_active_for_scope = MagicMock(return_value=active_reservations or [])
    fetcher = AsyncMock(return_value=venue_positions or [])
    svc = PositionDriftService(
        positions_repo=positions_repo,
        reservations_repo=reservations_repo,
        live_positions_fetcher=fetcher,
        config=config or _cfg(),
    )
    return svc, positions_repo, reservations_repo


@pytest.mark.asyncio
async def test_local_equals_venue_is_clean():
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[{"asset": "0xtok", "conditionId": "cond1", "size": "10"}],
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    assert report.clean is True
    assert len(report.records) == 1
    assert report.records[0].status == PositionDriftStatus.CLEAN
    assert svc.paused_scopes("u1") == set()


@pytest.mark.asyncio
async def test_local_greater_than_venue_pauses_and_requires_review():
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[{"asset": "0xtok", "conditionId": "cond1", "size": "5"}],
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    rec = report.records[0]
    assert rec.status == PositionDriftStatus.SIZE_MISMATCH_NEGATIVE
    assert rec.action == PositionDriftAction.REQUIRE_OPERATOR_REVIEW
    assert svc.is_scope_paused(user_id="u1", condition_id="cond1", token_id="0xtok") is True
    assert report.clean is False


@pytest.mark.asyncio
async def test_venue_greater_than_local_default_pauses():
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "5")],
        venue_positions=[{"asset": "0xtok", "conditionId": "cond1", "size": "10"}],
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    rec = report.records[0]
    assert rec.status == PositionDriftStatus.SIZE_MISMATCH_POSITIVE
    assert rec.action == PositionDriftAction.PAUSE_SCOPE
    assert svc.is_scope_paused(user_id="u1", condition_id="cond1", token_id="0xtok") is True


@pytest.mark.asyncio
async def test_venue_greater_than_local_with_external_tracking_does_not_pause():
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "5")],
        venue_positions=[{"asset": "0xtok", "conditionId": "cond1", "size": "10"}],
        config=_cfg(allow_external_position_tracking=True),
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    rec = report.records[0]
    assert rec.status == PositionDriftStatus.SIZE_MISMATCH_POSITIVE
    assert rec.action == PositionDriftAction.UPDATE_LOCAL_READONLY
    assert svc.is_scope_paused(user_id="u1", condition_id="cond1", token_id="0xtok") is False


@pytest.mark.asyncio
async def test_local_only_with_no_active_reservations_prunes_when_allowed():
    svc, positions_repo, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[],
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    rec = report.records[0]
    assert rec.action == PositionDriftAction.PRUNE_LOCAL_ZERO_POSITION
    assert rec.status == PositionDriftStatus.LOCAL_MISSING_VENUE_POSITION
    positions_repo.delete_position.assert_called_once_with("u1", "cond1", token_id="0xtok")
    assert svc.is_scope_paused(user_id="u1", condition_id="cond1", token_id="0xtok") is False


@pytest.mark.asyncio
async def test_local_only_with_active_reservation_does_not_prune():
    svc, positions_repo, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[],
        active_reservations=[SimpleNamespace(id="res-1")],
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    rec = report.records[0]
    assert rec.action == PositionDriftAction.PAUSE_SCOPE
    assert rec.status == PositionDriftStatus.UNRESOLVED
    positions_repo.delete_position.assert_not_called()
    assert svc.is_scope_paused(user_id="u1", condition_id="cond1", token_id="0xtok") is True


@pytest.mark.asyncio
async def test_local_only_with_auto_prune_disabled_pauses():
    svc, positions_repo, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[],
        config=_cfg(allow_auto_prune_zero=False),
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    rec = report.records[0]
    assert rec.action == PositionDriftAction.PAUSE_SCOPE
    positions_repo.delete_position.assert_not_called()


@pytest.mark.asyncio
async def test_venue_only_no_local_pauses_by_default():
    svc, _, _ = _make_svc(
        local_positions=[],
        venue_positions=[{"asset": "0xtok-new", "conditionId": "cond-ext", "size": "7"}],
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    assert len(report.records) == 1
    rec = report.records[0]
    assert rec.status == PositionDriftStatus.VENUE_MISSING_LOCAL_POSITION
    assert rec.action == PositionDriftAction.PAUSE_SCOPE
    assert svc.is_scope_paused(user_id="u1", condition_id="cond-ext", token_id="0xtok-new") is True


@pytest.mark.asyncio
async def test_mark_resolved_clears_paused_scope():
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[{"asset": "0xtok", "conditionId": "cond1", "size": "5"}],
    )
    await svc.scan_user(user_id="u1", wallet="0xfunder")
    assert svc.is_scope_paused(user_id="u1", condition_id="cond1", token_id="0xtok") is True
    svc.mark_resolved(user_id="u1", condition_id="cond1", token_id="0xtok")
    assert svc.is_scope_paused(user_id="u1", condition_id="cond1", token_id="0xtok") is False


@pytest.mark.asyncio
async def test_fetcher_failure_returns_unresolved():
    svc, _, _ = _make_svc(local_positions=[], venue_positions=[])
    svc._live_positions_fetcher = AsyncMock(side_effect=RuntimeError("network"))
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    assert report.clean is False
    assert report.records[0].status == PositionDriftStatus.UNRESOLVED
