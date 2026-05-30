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
        prune_holdoff_seconds=60.0,
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
    positions_repo.list_open_positions = AsyncMock(return_value=local_positions or [])
    positions_repo.delete_position = AsyncMock()
    reservations_repo = MagicMock()
    reservations_repo.list_active_for_scope = AsyncMock(return_value=active_reservations or [])
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


@pytest.mark.asyncio
async def test_was_recently_pruned_marks_after_prune_action():
    """A scope pruned via PRUNE_LOCAL_ZERO_POSITION reports recently pruned."""
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[],  # venue has zero shares → drift prunes local
    )
    report = await svc.scan_user(user_id="u1", wallet="0xfunder")
    # Confirm the single record is the prune action.
    assert any(r.action == PositionDriftAction.PRUNE_LOCAL_ZERO_POSITION for r in report.records)
    assert svc.was_recently_pruned(
        user_id="u1", condition_id="cond1", token_id="0xtok"
    ) is True


@pytest.mark.asyncio
async def test_was_recently_pruned_honours_ttl():
    """Beyond ttl_seconds, the stamp is dropped and lookups return False."""
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[],
    )
    await svc.scan_user(user_id="u1", wallet="0xfunder")
    # Manually advance: pass a ``now`` 120s into the future, ttl=60s.
    far_future = svc._prune_timestamps["u1|cond1|0xtok"] + 120.0
    assert svc.was_recently_pruned(
        user_id="u1", condition_id="cond1", token_id="0xtok",
        ttl_seconds=60.0, now=far_future,
    ) is False
    # Side-effect: TTL elapse drops the stamp so memory stays bounded.
    assert "u1|cond1|0xtok" not in svc._prune_timestamps


@pytest.mark.asyncio
async def test_was_recently_pruned_uses_configured_holdoff():
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[],
        config=_cfg(prune_holdoff_seconds=180.0),
    )
    await svc.scan_user(user_id="u1", wallet="0xfunder")
    future = svc._prune_timestamps["u1|cond1|0xtok"] + 120.0
    assert svc.was_recently_pruned(
        user_id="u1", condition_id="cond1", token_id="0xtok", now=future
    ) is True


@pytest.mark.asyncio
async def test_was_recently_pruned_false_for_non_prune_status():
    """A scope that drift only paused (no prune) must NOT trigger the holdoff."""
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[],
        # Disable auto-prune so the path falls into PAUSE_SCOPE instead.
        config=_cfg(allow_auto_prune_zero=False),
    )
    await svc.scan_user(user_id="u1", wallet="0xfunder")
    assert svc.was_recently_pruned(
        user_id="u1", condition_id="cond1", token_id="0xtok"
    ) is False


@pytest.mark.asyncio
async def test_clear_prune_record_drops_holdoff():
    svc, _, _ = _make_svc(
        local_positions=[_pos("0xtok", "cond1", "10")],
        venue_positions=[],
    )
    await svc.scan_user(user_id="u1", wallet="0xfunder")
    assert svc.was_recently_pruned(
        user_id="u1", condition_id="cond1", token_id="0xtok"
    ) is True
    svc.clear_prune_record(user_id="u1", condition_id="cond1", token_id="0xtok")
    assert svc.was_recently_pruned(
        user_id="u1", condition_id="cond1", token_id="0xtok"
    ) is False
