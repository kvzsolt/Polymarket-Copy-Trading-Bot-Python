"""Unavailable inventory inhibits the account and exposes sanitized state."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from config.app_config import PositionDriftConfig
from services.position_drift_service import PositionDriftService


@pytest.mark.asyncio
async def test_failed_snapshot_pauses_all_account_scopes_and_recovers() -> None:
    fetcher = AsyncMock(side_effect=ValueError("synthetic transport failure"))
    service = PositionDriftService(
        positions_repo=SimpleNamespace(list_held_positions=AsyncMock(return_value=[])),
        reservations_repo=None,
        live_positions_fetcher=fetcher,
        config=PositionDriftConfig(),
    )
    failed = await service.scan_user(user_id="account-1", wallet="wallet")
    assert not failed.clean
    assert service.is_scope_paused(user_id="account-1", condition_id="condition", token_id="123")
    assert not service.is_scope_paused(
        user_id="account-2", condition_id="condition", token_id="123"
    )
    fetcher.side_effect = None
    fetcher.return_value = []
    recovered = await service.scan_user(user_id="account-1", wallet="wallet")
    assert recovered.clean
    assert not recovered.paused_scopes
    assert not service.is_scope_paused(
        user_id="account-1", condition_id="condition", token_id="123"
    )


@pytest.mark.asyncio
async def test_snapshot_failure_does_not_expose_provider_error(caplog) -> None:
    secret = "SYNTHETIC_DSN_PASSWORD_DO_NOT_EXPOSE"
    service = PositionDriftService(
        positions_repo=SimpleNamespace(list_held_positions=AsyncMock(return_value=[])),
        reservations_repo=None,
        live_positions_fetcher=AsyncMock(side_effect=ValueError(secret)),
        config=PositionDriftConfig(),
    )
    result = await service.scan_user(user_id="account-1", wallet="wallet")
    assert secret not in repr(result)
    assert secret not in caplog.text
    assert result.records[0].reason == "venue snapshot fetch failed: ValueError"


@pytest.mark.asyncio
async def test_unknown_reservation_ownership_cannot_prune_inventory() -> None:
    from decimal import Decimal

    repo = SimpleNamespace(
        list_held_positions=AsyncMock(
            return_value=[
                SimpleNamespace(
                    condition_id="condition",
                    token_id="123",
                    size_tokens=Decimal(5),
                )
            ]
        ),
        delete_position_if_unchanged=AsyncMock(return_value=True),
    )
    service = PositionDriftService(
        positions_repo=repo,
        reservations_repo=SimpleNamespace(
            list_active_for_scope=AsyncMock(side_effect=ValueError("SYNTHETIC_RESERVATION_SECRET"))
        ),
        live_positions_fetcher=AsyncMock(return_value=[]),
        config=PositionDriftConfig(allow_auto_prune_zero=True),
    )
    result = await service.scan_user(user_id="account-1", wallet="wallet")
    assert not result.clean
    repo.delete_position_if_unchanged.assert_not_awaited()
    assert service.is_scope_paused(user_id="account-1", condition_id="condition", token_id="123")
    assert "SYNTHETIC_RESERVATION_SECRET" not in repr(result)
