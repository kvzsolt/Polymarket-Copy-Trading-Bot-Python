"""Contracts for the discovery-refresh observability extras.

``LeaderRegistry`` consumes a duck-typed discovery service, so the cycle
summary must be read defensively: a fake or legacy service that does not
publish one still logs a healthy refresh.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrators.leader_registry import LeaderRegistry
from services.discovery.cycle_summary import DiscoveryCycleSummary


def _discovery() -> SimpleNamespace:
    api = SimpleNamespace(has_token=True)
    scheduler = SimpleNamespace(enabled=True, max_active_leaders=3, start_from_roster=False)
    hysteresis = SimpleNamespace(
        promote_score=0.55,
        demote_score=0.50,
        min_tenure_cycles=3,
        min_dwell_seconds=10800,
    )
    return SimpleNamespace(api=api, scheduler=scheduler, hysteresis=hysteresis)


def _registry(summary: DiscoveryCycleSummary | None) -> LeaderRegistry:
    service = MagicMock()
    service.run = AsyncMock(return_value=[])
    service.last_cycle_degraded = False
    service.config_hash = "hash-1"
    if summary is None:
        del service.last_cycle_summary
    else:
        service.last_cycle_summary = summary
    leaders_repo = MagicMock()
    leaders_repo.list_active = AsyncMock(return_value=[])
    leaders_repo.list_roster = AsyncMock(return_value=[])
    leaders_repo.save_roster = AsyncMock()
    leaders_repo.upsert_active = AsyncMock()
    leaders_repo.deactivate = AsyncMock()
    return LeaderRegistry(
        discovery_service=service,
        leaders_repo=leaders_repo,
        config=SimpleNamespace(discovery=_discovery()),
        readiness_tracker=MagicMock(),
        logger_instance=logging.getLogger("test.LeaderRegistry"),
    )


def _succeeded(caplog: pytest.LogCaptureFixture) -> logging.LogRecord:
    records = [
        item for item in caplog.records if item.getMessage() == "discovery_refresh_succeeded"
    ]
    assert records, "discovery_refresh_succeeded was not logged"
    return records[0]


@pytest.mark.asyncio
async def test_refresh_logs_cycle_summary_extras(caplog: pytest.LogCaptureFixture) -> None:
    registry = _registry(
        DiscoveryCycleSummary(
            accepted=1,
            rejected=2,
            rejection_reasons={"hyperactive_trader": 2, "bankroll_incompatible": 1},
        )
    )

    with caplog.at_level(logging.INFO, logger="test.LeaderRegistry"):
        await registry.refresh(force_discovery=True)

    record = _succeeded(caplog)
    assert record.accepted_count == 1
    assert record.rejected_count == 2
    assert record.rejection_reasons == {"hyperactive_trader": 2, "bankroll_incompatible": 1}


@pytest.mark.asyncio
async def test_refresh_logs_without_cycle_summary(caplog: pytest.LogCaptureFixture) -> None:
    registry = _registry(None)

    with caplog.at_level(logging.INFO, logger="test.LeaderRegistry"):
        await registry.refresh(force_discovery=True)

    record = _succeeded(caplog)
    assert record.candidate_count == 0
    assert getattr(record, "accepted_count", None) is None
    assert getattr(record, "rejection_reasons", None) is None
