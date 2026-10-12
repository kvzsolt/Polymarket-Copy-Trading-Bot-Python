"""Standalone reservation reaper (concurrency-audit C8-adjacent gap).

When risk management is disabled, the in-session reservation-expiry reaper that
normally lives in the risk loop does not run, so a TTL-expired reservation on a
stuck submitted_unconfirmed order would strand capital/shares until restart.
``app._reap_reservations_once`` is the standalone reaper used in that config; it
delegates to the same vetted per-context reaper the risk loop uses.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import pmcopy.app as app

pytestmark = pytest.mark.asyncio


def _ctx_with_reservation_service(spy: AsyncMock) -> SimpleNamespace:
    # Shape mirrors what _reap_expired_reservations_for_context resolves:
    # context.execution_coordinator.reservation_service.release_expired_active().
    return SimpleNamespace(
        execution_coordinator=SimpleNamespace(
            reservation_service=SimpleNamespace(release_expired_active=spy)
        ),
        extra={},
    )


async def test_reaper_calls_release_expired_active_for_each_context():
    spy_a = AsyncMock(return_value=[])
    spy_b = AsyncMock(return_value=[])
    contexts = [
        _ctx_with_reservation_service(spy_a),
        _ctx_with_reservation_service(spy_b),
    ]

    await app._reap_reservations_once(contexts)

    spy_a.assert_awaited_once()
    spy_b.assert_awaited_once()


async def test_reaper_is_resilient_to_a_failing_context():
    boom = AsyncMock(side_effect=RuntimeError("db down"))
    ok = AsyncMock(return_value=[])
    contexts = [
        _ctx_with_reservation_service(boom),
        _ctx_with_reservation_service(ok),
    ]

    # A failure in one context must not stop the sweep of the others, and must
    # not propagate.
    await app._reap_reservations_once(contexts)

    boom.assert_awaited_once()
    ok.assert_awaited_once()


async def test_reaper_noop_on_empty_contexts():
    await app._reap_reservations_once([])
