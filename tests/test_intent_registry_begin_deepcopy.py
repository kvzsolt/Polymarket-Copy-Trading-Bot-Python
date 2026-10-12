"""Regression: IntentRegistry.begin_intent returns a deep copy.

Pre-fix begin_intent returned the live IntentRecord stored in
``self._intents`` while get() and open_intent_for() correctly returned
``copy.deepcopy(record)``. The asymmetric handling meant a caller could
mutate registry-owned state without holding the lock — including
flipping ``record.state = IntentState.DONE`` and bypassing the
reconciler-owned finalization guard.

Test pins: the returned record is a deep copy, so caller mutation
cannot leak into the registry's internal record.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.services.intentRegistry import (
    IntentRegistry,
    IntentSide,
    IntentState,
    Reservation,
)
from pmcopy.services.positionActionCoordinator import PositionScope


@pytest.mark.asyncio
async def test_begin_intent_returns_deep_copy():
    reg = IntentRegistry()
    scope = PositionScope(
        user_id="u",
        condition_id="c",
        token_id="1" * 64,
    )

    rec = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("10")),
    )

    # Mutate the returned record — registry-internal copy must be untouched.
    original_state = rec.state
    rec.state = IntentState.DONE

    fresh = reg.get(rec.intent_id)
    assert fresh is not None
    assert fresh.state == original_state, (
        "begin_intent must return a deep copy; mutating it changed the "
        f"registry-internal state to {fresh.state}"
    )
    assert id(fresh) != id(rec), "deep copy contract: identities must differ"
