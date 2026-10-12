"""Regression coverage for authoritative on-chain leader SELL sizing.

The resolver must reconstruct the leader's pre-fill position from a historical
Polygon balance, preserve exact decimal arithmetic across same-block fills,
and provide a bounded emergency exit when both exact sources are unavailable.
"""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import pytest

from pmcopy.services.onchain_detection.leader_sell_fraction_resolver import (
    LeaderSellFractionResolver,
    SellFractionResolutionCode,
)
from pmcopy.services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)

_LEADER = "0x" + "ab" * 20
_TOKEN = "123456"
_BLOCK_HASH = "0x" + "ef" * 32


class _HistoricalBalanceReader:
    """Return configured historical balances without network access."""

    def __init__(self, values: list[Decimal | Exception]) -> None:
        self._values = list(values)
        self.calls: list[tuple[str, str, int]] = []

    async def balance_at_block(
        self,
        leader_wallet: str,
        token_id: str,
        block_number: int,
    ) -> Decimal | None:
        """Return the next configured balance for the requested block."""

        self.calls.append((leader_wallet, token_id, block_number))
        value = self._values.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class _FallbackFractionReader:
    """Return a configured Data API fraction without external I/O."""

    def __init__(self, value: Decimal | None = None, error: Exception | None = None) -> None:
        self._value = value
        self._error = error
        self.calls: list[str] = []

    async def fraction_for(self, signal: OnchainTradeSignal) -> Decimal | None:
        """Return the configured fallback fraction for ``signal``."""

        self.calls.append(signal.tx_hash)
        if self._error is not None:
            raise self._error
        return self._value


class _BuyScopeGuard:
    """Keep BUY-pause state across resolver instances like durable storage."""

    def __init__(self) -> None:
        self.paused: set[tuple[str, str]] = set()
        self.pause_reasons: list[str] = []

    async def pause_buys(
        self,
        leader_wallet: str,
        token_id: str,
        *,
        reason: str,
    ) -> None:
        """Persist one degraded leader/token scope."""

        self.paused.add((leader_wallet.lower(), token_id))
        self.pause_reasons.append(reason)

    async def clear_buys_pause(self, leader_wallet: str, token_id: str) -> None:
        """Clear a scope after an authoritative source recovers."""

        self.paused.discard((leader_wallet.lower(), token_id))

    async def is_buy_paused(self, leader_wallet: str, token_id: str) -> bool:
        """Return the durable state for one scope."""

        return (leader_wallet.lower(), token_id) in self.paused


class _ScriptedDeadline:
    """Apply deterministic timeout outcomes without sleeping."""

    def __init__(self, outcomes: list[str]) -> None:
        self._outcomes = list(outcomes)
        self.timeouts: list[float] = []

    async def run(self, operation, *, timeout_seconds: float):
        """Raise immediately or execute the supplied source operation."""

        self.timeouts.append(timeout_seconds)
        outcome = self._outcomes.pop(0)
        if outcome == "timeout":
            raise TimeoutError("source deadline")
        return await operation()


def _signal(
    *,
    side: str = "SELL",
    size: str = "30",
    log_index: int | None = 7,
    block_number: int | None = 101,
    provisional: bool = False,
    tx_suffix: str = "01",
) -> OnchainTradeSignal:
    """Build one deterministic signal at the resolver boundary."""

    return OnchainTradeSignal(
        leader_wallet=_LEADER,
        token_id=_TOKEN,
        side=side,
        size=Decimal(size),
        price=Decimal("0.22"),
        usdc_size=Decimal(size) * Decimal("0.22"),
        tx_hash="0x" + tx_suffix.rjust(64, "0"),
        confidence=(
            DetectionConfidence.PENDING_PROBABLE
            if provisional
            else DetectionConfidence.MINED_AUTHORITATIVE
        ),
        maker_side=1,
        watched_role="maker",
        event_ts=1_700_000_000,
        log_index=log_index,
        condition_id="0x" + "cd" * 32,
        provisional=provisional,
        exchange="0x" + "11" * 20,
        order_hash=None,
        block_number=block_number,
        block_hash=_BLOCK_HASH if block_number is not None else "",
    )


@pytest.mark.asyncio
async def test_exact_fraction_uses_pre_block_balance_and_same_block_remainder() -> None:
    """Sequential mined fills divide by the leader balance before each fill."""

    history = _HistoricalBalanceReader([Decimal("100")])
    fallback = _FallbackFractionReader()
    resolver = LeaderSellFractionResolver(
        historical_balances=history,
        fallback_fractions=fallback,
    )

    first = await resolver.resolve(_signal(size="30", log_index=7, tx_suffix="01"))
    second = await resolver.resolve(_signal(size="35", log_index=9, tx_suffix="02"))

    assert first.dispatch is True
    assert first.signal.leader_sell_fraction == Decimal("0.3")
    assert first.signal.sell_fraction_resolution == SellFractionResolutionCode.HISTORICAL_RPC
    assert second.signal.leader_sell_fraction == Decimal("0.5")
    assert second.signal.sell_fraction_resolution == SellFractionResolutionCode.HISTORICAL_RPC
    assert history.calls == [(_LEADER, _TOKEN, 100)]
    assert fallback.calls == []
    assert second.signal.as_raw_signal_dict()["_leader_sell_fraction"] == "0.5"


@pytest.mark.asyncio
async def test_same_block_buy_updates_balance_before_later_sell() -> None:
    """An authoritative BUY in the block increases the next SELL denominator."""

    resolver = LeaderSellFractionResolver(
        historical_balances=_HistoricalBalanceReader([Decimal("100")]),
        fallback_fractions=_FallbackFractionReader(),
    )

    buy = await resolver.resolve(_signal(side="BUY", size="20", log_index=3, tx_suffix="03"))
    sell = await resolver.resolve(_signal(size="60", log_index=4, tx_suffix="04"))

    assert buy.dispatch is True
    assert buy.signal.leader_sell_fraction is None
    assert sell.signal.leader_sell_fraction == Decimal("0.5")


@pytest.mark.asyncio
async def test_data_api_fraction_is_used_when_historical_rpc_is_unavailable() -> None:
    """The immediate public-data fallback is preferred over emergency sizing."""

    resolver = LeaderSellFractionResolver(
        historical_balances=_HistoricalBalanceReader([RuntimeError("archive unavailable")]),
        fallback_fractions=_FallbackFractionReader(Decimal("0.869741")),
    )

    outcome = await resolver.resolve(_signal())

    assert outcome.dispatch is True
    assert outcome.signal.leader_sell_fraction == Decimal("0.869741")
    assert outcome.signal.sell_fraction_resolution == SellFractionResolutionCode.DATA_API
    assert resolver.is_scope_degraded(_LEADER, _TOKEN) is False


@pytest.mark.asyncio
async def test_malformed_historical_balance_uses_data_api_fraction() -> None:
    """Malformed RPC data is unavailable evidence, not a dropped leader SELL."""

    resolver = LeaderSellFractionResolver(
        historical_balances=_HistoricalBalanceReader([cast(Decimal, "not-a-balance")]),
        fallback_fractions=_FallbackFractionReader(Decimal("0.4")),
    )

    outcome = await resolver.resolve(_signal())

    assert outcome.dispatch is True
    assert outcome.signal.leader_sell_fraction == Decimal("0.4")
    assert outcome.signal.sell_fraction_resolution == SellFractionResolutionCode.DATA_API


@pytest.mark.asyncio
async def test_historical_timeout_falls_back_within_the_same_bounded_deadline() -> None:
    """A hung archive RPC cannot prevent the immediate Data API fallback."""

    deadline = _ScriptedDeadline(["timeout", "run"])
    history = _HistoricalBalanceReader([Decimal("100")])
    resolver = LeaderSellFractionResolver(
        historical_balances=history,
        fallback_fractions=_FallbackFractionReader(Decimal("0.4")),
        source_timeout_seconds=3.0,
        source_deadline=deadline,
    )

    outcome = await resolver.resolve(_signal())

    assert outcome.signal.leader_sell_fraction == Decimal("0.4")
    assert outcome.signal.sell_fraction_resolution == SellFractionResolutionCode.DATA_API
    assert deadline.timeouts == [3.0, 3.0]
    assert history.calls == []


@pytest.mark.asyncio
async def test_mined_sell_uses_emergency_fraction_and_pauses_scope_when_sources_fail() -> None:
    """An authoritative SELL still exits 15%, then blocks new BUYs for the scope."""

    guard = _BuyScopeGuard()
    resolver = LeaderSellFractionResolver(
        historical_balances=_HistoricalBalanceReader([RuntimeError("rpc unavailable")]),
        fallback_fractions=_FallbackFractionReader(error=RuntimeError("data unavailable")),
        emergency_fraction=Decimal("0.15"),
        buy_scope_guard=guard,
    )

    sell = await resolver.resolve(_signal())
    buy = await resolver.resolve(_signal(side="BUY", size="5", log_index=8, tx_suffix="05"))

    assert sell.dispatch is True
    assert sell.signal.leader_sell_fraction == Decimal("0.15")
    assert sell.signal.sell_fraction_resolution == SellFractionResolutionCode.EMERGENCY
    assert resolver.is_scope_degraded(_LEADER, _TOKEN) is True
    assert guard.paused == {(_LEADER.lower(), _TOKEN)}
    assert guard.pause_reasons == ["authoritative_sources_unavailable"]
    assert buy.dispatch is False
    assert buy.reason == "leader_sell_fraction_scope_degraded"


@pytest.mark.asyncio
async def test_malformed_data_api_fraction_uses_emergency_sell_policy() -> None:
    """Malformed fallback data still dispatches the bounded authoritative SELL."""

    guard = _BuyScopeGuard()
    resolver = LeaderSellFractionResolver(
        historical_balances=_HistoricalBalanceReader([RuntimeError("rpc unavailable")]),
        fallback_fractions=_FallbackFractionReader(cast(Decimal, "not-a-fraction")),
        buy_scope_guard=guard,
    )

    outcome = await resolver.resolve(_signal())

    assert outcome.dispatch is True
    assert outcome.signal.leader_sell_fraction == Decimal("0.15")
    assert outcome.signal.sell_fraction_resolution == SellFractionResolutionCode.EMERGENCY
    assert guard.paused == {(_LEADER.lower(), _TOKEN)}


@pytest.mark.asyncio
async def test_durable_emergency_pause_blocks_buy_after_resolver_restart() -> None:
    """A polling/restarted resolver cannot forget an emergency BUY pause."""

    guard = _BuyScopeGuard()
    first = LeaderSellFractionResolver(
        historical_balances=_HistoricalBalanceReader([RuntimeError("rpc unavailable")]),
        fallback_fractions=_FallbackFractionReader(error=RuntimeError("data unavailable")),
        buy_scope_guard=guard,
    )
    await first.resolve(_signal(tx_suffix="08"))
    restarted = LeaderSellFractionResolver(
        historical_balances=_HistoricalBalanceReader([RuntimeError("still unavailable")]),
        fallback_fractions=_FallbackFractionReader(),
        buy_scope_guard=guard,
    )

    outcome = await restarted.resolve(_signal(side="BUY", size="5", log_index=8, tx_suffix="09"))

    assert outcome.dispatch is False
    assert outcome.reason == "leader_sell_fraction_scope_degraded"


@pytest.mark.asyncio
async def test_authoritative_balance_recovery_clears_durable_buy_pause() -> None:
    """A successful historical read re-enables BUYs for only that scope."""

    guard = _BuyScopeGuard()
    await guard.pause_buys(
        _LEADER,
        _TOKEN,
        reason="authoritative_sources_unavailable",
    )
    resolver = LeaderSellFractionResolver(
        historical_balances=_HistoricalBalanceReader([Decimal("100")]),
        fallback_fractions=_FallbackFractionReader(),
        buy_scope_guard=guard,
    )

    outcome = await resolver.resolve(_signal(side="BUY", size="5", log_index=8, tx_suffix="10"))

    assert outcome.dispatch is True
    assert await guard.is_buy_paused(_LEADER, _TOKEN) is False


@pytest.mark.asyncio
async def test_provisional_sell_never_uses_emergency_fraction() -> None:
    """Unmined pending signals cannot trigger the emergency SELL policy."""

    history = _HistoricalBalanceReader([RuntimeError("must not be called")])
    fallback = _FallbackFractionReader(error=RuntimeError("must not be called"))
    resolver = LeaderSellFractionResolver(
        historical_balances=history,
        fallback_fractions=fallback,
    )

    outcome = await resolver.resolve(_signal(provisional=True, block_number=None, log_index=None))

    assert outcome.dispatch is True
    assert outcome.signal.leader_sell_fraction is None
    assert history.calls == []
    assert fallback.calls == []


@pytest.mark.asyncio
async def test_duplicate_signal_reuses_resolution_without_advancing_balance_twice() -> None:
    """A repeated canonical fill returns its cached exact fraction idempotently."""

    history = _HistoricalBalanceReader([Decimal("100")])
    resolver = LeaderSellFractionResolver(
        historical_balances=history,
        fallback_fractions=_FallbackFractionReader(),
    )
    duplicate = _signal(size="30", log_index=7, tx_suffix="06")

    first = await resolver.resolve(duplicate)
    repeated = await resolver.resolve(duplicate)
    next_fill = await resolver.resolve(_signal(size="35", log_index=8, tx_suffix="07"))

    assert repeated == first
    assert next_fill.signal.leader_sell_fraction == Decimal("0.5")
    assert history.calls == [(_LEADER, _TOKEN, 100)]
