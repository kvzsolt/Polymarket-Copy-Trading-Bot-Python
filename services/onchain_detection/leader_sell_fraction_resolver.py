"""Resolve authoritative leader SELL fractions before copy dispatch.

The service keeps provider I/O behind narrow injected ports. Historical
Polygon balances are the primary source, the public Data API is a fallback,
and a bounded emergency fraction is used only for authoritative mined SELLs.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol, TypeVar

from services.onchain_detection.trade_detection_models import OnchainTradeSignal
from services.onchain_detection.leader_sell_scope_guard import (
    LeaderSellBuyScopeGuard,
    LeaderSellScopeGuard,
)

_ZERO = Decimal("0")
_ONE = Decimal("1")
_KeyT = TypeVar("_KeyT")
_ValueT = TypeVar("_ValueT")


class SellFractionResolutionCode(StrEnum):
    """Machine-readable source used to resolve a leader SELL fraction."""

    HISTORICAL_RPC = "historical_rpc"
    DATA_API = "data_api"
    EMERGENCY = "emergency"


class HistoricalLeaderBalanceReader(Protocol):
    """Read one leader's outcome-token balance at a historical block."""

    async def balance_at_block(
        self,
        leader_wallet: str,
        token_id: str,
        block_number: int,
    ) -> Decimal | None:
        """Return the exact token balance at the end of ``block_number``."""

        ...


class LeaderSellFractionFallback(Protocol):
    """Resolve a SELL fraction from an independent public-data source."""

    async def fraction_for(self, signal: OnchainTradeSignal) -> Decimal | None:
        """Return a fraction in ``(0, 1]`` or ``None`` when unresolved."""

        ...


class FractionSourceDeadline(Protocol):
    """Bound one external leader-position source operation."""

    async def run(
        self,
        operation: Callable[[], Awaitable[Decimal | None]],
        *,
        timeout_seconds: float,
    ) -> Decimal | None:
        """Return the source result or raise ``TimeoutError`` at the deadline."""

        ...


class AsyncioFractionSourceDeadline:
    """Use ``asyncio.wait_for`` to bound production source operations."""

    async def run(
        self,
        operation: Callable[[], Awaitable[Decimal | None]],
        *,
        timeout_seconds: float,
    ) -> Decimal | None:
        """Execute one operation within ``timeout_seconds``.

        Args:
            operation: Deferred external source call.
            timeout_seconds: Positive timeout budget.

        Returns:
            The source result.

        Raises:
            TimeoutError: If the operation does not finish in time.
        """

        return await asyncio.wait_for(operation(), timeout=timeout_seconds)


@dataclass(frozen=True, slots=True)
class LeaderSellResolutionOutcome:
    """Result of preparing one signal for copy dispatch."""

    signal: OnchainTradeSignal
    dispatch: bool
    reason: str = ""


@dataclass(slots=True)
class _BlockPositionState:
    """Track an exact observed leader balance within one Polygon block."""

    balance: Decimal
    last_log_index: int


class LeaderSellFractionResolver:
    """Resolve and cache leader SELL fractions with bounded failover.

    The historical reader supplies the balance at the end of the preceding
    block. Authoritative signals are delivered in log order by the detection
    runtime, so the resolver applies each observed BUY/SELL delta to retain
    the exact balance immediately before the next fill in the same block.
    Duplicate canonical fills reuse a cached outcome and never advance state.
    """

    def __init__(
        self,
        *,
        historical_balances: HistoricalLeaderBalanceReader,
        fallback_fractions: LeaderSellFractionFallback,
        emergency_fraction: Decimal = Decimal("0.15"),
        cache_size: int = 4096,
        logger: logging.Logger | None = None,
        buy_scope_guard: LeaderSellBuyScopeGuard | None = None,
        source_timeout_seconds: float = 3.0,
        source_deadline: FractionSourceDeadline | None = None,
    ) -> None:
        """Bind exact/fallback readers and the emergency policy.

        Args:
            historical_balances: Historical Polygon balance reader.
            fallback_fractions: Independent public Data API resolver.
            emergency_fraction: Bounded fraction used after both readers fail.
            cache_size: Maximum cached fill outcomes and block states.
            logger: Structured diagnostic logger.
            buy_scope_guard: Shared durable BUY-pause authority.
            source_timeout_seconds: Maximum duration of each external source.
            source_deadline: Injectable async timeout authority.

        Raises:
            ValueError: If the fraction or cache bound is invalid.
        """

        if not emergency_fraction.is_finite() or not (_ZERO < emergency_fraction <= _ONE):
            raise ValueError("emergency_fraction must be finite and in (0, 1]")
        if cache_size <= 0:
            raise ValueError("cache_size must be positive")
        if source_timeout_seconds <= 0:
            raise ValueError("source_timeout_seconds must be positive")
        self._historical_balances = historical_balances
        self._fallback_fractions = fallback_fractions
        self._emergency_fraction = emergency_fraction
        self._cache_size = cache_size
        self._logger = logger if logger is not None else logging.getLogger(__name__)
        self._buy_scope_guard = (
            buy_scope_guard if buy_scope_guard is not None else LeaderSellScopeGuard()
        )
        self._source_timeout_seconds = float(source_timeout_seconds)
        self._source_deadline = (
            source_deadline if source_deadline is not None else AsyncioFractionSourceDeadline()
        )
        self._block_states: OrderedDict[tuple[str, str, int, str], _BlockPositionState] = (
            OrderedDict()
        )
        self._resolved: OrderedDict[str, LeaderSellResolutionOutcome] = OrderedDict()
        self._degraded_scopes: set[tuple[str, str]] = set()

    async def resolve(self, signal: OnchainTradeSignal) -> LeaderSellResolutionOutcome:
        """Return a dispatch decision with an exact or bounded SELL fraction.

        Args:
            signal: Canonical pending or authoritative on-chain fill.

        Returns:
            A signal ready for dispatch, or a denied BUY while its leader/token
            scope is degraded after emergency SELL sizing.
        """

        identity = self._identity(signal)
        cached = self._resolved.get(identity)
        if cached is not None:
            self._resolved.move_to_end(identity)
            return cached

        if signal.provisional:
            return self._remember(identity, LeaderSellResolutionOutcome(signal, True))

        scope = self._scope(signal.leader_wallet, signal.token_id)
        side = str(signal.side or "").upper()
        state = await self._exact_block_state(signal)
        if state is not None:
            outcome = self._resolve_from_state(signal, state)
            if outcome is not None:
                cleared = await self._clear_scope(signal)
                if side == "BUY" and not cleared:
                    return self._remember(identity, self._blocked_buy(signal))
                return self._remember(identity, outcome)

        if side == "BUY":
            if scope in self._degraded_scopes or await self._buy_scope_guard.is_buy_paused(
                signal.leader_wallet,
                signal.token_id,
            ):
                return self._remember(identity, self._blocked_buy(signal))
            return self._remember(identity, LeaderSellResolutionOutcome(signal, True))

        if side != "SELL":
            return self._remember(identity, LeaderSellResolutionOutcome(signal, True))

        fallback_fraction = await self._fallback_fraction(signal)
        if fallback_fraction is not None:
            await self._clear_scope(signal)
            resolved = replace(
                signal,
                leader_sell_fraction=fallback_fraction,
                sell_fraction_resolution=SellFractionResolutionCode.DATA_API.value,
            )
            return self._remember(identity, LeaderSellResolutionOutcome(resolved, True))

        self._degraded_scopes.add(scope)
        await self._buy_scope_guard.pause_buys(
            signal.leader_wallet,
            signal.token_id,
            reason="authoritative_sources_unavailable",
        )
        resolved = replace(
            signal,
            leader_sell_fraction=self._emergency_fraction,
            sell_fraction_resolution=SellFractionResolutionCode.EMERGENCY.value,
        )
        self._logger.error(
            "leader_sell_fraction_emergency | leader=%s token=%s tx=%s fraction=%s "
            "reason=authoritative_sources_unavailable",
            signal.leader_wallet,
            signal.token_id,
            signal.tx_hash,
            self._emergency_fraction,
        )
        return self._remember(identity, LeaderSellResolutionOutcome(resolved, True))

    async def _clear_scope(self, signal: OnchainTradeSignal) -> bool:
        cleared = await self._buy_scope_guard.clear_buys_pause(
            signal.leader_wallet,
            signal.token_id,
        )
        if cleared is not False:
            self._degraded_scopes.discard(self._scope(signal.leader_wallet, signal.token_id))
            return True
        return False

    @staticmethod
    def _blocked_buy(signal: OnchainTradeSignal) -> LeaderSellResolutionOutcome:
        return LeaderSellResolutionOutcome(
            signal=signal,
            dispatch=False,
            reason="leader_sell_fraction_scope_degraded",
        )

    def is_scope_degraded(self, leader_wallet: str, token_id: str) -> bool:
        """Return whether emergency sizing currently pauses scope BUYs."""

        return self._scope(leader_wallet, token_id) in self._degraded_scopes

    async def _exact_block_state(
        self,
        signal: OnchainTradeSignal,
    ) -> _BlockPositionState | None:
        block_number = signal.block_number
        log_index = signal.log_index
        if block_number is None or block_number <= 0 or log_index is None or log_index < 0:
            return None
        key = (
            signal.leader_wallet.strip().lower(),
            str(signal.token_id),
            block_number,
            str(signal.block_hash or "").strip().lower(),
        )
        state = self._block_states.get(key)
        if state is not None:
            self._block_states.move_to_end(key)
            if log_index <= state.last_log_index:
                self._logger.warning(
                    "leader_sell_fraction_out_of_order | leader=%s token=%s block=%s "
                    "log_index=%s prior_log_index=%s",
                    signal.leader_wallet,
                    signal.token_id,
                    block_number,
                    log_index,
                    state.last_log_index,
                )
                return None
            return state
        try:
            raw_balance = await self._source_deadline.run(
                lambda: self._historical_balances.balance_at_block(
                    signal.leader_wallet,
                    signal.token_id,
                    block_number - 1,
                ),
                timeout_seconds=self._source_timeout_seconds,
            )
        except Exception as exc:
            self._logger.warning(
                "leader_sell_fraction_rpc_unavailable | leader=%s token=%s block=%s error_type=%s",
                signal.leader_wallet,
                signal.token_id,
                block_number,
                type(exc).__name__,
            )
            return None
        if raw_balance is None:
            return None
        try:
            balance = raw_balance if isinstance(raw_balance, Decimal) else Decimal(str(raw_balance))
        except (InvalidOperation, TypeError, ValueError) as exc:
            self._logger.warning(
                "leader_sell_fraction_rpc_invalid | leader=%s token=%s block=%s error_type=%s",
                signal.leader_wallet,
                signal.token_id,
                block_number,
                type(exc).__name__,
            )
            return None
        if not balance.is_finite() or balance < _ZERO:
            return None
        state = _BlockPositionState(balance=balance, last_log_index=-1)
        self._block_states[key] = state
        self._trim(self._block_states)
        return state

    def _resolve_from_state(
        self,
        signal: OnchainTradeSignal,
        state: _BlockPositionState,
    ) -> LeaderSellResolutionOutcome | None:
        size = signal.size if isinstance(signal.size, Decimal) else Decimal(str(signal.size))
        if not size.is_finite() or size <= _ZERO:
            return None
        side = str(signal.side or "").upper()
        if side == "BUY":
            state.balance += size
            state.last_log_index = int(signal.log_index or 0)
            return LeaderSellResolutionOutcome(signal, True)
        if side != "SELL" or state.balance <= _ZERO:
            return None
        fraction = min(_ONE, size / state.balance)
        resolved = replace(
            signal,
            leader_sell_fraction=fraction,
            sell_fraction_resolution=SellFractionResolutionCode.HISTORICAL_RPC.value,
        )
        state.balance = max(_ZERO, state.balance - size)
        state.last_log_index = int(signal.log_index or 0)
        return LeaderSellResolutionOutcome(resolved, True)

    async def _fallback_fraction(self, signal: OnchainTradeSignal) -> Decimal | None:
        try:
            raw_fraction = await self._source_deadline.run(
                lambda: self._fallback_fractions.fraction_for(signal),
                timeout_seconds=self._source_timeout_seconds,
            )
        except Exception as exc:
            self._logger.warning(
                "leader_sell_fraction_data_unavailable | leader=%s token=%s tx=%s error_type=%s",
                signal.leader_wallet,
                signal.token_id,
                signal.tx_hash,
                type(exc).__name__,
            )
            return None
        if raw_fraction is None:
            return None
        try:
            fraction = (
                raw_fraction if isinstance(raw_fraction, Decimal) else Decimal(str(raw_fraction))
            )
        except (InvalidOperation, TypeError, ValueError) as exc:
            self._logger.warning(
                "leader_sell_fraction_data_invalid | leader=%s token=%s tx=%s error_type=%s",
                signal.leader_wallet,
                signal.token_id,
                signal.tx_hash,
                type(exc).__name__,
            )
            return None
        if not fraction.is_finite() or not (_ZERO < fraction <= _ONE):
            return None
        return fraction

    def _remember(
        self,
        identity: str,
        outcome: LeaderSellResolutionOutcome,
    ) -> LeaderSellResolutionOutcome:
        self._resolved[identity] = outcome
        self._resolved.move_to_end(identity)
        self._trim(self._resolved)
        return outcome

    def _trim(self, values: OrderedDict[_KeyT, _ValueT]) -> None:
        while len(values) > self._cache_size:
            values.popitem(last=False)

    @staticmethod
    def _identity(signal: OnchainTradeSignal) -> str:
        return "|".join(
            (
                signal.tx_hash.strip().lower(),
                str(signal.log_index if signal.log_index is not None else ""),
                str(signal.order_hash or "").strip().lower(),
                signal.leader_wallet.strip().lower(),
                str(signal.token_id),
                str(signal.side or "").upper(),
            )
        )

    @staticmethod
    def _scope(leader_wallet: str, token_id: str) -> tuple[str, str]:
        return leader_wallet.strip().lower(), str(token_id)


__all__ = [
    "AsyncioFractionSourceDeadline",
    "FractionSourceDeadline",
    "HistoricalLeaderBalanceReader",
    "LeaderSellFractionFallback",
    "LeaderSellFractionResolver",
    "LeaderSellResolutionOutcome",
    "SellFractionResolutionCode",
]
