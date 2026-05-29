from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from domain.discovery_models import ObservedTrade


@dataclass(frozen=True)
class GroupedSignal:
    wallet: str
    condition_id: str
    token_id: str
    side: str
    first_ts: int
    last_ts: int
    total_size: Decimal
    total_usdc_size: Decimal
    vwap_price: Decimal
    tx_hashes: tuple[str, ...]
    trade_ids: tuple[str, ...]
    title: str
    market_slug: str
    event_slug: str
    outcome: str
    raw_count: int


class SignalGrouper:
    def __init__(
        self,
        *,
        window_seconds: int,
        max_gap_seconds: int,
        min_group_notional_usdc: Decimal,
    ):
        self.window_seconds = int(window_seconds)
        self.max_gap_seconds = int(max_gap_seconds)
        self.min_group_notional_usdc = min_group_notional_usdc

    def group(self, trades: Iterable[ObservedTrade]) -> list[GroupedSignal]:
        ordered = sorted(
            trades, key=lambda t: (int(t.event_ts), str(t.tx_hash), str(t.trade_id))
        )
        groups: list[_MutableGroup] = []

        for trade in ordered:
            key = (
                trade.trader_wallet.lower(),
                trade.condition_id,
                trade.token_id,
                trade.side.upper(),
            )
            last = groups[-1] if groups else None
            if last is not None and last.key == key and self._same_group(last, trade):
                last.add(trade)
            else:
                groups.append(_MutableGroup.from_trade(key, trade))

        signals = [g.freeze() for g in groups]
        return [
            signal
            for signal in signals
            if signal.total_usdc_size >= self.min_group_notional_usdc
        ]

    def _same_group(self, group: "_MutableGroup", trade: ObservedTrade) -> bool:
        ts = int(trade.event_ts)
        if ts - group.last_ts > self.max_gap_seconds:
            return False
        if ts - group.first_ts > self.window_seconds:
            return False
        return True


@dataclass
class _MutableGroup:
    key: tuple[str, str, str, str]
    wallet: str
    condition_id: str
    token_id: str
    side: str
    first_ts: int
    last_ts: int
    total_size: Decimal
    total_usdc_size: Decimal
    tx_hashes: list[str]
    trade_ids: list[str]
    title: str
    market_slug: str
    event_slug: str
    outcome: str
    raw_count: int

    @classmethod
    def from_trade(cls, key, trade: ObservedTrade) -> "_MutableGroup":
        return cls(
            key=key,
            wallet=trade.trader_wallet.lower(),
            condition_id=trade.condition_id,
            token_id=trade.token_id,
            side=trade.side.upper(),
            first_ts=int(trade.event_ts),
            last_ts=int(trade.event_ts),
            total_size=trade.size,
            total_usdc_size=trade.usdc_size,
            tx_hashes=[trade.tx_hash] if trade.tx_hash else [],
            trade_ids=[trade.trade_id] if trade.trade_id else [],
            title=trade.title,
            market_slug=trade.market_slug,
            event_slug=trade.event_slug,
            outcome=trade.outcome,
            raw_count=1,
        )

    def add(self, trade: ObservedTrade) -> None:
        self.last_ts = max(self.last_ts, int(trade.event_ts))
        self.total_size += trade.size
        self.total_usdc_size += trade.usdc_size
        if trade.tx_hash:
            self.tx_hashes.append(trade.tx_hash)
        if trade.trade_id:
            self.trade_ids.append(trade.trade_id)
        self.raw_count += 1

    def freeze(self) -> GroupedSignal:
        vwap = (
            self.total_usdc_size / self.total_size
            if self.total_size > 0
            else Decimal("0")
        )
        return GroupedSignal(
            wallet=self.wallet,
            condition_id=self.condition_id,
            token_id=self.token_id,
            side=self.side,
            first_ts=self.first_ts,
            last_ts=self.last_ts,
            total_size=self.total_size,
            total_usdc_size=self.total_usdc_size,
            vwap_price=vwap,
            tx_hashes=tuple(self.tx_hashes),
            trade_ids=tuple(self.trade_ids),
            title=self.title,
            market_slug=self.market_slug,
            event_slug=self.event_slug,
            outcome=self.outcome,
            raw_count=self.raw_count,
        )

