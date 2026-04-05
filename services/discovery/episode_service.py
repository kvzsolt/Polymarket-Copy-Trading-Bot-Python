"""Phase 3 — Episode builder.

Groups related trades on the same (trader, condition_id, token_id) into
coherent episodes: initial entry → adds → trims → close.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Sequence

from domain.discovery_models import (
    Episode,
    EpisodeEvent,
    EpisodeState,
    ObservedTrade,
)


class EpisodeService:
    """Builds and maintains episodes from chronologically-sorted trades."""

    def build_episodes(
        self,
        trades: Sequence[ObservedTrade],
    ) -> list[Episode]:
        """Given trades for ONE (trader, condition_id, token_id) sorted by
        event_ts, produce a list of Episodes.

        A new episode starts whenever the net position returns to zero
        then a new trade appears.
        """
        if not trades:
            return []

        episodes: list[Episode] = []
        current: Episode | None = None
        net_position = Decimal("0")
        direction_side: str = ""

        for t in trades:
            delta = t.size if t.side == "BUY" else -t.size

            if current is None:
                current = self._start_episode(t)
                direction_side = t.side
                net_position = delta
                self._add_event(current, t, "entry")
                continue

            new_net = net_position + delta

            # Determine event type
            if t.side == direction_side:
                event_type = "add"
            else:
                if new_net == Decimal("0"):
                    event_type = "close"
                else:
                    event_type = "trim"

            self._add_event(current, t, event_type)

            if new_net == Decimal("0"):
                self._close_episode(current, t.event_ts)
                episodes.append(current)
                current = None
                net_position = Decimal("0")
                direction_side = ""
            elif (net_position > 0 and new_net < 0) or (net_position < 0 and new_net > 0):
                # Position flipped sign — close current and start a new one
                self._close_episode(current, t.event_ts)
                episodes.append(current)
                current = self._start_episode(t)
                direction_side = t.side
                net_position = new_net
                self._add_event(current, t, "entry")
            else:
                net_position = new_net
                self._update_state(current, event_type)

        # If we still have an open episode, include it
        if current is not None:
            episodes.append(current)

        # Compute summary stats for each episode
        for ep in episodes:
            self._compute_summary(ep)

        return episodes

    # ------------------------------------------------------------------

    _DIRECTION_MAP = {"BUY": "LONG", "SELL": "SHORT"}

    def _start_episode(self, t: ObservedTrade) -> Episode:
        return Episode(
            episode_id=f"{t.trader_wallet}:{t.condition_id}:{t.token_id}:{t.event_ts}",
            trader_wallet=t.trader_wallet,
            condition_id=t.condition_id,
            token_id=t.token_id,
            state=EpisodeState.OPEN,
            direction=self._DIRECTION_MAP.get(t.side, "LONG"),
            opened_ts=t.event_ts,
        )

    def _add_event(self, ep: Episode, t: ObservedTrade, event_type: str) -> None:
        ep.events.append(
            EpisodeEvent(
                trade_id=t.tx_hash or t.trade_id,
                side=t.side,
                size=t.size,
                price=t.price,
                usdc_size=t.usdc_size,
                ts=t.event_ts,
                event_type=event_type,
            )
        )

    def _update_state(self, ep: Episode, event_type: str) -> None:
        if event_type == "add":
            if ep.state in (EpisodeState.OPEN, EpisodeState.ADDING):
                ep.state = EpisodeState.ADDING
        elif event_type == "trim":
            ep.state = EpisodeState.TRIMMING

    def _close_episode(self, ep: Episode, ts: int) -> None:
        ep.state = EpisodeState.CLOSED
        ep.closed_ts = ts

    def _compute_summary(self, ep: Episode) -> None:
        """Compute entry/exit summary from events."""
        entry_side = "BUY" if ep.direction == "LONG" else "SELL"
        exit_side = "SELL" if ep.direction == "LONG" else "BUY"

        entry_notional = Decimal("0")
        entry_size = Decimal("0")
        exit_notional = Decimal("0")
        exit_size = Decimal("0")

        for ev in ep.events:
            if ev.side == entry_side:
                entry_size += ev.size
                entry_notional += ev.size * ev.price
            elif ev.side == exit_side:
                exit_size += ev.size
                exit_notional += ev.size * ev.price

        ep.entry_size = entry_size
        ep.entry_avg_price = (entry_notional / entry_size) if entry_size else Decimal("0")
        ep.exit_size = exit_size
        ep.exit_avg_price = (exit_notional / exit_size) if exit_size else Decimal("0")

        if exit_size > 0 and entry_size > 0:
            if ep.direction == "LONG":
                ep.realized_pnl = exit_size * (ep.exit_avg_price - ep.entry_avg_price)
            else:
                ep.realized_pnl = exit_size * (ep.entry_avg_price - ep.exit_avg_price)
