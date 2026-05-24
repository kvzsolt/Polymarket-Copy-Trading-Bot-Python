"""Read-only raw-feature joiner.

Consumes ``ScoringInputRecord`` dicts and joins orderbook / trade evidence
from ``DiscoveryRepo``, emitting ``RawFeatureRecord`` instances with honest
availability flags.

Design rules:
* Never fabricates missing data (orderbooks, price history).
* Never computes composite scores — only raw inputs + presence flags.
* Stateless / read-only — all state lives in the repo.
"""
from __future__ import annotations

from decimal import Decimal

from domain.discovery_models import (
    OrderbookSnapshot,
    ReconstructedPositionStatus,
)
from domain.raw_feature_models import (
    DelaySurvivalInputs,
    EntryAlphaInputs,
    EvidencePresenceFlags,
    RawFeatureRecord,
    SlippageCopyabilityInputs,
)
from domain.scoring_models import PositionKey, ScoringInputRecord
from storage.sqlite.discovery_repo import DiscoveryRepo

# Window durations in seconds
_WINDOW_5M = 5 * 60
_WINDOW_30M = 30 * 60
_WINDOW_4H = 4 * 60 * 60


class RawFeatureJoiner:
    """Joins scoring-input records with stored evidence to produce raw features."""

    def __init__(self, repo: DiscoveryRepo) -> None:
        self._repo = repo

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def join(
        self,
        records: dict[PositionKey, ScoringInputRecord],
        *,
        reference_ts: int | None = None,
    ) -> dict[PositionKey, RawFeatureRecord]:
        """Produce raw feature records for every scoring-input record.

        Parameters
        ----------
        records:
            Output of ``ScoringInputAssemblerService.assemble_for_trader``.
        reference_ts:
            Optional override for "now".  When *None*, each record's own
            ``market_context.reference_ts`` (or ``evidence.last_observed_trade_ts``)
            is used.
        """
        out: dict[PositionKey, RawFeatureRecord] = {}
        for key, rec in records.items():
            out[key] = self._build_one(rec, reference_ts)
        return out

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_one(
        self,
        rec: ScoringInputRecord,
        ref_ts_override: int | None,
    ) -> RawFeatureRecord:
        ref_ts = ref_ts_override or rec.market_context.reference_ts

        # Fetch orderbook snapshots for this token
        ob_snaps = self._repo.list_orderbook_snapshots(
            rec.token_id, limit=10,
        )

        # Fetch trades for entry-alpha windows
        trades = self._repo.list_trades(
            trader_wallet=rec.trader_wallet,
            condition_id=rec.condition_id,
            token_id=rec.token_id,
        )

        entry_alpha = self._build_entry_alpha(trades, rec, ref_ts)
        delay_survival = self._build_delay_survival(rec, ref_ts)
        slippage = self._build_slippage(ob_snaps, ref_ts)
        flags = self._build_evidence_flags(rec, ob_snaps, trades)

        return RawFeatureRecord(
            trader_wallet=rec.trader_wallet,
            condition_id=rec.condition_id,
            token_id=rec.token_id,
            current_size=rec.current_size,
            avg_cost=rec.avg_cost,
            realized_pnl=rec.realized_pnl,
            position_status=rec.position_status,
            reconstruction_confidence=rec.reconstruction_confidence,
            entry_alpha=entry_alpha,
            delay_survival=delay_survival,
            slippage_copyability=slippage,
            evidence_flags=flags,
        )

    # -- entry-alpha --------------------------------------------------------

    def _build_entry_alpha(
        self,
        trades: list,
        rec: ScoringInputRecord,
        ref_ts: int | None,
    ) -> EntryAlphaInputs:
        last_ts = rec.evidence.last_observed_trade_ts

        if not trades or last_ts is None or ref_ts is None:
            return EntryAlphaInputs(
                last_trade_ts=last_ts,
                reference_ts=ref_ts,
            )

        # Compute per-window availability and price from actual trades
        w5 = self._window_stats(trades, last_ts, _WINDOW_5M)
        w30 = self._window_stats(trades, last_ts, _WINDOW_30M)
        w4h = self._window_stats(trades, last_ts, _WINDOW_4H)

        return EntryAlphaInputs(
            last_trade_ts=last_ts,
            reference_ts=ref_ts,
            window_5m_available=w5[0],
            window_5m_entry_price=w5[1],
            window_5m_trade_count=w5[2],
            window_30m_available=w30[0],
            window_30m_entry_price=w30[1],
            window_30m_trade_count=w30[2],
            window_4h_available=w4h[0],
            window_4h_entry_price=w4h[1],
            window_4h_trade_count=w4h[2],
        )

    @staticmethod
    def _window_stats(
        trades: list,
        anchor_ts: int,
        window_seconds: int,
    ) -> tuple[bool, Decimal | None, int]:
        """Return (available, vwap_entry_price, count) for BUY trades in window."""
        cutoff = anchor_ts - window_seconds
        buys = [
            t for t in trades
            if t.side == "BUY" and t.event_ts >= cutoff
        ]
        if not buys:
            return (False, None, 0)

        total_size = sum(t.size for t in buys)
        if total_size == 0:
            return (True, None, len(buys))

        vwap = sum(t.price * t.size for t in buys) / total_size
        return (True, vwap, len(buys))

    # -- delay-survival -----------------------------------------------------

    @staticmethod
    def _build_delay_survival(
        rec: ScoringInputRecord,
        ref_ts: int | None,
    ) -> DelaySurvivalInputs:
        last_ts = rec.evidence.last_observed_trade_ts
        delay: int | None = None
        if last_ts is not None and ref_ts is not None:
            delay = max(ref_ts - last_ts, 0)

        still_open = rec.position_status == ReconstructedPositionStatus.OPEN

        return DelaySurvivalInputs(
            delay_seconds=delay,
            position_still_open=still_open,
            reconstruction_confidence=rec.reconstruction_confidence,
            has_mismatch=len(rec.mismatch_types) > 0,
        )

    # -- slippage / copyability ---------------------------------------------

    @staticmethod
    def _build_slippage(
        ob_snaps: list[OrderbookSnapshot],
        ref_ts: int | None,
    ) -> SlippageCopyabilityInputs:
        if not ob_snaps:
            return SlippageCopyabilityInputs()

        # Use the most recent snapshot
        snap = ob_snaps[0]
        age: int | None = None
        if ref_ts is not None:
            age = max(ref_ts - snap.snapshot_ts, 0)

        has_depth = snap.bids_json not in ("[]", "") or snap.asks_json not in ("[]", "")

        return SlippageCopyabilityInputs(
            has_orderbook_snapshot=True,
            snapshot_ts=snap.snapshot_ts,
            best_bid=snap.best_bid,
            best_ask=snap.best_ask,
            mid_price=snap.mid_price,
            spread=snap.spread,
            book_depth_available=has_depth,
            snapshot_age_seconds=age,
        )

    # -- evidence flags -----------------------------------------------------

    @staticmethod
    def _build_evidence_flags(
        rec: ScoringInputRecord,
        ob_snaps: list[OrderbookSnapshot],
        trades: list,
    ) -> EvidencePresenceFlags:
        return EvidencePresenceFlags(
            has_position_snapshot=rec.evidence.has_snapshot,
            has_market_meta=rec.market_context.has_market_meta,
            has_orderbook_snapshot=len(ob_snaps) > 0,
            has_trade_history=len(trades) > 0,
            has_price_history=False,  # no price-history table yet — honest absence
            trade_count=len(trades),
            orderbook_snapshot_count=len(ob_snaps),
        )
