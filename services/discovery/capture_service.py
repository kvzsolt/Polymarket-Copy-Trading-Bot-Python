"""Async capture service for the honest copy-leader research spine.

This service persists:
- observed leader trades from the activity feed
- opportunistic market metadata from those payloads
- public orderbook snapshots captured near newly observed trades
- current leader position snapshots

It intentionally does not infer bankroll, inventory, or synthetic fills.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any

from domain.discovery_models import ObservedTrade, OrderbookSnapshot
from services.discovery.ingestion_service import IngestionService
from storage.sqlite.discovery_repo import DiscoveryRepo
from utils.createClobClient import CLOB_HTTP_URL
from utils.fetchData import fetch_data
from utils.logger import get_logger, redact_error_detail

logger = get_logger("DiscoveryCapture")


class DiscoveryCaptureService:
    def __init__(self, db_path: str, *, orderbook_levels: int = 5):
        self._repo = DiscoveryRepo(db_path)
        self._ingestion = IngestionService(self._repo)
        self._orderbook_levels = max(1, int(orderbook_levels))

    async def capture_observed_trades(self, raw_activities: list[dict[str, Any]]) -> None:
        if not raw_activities:
            return

        for raw in raw_activities:
            try:
                self._ingestion.ingest_market(raw)
            except Exception as exc:
                logger.warning("market_capture_failed", extra={"error": redact_error_detail(exc)})

        result = self._ingestion.ingest_activity_batch_detailed(raw_activities)
        if not result.inserted_trades:
            return

        capture_targets: dict[tuple[str, str], ObservedTrade] = {}
        for trade in result.inserted_trades:
            key = (trade.token_id, trade.tx_hash)
            previous = capture_targets.get(key)
            if previous is None or trade.event_ts >= previous.event_ts:
                capture_targets[key] = trade

        for trade in capture_targets.values():
            await self._capture_orderbook_snapshot(trade)

    async def capture_position_snapshots(
        self,
        trader_wallet: str,
        raw_positions: list[dict[str, Any]],
        *,
        snapshot_ts: int | None = None,
    ) -> int:
        for raw in raw_positions:
            try:
                self._ingestion.ingest_market(raw)
            except Exception as exc:
                logger.warning("market_capture_failed", extra={"error": redact_error_detail(exc)})
        return self._ingestion.ingest_position_snapshots(
            trader_wallet,
            raw_positions,
            snapshot_ts=snapshot_ts,
        )

    async def capture_closed_positions(
        self,
        trader_wallet: str,
        raw_positions: list[dict[str, Any]],
    ) -> int:
        for raw in raw_positions:
            try:
                self._ingestion.ingest_market(raw)
            except Exception as exc:
                logger.warning("market_capture_failed", extra={"error": redact_error_detail(exc)})
        return self._ingestion.ingest_closed_position_snapshots(trader_wallet, raw_positions)

    async def _capture_orderbook_snapshot(self, trade: ObservedTrade) -> None:
        if not trade.token_id:
            return

        url = f"{CLOB_HTTP_URL}/book?token_id={trade.token_id}"
        try:
            payload = await fetch_data(url, silent=True)
        except Exception as exc:
            logger.warning(
                "orderbook_capture_failed",
                extra={
                    "token_id": trade.token_id,
                    "tx_hash": trade.tx_hash,
                    "error": redact_error_detail(exc),
                },
            )
            return

        snapshot = self._build_orderbook_snapshot(trade, payload)
        if snapshot is None:
            return

        self._repo.insert_orderbook_snapshot(snapshot)
        self._ingestion.ingest_market(
            {
                "conditionId": trade.condition_id,
                "asset": trade.token_id,
                "outcome": trade.outcome,
                "title": trade.title,
                "marketSlug": trade.market_slug,
                "eventSlug": trade.event_slug,
                "neg_risk": payload.get("neg_risk") if isinstance(payload, dict) else None,
            }
        )

    def _build_orderbook_snapshot(
        self,
        trade: ObservedTrade,
        payload: Any,
    ) -> OrderbookSnapshot | None:
        if not isinstance(payload, dict):
            return None

        bids = _normalize_levels(payload.get("bids"), self._orderbook_levels)
        asks = _normalize_levels(payload.get("asks"), self._orderbook_levels)
        best_bid = Decimal(bids[0][0]) if bids else None
        best_ask = Decimal(asks[0][0]) if asks else None
        mid_price = ((best_bid + best_ask) / Decimal("2")) if best_bid is not None and best_ask is not None else None
        spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None

        return OrderbookSnapshot(
            token_id=trade.token_id,
            condition_id=trade.condition_id,
            snapshot_ts=int(time.time() * 1000),
            trader_wallet=trade.trader_wallet,
            reference_tx_hash=trade.tx_hash,
            reference_trade_id=trade.trade_id,
            reference_event_ts=trade.event_ts,
            bids_json=json.dumps(bids),
            asks_json=json.dumps(asks),
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid_price,
            spread=spread,
        )


def _normalize_levels(levels: Any, limit: int) -> list[list[str]]:
    if not isinstance(levels, list):
        return []

    normalized: list[list[str]] = []
    for level in levels[:limit]:
        if not isinstance(level, dict):
            continue
        price = _safe_decimal(level.get("price"))
        size = _safe_decimal(level.get("size"))
        if price is None or size is None:
            continue
        normalized.append([str(price), str(size)])
    return normalized


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None
