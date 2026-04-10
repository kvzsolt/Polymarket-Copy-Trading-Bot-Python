"""Async capture service for the honest copy-leader research spine.

This service persists:
- observed leader trades from the activity feed
- opportunistic market metadata from those payloads
- public orderbook snapshots captured near newly observed trades
- current leader position snapshots

It intentionally does not infer bankroll, inventory, or synthetic fills.
"""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Any

import aiohttp

from domain.discovery_models import ObservedTrade, OrderbookSnapshot
from services.discovery.ingestion_service import IngestionService
from storage.sqlite.discovery_repo import DiscoveryRepo
from utils import is_valid_clob_token_id
from utils.createClobClient import CLOB_HTTP_URL
from utils.fetchData import fetch_data
from utils.logger import get_logger, redact_error_detail, safe_error_detail

logger = get_logger("DiscoveryCapture")


class DiscoveryCaptureService:
    def __init__(
        self,
        db_path: str,
        *,
        orderbook_levels: int = 5,
        orderbook_error_log_window_seconds: float = 90.0,
    ):
        self._repo = DiscoveryRepo(db_path)
        self._ingestion = IngestionService(self._repo)
        self._orderbook_levels = max(1, int(orderbook_levels))
        self._orderbook_error_log_window_seconds = max(0.0, float(orderbook_error_log_window_seconds))
        self._orderbook_failure_log_state: dict[tuple[str, str, int | None], tuple[float, int]] = {}

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
        if not is_valid_clob_token_id(trade.token_id):
            self._log_orderbook_capture_failed(
                trade,
                failure_class="invalid_token_id",
                retryable=False,
                error="token_id is not a valid CLOB token id",
            )
            return

        url = f"{CLOB_HTTP_URL}/book?token_id={trade.token_id}"
        try:
            payload = await fetch_data(url, silent=True)
        except Exception as exc:
            failure_class, retryable, http_status = self._classify_orderbook_error(exc)
            self._log_orderbook_capture_failed(
                trade,
                failure_class=failure_class,
                retryable=retryable,
                error=safe_error_detail(exc),
                http_status=http_status,
            )
            return

        snapshot = self._build_orderbook_snapshot(trade, payload)
        if snapshot is None:
            self._log_orderbook_capture_failed(
                trade,
                failure_class="invalid_payload",
                retryable=False,
                error="orderbook payload missing or malformed",
            )
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

    @staticmethod
    def _classify_orderbook_error(exc: Exception) -> tuple[str, bool, int | None]:
        if isinstance(exc, aiohttp.ClientResponseError):
            status = int(exc.status or 0)
            if status in (400, 404, 410, 422):
                return "http_terminal", False, status
            if status == 429:
                return "http_rate_limited", True, status
            if status >= 500:
                return "http_server_error", True, status
            return "http_error", False, status
        if isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientConnectionError, aiohttp.ServerDisconnectedError)):
            return "transport_error", True, None
        return "unexpected_error", False, None

    def _log_orderbook_capture_failed(
        self,
        trade: ObservedTrade,
        *,
        failure_class: str,
        retryable: bool,
        error: str,
        http_status: int | None = None,
    ) -> None:
        should_log, suppressed = self._should_log_orderbook_failure(
            token_id=trade.token_id,
            failure_class=failure_class,
            http_status=http_status,
        )
        if not should_log:
            return

        extra = {
            "token_id": trade.token_id,
            "condition_id": trade.condition_id,
            "trader_wallet": trade.trader_wallet,
            "tx_hash": trade.tx_hash,
            "trade_id": trade.trade_id,
            "failure_class": failure_class,
            "retryable": retryable,
            "terminal": not retryable,
            "error": error,
        }
        if http_status is not None:
            extra["http_status"] = http_status
        if suppressed > 0:
            extra["suppressed_duplicates"] = suppressed
        logger.warning("orderbook_capture_failed", extra=extra)

    def _should_log_orderbook_failure(
        self,
        *,
        token_id: str,
        failure_class: str,
        http_status: int | None,
    ) -> tuple[bool, int]:
        if self._orderbook_error_log_window_seconds <= 0:
            return True, 0

        key = (token_id, failure_class, http_status)
        now = time.monotonic()
        previous = self._orderbook_failure_log_state.get(key)

        if previous is None:
            self._orderbook_failure_log_state[key] = (now, 0)
            return True, 0

        last_logged_at, suppressed = previous
        if (now - last_logged_at) >= self._orderbook_error_log_window_seconds:
            self._orderbook_failure_log_state[key] = (now, 0)
            return True, suppressed

        self._orderbook_failure_log_state[key] = (last_logged_at, suppressed + 1)
        return False, 0

    def _build_orderbook_snapshot(
        self,
        trade: ObservedTrade,
        payload: Any,
    ) -> OrderbookSnapshot | None:
        if not isinstance(payload, dict):
            return None

        bids = _sorted_levels(
            _normalize_levels(payload.get("bids"), self._orderbook_levels),
            reverse=True,
        )
        asks = _sorted_levels(
            _normalize_levels(payload.get("asks"), self._orderbook_levels),
            reverse=False,
        )
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
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = _safe_decimal(level.get("price"))
        size = _safe_decimal(level.get("size"))
        if price is None or size is None:
            continue
        normalized.append([str(price), str(size)])
    return normalized[:limit]


def _sorted_levels(levels: list[list[str]], *, reverse: bool) -> list[list[str]]:
    return sorted(levels, key=lambda level: Decimal(level[0]), reverse=reverse)


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None
