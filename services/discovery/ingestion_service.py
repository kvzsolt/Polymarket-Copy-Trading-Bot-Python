"""Phase 1 — Data ingestion service for leader discovery.

Normalises raw API payloads from the Polymarket data/activity API and
stores them via DiscoveryRepo.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from domain.discovery_models import (
    ClosedPositionSnapshot,
    MarketMeta,
    ObservedTrade,
    PositionSnapshotRecord,
    TraderRecord,
)
from storage.postgres.discovery_repo import DiscoveryRepo


@dataclass(frozen=True)
class ActivityBatchIngestResult:
    inserted_count: int
    skipped_count: int
    inserted_trades: tuple[ObservedTrade, ...]


class IngestionService:
    """Thin service that normalises raw API dicts into domain models and
    persists them.  Designed to be called from a polling loop or batch job."""

    def __init__(self, repo: DiscoveryRepo):
        self.repo = repo

    # ------------------------------------------------------------------
    # Activity / trades
    # ------------------------------------------------------------------

    async def ingest_activity_batch(
        self, raw_activities: list[dict[str, Any]]
    ) -> tuple[int, int]:
        """Normalise and store a batch of raw activity dicts.

        Returns (inserted_count, skipped_duplicate_count).
        """
        result = await self.ingest_activity_batch_detailed(raw_activities)
        return result.inserted_count, result.skipped_count

    async def ingest_activity_batch_detailed(
        self, raw_activities: list[dict[str, Any]]
    ) -> ActivityBatchIngestResult:
        inserted = 0
        skipped = 0
        inserted_trades: list[ObservedTrade] = []
        for raw in raw_activities:
            trade = self.normalise_activity(raw)
            if trade is None:
                skipped += 1
                continue
            result = await self.repo.insert_trade(trade)
            if result is not None:
                inserted += 1
                inserted_trades.append(trade)
                await self._touch_trader(trade.trader_wallet, trade.event_ts)
            else:
                skipped += 1
        return ActivityBatchIngestResult(
            inserted_count=inserted,
            skipped_count=skipped,
            inserted_trades=tuple(inserted_trades),
        )

    def normalise_activity(self, raw: dict[str, Any]) -> ObservedTrade | None:
        return self._normalise_activity(raw)

    def _normalise_activity(self, raw: dict[str, Any]) -> ObservedTrade | None:
        """Map a raw Polymarket activity dict to ObservedTrade.

        Returns None if mandatory fields are missing or un-parseable.
        """
        try:
            wallet = (raw.get("user") or raw.get("proxyWallet") or "").lower().strip()
            if not wallet:
                return None
            side = (raw.get("side") or "").upper()
            if side not in ("BUY", "SELL"):
                return None

            size = _safe_decimal(raw.get("size"), Decimal("0"))
            price = _safe_decimal(raw.get("price"), Decimal("0"))
            usdc = _safe_decimal(raw.get("usdcSize"), size * price)
            tx = raw.get("transactionHash") or raw.get("tx_hash") or ""
            if not tx:
                return None

            cond_id = raw.get("conditionId") or raw.get("condition_id") or ""
            token_id = raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or ""
            if not cond_id or not token_id:
                return None
            event_ts = int(raw.get("timestamp") or raw.get("event_ts") or time.time())
            trade_id = raw.get("id") or raw.get("tradeID") or raw.get("trade_id") or ""
            fee = _safe_decimal(raw.get("fee"), Decimal("0"))

            return ObservedTrade(
                trader_wallet=wallet,
                condition_id=cond_id,
                token_id=token_id,
                side=side,
                size=size,
                price=price,
                usdc_size=usdc,
                tx_hash=tx,
                event_ts=event_ts,
                trade_id=str(trade_id),
                fee=fee,
                outcome=str(raw.get("outcome") or ""),
                title=str(raw.get("title") or raw.get("question") or ""),
                market_slug=str(raw.get("marketSlug") or raw.get("slug") or ""),
                event_slug=str(raw.get("eventSlug") or ""),
                raw_json=json.dumps(raw, default=str),
                source="activity_api",
            )
        except (TypeError, ValueError, KeyError):
            return None

    async def _touch_trader(self, wallet: str, ts: int) -> None:
        existing = await self.repo.get_trader(wallet)
        if existing is None:
            await self.repo.upsert_trader(
                TraderRecord(wallet=wallet, first_seen_ts=ts, last_seen_ts=ts, trade_count=1)
            )
        else:
            await self.repo.upsert_trader(
                TraderRecord(
                    wallet=wallet,
                    first_seen_ts=existing.first_seen_ts,
                    last_seen_ts=max(existing.last_seen_ts, ts),
                    trade_count=existing.trade_count + 1,
                    source=existing.source,
                    meta=existing.meta,
                )
            )

    # ------------------------------------------------------------------
    # Position snapshots
    # ------------------------------------------------------------------

    async def ingest_position_snapshots(
        self,
        trader_wallet: str,
        raw_positions: list[dict[str, Any]],
        snapshot_ts: int | None = None,
    ) -> int:
        """Store a batch of position snapshots.  Returns count inserted."""
        ts = snapshot_ts or int(time.time())
        count = 0
        for raw in raw_positions:
            snap = self._normalise_position(trader_wallet, raw, ts)
            if snap is None:
                continue
            if await self.repo.insert_position_snapshot(snap) is not None:
                count += 1
        return count

    def _normalise_position(
        self, wallet: str, raw: dict[str, Any], ts: int
    ) -> PositionSnapshotRecord | None:
        try:
            cond_id = raw.get("conditionId") or raw.get("condition_id") or ""
            token_id = raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or ""
            if not cond_id or not token_id:
                return None
            size = _safe_decimal(raw.get("size"), Decimal("0"))
            avg_price = _safe_decimal(raw.get("avgPrice") or raw.get("avg_price"), Decimal("0"))
            current_value = _safe_decimal(
                raw.get("currentValue") or raw.get("current_value"),
                Decimal("0"),
            )
            current_price = _safe_decimal(
                raw.get("curPrice") or raw.get("cur_price") or raw.get("currentPrice"),
                Decimal("0"),
            )
            realized_pnl = _safe_decimal(
                raw.get("realizedPnl") or raw.get("realized_pnl"),
                Decimal("0"),
            )
            return PositionSnapshotRecord(
                trader_wallet=wallet.lower().strip(),
                condition_id=cond_id,
                token_id=token_id,
                size=size,
                avg_price=avg_price,
                current_value=current_value,
                current_price=current_price,
                realized_pnl=realized_pnl,
                snapshot_ts=ts,
                raw_json=json.dumps(raw, default=str),
            )
        except (TypeError, ValueError, KeyError):
            return None

    async def ingest_closed_position_snapshots(
        self,
        trader_wallet: str,
        raw_positions: list[dict[str, Any]],
    ) -> int:
        """Store closed-position snapshots when the source feed provides them.

        This method is intentionally strict: it refuses to infer missing exit
        prices, realized PnL, or inventory.
        """
        count = 0
        for raw in raw_positions:
            snap = self._normalise_closed_position(trader_wallet, raw)
            if snap is None:
                continue
            if await self.repo.insert_closed_position(snap) is not None:
                count += 1
        return count

    def _normalise_closed_position(
        self, wallet: str, raw: dict[str, Any]
    ) -> ClosedPositionSnapshot | None:
        try:
            cond_id = raw.get("conditionId") or raw.get("condition_id") or ""
            token_id = raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or ""
            entry_size = _safe_decimal(
                raw.get("entrySize") or raw.get("entry_size") or raw.get("size"),
                Decimal("0"),
            )
            entry_avg_price = _safe_decimal(
                raw.get("avgPrice") or raw.get("entryAvgPrice") or raw.get("entry_avg_price"),
                Decimal("0"),
            )
            exit_avg_price = _safe_decimal(
                raw.get("exitAvgPrice") or raw.get("exit_avg_price"),
                Decimal("-1"),
            )
            realized_pnl = _safe_decimal(
                raw.get("realizedPnl") or raw.get("realized_pnl"),
                Decimal("0"),
            )
            closed_ts = _safe_timestamp(
                raw.get("closedAt") or raw.get("closed_at") or raw.get("timestamp")
            )
            if not cond_id or not token_id or exit_avg_price < 0 or closed_ts is None:
                return None
            return ClosedPositionSnapshot(
                trader_wallet=wallet.lower().strip(),
                condition_id=cond_id,
                token_id=token_id,
                entry_size=entry_size,
                entry_avg_price=entry_avg_price,
                exit_avg_price=exit_avg_price,
                realized_pnl=realized_pnl,
                closed_ts=closed_ts,
                close_reason=str(raw.get("closeReason") or raw.get("close_reason") or "trade"),
                raw_json=json.dumps(raw, default=str),
            )
        except (TypeError, ValueError, KeyError):
            return None

    # ------------------------------------------------------------------
    # Market metadata
    # ------------------------------------------------------------------

    async def ingest_market(self, raw: dict[str, Any]) -> bool:
        """Normalise and store market metadata.  Returns True on success."""
        cond_id = raw.get("condition_id") or raw.get("conditionId") or ""
        if not cond_id:
            return False
        existing = await self.repo.get_market(cond_id)
        yes_token_id, no_token_id, tokens_json = _extract_token_ids(raw)
        question = str(raw.get("question") or raw.get("title") or "")
        slug = str(raw.get("slug") or raw.get("marketSlug") or "")
        event_slug = str(raw.get("eventSlug") or "")
        resolved = _safe_bool(raw.get("resolved") or raw.get("closed"))
        active = _safe_bool(raw.get("active"), default=(not resolved))
        if existing is not None:
            question = question or existing.question
            slug = slug or existing.slug
            event_slug = event_slug or existing.event_slug
            yes_token_id = yes_token_id or existing.yes_token_id
            no_token_id = no_token_id or existing.no_token_id
            if tokens_json == "[]":
                tokens_json = existing.tokens_json
            category = str(raw.get("category") or existing.category or "")
            end_date_ts = _safe_timestamp(
                raw.get("end_date_iso") or raw.get("endDate") or raw.get("end_date")
            )
            if end_date_ts is None:
                end_date_ts = existing.end_date_ts
            active = active if "active" in raw or "resolved" in raw or "closed" in raw else existing.active
            resolved = resolved if "resolved" in raw or "closed" in raw else existing.resolved
            neg_risk = (
                _safe_bool(raw.get("negRisk") or raw.get("neg_risk"))
                if ("negRisk" in raw or "neg_risk" in raw)
                else existing.neg_risk
            )
        else:
            category = str(raw.get("category", ""))
            end_date_ts = _safe_timestamp(
                raw.get("end_date_iso") or raw.get("endDate") or raw.get("end_date")
            )
            neg_risk = _safe_bool(raw.get("negRisk") or raw.get("neg_risk"))
        await self.repo.upsert_market(
            MarketMeta(
                condition_id=cond_id,
                question=question,
                slug=slug,
                event_slug=event_slug,
                category=category,
                end_date_ts=end_date_ts,
                active=active,
                resolved=resolved,
                neg_risk=neg_risk,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                tokens_json=tokens_json,
                meta_json=json.dumps(raw, default=str),
            )
        )
        return True


def _safe_decimal(val: Any, default: Decimal) -> Decimal:
    if val is None:
        return default
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return default


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    return str(val).strip().lower() in {"1", "true", "yes"}


def _safe_timestamp(val: Any) -> int | None:
    if val is None or val == "":
        return None
    if isinstance(val, (int, float)):
        raw = float(val)
        return int(raw / 1000) if raw > 1e12 else int(raw)
    text = str(val).strip()
    if not text:
        return None
    try:
        raw = float(text)
        return int(raw / 1000) if raw > 1e12 else int(raw)
    except ValueError:
        pass
    normalized = text.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(normalized).timestamp())
    except ValueError:
        return None


def _extract_token_ids(raw: dict[str, Any]) -> tuple[str, str, str]:
    tokens = raw.get("tokens") or []
    yes_token_id = ""
    no_token_id = ""

    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            outcome = str(token.get("outcome") or token.get("name") or "").strip().lower()
            token_id = str(
                token.get("token_id")
                or token.get("tokenID")
                or token.get("tokenId")
                or token.get("asset_id")
                or token.get("id")
                or ""
            ).strip()
            if not token_id:
                continue
            if outcome == "yes" and not yes_token_id:
                yes_token_id = token_id
            if outcome == "no" and not no_token_id:
                no_token_id = token_id

    raw_token_id = str(raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or "").strip()
    raw_outcome = str(raw.get("outcome") or "").strip().lower()
    if raw_token_id and raw_outcome == "yes" and not yes_token_id:
        yes_token_id = raw_token_id
    if raw_token_id and raw_outcome == "no" and not no_token_id:
        no_token_id = raw_token_id

    return yes_token_id, no_token_id, json.dumps(tokens, default=str)
