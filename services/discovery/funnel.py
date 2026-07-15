from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Protocol


from integrations.falcon.client import (
    FalconClient,
    hscore_rows,
    market_insights,
    row_value,
    social_pulse,
    wallet360,
    wallet_trades,
)
from services.discovery.longitudinal import longitudinal_rank
from services.discovery.models import Candidate, discovery_config_hash
from services.discovery.scoring import CompositeScorer, as_float, win_rate_consistency
from utils.logger import get_logger, redact_error_detail

logger = get_logger("TraderDiscovery")


class DiscoveryRepository(Protocol):
    async def begin_cycle(self, config_hash: str) -> str: ...

    async def persist_cycle(
        self,
        cycle_id: str,
        candidates: list[Candidate],
        config_hash: str,
    ) -> None: ...

    async def complete_cycle(
        self,
        cycle_id: str,
        *,
        status: str,
        candidate_count: int,
        kept_count: int,
        error: str | None = None,
    ) -> None: ...

    async def score_history(self, wallet: str, *, since_ts: float) -> list[Mapping[str, Any]]: ...

    async def latest_candidates(self) -> list[Candidate]: ...

    async def persist_rescore(
        self,
        candidates: list[Candidate],
        config_hash: str,
    ) -> None: ...


class DiscoveryFunnel:
    """H-Score -> Wallet360 -> trades -> markets -> optional social -> rank."""

    def __init__(
        self,
        client: FalconClient,
        config: Any,
        scorer: CompositeScorer,
        repository: DiscoveryRepository,
        backfill_service: Any | None = None,
    ) -> None:
        self.client = client
        self.config = config
        self.scorer = scorer
        self.repository = repository
        self.backfill_service = backfill_service
        self.config_hash = discovery_config_hash(config)

    async def run(self) -> list[Candidate]:
        cycle_id = await self.repository.begin_cycle(self.config_hash)
        candidates: list[Candidate] = []
        try:
            rows = await hscore_rows(self.client, self.config.shortlist)
            for row in rows:
                wallet = str(
                    row_value(
                        row,
                        "proxy_wallet",
                        "proxyWallet",
                        "wallet",
                        "address",
                        default="",
                    )
                ).strip().lower()
                if _is_wallet(wallet):
                    candidates.append(Candidate(wallet=wallet, hscore=dict(row)))

            if not candidates:
                raise RuntimeError("Heisenberg H-Score returned no valid wallets")

            semaphore = asyncio.Semaphore(self.config.scheduler.evaluation_concurrency)
            await asyncio.gather(
                *(self._evaluate(candidate, semaphore) for candidate in candidates)
            )
            evaluation_errors = sum(
                str(candidate.rejected_reason or "").startswith("evaluation_error:")
                for candidate in candidates
            )
            if (
                evaluation_errors / len(candidates)
                > self.config.scheduler.max_evaluation_error_ratio
            ):
                # Keep the rejected snapshots for diagnosis, but fail the
                # cycle so LeaderRegistry never interprets an agent outage as
                # evidence that every active wallet deserves demotion.
                await self.repository.persist_cycle(
                    cycle_id,
                    candidates,
                    self.config_hash,
                )
                raise RuntimeError(
                    "candidate evaluation error ratio exceeded configured limit"
                )
            kept = [candidate for candidate in candidates if candidate.rejected_reason is None]
            kept.sort(key=lambda candidate: (-candidate.base_score, candidate.wallet))
            if self.backfill_service is not None:
                backfill_limit = self.config.longitudinal.backfill_candidates_per_cycle
                await asyncio.gather(
                    *(
                        self.backfill_service.ensure_wallet(candidate.wallet)
                        for candidate in kept[:backfill_limit]
                    )
                )
            await asyncio.gather(
                *(self._apply_longitudinal(candidate) for candidate in kept)
            )
            kept.sort(key=lambda candidate: (-candidate.score, candidate.wallet))
            await self.repository.persist_cycle(
                cycle_id,
                candidates,
                self.config_hash,
            )
            await self.repository.complete_cycle(
                cycle_id,
                status="completed",
                candidate_count=len(candidates),
                kept_count=len(kept),
            )
            return kept
        except Exception as exc:
            await self.repository.complete_cycle(
                cycle_id,
                status="failed",
                candidate_count=len(candidates),
                kept_count=0,
                error=redact_error_detail(exc),
            )
            raise

    async def discover_candidates(self, limit: int | None = None) -> list[Candidate]:
        """Compatibility-neutral public name used by CLI and startup wiring."""
        candidates = await self.run()
        return candidates if limit is None else candidates[: max(0, int(limit))]

    async def rescore(self) -> list[Candidate]:
        """Re-rank the latest accepted roster without external API calls."""
        candidates = await self.repository.latest_candidates()
        for candidate in candidates:
            await self._apply_longitudinal(candidate)
        candidates.sort(key=lambda candidate: (-candidate.score, candidate.wallet))
        await self.repository.persist_rescore(candidates, self.config_hash)
        return candidates

    async def _evaluate(
        self,
        candidate: Candidate,
        semaphore: asyncio.Semaphore,
    ) -> None:
        async with semaphore:
            try:
                snapshots = await asyncio.gather(
                    *(
                        wallet360(self.client, candidate.wallet, window)
                        for window in self.config.wallet360.windows
                    )
                )
                if any(not snapshot for snapshot in snapshots):
                    raise ValueError("Wallet360 returned an empty window")
                candidate.wallet360 = dict(zip(self.config.wallet360.windows, snapshots))
                if rejection := self._audit_wallet360(candidate):
                    candidate.rejected_reason = rejection
                    return

                now = int(time.time())
                candidate.recent_trades = await wallet_trades(
                    self.client,
                    candidate.wallet,
                    start_time=now - self.config.recency.lookback_hours * 3600,
                    end_time=now,
                    page_size=self.config.recency.page_size,
                    max_pages=self.config.recency.max_pages,
                )
                if rejection := self._audit_recency(candidate, now=now):
                    candidate.rejected_reason = rejection
                    return

                candidate.market_quality = await self._market_gate(candidate)
                if not candidate.market_quality.get("ok", False):
                    candidate.rejected_reason = "market_quality"
                    return

                social = self.config.social
                if social.enabled and social.keywords:
                    candidate.social = await social_pulse(
                        self.client,
                        social.keywords,
                        social.hours_back,
                    )
                    if not candidate.social:
                        raise ValueError("Social Pulse returned no payload")
                    if as_float(row_value(candidate.social, "acceleration", default=0.0)) < social.min_acceleration:
                        candidate.rejected_reason = "weak_social_acceleration"
                        return
                    diversity = as_float(
                        row_value(
                            candidate.social,
                            "author_diversity_pct",
                            "author_diversity",
                            default=0.0,
                        )
                    )
                    if diversity < social.min_author_diversity_pct:
                        candidate.rejected_reason = "weak_social_diversity"
                        return

                self.scorer.score(candidate, self.config)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "candidate_evaluation_failed",
                    extra={
                        "wallet": candidate.wallet,
                        "error": redact_error_detail(exc),
                    },
                )
                candidate.rejected_reason = f"evaluation_error:{type(exc).__name__}"

    def _audit_wallet360(self, candidate: Candidate) -> str | None:
        config = self.config.wallet360
        window_15 = candidate.wallet360.get("15", {})
        if config.reject_bot_flagged and _as_bool(
            row_value(window_15, "bot_flag", "is_bot", "botFlag", default=False)
        ):
            return "bot_flagged"
        drawdown = as_float(
            row_value(window_15, "max_drawdown_pct", "max_drawdown", default=100.0),
            100.0,
        )
        if drawdown > config.max_drawdown_pct:
            return "drawdown"
        diversity = int(
            as_float(
                row_value(
                    window_15,
                    "market_diversity",
                    "markets_traded",
                    "unique_markets",
                    default=0,
                )
            )
        )
        if diversity < config.min_market_diversity:
            return "low_diversity"
        stake_variation = as_float(
            row_value(window_15, "bet_size_cv", "stake_cv", "size_cv", default=99.0),
            99.0,
        )
        if stake_variation > config.max_stake_cv:
            return "erratic_sizing"
        rates = [
            as_float(
                row_value(candidate.wallet360.get(window, {}), "win_rate", "win_rate_pct")
            )
            for window in config.windows
        ]
        if win_rate_consistency(rates) < config.min_win_rate_consistency:
            return "inconsistent_win_rate"
        return None

    def _audit_recency(self, candidate: Candidate, *, now: int) -> str | None:
        if len(candidate.recent_trades) < self.config.recency.min_trades_lookback:
            return "insufficient_recent_trades"
        timestamps = [
            _timestamp(
                row_value(
                    trade,
                    "timestamp",
                    "event_ts",
                    "created_at",
                    "matched_at",
                    default=0,
                )
            )
            for trade in candidate.recent_trades
        ]
        last_trade = max(timestamps, default=0.0)
        if last_trade <= 0:
            return "missing_trade_timestamp"
        if now - last_trade > self.config.recency.max_hours_since_last_trade * 3600:
            return "stale_activity"
        return None

    async def _market_gate(self, candidate: Candidate) -> dict[str, Any]:
        condition_ids: list[str] = []
        for trade in sorted(
            candidate.recent_trades,
            key=lambda row: _timestamp(
                row_value(row, "timestamp", "event_ts", "created_at", default=0)
            ),
            reverse=True,
        ):
            condition_id = str(
                row_value(trade, "condition_id", "conditionId", "market", default="")
            ).strip()
            if condition_id and condition_id not in condition_ids:
                condition_ids.append(condition_id)
            if len(condition_ids) >= self.config.market.max_markets_per_candidate:
                break
        if not condition_ids:
            return {"ok": False, "pass_ratio": 0.0, "markets": []}

        rows = await asyncio.gather(
            *(
                market_insights(self.client, condition_id, self.config.market)
                for condition_id in condition_ids
            )
        )
        passed = 0
        details: list[dict[str, Any]] = []
        for condition_id, row in zip(condition_ids, rows):
            if not row:
                raise ValueError(
                    f"Market Insights returned no payload for {condition_id}"
                )
            volume = as_float(row_value(row, "volume_24h", "volume24h", default=0.0))
            liquidity = as_float(
                row_value(row, "liquidity_percentile", "liquidity_pctile", default=0.0)
            )
            top_wallet = as_float(
                row_value(row, "top1_wallet_pct", "top_wallet_pct", default=100.0),
                100.0,
            )
            unique_traders = int(
                as_float(
                    row_value(row, "unique_traders_7d", "traders_7d", default=0)
                )
            )
            ok = (
                volume >= self.config.market.min_volume_24h
                and liquidity >= self.config.market.min_liquidity_percentile
                and top_wallet <= self.config.market.max_top1_wallet_pct
                and unique_traders >= self.config.market.min_unique_traders_7d
            )
            passed += int(ok)
            details.append(
                {
                    "condition_id": condition_id,
                    "ok": ok,
                    "volume_24h": volume,
                    "liquidity_percentile": liquidity,
                    "top1_wallet_pct": top_wallet,
                    "unique_traders_7d": unique_traders,
                    "raw": row,
                }
            )
        ratio = passed / len(details)
        return {
            "ok": ratio >= self.config.market.min_pass_ratio,
            "pass_ratio": ratio,
            "markets": details,
        }

    async def _apply_longitudinal(self, candidate: Candidate) -> None:
        config = self.config.longitudinal
        if not config.enabled:
            return
        history = await self.repository.score_history(
            candidate.wallet,
            since_ts=time.time() - config.history_days * 86400,
        )
        samples = list(history)
        samples.append({"ts": time.time(), **candidate.metrics})
        candidate.longitudinal_score = longitudinal_rank(samples, config=config)
        candidate.score = (
            config.base_score_weight * candidate.base_score
            + config.longitudinal_score_weight * candidate.longitudinal_score
        )


def _is_wallet(value: str) -> bool:
    return len(value) == 42 and value.startswith("0x") and all(
        character in "0123456789abcdef" for character in value[2:]
    )


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def _timestamp(value: Any) -> float:
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed / 1000.0 if parsed > 1_000_000_000_000 else parsed
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        try:
            parsed = float(value)
            return parsed / 1000.0 if parsed > 1_000_000_000_000 else parsed
        except ValueError:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return 0.0
    return 0.0


__all__ = ["DiscoveryFunnel", "DiscoveryRepository"]
