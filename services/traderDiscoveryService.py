"""Falcon + Polymarket discovery service.

Discovery is DB-only: it scores candidate wallets and stores evaluations.
It does not mutate runtime env files and does not start live monitoring.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from config.app_config import AppConfig
from services.discovery.candidate_scoring import (
    FalconDiscoveryEvaluation,
    falcon_quality_score,
)
from utils.logger import get_logger

logger = get_logger("TraderDiscoveryService")


@dataclass
class TraderProfile:
    wallet: str
    name: str = ""
    overall_gain: float = 0.0
    win_amount: float = 0.0
    loss_amount: float = 0.0
    win_rate: float = 0.0
    total_positions: int = 0
    active_positions: int = 0
    event_count: int = 0
    win_count: int = 0
    rank: int = 0
    total_current_value: float = 0.0
    tags: str = ""
    composite_score: float = 0.0
    positions: list[dict] = field(default_factory=list)
    discovered_at: float = field(default_factory=time.time)


@dataclass(frozen=True)
class RankedLeader:
    wallet: str
    score: float
    recent_trade_count: int
    recent_notional_usdc: float


class TraderDiscoveryService:
    def __init__(
        self,
        leader_scorer=None,
        *,
        config: AppConfig | None = None,
        falcon_candidates=None,
        history_backfill=None,
        discovery_repo=None,
        evaluation_repo=None,
        grouper=None,
        classifier=None,
        replay_service=None,
        scoring_service=None,
        recent_state_service=None,
        trust_store=None,
        logger_instance=None,
    ):
        self.leader_scorer = leader_scorer
        self.config = config
        self.falcon_candidates = falcon_candidates
        self.history_backfill = history_backfill
        self.discovery_repo = discovery_repo
        self.evaluation_repo = evaluation_repo or discovery_repo
        self.grouper = grouper
        self.classifier = classifier
        self.replay_service = replay_service
        self.scoring_service = scoring_service
        self.recent_state_service = recent_state_service
        self.trust_store = trust_store
        self.logger = logger_instance or logger
        self._cache: dict[str, TraderProfile] = {}

    async def discover_candidates(self, **_ignored: Any) -> list[TraderProfile]:
        if not self._fully_wired():
            self.logger.info("discovery_disabled_or_unwired")
            return []

        if not self.config.discovery.falcon_api_token:
            self.logger.warning("falcon_discovery_skipped_missing_token")
            return []

        candidates = await self.falcon_candidates.fetch_candidates()
        profiles: list[TraderProfile] = []

        for cand in candidates:
            try:
                profile = await self._evaluate_one(cand)
            except Exception as exc:
                wallet = str(getattr(cand, "wallet", "") or "")
                self.logger.warning(
                    "candidate_eval_failed | wallet=%s error_type=%s error=%r",
                    wallet,
                    type(exc).__name__,
                    exc,
                    exc_info=True,
                    extra={
                        "wallet": wallet,
                        "error_type": type(exc).__name__,
                        "error": repr(exc),
                    },
                )
                await self._upsert_failed_evaluation(
                    cand,
                    "candidate_eval_exception",
                    exc,
                )
                continue
            if profile is not None:
                profiles.append(profile)
                self._cache[profile.wallet.lower()] = profile

        profiles.sort(key=lambda p: (-p.composite_score, p.wallet))
        return profiles

    async def _evaluate_one(self, cand) -> TraderProfile | None:
        wallet = str(getattr(cand, "wallet", "") or "").lower()
        self.logger.info("candidate_eval_started", extra={"wallet": wallet})

        try:
            backfill = await self.history_backfill.backfill_wallet(wallet)
        except Exception as exc:
            self.logger.warning(
                "candidate_backfill_failed | wallet=%s error_type=%s error=%r",
                wallet,
                type(exc).__name__,
                exc,
                exc_info=True,
                extra={
                    "wallet": wallet,
                    "error_type": type(exc).__name__,
                    "error": repr(exc),
                },
            )
            await self._upsert_failed_evaluation(cand, "backfill_failed", exc)
            return None

        self.logger.info(
            "candidate_backfill_complete",
            extra={
                "wallet": wallet,
                "fetched": backfill.fetched,
                "inserted": backfill.inserted,
                "skipped": backfill.skipped,
                "errors": getattr(backfill, "errors", 0),
                "earliest_ts": backfill.earliest_ts,
                "latest_ts": backfill.latest_ts,
            },
        )

        trades = await self.discovery_repo.list_trades(
            wallet,
            limit=int(self.config.discovery.discovery_history_trade_limit),
        )
        self.logger.info(
            "candidate_trades_loaded",
            extra={"wallet": wallet, "trade_count": len(trades)},
        )
        if not trades:
            reason = (
                "backfill_failed"
                if int(getattr(backfill, "errors", 0) or 0) > 0
                else "insufficient_history"
            )
            self.logger.info(
                "candidate_insufficient_history",
                extra={"wallet": wallet, "reason": reason},
            )
            await self._upsert_failed_evaluation(cand, reason)
            return None

        signals = self.grouper.group(trades)
        self.logger.info(
            "candidate_signals_grouped",
            extra={"wallet": wallet, "signal_count": len(signals)},
        )

        market_classes = []
        for signal in signals:
            market = await self.discovery_repo.get_market(signal.condition_id)
            market_classes.append(self.classifier.classify_signal(signal, market))

        replay = await self.replay_service.replay_wallet(wallet, signals)
        self.logger.info(
            "candidate_replay_complete",
            extra={
                "wallet": wallet,
                "meaningful_signals": replay.meaningful_signals,
                "copyable_signals": replay.copyable_signals,
                "copyable_signal_ratio": replay.copyable_signal_ratio,
                "copyable_notional_ratio": replay.copyable_notional_ratio,
            },
        )
        recent_state = await self._fetch_recent_state(cand)
        evaluation = await self.scoring_service.evaluate(
            candidate=cand,
            signals=signals,
            market_classes=market_classes,
            replay=replay,
            recent_state=recent_state,
        )
        self.logger.info(
            "candidate_evaluation_complete",
            extra={
                "wallet": wallet,
                "status": evaluation.status,
                "eligible_live": evaluation.eligible_live,
                "eligible_shadow": evaluation.eligible_shadow,
                "final_score": evaluation.final_score,
                "rejection_reason": evaluation.rejection_reason,
            },
        )
        await self.evaluation_repo.upsert_evaluation(evaluation)
        if self.trust_store is not None:
            self.trust_store.upsert_evaluation(evaluation)

        return TraderProfile(
            wallet=wallet,
            composite_score=evaluation.final_score,
            total_positions=replay.meaningful_signals,
            active_positions=replay.copyable_signals,
            overall_gain=float(cand.pnl_15d),
            win_rate=float(cand.win_rate_15d),
            event_count=len(signals),
            total_current_value=float(replay.copyable_target_notional_sum),
            tags=evaluation.status,
        )

    async def _upsert_failed_evaluation(
        self,
        cand,
        reason: str,
        error: Exception | None = None,
    ) -> None:
        if self.evaluation_repo is None:
            return

        wallet = str(getattr(cand, "wallet", "") or "").lower()
        raw = getattr(cand, "raw", {}) or {}
        if not isinstance(raw, dict):
            raw = {"raw": raw}

        replay_json = {"reason": reason}
        if error is not None:
            replay_json.update(
                {
                    "error": repr(error),
                    "error_type": type(error).__name__,
                }
            )

        evaluation = FalconDiscoveryEvaluation(
            wallet=wallet,
            status="rejected",
            final_score=0.0,
            falcon_quality_score=falcon_quality_score(cand) if cand is not None else 0.0,
            copyability_score=0.0,
            recent_state_score=0.0,
            market_fit_score=0.0,
            h_score=float(getattr(cand, "h_score", 0.0) or 0.0),
            roi_15d=float(getattr(cand, "roi_15d", 0.0) or 0.0),
            win_rate_15d=float(getattr(cand, "win_rate_15d", 0.0) or 0.0),
            sharpe_15d=float(getattr(cand, "sharpe_15d", 0.0) or 0.0),
            pnl_15d=float(getattr(cand, "pnl_15d", 0.0) or 0.0),
            total_trades_15d=int(getattr(cand, "total_trades_15d", 0) or 0),
            markets_traded_15d=int(getattr(cand, "markets_traded_15d", 0) or 0),
            trajectory=str(getattr(cand, "trajectory", "unknown") or "unknown"),
            total_grouped_signals=0,
            meaningful_signals=0,
            copyable_signals=0,
            copyable_signal_ratio=0.0,
            copyable_notional_ratio=0.0,
            sports_esports_ratio=0.0,
            fast_crypto_signal_ratio=0.0,
            fast_crypto_notional_ratio=0.0,
            pnl_7d=Decimal("0"),
            pnl_3d=Decimal("0"),
            rejection_reason=reason,
            eligible_live=False,
            eligible_shadow=False,
            falcon_raw_json=raw,
            replay_json=replay_json,
        )
        try:
            await self.evaluation_repo.upsert_evaluation(evaluation)
            if self.trust_store is not None:
                self.trust_store.upsert_evaluation(evaluation)
        except Exception as exc:
            self.logger.warning(
                "candidate_failed_evaluation_upsert_failed | wallet=%s reason=%s error_type=%s error=%r",
                wallet,
                reason,
                type(exc).__name__,
                exc,
                exc_info=True,
                extra={
                    "wallet": wallet,
                    "reason": reason,
                    "error_type": type(exc).__name__,
                    "error": repr(exc),
                },
            )

    def get_cached_profile(self, wallet: str) -> TraderProfile | None:
        return self._cache.get(str(wallet).lower())

    async def discover_ranked_leaders(self, limit: int = 50) -> list[RankedLeader]:
        profiles = await self.discover_candidates()
        ranked: list[RankedLeader] = []
        for profile in profiles[:limit]:
            ranked.append(
                RankedLeader(
                    wallet=profile.wallet,
                    score=profile.composite_score,
                    recent_trade_count=profile.total_positions,
                    recent_notional_usdc=float(
                        Decimal(str(profile.total_current_value or 0))
                    ),
                )
            )
        return ranked

    def _fully_wired(self) -> bool:
        return all(
            dep is not None
            for dep in (
                self.config,
                self.falcon_candidates,
                self.history_backfill,
                self.discovery_repo,
                self.evaluation_repo,
                self.grouper,
                self.classifier,
                self.replay_service,
                self.scoring_service,
            )
        )

    async def _fetch_recent_state(self, cand):
        if self.recent_state_service is None:
            return None
        try:
            return await self.recent_state_service.fetch_recent_state(cand.wallet)
        except Exception as exc:
            self.logger.warning(
                "candidate_recent_state_failed",
                extra={"wallet": cand.wallet, "error": repr(exc)},
            )
            return None
