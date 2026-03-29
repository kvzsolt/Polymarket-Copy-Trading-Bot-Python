"""Trader Discovery Service — find and rank leaders via polymarketanalytics.com.

Uses three API endpoints:
1. Leaderboard: /api/traders-tag-performance (ranked traders with PnL, win rate, etc.)
2. Trader lookup: same endpoint with searchQuery param
3. Positions: /api/traders-positions (per-trader position details)
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

from config.env import ENV
from utils.logger import get_logger

logger = get_logger("TraderDiscoveryService")

_BASE_URL = "https://polymarketanalytics.com/api"

_LEADERBOARD_PARAMS = (
    "tag=Overall&sortDirection=ASC&sortColumn=rank"
    "&minPnL=-11120197&maxPnL=22053933"
    "&minActivePositions=0&maxActivePositions=121003"
    "&minWinAmount=0&maxWinAmount=87105012"
    "&minLossAmount=-98225209&maxLossAmount=0"
    "&minWinRate=0&maxWinRate=200"
    "&minCurrentValue=0&maxCurrentValue=1000000000000"
    "&minTotalPositions=1&maxTotalPositions=123975"
)


def _fetch_json(url: str, timeout: int = 15) -> Any:
    """Synchronous JSON fetch with proper User-Agent."""
    req = urllib.request.Request(
        url=url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _is_eth_address(value: str) -> bool:
    return isinstance(value, str) and len(value) == 42 and value.startswith("0x")


@dataclass
class TraderProfile:
    """Rich trader profile from polymarketanalytics.com."""
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
    # Computed score
    composite_score: float = 0.0
    # Position details (loaded separately)
    positions: list[dict] = field(default_factory=list)
    discovered_at: float = field(default_factory=time.time)


@dataclass(frozen=True)
class RankedLeader:
    """Legacy-compatible ranked leader."""
    wallet: str
    score: float
    recent_trade_count: int
    recent_notional_usdc: float


class TraderDiscoveryService:
    """Discover and rank leaders from polymarketanalytics.com."""

    def __init__(self, leader_scorer=None):
        self.leader_scorer = leader_scorer
        self._cache: dict[str, TraderProfile] = {}
        self._last_discovery: float = 0.0

    def _score_trader(self, profile: TraderProfile) -> float:
        """Compute composite score from trader metrics.

        Formula: 0.30*roi + 0.20*win_rate + 0.20*drawdown_control
                 + 0.15*consistency + 0.15*active_quality
        """
        # ROI normalized (log scale for large PnL differences)
        import math
        roi_raw = profile.overall_gain
        if roi_raw > 0:
            roi_score = min(1.0, math.log10(1 + roi_raw) / 7.0)  # 10M → ~1.0
        else:
            roi_score = 0.0

        # Win rate (already 0-1 from API)
        wr = profile.win_rate
        if wr > 1.0:
            wr = wr / 100.0  # normalize if percentage
        wr_score = min(1.0, max(0.0, wr))

        # Drawdown control: loss_amount relative to win_amount
        if profile.win_amount > 0:
            loss_ratio = abs(profile.loss_amount) / profile.win_amount
            dd_score = max(0.0, 1.0 - loss_ratio)
        else:
            dd_score = 0.0

        # Consistency: number of events (more diverse = more consistent)
        consistency_score = min(1.0, profile.event_count / 100.0)

        # Active position quality: has active positions with value
        if profile.active_positions > 0 and profile.total_current_value > 0:
            active_score = min(1.0, math.log10(1 + profile.total_current_value) / 5.0)
        else:
            active_score = 0.2  # Small base score — inactive isn't terrible

        composite = (
            0.30 * roi_score
            + 0.20 * wr_score
            + 0.20 * dd_score
            + 0.15 * consistency_score
            + 0.15 * active_score
        )
        return max(0.0, min(1.0, composite))

    async def discover_candidates(
        self,
        *,
        min_pnl: float | None = None,
        min_win_rate: float | None = None,
        min_positions: int | None = None,
        max_results: int | None = None,
    ) -> list[TraderProfile]:
        """Fetch top traders from polymarketanalytics leaderboard."""
        min_pnl = min_pnl if min_pnl is not None else float(ENV.get("MIN_LEADER_PNL", 1000.0))
        min_win_rate = min_win_rate if min_win_rate is not None else float(ENV.get("MIN_LEADER_WIN_RATE", 0.55))
        min_positions = min_positions if min_positions is not None else int(ENV.get("MIN_LEADER_POSITIONS", 20))
        max_results = max_results if max_results is not None else int(ENV.get("DISCOVERY_MAX_RESULTS", 50))

        url = f"{_BASE_URL}/traders-tag-performance?{_LEADERBOARD_PARAMS}&limit={max_results}&offset=0"
        logger.info(f"Discovering traders from polymarketanalytics (limit={max_results})")

        try:
            data = await asyncio.get_event_loop().run_in_executor(None, _fetch_json, url)
        except Exception as exc:
            logger.error(f"Discovery fetch failed: {exc}")
            return []

        rows = data.get("data", []) if isinstance(data, dict) else []
        candidates: list[TraderProfile] = []

        for row in rows:
            wallet = str(row.get("trader", "")).strip().lower()
            if not _is_eth_address(wallet):
                continue

            profile = TraderProfile(
                wallet=wallet,
                name=str(row.get("trader_name", "")),
                overall_gain=float(row.get("overall_gain", 0) or 0),
                win_amount=float(row.get("win_amount", 0) or 0),
                loss_amount=float(row.get("loss_amount", 0) or 0),
                win_rate=float(row.get("win_rate", 0) or 0),
                total_positions=int(row.get("total_positions", 0) or 0),
                active_positions=int(row.get("active_positions", 0) or 0),
                event_count=int(row.get("event_ct", 0) or 0),
                win_count=int(row.get("win_count", 0) or 0),
                rank=int(row.get("rank", 0) or 0),
                total_current_value=float(row.get("total_current_value", 0) or 0),
                tags=str(row.get("trader_tags", "")),
            )

            # Apply filters
            if profile.overall_gain < min_pnl:
                continue
            effective_wr = profile.win_rate if profile.win_rate <= 1.0 else profile.win_rate / 100.0
            if effective_wr < min_win_rate:
                continue
            if profile.total_positions < min_positions:
                continue

            profile.composite_score = self._score_trader(profile)
            candidates.append(profile)
            self._cache[wallet] = profile

        candidates.sort(key=lambda p: p.composite_score, reverse=True)
        logger.info(f"Discovered {len(candidates)} eligible traders")
        return candidates

    async def score_candidate(self, wallet: str) -> Optional[TraderProfile]:
        """Lookup a specific trader by wallet address."""
        wallet = wallet.strip().lower()

        # Check cache first
        if wallet in self._cache:
            cached = self._cache[wallet]
            if time.time() - cached.discovered_at < 3600:
                return cached

        url = f"{_BASE_URL}/traders-tag-performance?tag=Overall&sortDirection=ASC&limit=1&offset=0&searchQuery={wallet}"
        try:
            data = await asyncio.get_event_loop().run_in_executor(None, _fetch_json, url)
        except Exception as exc:
            logger.warning(f"Trader lookup failed for {wallet[:12]}…: {exc}")
            return None

        rows = data.get("data", []) if isinstance(data, dict) else []
        if not rows:
            return None

        row = rows[0]
        profile = TraderProfile(
            wallet=wallet,
            name=str(row.get("trader_name", "")),
            overall_gain=float(row.get("overall_gain", 0) or 0),
            win_amount=float(row.get("win_amount", 0) or 0),
            loss_amount=float(row.get("loss_amount", 0) or 0),
            win_rate=float(row.get("win_rate", 0) or 0),
            total_positions=int(row.get("total_positions", 0) or 0),
            active_positions=int(row.get("active_positions", 0) or 0),
            event_count=int(row.get("event_ct", 0) or 0),
            win_count=int(row.get("win_count", 0) or 0),
            rank=int(row.get("rank", 0) or 0),
            total_current_value=float(row.get("total_current_value", 0) or 0),
            tags=str(row.get("trader_tags", "")),
        )
        profile.composite_score = self._score_trader(profile)
        self._cache[wallet] = profile
        return profile

    async def get_trader_positions(self, wallet: str) -> list[dict]:
        """Get detailed position data for a trader."""
        wallet = wallet.strip().lower()
        url = f"{_BASE_URL}/traders-positions?trader_id={wallet}"
        try:
            data = await asyncio.get_event_loop().run_in_executor(None, _fetch_json, url)
        except Exception as exc:
            logger.warning(f"Position fetch failed for {wallet[:12]}…: {exc}")
            return []

        rows = data.get("data", []) if isinstance(data, dict) else []
        return rows

    async def get_eligible_leaders(self, min_score: float = 0.45) -> list[TraderProfile]:
        """Get already-discovered leaders above score threshold."""
        eligible = [
            p for p in self._cache.values()
            if p.composite_score >= min_score
        ]
        eligible.sort(key=lambda p: p.composite_score, reverse=True)
        return eligible

    def get_cached_profile(self, wallet: str) -> Optional[TraderProfile]:
        """Get cached profile without API call (for scoring integration)."""
        return self._cache.get(wallet.strip().lower())

    async def persist_discovered(self) -> None:
        """Save discovered leaders to MongoDB."""
        mongo_uri = ENV.get("MONGO_URI", "")
        if not mongo_uri or not self._cache:
            return

        try:
            from models.userHistory import get_db
            db = get_db()
            coll = db["discovered_leaders"]

            for wallet, profile in self._cache.items():
                coll.update_one(
                    {"wallet": wallet},
                    {"$set": {
                        "wallet": wallet,
                        "name": profile.name,
                        "overall_gain": profile.overall_gain,
                        "win_amount": profile.win_amount,
                        "loss_amount": profile.loss_amount,
                        "win_rate": profile.win_rate,
                        "total_positions": profile.total_positions,
                        "active_positions": profile.active_positions,
                        "event_count": profile.event_count,
                        "win_count": profile.win_count,
                        "rank": profile.rank,
                        "total_current_value": profile.total_current_value,
                        "tags": profile.tags,
                        "composite_score": profile.composite_score,
                        "updated_at": time.time(),
                    }},
                    upsert=True,
                )
            logger.info(f"Persisted {len(self._cache)} discovered leaders to MongoDB")
        except Exception as exc:
            logger.warning(f"Failed to persist discovered leaders: {exc}")

    # ── Legacy-compatible API ──

    async def discover_ranked_leaders(self, *, limit: int = 200) -> list[RankedLeader]:
        """Legacy API — wraps new discovery."""
        if not ENV.get("AUTO_DISCOVER_LEADERS", False) and not ENV.get("ENABLE_TRADER_DISCOVERY", False):
            return []

        candidates = await self.discover_candidates(max_results=limit)
        return [
            RankedLeader(
                wallet=p.wallet,
                score=p.composite_score,
                recent_trade_count=p.total_positions,
                recent_notional_usdc=p.overall_gain,
            )
            for p in candidates
        ]

    async def resolve_leaders(self, configured: list[str]) -> list[str]:
        """Merge configured leaders with discovered ones."""
        configured_clean = [w for w in configured if _is_eth_address(w)]
        if not ENV.get("AUTO_DISCOVER_LEADERS", False) and not ENV.get("ENABLE_TRADER_DISCOVERY", False):
            return configured_clean

        discovered = await self.discover_ranked_leaders()
        auto = [r.wallet for r in discovered]
        merged = list(dict.fromkeys(configured_clean + auto))
        if not merged:
            return configured_clean

        min_score = float(ENV.get("AUTO_DISCOVER_MIN_SCORE", 0.45) or 0.45)
        top_n = int(ENV.get("AUTO_DISCOVER_TOP_N", 5) or 5)
        keep = set(configured_clean)
        for item in discovered:
            if item.score >= min_score and len(keep) < (len(configured_clean) + top_n):
                keep.add(item.wallet)
        return [w for w in merged if w in keep]


async def discovery_background_loop(discovery: TraderDiscoveryService) -> None:
    """Background loop that runs discovery periodically."""
    interval = int(ENV.get("DISCOVERY_INTERVAL_SECONDS", 3600) or 3600)
    enabled = ENV.get("ENABLE_TRADER_DISCOVERY", False)

    if not enabled:
        logger.info("Trader discovery disabled")
        return

    logger.info(f"Trader discovery started: interval={interval}s")

    while True:
        try:
            candidates = await discovery.discover_candidates()
            if candidates:
                await discovery.persist_discovered()
                top3 = candidates[:3]
                logger.info(
                    f"Top 3 discovered: "
                    + ", ".join(
                        f"{p.name or p.wallet[:12]}… (score={p.composite_score:.2f}, "
                        f"PnL=${p.overall_gain:,.0f}, WR={p.win_rate:.0%})"
                        for p in top3
                    )
                )
        except Exception as exc:
            logger.error(f"Discovery loop error: {exc}")

        await asyncio.sleep(interval)
