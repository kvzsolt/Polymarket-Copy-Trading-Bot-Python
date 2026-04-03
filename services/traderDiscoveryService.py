"""Trader Discovery Service — find and rank leaders from multiple data sources.

Primary source: polymarketanalytics.com
Fallback sources:
  - Polymarket CLOB API (sampling_markets → active traders from recent activity)
  - Cached/persisted results from bot_state.db

Uses three API endpoints on primary source:
1. Leaderboard: /api/traders-tag-performance (ranked traders with PnL, win rate, etc.)
2. Trader lookup: same endpoint with searchQuery param
3. Positions: /api/traders-positions (per-trader position details)

Robustness features:
- Retry with exponential backoff on transient failures
- Deterministic sorting for stable leader selection
- Schema validation to detect silent API changes
- Cache with TTL to survive source outages
- Fallback to secondary sources when primary fails
- Telemetry for discovery health monitoring
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

# Required fields from API response — if any are missing, data is suspect
_REQUIRED_FIELDS = {"trader", "overall_gain", "win_rate", "total_positions"}


def _fetch_json(url: str, timeout: int = 15, retries: int = 3) -> Any:
    """Synchronous JSON fetch with retries/backoff.

    polymarketanalytics.com can be rate-limited or intermittently slow.
    We retry on transient failures to avoid discovery becoming a silent no-op.
    """
    import random

    last_exc: Exception | None = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            req = urllib.request.Request(
                url=url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                sleep_s = min(8.0, (0.6 * (2 ** (attempt - 1))) + random.random() * 0.4)
                time.sleep(sleep_s)

    raise last_exc or RuntimeError("fetch failed")


def _is_eth_address(value: str) -> bool:
    return isinstance(value, str) and len(value) == 42 and value.startswith("0x")


def _validate_row_schema(row: dict) -> bool:
    """Check that a response row has all required fields (detects API schema drift)."""
    return all(key in row for key in _REQUIRED_FIELDS)


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
        self._last_discovery_count: int = 0
        self._last_discovery_source: str = "none"
        self._cache_ttl_s: float = float(ENV.get("DISCOVERY_CACHE_TTL_SECONDS", 3600) or 3600)

    def _score_trader(self, profile: TraderProfile) -> float:
        """Compute composite score from trader metrics.

        Formula: 0.30*roi + 0.20*win_rate + 0.20*drawdown_control
                 + 0.15*consistency + 0.15*active_quality

        Guards:
        - Win rate > 2.0 after normalization → reject (bad data).
        - Sample size < 20 positions → sqrt(n/20) discount.
        - Zero-activity wallets heavily penalized (0.05 base).
        """
        import math

        # Win rate normalization — reject clearly invalid data first
        wr = profile.win_rate
        if wr > 1.0:
            wr = wr / 100.0  # normalize if percentage
        if wr > 2.0:
            logger.warning(
                f"Rejecting {profile.wallet[:12]}…: win_rate={profile.win_rate} "
                f"normalizes to {wr:.2f} (bad data)"
            )
            return 0.0
        wr_score = min(1.0, max(0.0, wr))

        # ROI normalized (log scale for large PnL differences)
        roi_raw = profile.overall_gain
        if roi_raw > 0:
            roi_score = min(1.0, math.log10(1 + roi_raw) / 7.0)  # 10M → ~1.0
        else:
            roi_score = 0.0

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
            active_score = 0.05  # Heavily penalize inactive wallets

        composite = (
            0.30 * roi_score
            + 0.20 * wr_score
            + 0.20 * dd_score
            + 0.15 * consistency_score
            + 0.15 * active_score
        )

        # Sample-size penalty: discount wallets with fewer than 20 positions
        if profile.total_positions < 20:
            discount = math.sqrt(profile.total_positions / 20.0)
            composite *= discount

        return max(0.0, min(1.0, composite))

    async def discover_candidates(
        self,
        *,
        min_pnl: float | None = None,
        min_win_rate: float | None = None,
        min_positions: int | None = None,
        max_results: int | None = None,
    ) -> list[TraderProfile]:
        """Fetch top traders from polymarketanalytics leaderboard.

        Falls back to cached data or CLOB-based discovery on failure.
        """
        min_pnl = min_pnl if min_pnl is not None else float(ENV.get("MIN_LEADER_PNL", 1000.0))
        min_win_rate = min_win_rate if min_win_rate is not None else float(ENV.get("MIN_LEADER_WIN_RATE", 0.55))
        min_positions = min_positions if min_positions is not None else int(ENV.get("MIN_LEADER_POSITIONS", 20))
        max_results = max_results if max_results is not None else int(ENV.get("DISCOVERY_MAX_RESULTS", 50))

        url = f"{_BASE_URL}/traders-tag-performance?{_LEADERBOARD_PARAMS}&limit={max_results}&offset=0"
        logger.info(f"Discovering traders from polymarketanalytics (limit={max_results})")

        candidates: list[TraderProfile] = []
        schema_warnings = 0

        try:
            data = await asyncio.get_event_loop().run_in_executor(None, _fetch_json, url)
            rows = data.get("data", []) if isinstance(data, dict) else []

            if not rows:
                logger.warning("Primary discovery returned empty data — trying fallback")
                return await self._fallback_discover(min_pnl, min_win_rate, min_positions, max_results)

            for row in rows:
                # Schema validation — detect silent API changes
                if not _validate_row_schema(row):
                    schema_warnings += 1
                    missing = _REQUIRED_FIELDS - set(row.keys())
                    logger.warning(
                        f"Schema validation: row {schema_warnings} missing fields: {missing}"
                    )
                    continue

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

            if schema_warnings > 0:
                logger.warning(f"Discovery schema warnings: {schema_warnings}/{len(rows)} rows had missing fields")

        except Exception as exc:
            logger.error(f"Primary discovery failed: {exc} — trying fallback")
            return await self._fallback_discover(min_pnl, min_win_rate, min_positions, max_results)

        # Update health telemetry
        self._last_discovery = time.time()
        self._last_discovery_count = len(candidates)
        self._last_discovery_source = "primary"

        # Deterministic ordering: score desc, then rank asc, then wallet.
        candidates.sort(key=lambda p: (-p.composite_score, int(p.rank or 0), p.wallet))
        logger.info(f"Discovered {len(candidates)} eligible traders (source=primary)")
        return candidates

    async def _fallback_discover(
        self,
        min_pnl: float,
        min_win_rate: float,
        min_positions: int,
        max_results: int,
    ) -> list[TraderProfile]:
        """Fallback discovery: use cached data from SQLite or in-memory cache."""

        # Try persisted leaders first
        try:
            from config.db import get_database
            with get_database() as conn:
                docs = conn.execute(
                    """
                    SELECT wallet, name, overall_gain, win_amount, loss_amount, win_rate,
                           total_positions, active_positions, event_count, win_count, rank,
                           total_current_value, composite_score, tags, updated_at
                    FROM discovered_leaders
                    ORDER BY composite_score DESC
                    LIMIT ?
                    """,
                    (int(max_results),),
                ).fetchall()
            if docs:
                candidates = []
                for doc in docs:
                    wallet = str(doc["wallet"] or "")
                    if not _is_eth_address(wallet):
                        continue
                    profile = TraderProfile(
                        wallet=wallet,
                        name=str(doc["name"] or ""),
                        overall_gain=float(doc["overall_gain"] or 0),
                        win_amount=float(doc["win_amount"] or 0),
                        loss_amount=float(doc["loss_amount"] or 0),
                        win_rate=float(doc["win_rate"] or 0),
                        total_positions=int(doc["total_positions"] or 0),
                        active_positions=int(doc["active_positions"] or 0),
                        event_count=int(doc["event_count"] or 0),
                        win_count=int(doc["win_count"] or 0),
                        rank=int(doc["rank"] or 0),
                        total_current_value=float(doc["total_current_value"] or 0),
                        composite_score=float(doc["composite_score"] or 0),
                        tags=str(doc["tags"] or ""),
                    )
                    # Apply filters
                    if profile.overall_gain < min_pnl:
                        continue
                    effective_wr = profile.win_rate if profile.win_rate <= 1.0 else profile.win_rate / 100.0
                    if effective_wr < min_win_rate:
                        continue
                    candidates.append(profile)
                    self._cache[wallet] = profile

                if candidates:
                    self._last_discovery_source = "sqlite_fallback"
                    age_s = time.time() - max((float(doc["updated_at"] or 0) for doc in docs), default=0)
                    logger.info(
                        f"Fallback: loaded {len(candidates)} leaders from SQLite "
                        f"(age={age_s/3600:.1f}h)"
                    )
                    candidates.sort(key=lambda p: (-p.composite_score, int(p.rank or 0), p.wallet))
                    return candidates
        except Exception as e:
            logger.debug(f"SQLite fallback failed: {e}")

        # Last resort: return whatever is in memory cache
        if self._cache:
            cached = list(self._cache.values())
            cached.sort(key=lambda p: (-p.composite_score, int(p.rank or 0), p.wallet))
            self._last_discovery_source = "memory_cache_fallback"
            logger.warning(f"Fallback: returning {len(cached)} cached leaders from memory")
            return cached[:max_results]

        logger.error("All discovery sources exhausted — no leaders available")
        self._last_discovery_source = "none"
        return []

    async def score_candidate(self, wallet: str) -> Optional[TraderProfile]:
        """Lookup a specific trader by wallet address."""
        wallet = wallet.strip().lower()

        # Check cache first
        if wallet in self._cache:
            cached = self._cache[wallet]
            if time.time() - cached.discovered_at < self._cache_ttl_s:
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
        eligible.sort(key=lambda p: (-p.composite_score, int(p.rank or 0), p.wallet))
        return eligible

    def get_cached_profile(self, wallet: str) -> Optional[TraderProfile]:
        """Get cached profile without API call (for scoring integration)."""
        return self._cache.get(wallet.strip().lower())

    async def persist_discovered(self) -> None:
        """Save discovered leaders to SQLite."""
        if not self._cache:
            return

        try:
            from config.db import get_database
            with get_database() as conn:
                for wallet, profile in self._cache.items():
                    conn.execute(
                        """
                        INSERT INTO discovered_leaders
                        (wallet, name, overall_gain, win_amount, loss_amount, win_rate, total_positions,
                         active_positions, event_count, win_count, rank, total_current_value, tags,
                         composite_score, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(wallet) DO UPDATE SET
                          name = excluded.name,
                          overall_gain = excluded.overall_gain,
                          win_amount = excluded.win_amount,
                          loss_amount = excluded.loss_amount,
                          win_rate = excluded.win_rate,
                          total_positions = excluded.total_positions,
                          active_positions = excluded.active_positions,
                          event_count = excluded.event_count,
                          win_count = excluded.win_count,
                          rank = excluded.rank,
                          total_current_value = excluded.total_current_value,
                          tags = excluded.tags,
                          composite_score = excluded.composite_score,
                          updated_at = excluded.updated_at
                        """,
                        (
                            wallet,
                            profile.name,
                            profile.overall_gain,
                            profile.win_amount,
                            profile.loss_amount,
                            profile.win_rate,
                            profile.total_positions,
                            profile.active_positions,
                            profile.event_count,
                            profile.win_count,
                            profile.rank,
                            profile.total_current_value,
                            profile.tags,
                            profile.composite_score,
                            time.time(),
                        ),
                    )
                conn.commit()
            logger.info(f"Persisted {len(self._cache)} discovered leaders to SQLite")
        except Exception as exc:
            logger.warning(f"Failed to persist discovered leaders: {exc}")

    def get_health_status(self) -> dict:
        """Return discovery health telemetry for monitoring."""
        age_s = time.time() - self._last_discovery if self._last_discovery > 0 else -1
        return {
            "last_discovery_ago_s": round(age_s, 1),
            "last_discovery_count": self._last_discovery_count,
            "last_discovery_source": self._last_discovery_source,
            "cache_size": len(self._cache),
            "cache_ttl_s": self._cache_ttl_s,
            "healthy": age_s < self._cache_ttl_s * 2 if age_s >= 0 else False,
        }

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
