"""Risk monitor — background task that checks open positions for advanced exit triggers."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections import OrderedDict
from collections.abc import Sized
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from config.app_config import AppConfig
    from domain.trading_context import TradingContext
    from services.healthServer import ReadinessTracker

from services.metrics import metrics
from services.executionService import ExecutionRequest
from services.positionActionCoordinator import PositionScope
from services.risk.risk_engine import active_policy
from storage.postgres.fills_repo import FillRecord
from utils import is_valid_clob_token_id
from integrations.polymarket.models import PRICE_TOKEN_NOT_FOUND
from utils.fetchData import fetch_data
from utils.logger import get_logger

# ---------------------------------------------------------------------------
# Venue circuit breaker — suppresses price polls during exchange outages.
# ---------------------------------------------------------------------------
_DEFAULT_CB_THRESHOLD = 3  # consecutive failures to open
_DEFAULT_CB_COOLDOWN = 60.0  # seconds before half-open retry


class VenueCircuitBreaker:
    """Tracks consecutive 404 / venue-unavailable failures across all tokens.

    When the threshold is exceeded the breaker opens and all price fetches
    are suppressed for *cooldown* seconds, preventing log spam and wasted
    API calls during a Polymarket outage.
    """

    def __init__(
        self,
        *,
        threshold: int = _DEFAULT_CB_THRESHOLD,
        cooldown: float = _DEFAULT_CB_COOLDOWN,
    ) -> None:
        self._threshold = max(1, threshold)
        self._cooldown = max(5.0, cooldown)
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> str:
        """``closed`` | ``open`` | ``half_open``"""
        if self._opened_at is None:
            return "closed"
        if time.time() - self._opened_at >= self._cooldown:
            return "half_open"
        return "open"

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._threshold and self._opened_at is None:
            self._opened_at = time.time()
            logger.warning(
                "venue_circuit_breaker_opened | failures=%d cooldown=%.0fs",
                self._consecutive_failures,
                self._cooldown,
            )

    def should_suppress(self) -> bool:
        """Return *True* when the breaker is open (polls should be skipped)."""
        state = self.state
        if state == "closed":
            return False
        if state == "half_open":
            return False  # allow one probe
        return True  # open

    def reset(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None


logger = get_logger("RiskMonitor")

_INSUFFICIENT_BALANCE_PATTERNS = (
    "insufficient_balance",
    "insufficient balance",
    "not enough balance",
    "allowance",
    # ``redact_error_detail`` maps venue balance text to this category, so an
    # already-redacted error string must still register as a drift signal.
    "insufficient_funds",
)


def _is_drift_error(msg: str) -> bool:
    lower = msg.lower()
    return any(p in lower for p in _INSUFFICIENT_BALANCE_PATTERNS)


def _risk_exit_signal_id(
    pos: Any,
    *,
    reason: str,
    position_manager: Any | None = None,
) -> str:
    """Build a stable idempotency key for a protective risk-exit SELL.

    C3 — the key must be stable across retries for the same position so the
    submission idempotency lock can collapse a duplicate dispatch against an
    in-flight prior POST (previously ``time.time_ns()`` produced a fresh key
    every tick, defeating dedup and causing oversell/phantom-prune storms).

    The key is ``risk:{reason}:{condition_id}:{token_id}:{epoch}`` where
    ``epoch`` is the in-memory tracked position's ``entry_time`` (which resets
    on re-entry, so a freshly re-entered position is not dedup-suppressed by
    the prior position's one-shot trim). When the tracked position is absent
    (e.g. fresh boot before rehydrate) the epoch falls back to a stable hash
    of immutable DB fields — never the wall clock.
    """
    condition_id = str(getattr(pos, "condition_id", "") or "")
    token_id = str(getattr(pos, "token_id", "") or "")
    epoch: Any = None
    if position_manager is not None:
        get_position = getattr(position_manager, "get_position", None)
        if callable(get_position):
            try:
                tracked = get_position(condition_id, token_id)
            except TypeError:
                try:
                    tracked = get_position(condition_id)
                except Exception:
                    tracked = None
            except Exception:
                tracked = None
            if tracked is not None:
                epoch = getattr(tracked, "entry_time", None)
    if epoch is None:
        # Stable fallback: hash immutable DB position fields so the same
        # position yields the same key across ticks even without the in-memory
        # tracked position.
        leader_id = str(getattr(pos, "leader_id", "") or "")
        avg_entry = str(getattr(pos, "avg_entry_price", "") or "")
        size_tokens = str(getattr(pos, "size_tokens", "") or "")
        epoch = _stable_hash(f"{condition_id}|{token_id}|{leader_id}|{avg_entry}|{size_tokens}")
    return f"risk:{reason}:{condition_id}:{token_id}:{epoch}"


def _stable_hash(value: str) -> str:
    import hashlib

    return hashlib.sha1(value.encode("utf-8"), usedforsecurity=False).hexdigest()[:16]


async def _fetch_live_risk_position_size(
    context: TradingContext,
    *,
    token_id: str,
) -> Decimal | None:
    wallet = str(getattr(context, "wallet_address", "") or "").strip().lower()
    if not wallet:
        return None
    try:
        positions = await fetch_data(
            f"https://data-api.polymarket.com/positions?user={wallet}",
            silent=True,
        )
    except Exception:
        return None
    live_size = Decimal("0")
    if isinstance(positions, list):
        for position in positions:
            if not isinstance(position, dict):
                continue
            live_token_id = str(
                position.get("asset") or position.get("tokenID") or position.get("token_id") or ""
            ).strip()
            if live_token_id != token_id:
                continue
            try:
                live_size = Decimal(str(position.get("size") or "0"))
            except Exception:
                live_size = Decimal("0")
            break
    return live_size


# Per-context "already warned about this token" set lives in context.extra
# under this key. Module-level state would leak warning suppression across
# users in multi-user runtimes and grow unbounded.
_WARNED_INVALID_TOKENS_KEY = "warned_invalid_tokens"
_DEFAULT_RISK_PRICE_SEMAPHORE = 8
_MIN_RISK_CACHE_STALE_SECONDS = 0.75
# Dead / illiquid markets return ``price=0`` on every probe. Cache that
# state with this TTL so the risk loop stops pounding ``/price`` for
# positions the venue isn't quoting. A real fill via user WSS evicts the
# entry (clients/user_ws.py), so genuine reactivation re-arms the probe.
_DORMANT_TOKEN_TTL_SECONDS = 300.0

_RISK_PRE_RECONCILE_BACKOFF_KEY = "risk_pre_reconcile_backoff"
# Must exceed the worst case of ONE healthy-but-slow venue order-status GET:
# the first aiohttp attempt can burn its full 10s total timeout, and
# ``_async_signed_get`` then retries once (idempotent GET) after 0.15s, which
# can burn another full 10s — ~20.15s end to end. The previous 12.0s default
# undercounted the retry as "retry delay" (0.15s) instead of a second full
# request, so the outer guard cancelled every in-flight reconcile whenever the
# venue was slow and live-spammed "Risk pre-reconcile TIMEOUT ... after 12.0s".
_DEFAULT_RISK_PRE_RECONCILE_TIMEOUT_SECONDS = 25.0
_DEFAULT_RISK_PRE_RECONCILE_BACKOFF_SECONDS = 30.0
_MAX_RISK_PRE_RECONCILE_BACKOFF_SECONDS = 300.0

# Cooldown after a terminal (rejected/cancelled) risk-exit submission. The
# near-max/stop-loss decision is stateless — it re-fires whenever the live
# price still satisfies the trigger — so without a cooldown a doomed SELL is
# resubmitted at the risk-loop cadence (observed 2026-07-04: 1 Hz for ~3 min
# against a phantom position, plus one risk_triggers spam row per attempt).
# 0 disables (legacy behavior). Transient failures are NOT suppressed.
_DEFAULT_RISK_EXIT_REJECT_COOLDOWN_SECONDS = 60.0
_RISK_EXIT_REJECT_COOLDOWN_KEY = "risk_exit_reject_cooldown_until"


@dataclass
class RiskPreReconcileBackoff:
    consecutive_timeouts: int = 0
    next_attempt_at: float = 0.0


@dataclass
class CachedRiskPrice:
    price: Decimal
    size: Decimal | None
    fetched_at: float
    # When set, the token last responded with a zero / unquotable price and
    # the refresh path should skip it until ``fetched_at + dormant_ttl``.
    dormant_ttl: float = 0.0

    @property
    def is_dormant(self) -> bool:
        return self.dormant_ttl > 0.0


class RiskPriceCache:
    # Issue #99: bound the per-process price cache to prevent unbounded
    # memory growth on a long-running bot. Oldest-inserted entries are
    # evicted first; active tokens are unaffected because each set() bumps
    # them back to the MRU end.
    def __init__(self, *, max_size: int | None = None) -> None:
        if max_size is None:
            max_size = 5000
        self._max_size = max_size
        self._entries: OrderedDict[str, CachedRiskPrice] = OrderedDict()

    def get_fresh(
        self, token_id: str, *, stale_after: float, now: float | None = None
    ) -> CachedRiskPrice | None:
        entry = self._entries.get(token_id)
        if entry is None:
            return None
        current_ts = now if now is not None else time.time()
        # Dormant entries are intentionally hidden from sizing callers — a
        # zero price is not a tradeable quote. Refresh path uses
        # ``is_dormant_fresh`` to gate re-probes separately.
        if entry.is_dormant:
            return None
        if current_ts - entry.fetched_at > stale_after:
            return None
        # Touch on hit: cold tokens drift toward the eviction edge.
        self._entries.move_to_end(token_id)
        return entry

    def is_dormant_fresh(self, token_id: str, *, now: float | None = None) -> bool:
        """True when ``token_id`` is in the dormant window (skip refresh)."""
        entry = self._entries.get(token_id)
        if entry is None or not entry.is_dormant:
            return False
        current_ts = now if now is not None else time.time()
        return (current_ts - entry.fetched_at) <= entry.dormant_ttl

    def set(
        self,
        token_id: str,
        *,
        price: Decimal,
        size: Decimal | None = None,
        fetched_at: float | None = None,
    ) -> None:
        self._entries[token_id] = CachedRiskPrice(
            price=price,
            size=size,
            fetched_at=fetched_at if fetched_at is not None else time.time(),
        )
        self._entries.move_to_end(token_id)
        while len(self._entries) > self._max_size:
            self._entries.popitem(last=False)

    def mark_dormant(
        self,
        token_id: str,
        *,
        ttl: float = _DORMANT_TOKEN_TTL_SECONDS,
        fetched_at: float | None = None,
    ) -> None:
        """Record a zero-price response so the refresh loop skips this token
        until ``fetched_at + ttl``. Sizing callers (``get_fresh``) still see
        no quote — zero price is not a tradeable bid.
        """
        self._entries[token_id] = CachedRiskPrice(
            price=Decimal("0"),
            size=None,
            fetched_at=fetched_at if fetched_at is not None else time.time(),
            dormant_ttl=max(0.0, float(ttl)),
        )
        self._entries.move_to_end(token_id)
        while len(self._entries) > self._max_size:
            self._entries.popitem(last=False)

    def evict(self, token_id: str) -> None:
        """Remove ``token_id`` from the cache. Used by WSS fill hooks to
        re-arm probing on a previously dormant market that just saw activity.
        """
        self._entries.pop(token_id, None)


def _risk_cache_stale_after(refresh_interval: float) -> float:
    return max(_MIN_RISK_CACHE_STALE_SECONDS, float(refresh_interval) * 3.0)


def _ensure_context_venue_circuit_breaker(
    context: TradingContext,
) -> VenueCircuitBreaker:
    breaker = context.extra.get("venue_circuit_breaker")
    if isinstance(breaker, VenueCircuitBreaker):
        return breaker
    breaker = VenueCircuitBreaker()
    context.extra["venue_circuit_breaker"] = breaker
    return breaker


async def _fetch_and_store_price(
    client,
    token_id: str,
    cache: RiskPriceCache,
    semaphore: asyncio.Semaphore,
    *,
    circuit_breaker: VenueCircuitBreaker | None = None,
    market_store: Any | None = None,
    condition_id: str | None = None,
) -> None:
    if circuit_breaker is not None and circuit_breaker.should_suppress():
        return
    # Dormant gate: skip HTTP entirely when this token returned ``price=0``
    # within the dormant TTL. Covers ``_get_context_cached_price`` (called
    # per-position by the stop-loss loop) which would otherwise drive a
    # per-tick ``/price`` call for every dead position because ``get_fresh``
    # hides the dormant entry, forcing a re-fetch.
    if cache.is_dormant_fresh(token_id):
        return
    async with semaphore:
        try:
            price_data = await client.get_price(token_id, "SELL")
        except Exception:
            if circuit_breaker is not None:
                circuit_breaker.record_failure()
            raise

    # Terminal sentinel: venue answered 404 ("No orderbook exists for the
    # requested token id" per Polymarket OpenAPI) → token retired / market
    # resolved. Mark the market_store so every future risk tick short-circuits
    # via ``_is_token_resolved``. Do NOT trip the circuit breaker — 404 is
    # market state, not venue health. Distinct from 5xx (handled below).
    if price_data is PRICE_TOKEN_NOT_FOUND:
        if market_store is not None:
            try:
                market_store.mark_resolved(token_id, condition_id)
            except Exception:
                logger.debug("market_store_mark_resolved_failed", exc_info=True)
        return

    # Breaker semantics: only HTTP / venue-availability errors count as
    # failures. ``client.get_price`` swallows HTTP errors and returns None,
    # so a None / malformed payload is the real signal of a venue problem.
    # A valid HTTP 200 with ``price=0`` (no resting bid on the SELL side —
    # normal on settled / illiquid markets) is a healthy venue response and
    # must NOT trip the breaker, otherwise a single dead-market position
    # floods logs with open/close oscillation until it's manually closed.
    if not price_data or price_data.get("price") is None:
        if circuit_breaker is not None:
            circuit_breaker.record_failure()
        return

    try:
        price = Decimal(str(price_data["price"]).strip())
    except Exception:
        if circuit_breaker is not None:
            circuit_breaker.record_failure()
        return

    # HTTP succeeded with parseable price → venue is healthy regardless of
    # whether the price is zero. Record success before the value gate so the
    # breaker reflects venue state, not market state.
    if circuit_breaker is not None:
        circuit_breaker.record_success()

    if price <= 0:
        # Park this token: dead-market or no-bid state cached for
        # _DORMANT_TOKEN_TTL_SECONDS so the refresh loop stops asking. A WSS
        # fill on this token evicts the entry, re-arming the probe.
        cache.mark_dormant(token_id)
        return

    size_raw = price_data.get("size")
    size: Decimal | None = None
    if size_raw is not None:
        try:
            size = Decimal(str(size_raw).strip())
        except Exception:
            size = None

    cache.set(token_id, price=price, size=size)


async def _refresh_price_cache_for_tokens(
    client,
    token_ids: list[str],
    cache: RiskPriceCache,
    *,
    max_concurrency: int = _DEFAULT_RISK_PRICE_SEMAPHORE,
    circuit_breaker: VenueCircuitBreaker | None = None,
    market_store: Any | None = None,
    condition_lookup: dict[str, str] | None = None,
) -> None:
    """Refresh ``cache`` for every token in ``token_ids``.

    ``market_store`` + ``condition_lookup`` are optional plumbing that lets
    ``_fetch_and_store_price`` propagate a Polymarket 404 → terminal signal
    (``mark_resolved``) without re-querying the DB. Test fixtures that hand
    in a mock client + no real positions can leave both as ``None``.
    """
    unique_token_ids = [token_id for token_id in dict.fromkeys(token_ids) if token_id]
    if not unique_token_ids:
        return

    semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
    lookup = condition_lookup or {}
    await asyncio.gather(
        *[
            _fetch_and_store_price(
                client,
                token_id,
                cache,
                semaphore,
                circuit_breaker=circuit_breaker,
                market_store=market_store,
                condition_id=lookup.get(token_id),
            )
            for token_id in unique_token_ids
        ],
        return_exceptions=True,
    )


def _ensure_context_price_cache(context: TradingContext) -> RiskPriceCache:
    cache = context.extra.get("risk_price_cache")
    if isinstance(cache, RiskPriceCache):
        return cache
    # PR #105 review: prefer the typed cap from AppConfig.risk when a config
    # is in scope; the legacy env-driven default inside RiskPriceCache still
    # applies when no config is available.
    max_size: int | None = None
    config = getattr(context, "config", None)
    if config is not None:
        risk_cfg = getattr(config, "risk", None)
        if risk_cfg is not None:
            cap = getattr(risk_cfg, "risk_price_cache_max_size", None)
            if isinstance(cap, int) and cap > 0:
                max_size = cap
    cache = RiskPriceCache(max_size=max_size)
    context.extra["risk_price_cache"] = cache
    return cache


def _resolved_market_store(context: TradingContext):
    """Return the per-user ``MarketStateStore`` (if wired) so the risk monitor
    can short-circuit polls on resolved markets. Falls back to ``None`` when
    the runtime didn't attach a MarketDataRuntime (test fixtures, paper mode).

    The store carries authoritative resolved-state set by:
      - ``clients/market_ws.py::_handle_market_resolved`` (push, live)
      - ``orchestrators/runtime_bootstrap.py::hydrate_resolved_market_state``
        (REST gate, catches resolutions that happened while the bot was offline)
    """
    runtime = getattr(context, "market_data_runtime", None)
    if runtime is None:
        return None
    return getattr(runtime, "market_store", None)


def _is_token_resolved(context: TradingContext, token_id: str) -> bool:
    store = _resolved_market_store(context)
    if store is None:
        return False
    try:
        return bool(store.is_resolved(token_id))
    except Exception:
        return False


def _market_wss_live_for_token(
    context: TradingContext,
    token_id: str,
    *,
    max_silence_seconds: float,
) -> bool:
    """Return True when this token is subscribed and MarketWS is live.

    Risk pricing may reuse an unchanged WSS/rest-bootstrap quote while the
    stream is alive. A quiet esports book should not force repeated REST
    /price calls every risk tick.
    """
    runtime = getattr(context, "market_data_runtime", None)
    if runtime is None:
        return False

    subscribed = getattr(runtime, "_subscribed_tokens", None)
    if isinstance(subscribed, set) and str(token_id) not in subscribed:
        return False

    is_wss_down = getattr(runtime, "is_wss_down", None)
    if callable(is_wss_down):
        try:
            return not bool(is_wss_down(max_silence_seconds))
        except Exception:
            return False

    store = getattr(runtime, "market_store", None)
    age_fn = getattr(store, "wss_connection_age_seconds", None)
    if not callable(age_fn):
        return False
    try:
        age = age_fn()
    except Exception:
        return False
    return (
        isinstance(age, (int, float)) and not isinstance(age, bool) and age <= max_silence_seconds
    )


def _risk_wss_connection_max_silence(context: TradingContext) -> float:
    cfg = getattr(context, "config", None)
    market_cfg = getattr(cfg, "market_data", None) if cfg is not None else None
    value = getattr(market_cfg, "wss_connection_max_silence_seconds", 20.0)
    try:
        return max(0.0, float(value))
    except Exception:
        return 20.0


def _cache_market_store_risk_price(
    context: TradingContext,
    token_id: str,
    cache: RiskPriceCache,
    *,
    stale_after: float,
) -> Decimal | None:
    """Populate the risk cache from MarketStateStore when possible.

    Held long positions exit on SELL, so the risk mark should be the best bid.
    If the quote timestamp is old but the token is subscribed and MarketWS is
    live, the unchanged quote is still current enough for monitoring.
    """
    store = _resolved_market_store(context)
    if store is None:
        return None
    try:
        quote = store.get_quote(token_id)
    except Exception:
        return None
    if quote is None:
        return None

    quote_fresh = False
    try:
        quote_fresh = bool(quote.is_fresh(stale_after))
    except Exception:
        quote_fresh = False

    source = str(getattr(quote, "source", "") or "").lower()
    live_source = source in {"wss", "rest_bootstrap", "rest_snapshot"}
    if not quote_fresh and not (
        live_source
        and _market_wss_live_for_token(
            context,
            token_id,
            max_silence_seconds=_risk_wss_connection_max_silence(context),
        )
    ):
        return None

    price = getattr(quote, "best_bid", None)
    if price is None:
        return None
    try:
        price = Decimal(str(price))
    except Exception:
        return None
    if price <= 0:
        return None

    if not _risk_mark_passes_sanity(
        context,
        token_id=token_id,
        best_bid=price,
        best_ask=getattr(quote, "best_ask", None),
    ):
        return None

    cache.set(token_id, price=price)
    return price


def _risk_mark_max_spread_bps(context: TradingContext) -> float:
    cfg = getattr(context, "config", None)
    risk_cfg = getattr(cfg, "risk", None) if cfg is not None else None
    value = getattr(risk_cfg, "risk_mark_max_spread_bps", 2000)
    try:
        return max(0.0, float(value))
    except Exception:
        return 2000.0


def _risk_mark_max_abs_distance(context: TradingContext) -> float:
    cfg = getattr(context, "config", None)
    risk_cfg = getattr(cfg, "risk", None) if cfg is not None else None
    value = getattr(risk_cfg, "risk_mark_max_abs_distance", 0.0)
    try:
        return max(0.0, float(value))
    except Exception:
        return 0.0


def _risk_mark_passes_sanity(
    context: TradingContext,
    *,
    token_id: str,
    best_bid: Decimal,
    best_ask: Any,
) -> bool:
    """Reject risk marks from crossed or extremely wide books.

    The near-max/stop-loss triggers act directly on ``best_bid``; a corrupt or
    stale quote must not fire a protective SELL. A one-sided book (no ask) is
    accepted — rejecting it would blind risk monitoring on thin markets.
    Fail closed for the enabled exit features: a rejected mark means "no
    action this tick", never a sell.
    """
    if best_ask is None:
        return True
    try:
        ask = Decimal(str(best_ask))
    except Exception:
        return True
    if ask <= 0:
        return True
    reason = None
    if best_bid > ask:
        reason = "crossed_book"
    else:
        max_spread_bps = _risk_mark_max_spread_bps(context)
        if max_spread_bps > 0:
            mid = (best_bid + ask) / 2
            if mid > 0:
                spread_bps = float((ask - best_bid) / mid) * 10_000.0
                if spread_bps > max_spread_bps:
                    reason = "wide_spread"
        # Phase 2.8 — absolute bid/ask distance check. The bps-of-mid gate is
        # inert near 1.0 (bid >= 0.82 -> max spread < 2000 bps of mid), exactly
        # where near-max triggers live. This absolute price-point gap rejects
        # wide books near 1.0 that the bps gate misses. 0 disables.
        if reason is None:
            max_abs_distance = _risk_mark_max_abs_distance(context)
            if max_abs_distance > 0:
                abs_distance = float(ask - best_bid)
                if abs_distance >= max_abs_distance:
                    reason = "wide_abs_distance"
    if reason is None:
        return True
    if _should_log_active_reservation_skip(
        context,
        condition_id="risk_mark",
        token_id=token_id,
        reason=reason,
    ):
        logger.warning(
            "risk_mark_rejected | token=%s reason=%s best_bid=%s best_ask=%s",
            token_id[:16],
            reason,
            best_bid,
            ask,
        )
    return False


async def _refresh_context_price_cache(
    context: TradingContext,
    *,
    refresh_interval: float,
    max_concurrency: int = _DEFAULT_RISK_PRICE_SEMAPHORE,
) -> None:
    positions = list(await context.positions_repo.list_open_positions(context.user_id) or [])
    token_ids = [pos.token_id for pos in positions if is_valid_clob_token_id(pos.token_id)]
    if not token_ids:
        return
    # Build token → condition lookup so ``_fetch_and_store_price`` can
    # mark_resolved with the proper condition id on a 404 sentinel.
    condition_lookup = {
        str(pos.token_id): str(pos.condition_id or "")
        for pos in positions
        if is_valid_clob_token_id(pos.token_id)
    }
    cache = _ensure_context_price_cache(context)
    breaker = _ensure_context_venue_circuit_breaker(context)
    if breaker.should_suppress():
        return
    # Two-stage skip:
    #   1. Resolved markets — MarketStateStore.is_resolved == True. Set by
    #      WSS ``market_resolved`` push or the boot-time REST gate. Never
    #      polled again; the position is terminal until settlement/claim.
    #   2. Dormant tokens — zero-price cached within TTL (5min). Covers the
    #      "active market, no resting bid right now" case. WSS fill →
    #      ``cache.evict(token_id)`` re-arms the probe before the TTL expires.
    #   3. Fresh cached quotes — the risk loop already has a recent price;
    #      do not hit /price again just because the loop ticked.
    stale_after = _risk_cache_stale_after(refresh_interval)
    for tid in token_ids:
        if (
            not _is_token_resolved(context, tid)
            and not cache.is_dormant_fresh(tid)
            and cache.get_fresh(tid, stale_after=stale_after) is None
        ):
            _cache_market_store_risk_price(
                context,
                tid,
                cache,
                stale_after=stale_after,
            )
    refresh_ids = [
        tid
        for tid in token_ids
        if (
            not _is_token_resolved(context, tid)
            and not cache.is_dormant_fresh(tid)
            and cache.get_fresh(tid, stale_after=stale_after) is None
        )
    ]
    if not refresh_ids:
        return
    await _refresh_price_cache_for_tokens(
        context.client,
        refresh_ids,
        cache,
        max_concurrency=max_concurrency,
        circuit_breaker=breaker,
        market_store=_resolved_market_store(context),
        condition_lookup=condition_lookup,
    )


async def _get_context_cached_price(
    context: TradingContext,
    token_id: str,
    *,
    refresh_interval: float,
) -> Decimal | None:
    # Resolved markets never quote a tradeable price. Short-circuit ahead of
    # any HTTP fallback so the stop-loss loop stops calling /price for
    # terminal positions on every tick.
    if _is_token_resolved(context, token_id):
        return None
    cache = _ensure_context_price_cache(context)
    breaker = _ensure_context_venue_circuit_breaker(context)
    if breaker.should_suppress():
        return None
    stale_after = _risk_cache_stale_after(refresh_interval)
    entry = cache.get_fresh(token_id, stale_after=stale_after)
    if entry is not None:
        return entry.price
    market_price = _cache_market_store_risk_price(
        context,
        token_id,
        cache,
        stale_after=stale_after,
    )
    if market_price is not None:
        return market_price

    # Single-token refresh: look up the condition_id so the mark_resolved
    # path on a 404 sentinel can attach the correct market identifier.
    condition_lookup: dict[str, str] = {}
    try:
        positions = await context.positions_repo.list_open_positions(context.user_id)
        for pos in positions or []:
            if str(pos.token_id) == token_id:
                condition_lookup[token_id] = str(pos.condition_id or "")
                break
    except Exception:
        condition_lookup = {}

    await _refresh_price_cache_for_tokens(
        context.client,
        [token_id],
        cache,
        circuit_breaker=breaker,
        market_store=_resolved_market_store(context),
        condition_lookup=condition_lookup,
    )
    refreshed = cache.get_fresh(token_id, stale_after=stale_after)
    return refreshed.price if refreshed is not None else None


def _exit_monitor_enabled(config: AppConfig) -> bool:
    return bool(getattr(config.risk, "enable_risk_mgmt", False))


async def _dispatch_risk_exit(
    *,
    coordinator,
    execution_service,
    exec_req,
    user_id: str,
    condition_id: str,
    token_id: str,
    side: str,
    price: float,
    notional: float,
    source: str,
    leader_wallet: str | None,
    signal_id: str | None,
):
    """Single dispatch point for risk/protective SELL submissions.

    Routes through ExecutionCoordinator when wired (PR6); otherwise falls
    back to the low-level ExecutionService primitive via getattr alias to
    keep the coordinator-routing acceptance scan in PR6 clean.
    """
    if coordinator is not None:
        from decimal import Decimal as _D

        from services.execution.coordinator import ExecutionIntent

        intent = ExecutionIntent(
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
            side=side.upper(),
            size=_D(str(exec_req.amount)),
            price=_D(str(price or 0)),
            notional_usdc=_D(str(notional or 0)),
            idempotency_key=signal_id,
            source=source,
            signal_id=signal_id,
            leader_wallet=leader_wallet,
            max_slippage_bps=int(exec_req.max_slippage_bps),
            reason=exec_req.reason or source,
            recovery_metadata=exec_req.recovery_metadata,
        )
        if source == "protective_exit":
            coord_result = await coordinator.submit_protective_exit(intent)
        else:
            coord_result = await coordinator.submit_risk_exit(intent)
        if coord_result.raw_result is not None:
            return coord_result.raw_result
        from services.executionService import ExecutionResult as _ER
        from decimal import Decimal as _D2

        return _ER(
            accepted=coord_result.accepted,
            order_id=coord_result.order_id,
            status=coord_result.skip_code or coord_result.status or "rejected",
            requested_amount=_D2(str(exec_req.amount)),
            filled_amount=_D2("0"),
            avg_price=None,
            fees_paid=_D2("0"),
            error=coord_result.reason,
        )
    # Fallback path — coordinator missing. Live mode must fail closed.
    from services.execution.coordinator import is_live_mode

    cfg = getattr(execution_service, "_config", None)
    if is_live_mode(cfg):
        logger.error(
            "execution_coordinator_missing | risk path live mode requires coordinator | "
            "user=%s cond=%s token=%s side=%s source=%s",
            user_id,
            condition_id,
            token_id,
            side,
            source,
            extra={
                "type": "execution_coordinator_missing",
                "user_id": user_id,
                "condition_id": condition_id,
                "token_id": token_id,
                "side": side,
                "source": source,
            },
        )
        try:
            from services.metrics import (
                METRIC_COORDINATOR_MISSING_SKIP,
                metrics,
            )

            metrics.inc_sync(
                METRIC_COORDINATOR_MISSING_SKIP,
                labels={"side": side, "source": source},
            )
        except Exception:
            pass
        from services.executionService import ExecutionResult as _ER
        from decimal import Decimal as _D

        return _ER(
            accepted=False,
            order_id=None,
            status="execution_coordinator_missing",
            requested_amount=_D(str(exec_req.amount)),
            filled_amount=_D("0"),
            avg_price=None,
            fees_paid=_D("0"),
            error="ExecutionCoordinator not wired in live mode",
        )
    submitter = execution_service.execute
    return await submitter(exec_req)


async def rehydrate_position_manager(context: TradingContext) -> None:
    """Load persisted open positions into the context position manager."""
    pm = context.position_manager
    if not hasattr(pm, "track_buy"):
        return

    for pos in await context.positions_repo.list_open_positions(context.user_id):
        existing = None
        if hasattr(pm, "get_position"):
            try:
                existing = pm.get_position(pos.condition_id, pos.token_id)
            except Exception:
                existing = None
        if existing is not None:
            continue
        try:
            pm.track_buy(
                condition_id=pos.condition_id,
                asset=pos.token_id,
                entry_price=pos.avg_entry_price,
                size_tokens=pos.size_tokens,
                usdc_cost=pos.size_tokens * pos.avg_entry_price,
                leader_wallet=pos.leader_id or "",
            )
        except Exception:
            continue
        # Restore durable one-shot exit state. ``near_max_warn_done`` lives
        # in-memory on the tracked position; without this a restart re-arms
        # the 60% near-max trim and bleeds the position again on every boot.
        # A ``pending`` trigger row counts as done (crash between trigger and
        # fill-status update must not re-arm a sell); ``failed`` re-arms.
        try:
            triggers_repo = getattr(context, "risk_triggers_repo", None)
            if triggers_repo is not None and hasattr(pm, "get_position"):
                already_trimmed = await triggers_repo.has_trigger_for_reason(
                    user_id=context.user_id,
                    condition_id=pos.condition_id,
                    token_id=pos.token_id,
                    reason="near_max_partial_trim",
                )
                if already_trimmed:
                    tracked = pm.get_position(pos.condition_id, pos.token_id)
                    if tracked is not None:
                        tracked.exit_state.near_max_warn_done = True
        except Exception:
            logger.debug("near_max_warn_rehydrate_failed", exc_info=True)


def _fallback_risk_decision(
    entry_price: Decimal,
    current_price: Decimal,
) -> dict | None:
    """No implicit fallback exits. Advanced exit decisions live in PositionManager."""
    return None


def _extract_risk_condition_id(request: dict[str, object] | None) -> str | None:
    if not isinstance(request, dict):
        return None
    condition_id = str(request.get("condition_id") or "").strip()
    if condition_id:
        return condition_id
    signal_id = str(request.get("signal_id") or "").strip()
    if not signal_id.startswith("risk:"):
        return None
    parts = signal_id.split(":", 3)
    if len(parts) < 3:
        return None
    derived = str(parts[2] or "").strip()
    return derived or None


def _decimal_or_zero(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _matched_amount_from_payload(payload: dict[str, object] | None) -> Decimal:
    if not isinstance(payload, dict):
        return Decimal("0")
    return _decimal_or_zero(
        payload.get("size_matched")
        or payload.get("filled_size")
        or payload.get("matched_size")
        or payload.get("matched_amount")
        or payload.get("sizeMatched")
        or payload.get("filledSize")
        or payload.get("matchedSize")
        or payload.get("matchedAmount")
        or payload.get("size")
        or 0
    )


async def _active_sell_reserved_size(
    context: TradingContext,
    *,
    user_id: str,
    condition_id: str,
    token_id: str,
) -> Decimal:
    """Return active SELL reservations for this scoped position.

    An active SELL reservation means another protective/risk/copy exit already
    owns some or all free shares. The risk loop must not keep submitting fresh
    stop-loss intents against the same held shares while that reservation is
    waiting for reconciliation.
    """
    coordinator = getattr(context, "execution_coordinator", None)
    reservation_service = getattr(coordinator, "reservation_service", None)
    repo = getattr(coordinator, "reservations_repo", None)
    if repo is None:
        repo = getattr(reservation_service, "reservations_repo", None)
    if repo is None:
        repo = context.extra.get("reservations_repo")
    if reservation_service is None:
        reservation_service = context.extra.get("reservation_service")
    if getattr(reservation_service, "enabled", True) is False:
        return Decimal("0")
    list_active = getattr(repo, "list_active_for_scope", None)
    if not callable(list_active):
        return Decimal("0")

    try:
        lookup_result = list_active(user_id, condition_id, token_id)
        reservations = await lookup_result if inspect.isawaitable(lookup_result) else lookup_result
    except Exception:
        logger.debug("risk_active_reservation_lookup_failed", exc_info=True)
        return Decimal("0")

    if not isinstance(reservations, (list, tuple)):
        return Decimal("0")
    total = Decimal("0")
    for reservation in reservations:
        side = str(getattr(reservation, "side", "") or "").upper()
        status = str(getattr(reservation, "status", "") or "").lower()
        if side != "SELL" or status != "active":
            continue
        if _is_expired_orphan_reservation(reservation):
            release = getattr(reservation_service, "release_terminal", None)
            if callable(release):
                try:
                    release_result = release(
                        reservation.id,
                        reason="expired_orphan_before_risk_exit",
                    )
                    if inspect.isawaitable(release_result):
                        await release_result
                    logger.info(
                        "risk_released_expired_orphan_reservation | "
                        "reservation_id=%s cond=%s token=%s",
                        getattr(reservation, "id", None),
                        condition_id[:12],
                        token_id[:12],
                    )
                    continue
                except Exception:
                    logger.debug(
                        "risk_release_orphan_reservation_failed",
                        exc_info=True,
                    )
        total += _decimal_or_zero(getattr(reservation, "reserved_size", "0"))
    return total


def _is_expired_orphan_reservation(reservation: object) -> bool:
    if getattr(reservation, "order_id", None) or getattr(reservation, "venue_order_id", None):
        return False
    expires_at = getattr(reservation, "expires_at", None)
    if expires_at is None:
        return False
    now_ts = time.time()
    try:
        if hasattr(expires_at, "timestamp"):
            return float(expires_at.timestamp()) <= now_ts
        return float(expires_at) <= now_ts
    except Exception:
        return False


def _should_log_active_reservation_skip(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    reason: str,
    now: float | None = None,
    interval_seconds: float = 30.0,
) -> bool:
    current = time.time() if now is None else now
    key = f"{condition_id}:{token_id}:{reason}"
    state = context.extra.setdefault("risk_active_reservation_skip_logged_at", {})
    try:
        last = float(state.get(key, 0.0))
    except Exception:
        last = 0.0
    if current - last < interval_seconds:
        return False
    state[key] = current
    return True


async def _reap_expired_reservations_for_context(context: TradingContext) -> int:
    """Reconcile-and-release expired reservations during the runtime loop.

    Startup runs this once (``runtime_bootstrap``). Without an in-loop call a
    reservation stranded mid-session — e.g. a forced-exit FAK that returned
    ``submitted_unconfirmed`` and was kept active pending reconciliation — would
    never be reaped until the next restart, deadlocking the risk loop on
    ``risk_exit_already_reserved`` (the orphan-release path refuses any
    reservation carrying an order_id). ``release_expired_active`` only acts on
    reservations past their TTL and only releases when reconciliation confirms
    the linked order is venue-terminal, so a still-live order is never
    blind-released — duplicate-order / stale-lock protections are preserved.
    """
    coordinator = getattr(context, "execution_coordinator", None)
    reservation_service = getattr(coordinator, "reservation_service", None)
    if reservation_service is None:
        reservation_service = context.extra.get("reservation_service")
    if getattr(reservation_service, "enabled", True) is False:
        return 0
    release_expired = getattr(reservation_service, "release_expired_active", None)
    if not callable(release_expired):
        return 0
    try:
        release_result = release_expired()
        released = await release_result if inspect.isawaitable(release_result) else release_result
    except Exception:
        logger.debug("risk_reservation_reaper_failed", exc_info=True)
        return 0
    count = len(released) if isinstance(released, Sized) else 0
    if count:
        logger.info(
            "risk_reaped_expired_reservations | count=%d user=%s",
            count,
            context.user_id,
        )
    return count


async def _attempt_phantom_prune_on_no_fill(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    leader_id: str | None,
) -> dict:
    """Reconcile a forced-exit no-fill against live venue holdings and prune.

    Near-max / resolved markets return a terminal ``cancelled`` (not an
    insufficient-balance drift error), so a manually-sold or settled position
    would otherwise linger in the DB and re-fire the exit forever. This fetches
    live holdings; ``reconcile_position_drift`` deletes the local row only when
    the venue reports ~zero shares and no reservation is active (a real position
    with venue shares is never deleted). Returns the drift result dict (empty on
    failure).
    """
    try:
        from orchestrators.trade_pipeline import get_reconciliation_service

        drift_reconciler = get_reconciliation_service(context)

        async def _fetcher():
            return await _fetch_live_risk_position_size(context, token_id=token_id)

        result = await drift_reconciler.reconcile_position_drift(
            user_id=context.user_id,
            condition_id=condition_id,
            token_id=token_id,
            leader_id=leader_id or "risk-monitor",
            live_position_fetcher=_fetcher,
        )
        return result or {}
    except Exception:
        logger.debug("risk_drift_reconciliation_error", exc_info=True)
        return {}


def _should_attempt_no_fill_prune(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    now: float | None = None,
    interval_seconds: float = 30.0,
) -> bool:
    """Throttle live-holdings drift checks on repeated forced-exit no-fills.

    A real-but-illiquid position keeps returning no-fill every cycle; without a
    throttle we would hammer the venue holdings endpoint at the risk-loop
    cadence. One check per ``interval_seconds`` per scope is enough to catch a
    manual sell / resolution promptly while bounding REST load.
    """
    current = time.time() if now is None else now
    key = f"{condition_id}:{token_id}"
    state = context.extra.setdefault("risk_no_fill_prune_attempted_at", {})
    try:
        last = float(state.get(key, 0.0) or 0.0)
    except Exception:
        last = 0.0
    if current - last < interval_seconds:
        return False
    state[key] = current
    return True


def _risk_exit_reject_cooldown_seconds(context: TradingContext) -> float:
    """Resolve the terminal-exit cooldown: typed AppConfig first, env fallback.

    0 is a valid, meaningful value (operator opt-out to legacy behavior), so
    this does NOT go through ``_float_env_or_default`` (which maps <=0 to the
    default).
    """
    config = getattr(context, "config", None)
    if config is not None:
        risk_cfg = getattr(config, "risk", None)
        val = getattr(risk_cfg, "risk_exit_reject_cooldown_seconds", None)
        if isinstance(val, (int, float)) and float(val) >= 0.0:
            return float(val)
    return _DEFAULT_RISK_EXIT_REJECT_COOLDOWN_SECONDS


def _risk_exit_cooldown_active(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    now: float | None = None,
) -> bool:
    current = time.time() if now is None else now
    state = context.extra.get(_RISK_EXIT_REJECT_COOLDOWN_KEY)
    if not isinstance(state, dict):
        return False
    try:
        until = float(state.get(f"{condition_id}:{token_id}", 0.0) or 0.0)
    except Exception:
        return False
    return current < until


def _arm_risk_exit_reject_cooldown(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    now: float | None = None,
) -> None:
    """Suppress re-dispatch for this scope after a terminal exit failure.

    Keyed on ``ExecutionResult.status`` by the callers (rejected/cancelled),
    deliberately NOT on error-string classification, so the storm-stopper
    holds even if venue error classification regresses to an opaque shape.
    """
    seconds = _risk_exit_reject_cooldown_seconds(context)
    if seconds <= 0:
        return
    current = time.time() if now is None else now
    state = context.extra.setdefault(_RISK_EXIT_REJECT_COOLDOWN_KEY, {})
    state[f"{condition_id}:{token_id}"] = current + seconds


def _should_arm_risk_exit_reject_cooldown(status: object, error: object = None) -> bool:
    status_lower = str(status or "").lower()
    if status_lower in ("rejected", "cancelled"):
        return True
    if status_lower == "failed" and _is_drift_error(str(error or "")):
        return True
    return False


def _risk_exit_fully_closed_position(updated_pos: object | None) -> bool:
    """Return True only when the protective SELL fully closed the position.

    Partial fills are real and must still update fills/positions/PnL, but they
    must NOT activate a durable re-entry block.  The block represents a
    completed protective exit regime, not merely a partial de-risking step.
    """
    return updated_pos is None


async def reconcile_unconfirmed_risk_orders(
    context: TradingContext,
    *,
    skip_locked_scopes: bool = False,
) -> int:
    """Converge risk-monitor-originated submitted_unconfirmed SELLs.

    Issue #71 — per-record settlement delegates to ``ReconciliationService``,
    the canonical owner of terminal-state transitions and fill apply. This
    function still orchestrates the batch (filtering, scope locking) so that
    risk SELLs serialize against the trade pipeline and live risk-monitor
    actions on the same scoped position.

    ``skip_locked_scopes`` — the risk loop's pre-reconcile phase passes True
    so a record whose scope lock is held by another consumer (execute_trades
    submit/confirm, manual sync, standalone reconciler) is skipped instead of
    queued behind that holder's venue I/O inside the pre-reconcile ceiling;
    the holder converges the position, or the next tick retries the row.
    """
    if not context.execution_service:
        return 0

    from orchestrators.trade_pipeline import get_reconciliation_service

    records = await context.orders_repo.get_unconfirmed_order_submissions(
        user_id=context.user_id,
        side="SELL",
    )
    if not records:
        return 0

    reconciler = get_reconciliation_service(context)
    reconciled = 0

    for record in records:
        request = record.get("request") or {}
        signal_id = str(request.get("signal_id") or "")
        if not signal_id.startswith("risk:"):
            continue

        order_id = str(record.get("order_id") or "").strip()
        token_id = str(request.get("token_id") or "").strip()
        condition_id = _extract_risk_condition_id(request)
        if not order_id or not token_id or not condition_id or not is_valid_clob_token_id(token_id):
            continue

        scope = PositionScope(
            user_id=context.user_id,
            condition_id=condition_id,
            token_id=token_id,
        )
        if skip_locked_scopes and context.position_coordinator.is_locked(scope):
            logger.debug(
                "risk_reconcile_skip_locked_scope | order_id=%s",
                order_id,
            )
            continue
        async with context.position_coordinator.acquire(scope):
            applied = await reconciler.reconcile_risk_record(
                record,
                user_id=context.user_id,
                condition_id=condition_id,
                token_id=token_id,
                confirm_fill=reconciler.confirm_fill,
                pnl_tracker=context.pnl_tracker,
                get_position=context.positions_repo.get_position,
                logger=logger,
            )
            if applied:
                reconciled += 1

    return reconciled


def _risk_pre_reconcile_timeout_seconds(
    poll_interval: float,
    configured: float | None = None,
) -> float:
    return max(
        0.05,
        float(poll_interval or 0.0),
        float(
            configured if configured is not None else _DEFAULT_RISK_PRE_RECONCILE_TIMEOUT_SECONDS
        ),
    )


def _risk_pre_reconcile_backoff_base_seconds(
    poll_interval: float,
    timeout_seconds: float,
    configured: float | None = None,
) -> float:
    return max(
        float(poll_interval or 0.0),
        timeout_seconds,
        float(
            configured if configured is not None else _DEFAULT_RISK_PRE_RECONCILE_BACKOFF_SECONDS
        ),
    )


def _risk_pre_reconcile_state(context: TradingContext) -> RiskPreReconcileBackoff:
    extra = getattr(context, "extra", None)
    if not isinstance(extra, dict):
        return RiskPreReconcileBackoff()
    state = extra.get(_RISK_PRE_RECONCILE_BACKOFF_KEY)
    if not isinstance(state, RiskPreReconcileBackoff):
        state = RiskPreReconcileBackoff()
        extra[_RISK_PRE_RECONCILE_BACKOFF_KEY] = state
    return state


def _risk_pre_reconcile_is_due(context: TradingContext, *, now: float) -> bool:
    return _risk_pre_reconcile_state(context).next_attempt_at <= now


def _record_risk_pre_reconcile_success(context: TradingContext) -> None:
    state = _risk_pre_reconcile_state(context)
    state.consecutive_timeouts = 0
    state.next_attempt_at = 0.0


def _record_risk_pre_reconcile_timeout(
    context: TradingContext,
    *,
    now: float,
    base_backoff_seconds: float,
) -> float:
    state = _risk_pre_reconcile_state(context)
    state.consecutive_timeouts += 1
    multiplier = 2 ** min(state.consecutive_timeouts - 1, 3)
    backoff_seconds = min(
        base_backoff_seconds * multiplier,
        max(base_backoff_seconds, _MAX_RISK_PRE_RECONCILE_BACKOFF_SECONDS),
    )
    state.next_attempt_at = now + backoff_seconds
    return backoff_seconds


async def _pre_reconcile_for_context(context: TradingContext) -> int:
    """Reconcile this context's unconfirmed orders. Best-effort; never raises.

    Issue #29 — converge unconfirmed order state BEFORE position evaluation so
    the monitor never acts on stale positions: unconfirmed SELLs that already
    filled on-exchange would otherwise still look open (→ duplicate protective
    SELLs), and unconfirmed BUYs that filled would be invisible (→ unprotected).
    Returns the number of orders reconciled (0 on error).

    Hoisted into its own function so the risk loop can run it OUTSIDE the
    per-cycle protective-exit timeout budget — these are venue order-status
    round-trips and must not starve the stop-loss evaluation every cycle
    (sym 1: "Risk loop TIMEOUT ... context skipped this cycle").
    """
    if not getattr(context, "execution_service", None):
        return 0
    try:
        from orchestrators.trade_pipeline import reconcile_submitted_unconfirmed

        # skip_locked_scopes: this phase is best-effort and runs under the
        # pre-reconcile wait_for ceiling. Blocking on a scope lock held by an
        # in-flight execute_trades submit/confirm or manual sync (both do venue
        # round-trips inside the lock) burned the whole ceiling and produced
        # the recurring "Risk pre-reconcile TIMEOUT" ops spam; the lock holder
        # (or the next tick / standalone reconciler) converges skipped rows.
        trade_n = await reconcile_submitted_unconfirmed(context, skip_locked_scopes=True)
        risk_n = await reconcile_unconfirmed_risk_orders(context, skip_locked_scopes=True)
        total_n = trade_n + risk_n
        if total_n:
            logger.info(
                "risk_monitor_pre_reconcile | reconciled=%d unconfirmed orders (trade=%d risk=%d)",
                total_n,
                trade_n,
                risk_n,
            )
        return total_n
    except Exception as exc:
        logger.warning("risk_monitor_pre_reconcile_error | %s", repr(exc))
        return 0


async def run_risk_check_for_context(
    context: TradingContext, *, pre_reconcile: bool = True
) -> None:
    if not context.execution_service:
        return

    # Issue #29 — reconcile unconfirmed orders BEFORE rehydration / position
    # evaluation. Direct callers (and tests) keep this inline by default; the
    # risk loop passes ``pre_reconcile=False`` because it runs the reconcile in
    # its own untimed phase so slow venue round-trips never consume the
    # protective-exit timeout budget (sym 1).
    if pre_reconcile:
        await _pre_reconcile_for_context(context)

    await rehydrate_position_manager(context)
    slippage_bps = int(float(context.extra.get("slippage_tolerance_bps", 200)))
    refresh_interval = float(context.extra.get("risk_check_interval_seconds", 0.25) or 0.25)

    # Read enable_tiered from context extra (set by bootstrap from config)
    enable_tiered = str(context.extra.get("enable_balance_tiered_policy", "false")).lower() in (
        "1",
        "true",
        "yes",
    )

    warned_tokens: set[str] = context.extra.setdefault(_WARNED_INVALID_TOKENS_KEY, set())
    for pos in await context.positions_repo.list_open_positions(context.user_id):
        if not is_valid_clob_token_id(pos.token_id):
            if pos.token_id not in warned_tokens:
                warned_tokens.add(pos.token_id)
                logger.warning(
                    "Skipping position with invalid token_id=%r (condition=%s) — "
                    "likely stale placeholder; close or delete manually",
                    pos.token_id,
                    pos.condition_id,
                )
            continue
        # --- Per-scoped-position action coordination (issue #28) ---
        _scope = PositionScope(
            user_id=context.user_id,
            condition_id=pos.condition_id,
            token_id=pos.token_id,
        )
        if context.position_coordinator.is_locked(_scope):
            logger.info(
                "risk_context_scope_busy_skipped | user=%s condition=%s token=%s",
                context.user_id,
                pos.condition_id[:12],
                pos.token_id[:12],
            )
            continue
        async with context.position_coordinator.acquire(_scope):
            try:
                current_price = await _get_context_cached_price(
                    context,
                    pos.token_id,
                    refresh_interval=refresh_interval,
                )
                if current_price is None or current_price <= 0:
                    continue

                action = None
                if hasattr(context.position_manager, "evaluate_risk_action"):
                    try:
                        # Decimal-internal: evaluate_risk_action normalises
                        # any numeric input via ``_to_dec`` so callers no
                        # longer have to round-trip through float at the
                        # boundary (Phase 2 / CLAUDE.md no-float-money).
                        action = context.position_manager.evaluate_risk_action(
                            pos.condition_id,
                            pos.token_id,
                            current_price,
                        )
                    except TypeError:
                        action = context.position_manager.evaluate_risk_action(
                            pos.condition_id,
                            current_price,
                        )
                if not action:
                    action = _fallback_risk_decision(pos.avg_entry_price, current_price)
                if not action:
                    continue

                fraction = max(0.0, min(1.0, float(action.get("size_fraction", 1.0) or 1.0)))
                reason = str(action.get("reason") or "risk_exit")
                trigger_notional = float(
                    (Decimal(str(pos.size_tokens)) * Decimal(str(current_price))).quantize(
                        Decimal("0.00000001")
                    )
                )

                # Capture block metadata at trigger time — but defer persisting the
                # risk block until after the protective SELL actually fills.  Writing
                # the block before execution creates a durable re-entry ban even when
                # the SELL fails, which is the core issue #27 lifecycle bug.
                _block_metadata: dict[str, object] = {
                    "leader_notional_at_trigger": trigger_notional,
                    "copied_position_notional_at_trigger": trigger_notional,
                    "trigger_price": float(current_price),
                    "trigger_size_tokens": float(pos.size_tokens),
                    "trigger_fraction": fraction,
                }

                # Simplified account-cap policy
                if fraction < 1.0 and enable_tiered:
                    policy = active_policy()
                    if not policy.allow_partial_exits:
                        logger.info(
                            "Balance policy [%s] upgrades partial exit → full for %s (was %.0f%%)",
                            policy.name,
                            pos.condition_id[:12],
                            fraction * 100,
                        )
                        fraction = 1.0

                _block_metadata["trigger_fraction"] = fraction

                sell_size = (pos.size_tokens * Decimal(str(fraction))).quantize(
                    Decimal("0.00000001")
                )
                if sell_size <= 0:
                    continue

                active_reserved = await _active_sell_reserved_size(
                    context,
                    user_id=context.user_id,
                    condition_id=pos.condition_id,
                    token_id=pos.token_id,
                )
                if active_reserved > 0:
                    if _should_log_active_reservation_skip(
                        context,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                        reason=reason,
                    ):
                        logger.info(
                            "risk_exit_already_reserved | cond=%s token=%s reason=%s "
                            "reserved=%s requested=%s",
                            pos.condition_id[:12],
                            pos.token_id[:12],
                            reason,
                            active_reserved,
                            sell_size,
                        )
                    continue

                if _risk_exit_cooldown_active(
                    context,
                    condition_id=pos.condition_id,
                    token_id=pos.token_id,
                ):
                    # Recent terminal rejection for this scope — skip before
                    # record_trigger so the DB is not spammed either.
                    logger.debug(
                        "risk_exit_reject_cooldown_active | cond=%s token=%s reason=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                    )
                    continue

                _trigger_id: int | None = None
                if context.risk_triggers_repo is not None:
                    try:
                        _trigger_id = await context.risk_triggers_repo.record_trigger(
                            user_id=context.user_id,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                            reason=reason,
                            source="risk_monitor",
                            fill_status="pending",
                            metadata=_block_metadata,
                        )
                    except Exception:
                        logger.debug("risk_trigger_record_error", exc_info=True)

                req = ExecutionRequest(
                    user_id=context.user_id,
                    leader_id=pos.leader_id or "risk-monitor",
                    token_id=pos.token_id,
                    side="SELL",
                    amount=sell_size,
                    max_slippage_bps=slippage_bps,
                    reason=reason,
                    signal_id=_risk_exit_signal_id(
                        pos,
                        reason=reason,
                        position_manager=getattr(context, "position_manager", None),
                    ),
                    condition_id=pos.condition_id,
                    recovery_metadata=_block_metadata,
                )
                # C4 — shield the protective-exit POST from the outer
                # ``wait_for`` ceiling so a loop timeout cannot cancel the
                # venue POST mid-flight (which previously left a live venue
                # order with no local record, then C3 re-fired a duplicate).
                # The shielded task completes its POST + persistence even if
                # the parent ``wait_for`` raises TimeoutError; the stable
                # idempotency key (C3) collapses any duplicate the next tick.
                result = await asyncio.shield(
                    _dispatch_risk_exit(
                        coordinator=getattr(context, "execution_coordinator", None),
                        execution_service=context.execution_service,
                        exec_req=req,
                        user_id=context.user_id,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                        side="SELL",
                        price=float(current_price) if current_price is not None else 0.0,
                        notional=float(
                            Decimal(str(pos.size_tokens)) * Decimal(str(current_price or 0))
                        ),
                        source="risk_exit",
                        leader_wallet=pos.leader_id,
                        signal_id=req.signal_id,
                    )
                )
                if not result.accepted:
                    logger.info(
                        "risk_exit_sell_not_filled | cond=%s token=%s reason=%s status=%s error=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                        result.status,
                        result.error,
                    )
                    # Phantom-position prune. An explicit insufficient-balance
                    # error is the strong signal, but a resolved/near-max market
                    # returns a terminal ``cancelled`` instead, so also probe
                    # live holdings on a throttled cadence. ``reconcile_position_
                    # drift`` only deletes when the venue reports ~zero shares and
                    # no reservation is active — a real position is never pruned.
                    drift_signal = _is_drift_error(str(result.error or ""))
                    if drift_signal or _should_attempt_no_fill_prune(
                        context,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                    ):
                        drift_result = await _attempt_phantom_prune_on_no_fill(
                            context,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                            leader_id=pos.leader_id,
                        )
                        if (
                            drift_result.get("reconciled")
                            and drift_result.get("action") == "deleted"
                        ):
                            logger.info(
                                "risk_drift_reconciled_position_deleted | cond=%s token=%s live_size=%s",
                                pos.condition_id[:12],
                                pos.token_id[:12],
                                str(drift_result.get("live_size", 0)),
                            )
                            if _trigger_id is not None and context.risk_triggers_repo is not None:
                                try:
                                    await context.risk_triggers_repo.update_fill_status(
                                        _trigger_id,
                                        fill_status="rejected",
                                        fill_order_id=result.order_id,
                                    )
                                except Exception:
                                    logger.debug("risk_trigger_update_error", exc_info=True)
                            continue
                    if _trigger_id is not None and context.risk_triggers_repo is not None:
                        try:
                            await context.risk_triggers_repo.update_fill_status(
                                _trigger_id,
                                fill_status="rejected",
                                fill_order_id=result.order_id,
                            )
                        except Exception:
                            logger.debug("risk_trigger_update_error", exc_info=True)
                    # Terminal statuses and failed insufficient-balance/allowance
                    # rejections arm cooldown. Plain transient failures keep the
                    # normal retry cadence.
                    if _should_arm_risk_exit_reject_cooldown(
                        result.status,
                        result.error,
                    ):
                        _arm_risk_exit_reject_cooldown(
                            context,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                        )
                    continue
                if result.filled_amount <= 0 and result.status in ("submitted_unconfirmed",):
                    logger.info(
                        "risk_exit_submitted_unconfirmed | cond=%s token=%s reason=%s order_id=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                        result.order_id,
                    )
                    if _trigger_id is not None and context.risk_triggers_repo is not None:
                        try:
                            await context.risk_triggers_repo.update_fill_status(
                                _trigger_id,
                                fill_status="pending",
                                fill_order_id=result.order_id,
                            )
                        except Exception:
                            logger.debug("risk_trigger_update_error", exc_info=True)
                    continue
                if result.filled_amount <= 0:
                    logger.info(
                        "risk_exit_sell_not_filled | cond=%s token=%s reason=%s status=%s error=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                        result.status,
                        result.error,
                    )
                    # Terminal accepted-but-zero-fill (e.g. a fully-killed FAK on
                    # a resolved/illiquid market). Probe live holdings — if the
                    # shares are gone, prune the phantom so the loop stops.
                    if _should_attempt_no_fill_prune(
                        context,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                    ):
                        drift_result = await _attempt_phantom_prune_on_no_fill(
                            context,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                            leader_id=pos.leader_id,
                        )
                        if (
                            drift_result.get("reconciled")
                            and drift_result.get("action") == "deleted"
                        ):
                            logger.info(
                                "risk_drift_reconciled_position_deleted | cond=%s token=%s live_size=%s",
                                pos.condition_id[:12],
                                pos.token_id[:12],
                                str(drift_result.get("live_size", 0)),
                            )
                    if _trigger_id is not None and context.risk_triggers_repo is not None:
                        try:
                            await context.risk_triggers_repo.update_fill_status(
                                _trigger_id,
                                fill_status="rejected",
                                fill_order_id=result.order_id,
                            )
                        except Exception:
                            logger.debug("risk_trigger_update_error", exc_info=True)
                    # Accepted-but-zero-fill is terminal for this attempt (fully
                    # killed FAK) — same 1 Hz refire shape as a rejection.
                    _arm_risk_exit_reject_cooldown(
                        context,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                    )
                    continue

                fill_size = Decimal(str(result.filled_amount))
                fill_price = Decimal(str(result.avg_price or current_price))

                # Persist risk block only when the protective SELL fully closes
                # the scoped position. A partial fill is real and should be
                # accounted for, but it must not create a durable re-entry ban.
                _block_metadata["fill_price"] = float(fill_price)
                _block_metadata["fill_size_tokens"] = float(fill_size)
                _block_metadata["fill_order_id"] = result.order_id

                if _trigger_id is not None and context.risk_triggers_repo is not None:
                    try:
                        await context.risk_triggers_repo.update_fill_status(
                            _trigger_id,
                            fill_status="filled",
                            fill_order_id=result.order_id,
                        )
                    except Exception:
                        logger.debug("risk_trigger_update_error", exc_info=True)

                # Concurrent-run dedup guard — PR #108 P1 finding.
                #
                # The ``idx_fills_dedup`` index keys on
                # ``(user_id, order_id, condition_id, side, fill_ts)``.
                # ``fill_ts`` is the only field that varies between two
                # concurrent ``run_risk_check_for_context`` invocations
                # for the same protective SELL (wall-clock T vs T+1), so
                # the unique index does NOT collapse them — both inserts
                # succeed with distinct ``fill_id`` values, both call
                # ``_apply_fill_in_conn`` (the second on size=0 because
                # the first already closed the position), the second
                # delete is a no-op, position state ends correct, but
                # the fills table now holds TWO rows for ONE exchange
                # fill. Downstream PnL replay (e.g. ``reconcile_trade_
                # record``) double-counts.
                #
                # Anchor dedup to the exchange-sourced ``result.order_id``
                # via ``try_claim_processed_trade`` — the same pattern
                # the trade pipeline uses upstream of its fill insert.
                # Only the winning coroutine reaches ``append_fill_and_
                # apply``; the loser bails out before mutating fills /
                # positions / blocks / PnL.
                risk_trade_key = (
                    f"risk_exit:{context.user_id}:{pos.condition_id}:"
                    f"{pos.token_id}:{result.order_id}"
                )
                claimed = await context.orders_repo.try_claim_processed_trade(
                    trade_key=risk_trade_key,
                    user_id=context.user_id,
                    leader_id=pos.leader_id,
                    tx_hash=None,
                    side="SELL",
                    asset=pos.token_id,
                    condition_id=pos.condition_id,
                    status="in_progress",
                )
                if not claimed:
                    logger.info(
                        "risk_exit_concurrent_claim_lost | user=%s condition=%s token=%s order=%s",
                        context.user_id,
                        pos.condition_id,
                        pos.token_id,
                        result.order_id,
                    )
                    continue

                # Freeze ``fill_ts`` once per claim so any retry within
                # this coroutine reuses the same timestamp the unique
                # index keys on, instead of re-sampling ``time.time()``.
                _fill_ts = int(time.time())

                # Atomic fill insert + position update — closes the
                # crash-window the protective-SELL path had between the
                # two independent pool connections. ``append_fill_and_
                # apply`` also short-circuits on duplicate fills (the
                # ON CONFLICT DO NOTHING branch returns fill_id=0 and
                # skips ``apply_fill``), preventing the double-count
                # regression flagged in PR #108.
                try:
                    (
                        fill_id,
                        updated_pos,
                    ) = await context.fills_repo.append_fill_and_apply(
                        FillRecord(
                            user_id=context.user_id,
                            leader_id=pos.leader_id,
                            order_id=result.order_id,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                            side="SELL",
                            size_tokens=fill_size,
                            price=fill_price,
                            fee=Decimal(str(result.fees_paid or 0)),
                            fill_ts=_fill_ts,
                        ),
                        context.positions_repo,
                    )
                except Exception:
                    # Release the claim so a retry can re-attempt rather
                    # than tombstoning the order_id behind a stale
                    # in_progress row.
                    try:
                        await context.orders_repo.mark_processed_trade(
                            trade_key=risk_trade_key,
                            user_id=context.user_id,
                            leader_id=pos.leader_id,
                            tx_hash=None,
                            side="SELL",
                            asset=pos.token_id,
                            condition_id=pos.condition_id,
                            status="retryable_failed",
                        )
                    except Exception:
                        logger.debug("risk_exit_claim_release_failed", exc_info=True)
                    raise
                if fill_id == 0:
                    # Duplicate protective SELL — fill was already persisted
                    # on a prior call. Risk block, PnL, and position-manager
                    # state were updated then; replaying them would create a
                    # false-positive durable re-entry block (the duplicate
                    # path returns updated_pos=None which would otherwise
                    # trip ``_risk_exit_fully_closed_position`` → True) and
                    # double-count realised PnL.
                    logger.info(
                        "risk_exit_duplicate_fill_ignored | user=%s condition=%s token=%s order=%s",
                        context.user_id,
                        pos.condition_id,
                        pos.token_id,
                        result.order_id,
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=risk_trade_key,
                        user_id=context.user_id,
                        leader_id=pos.leader_id,
                        tx_hash=None,
                        side="SELL",
                        asset=pos.token_id,
                        condition_id=pos.condition_id,
                        status="duplicate_ignored",
                    )
                    continue
                if _risk_exit_fully_closed_position(updated_pos):
                    await context.risk_blocks_repo.mark_risk_block(
                        user_id=context.user_id,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                        reason=reason,
                        source="risk_monitor",
                        leader_anchor=pos.leader_id,
                        metadata=_block_metadata,
                    )
                else:
                    logger.info(
                        "risk_exit_partial_fill_no_block | user=%s condition=%s token=%s remaining=%s",
                        context.user_id,
                        pos.condition_id,
                        pos.token_id,
                        getattr(updated_pos, "size_tokens", None),
                    )
                if hasattr(context.pnl_tracker, "record_sell"):
                    # Realised PnL in Decimal — only round-trip through
                    # float at the pnl_tracker boundary (the tracker's
                    # public API still types these as float).
                    entry_price_d = Decimal(str(pos.avg_entry_price))
                    fill_price_d = Decimal(str(fill_price))
                    fill_size_d = Decimal(str(fill_size))
                    realized_d = (fill_price_d - entry_price_d) * fill_size_d
                    usdc_value_d = fill_size_d * fill_price_d
                    context.pnl_tracker.record_sell(
                        condition_id=pos.condition_id,
                        price=float(fill_price_d),
                        size_tokens=float(fill_size_d),
                        usdc_value=float(usdc_value_d),
                        entry_price=float(entry_price_d),
                        realized_pnl=float(realized_d),
                        exit_reason=reason,
                        leader_wallet=pos.leader_id or "",
                    )
                metrics.inc(
                    "risk_exits_total",
                    labels={
                        "user_id": context.user_id,
                        "leader_id": str(pos.leader_id or ""),
                        "reason": reason,
                    },
                )
                if hasattr(context.position_manager, "track_sell"):
                    context.position_manager.track_sell(
                        pos.condition_id,
                        float(fill_price),
                        float(fill_size),
                        asset=pos.token_id,
                        reason=reason,
                    )
                    # NOTE: no manual ``release_scope`` here — the surrounding
                    # ``async with context.position_coordinator.acquire(_scope)``
                    # context manager already releases on exit. (Greptile P2 PR #63.)

                # Settle the dedup claim — any concurrent run that sampled
                # ``time.time()`` one second later and would have inserted
                # a phantom duplicate fill row now bounces off the claim
                # short-circuit instead.
                try:
                    await context.orders_repo.mark_processed_trade(
                        trade_key=risk_trade_key,
                        user_id=context.user_id,
                        leader_id=pos.leader_id,
                        tx_hash=None,
                        side="SELL",
                        asset=pos.token_id,
                        condition_id=pos.condition_id,
                        status="completed",
                    )
                except Exception:
                    logger.debug("risk_exit_claim_complete_failed", exc_info=True)
            except Exception as exc:
                logger.error(
                    "Risk context check failed user=%s condition=%s: %r",
                    context.user_id,
                    pos.condition_id,
                    exc,
                )


async def risk_monitor_loop_for_contexts(
    contexts: list[TradingContext],
    readiness_tracker: ReadinessTracker,
    *,
    config: AppConfig,
    interval: float | None = None,
) -> None:
    """Restart-safe risk monitor using persisted positions as the source of truth."""
    if not contexts:
        logger.warning("Risk monitor started with no contexts")
        return

    enabled = _exit_monitor_enabled(config)
    poll_interval = float(interval or config.risk.risk_check_interval)

    if not enabled:
        logger.info("Global risk management disabled — monitor will not run")
        return

    for context in contexts:
        context.extra["risk_check_interval_seconds"] = poll_interval

    # Reservation reaping is far cheaper than worthwhile every 0.25s tick;
    # throttle to a coarse cadence. The reaper only releases reservations past
    # their TTL once reconciliation confirms the order is terminal, so a longer
    # cadence than the poll interval is safe and keeps DB load low.
    reap_interval = max(poll_interval * 4, 10.0)
    last_reap = 0.0

    while True:
        readiness_tracker.update(last_risk_check_ts=int(time.time()))

        # Reap stranded reservations BEFORE this cycle's risk checks so a freed
        # reservation lets the same cycle retry the exit or prune the phantom.
        now_ts = time.time()
        if now_ts - last_reap >= reap_interval:
            last_reap = now_ts
            reap_tasks = [_reap_expired_reservations_for_context(context) for context in contexts]
            reap_results = await asyncio.gather(*reap_tasks, return_exceptions=True)
            for context, result in zip(contexts, reap_results, strict=False):
                if isinstance(result, Exception):
                    logger.error(
                        "Risk reservation reaper failed user=%s: %r",
                        context.user_id,
                        result,
                    )

        # Pre-reconcile unconfirmed orders in their OWN phase, ahead of the timed
        # protective-exit check below. These are venue order-status round-trips;
        # keeping them out of the per-context protective-exit budget stops them
        # from starving the stop-loss evaluation while copies settle (sym 1).
        #
        # A timeout here is usually a stuck/slow venue order-status call. Retrying
        # that same call every risk tick creates the live spam pattern observed in
        # ops ("Risk pre-reconcile TIMEOUT ... after 5.0s" every ~6s) and delays
        # protective-exit evaluation by the timeout every cycle. After a timeout,
        # back off this user's pre-reconcile attempts; the standalone reconciler
        # and WSS wake path can still converge state, while risk checks keep
        # running with ``pre_reconcile=False`` instead of parking behind venue IO.
        risk_config = getattr(config, "risk", None)
        prerec_ceiling = _risk_pre_reconcile_timeout_seconds(
            poll_interval,
            getattr(risk_config, "risk_pre_reconcile_timeout_seconds", None),
        )
        prerec_backoff_base = _risk_pre_reconcile_backoff_base_seconds(
            poll_interval,
            prerec_ceiling,
            getattr(risk_config, "risk_pre_reconcile_backoff_seconds", None),
        )
        prerec_now = time.monotonic()
        prerec_contexts = [
            context for context in contexts if _risk_pre_reconcile_is_due(context, now=prerec_now)
        ]
        if prerec_contexts:
            prereconcile_results = await asyncio.gather(
                *(
                    asyncio.wait_for(_pre_reconcile_for_context(context), timeout=prerec_ceiling)
                    for context in prerec_contexts
                ),
                return_exceptions=True,
            )
            for context, result in zip(prerec_contexts, prereconcile_results, strict=False):
                if isinstance(result, asyncio.TimeoutError):
                    backoff_seconds = _record_risk_pre_reconcile_timeout(
                        context,
                        now=time.monotonic(),
                        base_backoff_seconds=prerec_backoff_base,
                    )
                    logger.error(
                        "Risk pre-reconcile TIMEOUT user=%s after %.1fs — "
                        "backing off %.1fs; deferred to standalone reconciler",
                        context.user_id,
                        prerec_ceiling,
                        backoff_seconds,
                    )
                elif isinstance(result, Exception):
                    logger.error(
                        "Risk pre-reconcile failed user=%s: %r",
                        context.user_id,
                        result,
                    )
                else:
                    _record_risk_pre_reconcile_success(context)

        refresh_tasks = [
            _refresh_context_price_cache(context, refresh_interval=poll_interval)
            for context in contexts
        ]
        refresh_results = await asyncio.gather(*refresh_tasks, return_exceptions=True)
        for context, result in zip(contexts, refresh_results, strict=False):
            if isinstance(result, Exception):
                logger.error(
                    "Risk price cache refresh failed user=%s: %r",
                    context.user_id,
                    result,
                )
        # Pre-fix: sequential iteration blocked every context behind a
        # stalled venue call for any single one. Parallel gather mirrors
        # the refresh-tasks path above so one slow context never starves
        # the others. asyncio.wait_for per context puts a hard ceiling
        # on any single per-context check so a hung venue call cannot
        # block the loop indefinitely — the timeout budget is the same
        # poll_interval, which is the maximum any one cycle can take.
        ceiling = max(poll_interval, 5.0)
        check_tasks = [
            asyncio.wait_for(
                run_risk_check_for_context(c, pre_reconcile=False),
                timeout=ceiling,
            )
            for c in contexts
        ]
        check_results = await asyncio.gather(*check_tasks, return_exceptions=True)
        for context, result in zip(contexts, check_results, strict=False):
            if isinstance(result, asyncio.TimeoutError):
                logger.error(
                    f"Risk loop TIMEOUT user={context.user_id} "
                    f"after {ceiling:.1f}s — context skipped this cycle"
                )
            elif isinstance(result, Exception):
                logger.error(
                    "Risk loop error user=%s: %r",
                    context.user_id,
                    result,
                )
        await asyncio.sleep(poll_interval)
