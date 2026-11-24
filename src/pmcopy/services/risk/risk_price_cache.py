"""Risk-loop venue price cache, sanity gates, and venue circuit breaker."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pmcopy.application.trading_context import TradingContext

from pmcopy.application.runtime_state import resolve_risk_state
from pmcopy.integrations.polymarket.models import PRICE_TOKEN_NOT_FOUND
from pmcopy.services.telemetry import RiskTelemetryEmitter
from pmcopy.utils import is_valid_clob_token_id
from pmcopy.utils.logger import get_logger

logger = get_logger("RiskMonitor")

# Null-object emitter for contexts built without the telemetry field (older
# tests and tools construct ``TradingContext`` fakes). Every method is a no-op
# returning ``False``.
_DISABLED_RISK_TELEMETRY = RiskTelemetryEmitter(None)


def _risk_telemetry(context: "TradingContext") -> RiskTelemetryEmitter:
    """Return the context's protective-exit emitter, or a disabled null object.

    Args:
        context: Trading context, possibly a fake without the field.

    Returns:
        The injected emitter when present, else a total null object.
    """
    bundle = getattr(context, "telemetry", None)
    emitter = getattr(bundle, "risk", None)
    return emitter if isinstance(emitter, RiskTelemetryEmitter) else _DISABLED_RISK_TELEMETRY


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
        """Record a successful risk-loop iteration."""
        self._consecutive_failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        """Record a failed risk-loop iteration and its safe reason."""
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
        """Reset the risk-loop breaker after a successful recovery."""
        self._consecutive_failures = 0
        self._opened_at = None



_DEFAULT_RISK_PRICE_SEMAPHORE = 8
_MIN_RISK_CACHE_STALE_SECONDS = 0.75
# Dead / illiquid markets return ``price=0`` on every probe. Cache that
# state with this TTL so the risk loop stops pounding ``/price`` for
# positions the venue isn't quoting. A real fill via user WSS evicts the
# entry (clients/ws/user_ws.py), so genuine reactivation re-arms the probe.
_DORMANT_TOKEN_TTL_SECONDS = 300.0

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

# Cooldown after a terminal (rejected/cancelled) risk-exit submission.
_DEFAULT_RISK_EXIT_REJECT_COOLDOWN_SECONDS = 60.0


@dataclass
class RiskPreReconcileBackoff:
    """Track bounded backoff state for risk pre-reconciliation."""

    consecutive_timeouts: int = 0
    next_attempt_at: float = 0.0


@dataclass
class CachedRiskPrice:
    """Hold one exact risk price and its freshness metadata."""

    price: Decimal
    size: Decimal | None
    fetched_at: float
    # When set, the token last responded with a zero / unquotable price and
    # the refresh path should skip it until ``fetched_at + dormant_ttl``.
    dormant_ttl: float = 0.0

    @property
    def is_dormant(self) -> bool:
        """Return whether the cached risk price is dormant."""
        return self.dormant_ttl > 0.0


class RiskPriceCache:
    # Issue #99: bound the per-process price cache to prevent unbounded
    # memory growth on a long-running bot. Oldest-inserted entries are
    # evicted first; active tokens are unaffected because each set() bumps
    # them back to the MRU end.
    """Store bounded exact prices used by the risk loop."""

    def __init__(self, *, max_size: int | None = None) -> None:
        if max_size is None:
            max_size = 5000
        self._max_size = max_size
        self._entries: OrderedDict[str, CachedRiskPrice] = OrderedDict()

    def get_fresh(
        self, token_id: str, *, stale_after: float, now: float | None = None
    ) -> CachedRiskPrice | None:
        """Return a fresh cached risk price when available."""
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
        """Store an exact risk price with bounded cache eviction."""
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
    risk_state = resolve_risk_state(context)
    breaker = risk_state.venue_circuit_breaker
    if isinstance(breaker, VenueCircuitBreaker):
        return breaker
    breaker = VenueCircuitBreaker()
    risk_state.venue_circuit_breaker = breaker
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
    risk_state = resolve_risk_state(context)
    cache = risk_state.price_cache
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
    risk_state.price_cache = cache
    return cache


def _resolved_market_store(context: TradingContext):
    """Return the per-user ``MarketStateStore`` (if wired) so the risk monitor
    can short-circuit polls on resolved markets. Falls back to ``None`` when
    the runtime didn't attach a MarketDataRuntime (test fixtures, paper mode).

    The store carries authoritative resolved-state set by:
      - ``clients/ws/market_ws.py::_handle_market_resolved`` (push, live)
      - ``orchestrators/bootstrap/runtime_bootstrap.py::hydrate_resolved_market_state``
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
    state = resolve_risk_state(context).active_reservation_skip_logged_at
    try:
        last = float(state.get(key, 0.0))
    except Exception:
        last = 0.0
    if current - last < interval_seconds:
        return False
    state[key] = current
    return True


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
        # wide books near 1.0 that the bps gate misses. 0 disables. Strict:
        # a gap equal to the cap is a normal book (sports markets routinely
        # quote a 3-cent gap), only a wider gap is rejected.
        if reason is None:
            max_abs_distance = _risk_mark_max_abs_distance(context)
            if max_abs_distance > 0:
                abs_distance = float(ask - best_bid)
                if abs_distance > max_abs_distance:
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


