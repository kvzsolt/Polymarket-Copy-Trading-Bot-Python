from __future__ import annotations

import asyncio
import sqlite3
import time
import traceback as _tb
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any

from config.app_config import AppConfig
from domain.models import TradeSignal
from domain.trading_context import TradingContext
from services.entryPermissionService import EntryDecision, EntryPermissionService
from services.execution.reconciliation_service import ReconciliationService
from services.executionService import ExecutionRequest
from services.metrics import metrics
from domain.position_key import PositionKey
from services.positionActionCoordinator import PositionScope  # noqa: F401  back-compat alias
from services.riskEngine import headroom_for_buy
from services.riskEngine import active_policy
from storage.sqlite.fills_repo import FillRecord
from storage.sqlite.positions_repo import PositionRecord
from utils import is_valid_clob_token_id
from utils.dayEquity import get_day_start_equity
from utils.fetchData import fetch_data
from utils.getMyBalance import get_my_balance
from utils.logger import (
    get_logger,
    log_trade_execution,
    log_trade_submitted,
    log_trade_decision,
    log_trade_skip,
    print_executing_strategy,
    print_skip_reason,
    print_trade_result,
    shorten_addr,
)

logger = get_logger("TradePipeline")


def _positive_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(1, value)



def _analysis_semaphore(context: TradingContext) -> asyncio.Semaphore:
    semaphore = context.extra.get("_trade_analysis_semaphore")
    if semaphore is None:
        config_val = context.config.raw.get("TRADE_ANALYSIS_CONCURRENCY", 6) if context.config else 6
        semaphore = asyncio.Semaphore(_positive_int(config_val, 6))
        context.extra["_trade_analysis_semaphore"] = semaphore
    return semaphore



def _reconcile_semaphore(context: TradingContext) -> asyncio.Semaphore:
    semaphore = context.extra.get("_reconcile_unconfirmed_semaphore")
    if semaphore is None:
        config_val = context.config.raw.get("RECONCILE_CONCURRENCY", 4) if context.config else 4
        semaphore = asyncio.Semaphore(_positive_int(config_val, 4))
        context.extra["_reconcile_unconfirmed_semaphore"] = semaphore
    return semaphore


def get_reconciliation_service(context: TradingContext) -> ReconciliationService:
    """Return the shared reconciler bound to this trading context.

    The reconciler is the canonical owner of unconfirmed-order settlement
    (issue #71). One instance per ``TradingContext`` keeps batch reconciles
    consistent across trade and risk paths.
    """
    svc = context.extra.get("_reconciliation_service")
    if svc is None:
        svc = ReconciliationService(
            orders_repo=context.orders_repo,
            fills_repo=context.fills_repo,
            positions_repo=context.positions_repo,
            risk_blocks_repo=getattr(context, "risk_blocks_repo", None),
            risk_triggers_repo=getattr(context, "risk_triggers_repo", None),
            clob_gateway=context.extra.get("clob_gateway"),
            audit_logger=getattr(context, "audit_logger", None),
            reservation_service=context.extra.get("reservation_service"),
        )
        context.extra["_reconciliation_service"] = svc
    return svc


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _matched_amount_from_response(response: dict[str, Any] | None) -> Decimal:
    if not isinstance(response, dict):
        return Decimal("0")
    return _decimal_or_zero(
        response.get("size_matched")
        or response.get("filled_size")
        or response.get("size")
        or 0
    )


_MDS_RETRY_INTERVAL_S = 0.2
_MDS_DEFAULT_TIMEOUT_S = 2.0


def _market_data_subscribe_timeout(context: TradingContext) -> float:
    raw = None
    if context.config is not None:
        raw = context.config.raw.get("MARKET_DATA_SUBSCRIBE_TIMEOUT_SECONDS")
    try:
        value = float(raw) if raw is not None else _MDS_DEFAULT_TIMEOUT_S
    except (TypeError, ValueError):
        value = _MDS_DEFAULT_TIMEOUT_S
    return max(0.2, value)


async def _ensure_token_subscribed(context: TradingContext, token_id: str) -> None:
    """Subscribe ``token_id`` to the market WSS if a runtime is wired.

    Best-effort: a missing runtime (e.g. test contexts) is not fatal — sizing
    will still attempt to read MDS, which fails closed if data is unavailable.
    """
    runtime = getattr(context, "market_data_runtime", None)
    if runtime is None:
        return
    try:
        await runtime.subscribe_tokens([token_id])
    except Exception as exc:
        logger.warning(
            "trade_pipeline_subscribe_failed | token=%s error=%s",
            token_id,
            repr(exc),
        )


async def _resolve_market_bundle(
    context: TradingContext,
    token_id: str,
    *,
    timeout_seconds: float,
):
    """Wait up to ``timeout_seconds`` for fresh executable market data.

    Returns a ``MarketDataBundle`` or ``None`` if the token stays stale past
    the timeout. ``None`` triggers a per-signal ``market_data_stale`` skip in
    the caller; the trade is never placed off stale data.
    """
    mds = getattr(context, "market_data_service", None)
    if mds is None:
        return None

    # Imported lazily to keep the module independent of the MDS error types
    # at import time (and to avoid circular imports through trade_pipeline).
    from services.market_data_service import (  # noqa: WPS433  local import
        MarketDataError,
        MarketDataNotReady,
        StaleDataError,
    )

    deadline = time.monotonic() + max(0.0, timeout_seconds)
    last_error: Exception | None = None

    while True:
        try:
            return await mds.require_fresh_data(token_id)
        except MarketDataNotReady as exc:
            # Resolved markets / not-executable states will not recover within
            # the wait window. Fail closed immediately.
            last_error = exc
            break
        except StaleDataError as exc:
            last_error = exc
        except MarketDataError as exc:
            last_error = exc
            break
        except Exception as exc:  # pragma: no cover - defensive
            last_error = exc
            break

        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(_MDS_RETRY_INTERVAL_S)

    logger.info(
        "trade_pipeline_market_data_stale | token=%s reason=%s",
        token_id,
        repr(last_error) if last_error else "timeout",
    )
    return None


async def _prefetch_market_data_for_signals(
    context: TradingContext,
    normalized: list,
) -> tuple[dict[str, Any], set[str]]:
    """Subscribe each unique signal token and resolve a fresh bundle.

    Returns:
        bundles_by_token: token_id -> MarketDataBundle for tokens that
            became WSS-fresh within the configured timeout.
        stale_tokens: token_ids that timed out; the per-signal loop must
            skip these with ``skip_code="market_data_stale"``.
    """
    unique_tokens: list[str] = []
    seen: set[str] = set()
    for signal, _ in normalized:
        token_id = signal.token_id
        if token_id and token_id not in seen:
            seen.add(token_id)
            unique_tokens.append(token_id)

    if not unique_tokens:
        return {}, set()

    timeout_seconds = _market_data_subscribe_timeout(context)
    bundles_by_token: dict[str, Any] = {}
    stale_tokens: set[str] = set()

    analysis_sem = _analysis_semaphore(context)

    async def _resolve_one(token_id: str) -> tuple[str, Any]:
        async with analysis_sem:
            await _ensure_token_subscribed(context, token_id)
            bundle = await _resolve_market_bundle(
                context,
                token_id,
                timeout_seconds=timeout_seconds,
            )
            return token_id, bundle

    results = await asyncio.gather(
        *(_resolve_one(token_id) for token_id in unique_tokens),
        return_exceptions=True,
    )
    for entry in results:
        if isinstance(entry, Exception):
            logger.warning(
                "trade_pipeline_market_data_prefetch_error | error=%s",
                repr(entry),
            )
            continue
        token_id, bundle = entry
        if bundle is None:
            stale_tokens.add(token_id)
        else:
            bundles_by_token[token_id] = bundle
    return bundles_by_token, stale_tokens


async def _fetch_live_position_size(
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
    except Exception as exc:
        logger.warning(
            "live_position_sync_fetch_failed | user_id=%s token=%s error=%s",
            context.user_id,
            token_id,
            repr(exc),
        )
        return None

    live_size = Decimal("0")
    if isinstance(positions, list):
        for position in positions:
            if not isinstance(position, dict):
                continue
            live_token_id = str(
                position.get("asset")
                or position.get("tokenID")
                or position.get("token_id")
                or ""
            ).strip()
            if live_token_id != token_id:
                continue
            try:
                live_size = Decimal(str(position.get("size") or "0"))
            except Exception:
                live_size = Decimal("0")
            break
    return live_size


async def _sync_sell_position_from_live_holdings(
    context: TradingContext,
    *,
    leader_id: str,
    condition_id: str,
    token_id: str,
) -> Decimal | None:
    live_size = await _fetch_live_position_size(context, token_id=token_id)
    if live_size is None:
        return None

    existing = context.positions_repo.get_position(
        context.user_id,
        condition_id,
        token_id=token_id,
    )
    avg_entry_price = getattr(existing, "avg_entry_price", Decimal("0")) if existing else Decimal("0")

    if live_size <= Decimal("1e-12"):
        context.positions_repo.delete_position(context.user_id, condition_id, token_id=token_id)
        logger.info(
            "live_position_sync_deleted_local_position | user_id=%s leader_id=%s condition_id=%s token=%s",
            context.user_id,
            leader_id,
            condition_id,
            token_id,
        )
        return Decimal("0")

    context.positions_repo.upsert_position(
        PositionRecord(
            user_id=context.user_id,
            condition_id=condition_id,
            token_id=token_id,
            leader_id=leader_id,
            size_tokens=live_size,
            avg_entry_price=Decimal(str(avg_entry_price or 0)),
        )
    )
    logger.info(
        "live_position_sync_updated_local_position | user_id=%s leader_id=%s condition_id=%s token=%s live_size=%s",
        context.user_id,
        leader_id,
        condition_id,
        token_id,
        str(live_size),
    )
    return live_size


@dataclass
class TradePipelineResult:
    trade_key: str
    status: str
    reason: str | None
    order_id: str | None


@dataclass(frozen=True)
class _BuyRiskCap:
    allow: bool
    reason: str
    policy_name: str
    cap_notional_usdc: Decimal



def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def _first_text(raw: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _extract_trade_outcome(raw: dict[str, Any]) -> str:
    return _first_text(
        raw,
        "outcome",
        "outcomeName",
        "outcome_name",
        "tokenOutcome",
        "token_outcome",
        "selection",
        "selectionName",
        "choice",
    )


def _extract_trade_submarket(raw: dict[str, Any]) -> str:
    return _first_text(
        raw,
        "submarket",
        "subMarket",
        "sub_market",
        "marketSubtype",
        "market_subtype",
        "marketType",
        "market_type",
        "lineType",
        "line_type",
        "betType",
        "bet_type",
        "subMarketName",
        "groupItemTitle",
        "groupTitle",
    )


def _resolve_leader_tier(context: TradingContext, leader_id: str) -> str:
    """Look up the follow tier for a leader. Defaults to live_follow if no repo/record."""
    if not hasattr(context, "leaders_repo") or context.leaders_repo is None:
        return "live_follow"
    try:
        rec = context.leaders_repo.get_tier(leader_id)
        tier = rec.follow_tier if rec and rec.follow_tier else None
    except Exception:
        return "live_follow"
    if not tier or tier not in ("watchlist", "paper_follow", "live_follow"):
        return "live_follow"
    return tier



def build_trade_key(user_id: str, leader_id: str, trade: dict[str, Any]) -> str:
    tx_hash = trade.get("transactionHash") or trade.get("txHash") or ""
    side = str(trade.get("side") or "").strip().upper()
    asset = trade.get("tokenID") or trade.get("token_id") or trade.get("asset") or ""
    return f"{_normalize(user_id)}|{_normalize(leader_id)}|{_normalize(tx_hash)}|{side}|{_normalize(asset)}"



def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _max_non_terminal_failure_attempts(context: TradingContext) -> int:
    raw = _trade_config(context).execution.non_terminal_failure_max_attempts
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 3
    return max(1, value)


_RETRYABLE_FAILED_STATUS_PREFIX = "retryable_failed_"
_RETRYABLE_FAILED_STATUS = "retryable_failed"
_LEGACY_RETRY_PREFIX = "retry_pending_"


def _retry_attempt_from_status(status: str | None, retry_attempt: int | None = None) -> int:
    """Return the retry attempt number.

    Prefers the explicit ``retry_attempt`` column value when available.
    Falls back to parsing legacy status strings (``retryable_failed_<n>``
    or ``retry_pending_<n>``) for rows written before the column existed.
    """
    if retry_attempt is not None and retry_attempt > 0:
        return retry_attempt
    text = str(status or "").strip().lower()
    for prefix in (_RETRYABLE_FAILED_STATUS_PREFIX, _LEGACY_RETRY_PREFIX):
        if text.startswith(prefix):
            suffix = text.replace(prefix, "", 1)
            try:
                return int(suffix)
            except (TypeError, ValueError):
                return 0
    return 0


def _account_cap_policy_enabled(context: TradingContext) -> bool:
    return bool(_trade_config(context).risk.enable_balance_tiered_policy)


def _trade_config(context: TradingContext) -> AppConfig:
    """Return the AppConfig for this context.

    ``TracingContext.config`` is typed ``AppConfig | None`` and may be unset
    by integration tests or callers that don't run the full ``load_config()``
    startup path. Callers that *require* a real config (live tier gating)
    must inject one — the function raises when it is absent.
    """
    config = context.config
    if config is None:
        raise RuntimeError("TradingContext.config is required for live tier gating")
    return config


def _position_notional_usdc(position: Any) -> Decimal:
    size_tokens = _to_decimal(getattr(position, "size_tokens", 0))
    avg_entry_price = _to_decimal(getattr(position, "avg_entry_price", 0))
    notional = size_tokens * avg_entry_price
    return notional if notional > 0 else Decimal("0")


def _extract_leader_sell_fraction(raw_trade: dict[str, Any]) -> Decimal | None:
    raw_fraction = raw_trade.get("_leader_sell_fraction")
    if raw_fraction is None:
        raw_fraction = raw_trade.get("leaderSellFraction")
    if raw_fraction is None:
        return None
    try:
        fraction = Decimal(str(raw_fraction))
    except Exception:
        return None
    if fraction <= 0:
        return Decimal("0")
    if fraction > 1:
        return Decimal("1")
    return fraction


def _derive_sell_tokens(signal: TradeSignal, position: Any, raw_trade: dict[str, Any]) -> Decimal:
    """Derive the copied sell size from the leader's sell fraction.

    Preferred rule: sell the same fraction of *our* held shares as the leader
    sold from *their* position using the trade monitor's
    ``_leader_sell_fraction`` annotation.

    Legacy/raw trade feeds do not always include that annotation. In that case
    we fall back to a full close of the currently held position instead of
    skipping forever on an underspecified SELL signal.
    """
    leader_fraction = _extract_leader_sell_fraction(raw_trade)
    if leader_fraction is None:
        logger.info(
            "sell_fraction_missing | cond=%s leader=%s — defaulting to full close",
            signal.condition_id[:12],
            raw_trade.get("_leader_id", "")[:12],
            extra={
                "type": "sell_fraction_missing",
                "condition_id": signal.condition_id,
            },
        )
        leader_fraction = Decimal("1")
    held_tokens = _to_decimal(getattr(position, "size_tokens", 0))
    derived = held_tokens * leader_fraction
    return derived if derived > 0 else Decimal("0")


def _resolve_leader_sell_tokens(
    *,
    requested_sell_tokens: Decimal,
    max_sell_tokens: Decimal,
    market_min_size: float,
) -> Decimal:
    """Resolve leader-follow SELL size without decision-gate scoring.

    Leader SELLs are authoritative exits: copy the same fraction of our held
    position, capped by what we can actually sell. If that fraction is below
    the venue minimum, sell the minimum when possible; otherwise sell the
    largest remaining sellable amount. Risk-management exits may still use
    their own logic, but leader SELLs should not be blocked by copy-decision
    scoring.
    """
    max_sell_tokens = max(Decimal("0"), max_sell_tokens)
    target = min(max(Decimal("0"), requested_sell_tokens), max_sell_tokens)
    if target <= 0:
        return Decimal("0")

    min_tokens = _to_decimal(market_min_size)
    if min_tokens > 0 and target < min_tokens:
        return min(max_sell_tokens, min_tokens)
    return target


class _BalanceSnapshot:
    """Cached balance + exposure data shared across cap resolvers."""
    __slots__ = (
        "free_cash_usdc",
        "equity_usdc",
        "total_exposure_usdc",
        "market_exposure_usdc",
        "leader_exposure_usdc",
        "open_positions_count",
    )

    def __init__(
        self,
        free_cash_usdc: Decimal,
        total_exposure_usdc: Decimal,
        market_exposure_usdc: Decimal,
        leader_exposure_usdc: Decimal,
        open_positions_count: int,
    ) -> None:
        self.free_cash_usdc = free_cash_usdc
        self.total_exposure_usdc = total_exposure_usdc
        self.market_exposure_usdc = market_exposure_usdc
        self.leader_exposure_usdc = leader_exposure_usdc
        self.equity_usdc = free_cash_usdc + total_exposure_usdc
        self.open_positions_count = open_positions_count


async def _fetch_balance_snapshot(
    context: TradingContext,
    condition_id: str,
    leader_id: str,
) -> _BalanceSnapshot | None:
    """Single balance + positions fetch shared by both cap resolvers."""
    balance_usdc = await get_my_balance(context.wallet_address)
    if balance_usdc is None:
        return None

    free_cash_usdc = Decimal(str(balance_usdc))
    open_positions = context.positions_repo.list_open_positions(context.user_id)
    total_exposure_usdc = Decimal("0")
    market_exposure_usdc = Decimal("0")
    leader_exposure_usdc = Decimal("0")
    for position in open_positions:
        position_notional = _position_notional_usdc(position)
        total_exposure_usdc += position_notional
        if str(getattr(position, "condition_id", "") or "") == condition_id:
            market_exposure_usdc += position_notional
        if str(getattr(position, "leader_id", "") or "") == leader_id:
            leader_exposure_usdc += position_notional

    return _BalanceSnapshot(
        free_cash_usdc=free_cash_usdc,
        total_exposure_usdc=total_exposure_usdc,
        market_exposure_usdc=market_exposure_usdc,
        leader_exposure_usdc=leader_exposure_usdc,
        open_positions_count=len(open_positions),
    )


async def _resolve_position_pct_cap(
    context: TradingContext,
    signal: TradeSignal,
    snapshot: _BalanceSnapshot,
) -> _BuyRiskCap:
    free_cash_usdc = snapshot.free_cash_usdc
    market_exposure_usdc = snapshot.market_exposure_usdc
    max_position_pct = Decimal(str(_trade_config(context).sizing.max_position_pct or 0))
    if max_position_pct <= 0:
        # MAX_POSITION_PCT <= 0 means the cap is intentionally disabled by the
        # operator. Returning ``allow=False`` here would silently block every
        # BUY signal — that was a regression introduced when the legacy
        # ``_BuyRiskCap(True, "disabled", ...)`` branch was rewritten. Restore
        # the disabled-cap semantics so BUYs continue to flow uncapped.
        return _BuyRiskCap(
            allow=True,
            reason="disabled",
            policy_name="max_position_pct",
            cap_notional_usdc=Decimal("0"),
        )

    # Cap against FREE CASH only (not total equity). Using equity as the
    # denominator was allowing one position to consume the entire deposit
    # when existing open positions inflated the equity figure.
    cap_total_usdc = free_cash_usdc * max_position_pct
    remaining_cap_usdc = max(Decimal("0"), cap_total_usdc - market_exposure_usdc)
    if remaining_cap_usdc <= 0:
        return _BuyRiskCap(
            allow=False,
            reason="max_position_pct_reached",
            policy_name="max_position_pct",
            cap_notional_usdc=Decimal("0"),
        )

    return _BuyRiskCap(
        allow=True,
        reason="ok",
        policy_name="max_position_pct",
        cap_notional_usdc=min(remaining_cap_usdc, free_cash_usdc),
    )


async def _resolve_buy_risk_cap(
    context: TradingContext,
    leader_id: str,
    signal: TradeSignal,
    snapshot: _BalanceSnapshot,
) -> _BuyRiskCap:
    equity_usdc = snapshot.equity_usdc
    free_cash_usdc = snapshot.free_cash_usdc

    day_start_equity = Decimal(str(_trade_config(context).risk.day_start_equity_usdc or 0))
    if day_start_equity <= 0:
        day_start_equity = get_day_start_equity(equity_usdc)
    daily_loss_usdc = max(Decimal("0"), day_start_equity - equity_usdc)

    # The active pipeline does not yet persist per-position categories, so
    # category headroom cannot be reconstructed reliably here.
    headroom = headroom_for_buy(
        equity_usdc=equity_usdc,
        free_cash_usdc=free_cash_usdc,
        open_positions_count=snapshot.open_positions_count,
        market_exposure_usdc=snapshot.market_exposure_usdc,
        category_exposure_usdc=Decimal("0"),
        leader_exposure_usdc=snapshot.leader_exposure_usdc,
        daily_loss_usdc=daily_loss_usdc,
        day_start_equity_usdc=day_start_equity,
        config=_trade_config(context),
    )
    return _BuyRiskCap(
        allow=headroom.allow,
        reason=headroom.reason,
        policy_name=headroom.policy_name,
        cap_notional_usdc=headroom.cap_notional_usdc,
    )



def normalize_signals(raw_signals: list[dict[str, Any]], leader_id: str) -> list[tuple[TradeSignal, dict[str, Any]]]:
    out: list[tuple[TradeSignal, dict[str, Any]]] = []
    for raw in raw_signals:
        side = str(raw.get("side") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            continue

        condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "")
        token_id = str(raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or "")
        tx_hash = str(raw.get("transactionHash") or raw.get("txHash") or "")
        if not condition_id or not token_id or not tx_hash:
            continue
        if not is_valid_clob_token_id(token_id):
            logger.warning("normalize_signals: skipping signal with invalid token_id=%r", token_id)
            continue

        size = _to_decimal(raw.get("size") or 0)
        price = _to_decimal(raw.get("price") or 0)
        usdc_size = _to_decimal(raw.get("usdcSize") or 0)
        if usdc_size <= 0 and size > 0 and price > 0:
            usdc_size = size * price

        ts_raw = raw.get("timestamp") or int(time.time())
        ts = int(float(ts_raw) / 1000) if float(ts_raw) > 1e12 else int(float(ts_raw))

        signal = TradeSignal(
            leader_id=leader_id,
            condition_id=condition_id,
            token_id=token_id,
            side=side,
            size=size,
            price=price,
            usdc_size=usdc_size,
            tx_hash=tx_hash,
            event_ts=ts,
            title=str(raw.get("title") or ""),
            event_slug=str(raw.get("eventSlug") or raw.get("slug") or ""),
            outcome=_extract_trade_outcome(raw),
            submarket=_extract_trade_submarket(raw),
        )
        out.append((signal, raw))
    return out


async def _reconcile_one(
    context: TradingContext,
    record: dict,
    side: str,
) -> bool:
    """Reconcile a single unconfirmed BUY/SELL via the canonical reconciler.

    Issue #71 — terminal settlement is owned by ``ReconciliationService``;
    this wrapper supplies the per-context dependencies (fill confirmation,
    position lookup, PnL tracker) and preserves caller-facing logging tags.
    """
    if context.execution_service is None:
        return False

    reconciler = get_reconciliation_service(context)
    return await reconciler.reconcile_trade_record(
        record,
        side,
        confirm_fill=reconciler.confirm_fill,
        pnl_tracker=context.pnl_tracker,
        get_position=context.positions_repo.get_position,
        logger=logger,
    )


async def reconcile_submitted_unconfirmed(
    context: TradingContext,
    *,
    max_age_seconds: float = 300.0,
) -> int:
    """Poll exchange for BUY and SELL orders stuck at submitted_unconfirmed.

    BUY reconciliation: applies the fill and creates the local position so it
    becomes visible before the current batch of SELL signals is processed.

    SELL reconciliation: applies the fill to reduce/close the local position
    and records realized PnL. If the FOK was cancelled (no fill), the
    processed_trade row is deleted so the SELL can be retried next cycle.

    Called at the top of each execute_trades invocation.
    Returns the total number of orders reconciled (fills applied + retries unlocked).
    """
    if context.execution_service is None:
        return 0

    buy_pending = context.orders_repo.get_unconfirmed_buys(max_age_seconds=max_age_seconds)
    sell_pending = context.orders_repo.get_unconfirmed_sells(max_age_seconds=max_age_seconds)
    if not buy_pending and not sell_pending:
        return 0

    sem = _reconcile_semaphore(context)

    async def _reconcile_batch(records: list[dict[str, Any]], side: str) -> int:
        async def _run(record: dict[str, Any]) -> bool:
            # Per-scoped-position action coordination (issue #28): serialize
            # the reconciliation state-apply against the same scoped position
            # so risk monitor / execute_trades cannot observe stale state in
            # the middle of the fill-apply critical section.
            try:
                _scope = PositionKey.from_reconciliation_record(record)
            except ValueError:
                logger.warning(
                    "reconcile_skip_missing_scope | order_id=%s trade_key=%s side=%s",
                    record.get("order_id"), record.get("trade_key"), side,
                )
                return False
            async with sem:
                async with context.position_coordinator.acquire(_scope):
                    return await _reconcile_one(context, record, side)

        if not records:
            return 0
        results = await asyncio.gather(*(_run(record) for record in records), return_exceptions=True)
        batch_reconciled = 0
        for result in results:
            if isinstance(result, Exception):
                logger.warning("reconcile_batch_error | side=%s error=%s", side, repr(result))
                continue
            if result:
                batch_reconciled += 1
        return batch_reconciled

    reconciled = await _reconcile_batch(buy_pending, "BUY")
    reconciled += await _reconcile_batch(sell_pending, "SELL")

    if reconciled:
        metrics.inc("reconcile_unconfirmed_total", value=float(reconciled))

    return reconciled


async def execute_trades(
    context: TradingContext,
    leader_id: str,
    signals: list[dict[str, Any]],
    now_ts: float,
) -> list[TradePipelineResult]:
    if context.execution_service is None:
        raise RuntimeError("TradingContext.execution_service is required")

    # Reconcile any BUYs from previous cycles whose fill confirmation was delayed.
    # This must happen before SELL processing so that positions are visible.
    try:
        n = await reconcile_submitted_unconfirmed(context)
        if n:
            logger.info("reconcile_unconfirmed_batch | count=%d", n)
    except Exception as exc:
        logger.warning("reconcile_unconfirmed_error | %s", repr(exc))

    normalized = normalize_signals(signals, leader_id)
    results: list[TradePipelineResult] = []

    leader_tier_resolved = _resolve_leader_tier(context, leader_id)
    if _trade_config(context).execution.paper_trade_only and leader_tier_resolved == "live_follow":
        logger.info(
            "paper_trade_only_override_all",
            extra={
                "user_id": context.user_id,
                "leader_id": leader_id,
                "original_tier": leader_tier_resolved,
            },
        )
        leader_tier_resolved = "paper_follow"

    pnl_metrics = context.pnl_tracker.metrics() if context.pnl_tracker is not None and hasattr(context.pnl_tracker, 'metrics') else {}
    leader_score = context.leader_scorer.score_leader(
        leader_id,
        analytics={
            "net_pnl": float(pnl_metrics.get("net_pnl", 0.0) or 0.0),
            "win_rate": float(pnl_metrics.get("win_rate", 50.0) or 50.0),
        },
        recent_trade_count=len(signals),
    )

    market_min_size_by_token: dict[str, float] = {}
    tick_size_by_token: dict[str, float] = {}
    market_data_stale_tokens: set[str] = set()
    if normalized:
        bundles_by_token, market_data_stale_tokens = await _prefetch_market_data_for_signals(
            context, normalized
        )
        for token_id, bundle in bundles_by_token.items():
            try:
                market_min_size_by_token[token_id] = float(bundle.tick.min_order_size)
                tick_size_by_token[token_id] = float(bundle.tick.tick_size)
            except (TypeError, ValueError, AttributeError):
                # Defensive: a malformed bundle should not crash the loop.
                market_data_stale_tokens.add(token_id)
                market_min_size_by_token.pop(token_id, None)
                tick_size_by_token.pop(token_id, None)

    for signal, raw in normalized:
        labels = {"user_id": context.user_id, "leader_id": leader_id}
        sell_entry_price: float | None = None
        metrics.inc("trades_detected_total", labels=labels)

        trade_key = build_trade_key(context.user_id, leader_id, raw)
        if context.orders_repo.has_processed_trade(trade_key):
            metrics.inc("duplicate_order_prevented_total", labels=labels)
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="skipped",
                    reason="already_processed",
                    order_id=None,
                )
            )
            continue

        # ------------------------------------------------------------------
        # Market-data freshness gate — fail closed when WSS is not ready
        # ------------------------------------------------------------------
        # MarketDataService is the sole authority for executable market data.
        # If subscription + WSS handshake did not converge inside the
        # configured window, skip the signal rather than trade on stale or
        # REST-only prices/books.
        if signal.token_id in market_data_stale_tokens:
            skip_code = "market_data_stale"
            log_trade_skip(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(signal.size) if signal.size else None,
                price=float(signal.price) if signal.price else None,
                usdc_value=float(signal.usdc_size),
                skip_code=skip_code,
                reason="market data not WSS-fresh within timeout",
                event_slug=signal.event_slug,
                meta={"user_id": context.user_id, "leader_id": leader_id},
            )
            metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
            context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status=f"skipped_{skip_code}",
            )
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="skipped",
                    reason=skip_code,
                    order_id=None,
                )
            )
            continue

        # ------------------------------------------------------------------
        # Per-scoped-position action coordination (issue #28 / #67)
        # ------------------------------------------------------------------
        # Serialize all observe-then-mutate work on this scoped position
        # against the risk monitor's protective SELL path and any concurrent
        # reconciliation state-apply on the same (condition_id, token_id).
        # Unrelated positions remain fully concurrent.
        _scope = PositionKey.from_signal_event(signal, context.user_id)
        async with context.position_coordinator.acquire(_scope):
            logger.debug("trade_pipeline.signal_scope_acquired", extra=_scope.log_fields())
            # ------------------------------------------------------------------
            # Entry-permission gate — single authoritative BUY re-entry check
            # ------------------------------------------------------------------
            if signal.side == "BUY":
                _eps: EntryPermissionService | None = context.entry_permission_service
                if _eps is None:
                    logger.error(
                        "entry_permission_unavailable | user=%s leader=%s condition=%s token=%s",
                        context.user_id,
                        leader_id,
                        signal.condition_id,
                        signal.token_id,
                    )
                    skip_reason = "entry_policy_unavailable"
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_reason})
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{skip_reason}",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key=trade_key,
                            status="skipped",
                            reason=skip_reason,
                            order_id=None,
                        )
                    )
                    continue

                # NOTE: ``leader_notional_usdc`` MUST be the leader's CURRENT
                # total position notional in this market — not the size of a
                # single signal. Passing ``signal.usdc_size`` here causes the
                # ``fresh_leader_buy_after_net_flat`` clear rule to mis-fire on
                # any incremental BUY whose notional is < 10% of the trigger
                # notional, prematurely clearing the risk block. We do not
                # currently fetch the leader's live position on the BUY path,
                # so we pass ``None`` to disable that rule. The
                # ``leader_changed`` rule still applies. (Greptile P1 PR #63.)
                _decision: EntryDecision | None = await _eps.check_entry(
                    user_id=context.user_id,
                    condition_id=signal.condition_id,
                    token_id=signal.token_id,
                    leader_id=leader_id,
                    leader_notional_usdc=None,
                )

                if _decision is not None and not _decision.allowed:
                    skip_reason = "market_risk_quarantined"
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=float(signal.price or 0),
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_reason,
                        reason=(
                            f"entry denied by policy [{_decision.reason}]"
                            + (f": {_decision.deny_detail}" if _decision.deny_detail else "")
                        ),
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id, "policy_reason": _decision.reason},
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_reason})
                    metrics.inc("entry_denied_total", labels={**labels, "policy_reason": _decision.reason})
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{skip_reason}",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key=trade_key,
                            status="skipped",
                            reason=skip_reason,
                            order_id=None,
                        )
                    )
                    continue

                if _decision is not None and _decision.clear_rule_applied:
                    # Block was auto-cleared — emit a metric so we can track clear-rule firing
                    metrics.inc(
                        "entry_block_auto_cleared_total",
                        labels={**labels, "clear_rule": _decision.clear_rule_applied},
                    )

            # print_new_trade_detected removed: tradeMonitor already prints on detection

            # --- Tier gate: check follow tier before costly decision/execution ---
            leader_tier = leader_tier_resolved

            if leader_tier == "watchlist":
                config = _trade_config(context)
                if config.features.enable_tier_gate_watchlist:
                    if config.features.enable_tier_gate_watchlist_log:
                        logger.info(
                            "tier_gate_watchlist",
                            extra={
                                "user_id": context.user_id,
                                "leader_id": leader_id,
                                "trade_key": trade_key,
                                "side": signal.side,
                                "condition_id": signal.condition_id,
                            },
                        )
                    print_skip_reason("Tier gate: watchlist", "Leader is on watchlist — observing only")
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": "tier_watchlist"})
                    results.append(TradePipelineResult(trade_key, "skipped", "tier_watchlist", None))
                    continue
            if leader_tier == "paper_follow":
                logger.info(
                    "tier_gate_paper",
                    extra={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "trade_key": trade_key,
                        "side": signal.side,
                        "condition_id": signal.condition_id,
                        "usdc_size": float(signal.usdc_size),
                    },
                )
                print_skip_reason("Tier gate: paper follow", "Paper-tracking only — no live order submitted")
                metrics.inc("trades_paper_total", labels=labels)
                context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="paper_follow",
                )
                results.append(TradePipelineResult(trade_key, "paper", "tier_paper_follow", None))
                continue

            # --- Decision gate: score the leader and this specific trade ---
            copy_decision_svc = context.copy_decision_service

            # Fetch live price for decision scoring
            live_price = float(signal.price or 0)
            market_min_size = market_min_size_by_token.get(signal.token_id, 0.0)
            market_tick_size = tick_size_by_token.get(signal.token_id, 0.0)

            if signal.side == "BUY":
                sizing_result = context.copy_notional(signal.usdc_size)
                target_notional = sizing_result.final_notional_usdc
                configured_buy_max = Decimal(str(getattr(context, "buy_max_amount", target_notional) or target_notional))
                # BUY_MAX_AMOUNT is the hard ceiling for decision-layer upsizing.
                # Keep base_notional_usdc as the strategy target below, but let
                # CopyDecisionService apply trust sizing up to this ceiling before
                # we further clamp by always-on cash/position caps.
                max_notional_for_decision = configured_buy_max

                snapshot = await _fetch_balance_snapshot(
                    context, signal.condition_id, leader_id,
                )
                if snapshot is None:
                    reason = "balance_unavailable"
                    print_skip_reason("Balance unavailable", reason)
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code=reason,
                        reason=reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": reason},
                    )
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{reason}",
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", reason, None))
                    continue

                position_pct_cap = await _resolve_position_pct_cap(context, signal, snapshot)
                if not position_pct_cap.allow:
                    reason = (
                        f"position cap [{position_pct_cap.policy_name}] blocked BUY: "
                        f"{position_pct_cap.reason}"
                    )
                    print_skip_reason(
                        f"Position cap [{position_pct_cap.policy_name}] blocked BUY",
                        position_pct_cap.reason,
                    )
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code=position_pct_cap.reason,
                        reason=reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": position_pct_cap.reason},
                    )
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{position_pct_cap.reason}",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key,
                            "skipped",
                            position_pct_cap.reason,
                            None,
                        )
                    )
                    continue

                max_notional_for_decision = min(
                    max_notional_for_decision,
                    position_pct_cap.cap_notional_usdc,
                )

                buy_risk_cap = None
                if _account_cap_policy_enabled(context):
                    buy_risk_cap = await _resolve_buy_risk_cap(context, leader_id, signal, snapshot)
                    if not buy_risk_cap.allow:
                        reason = (
                            f"account-cap policy [{buy_risk_cap.policy_name}] headroom: "
                            f"{buy_risk_cap.reason}"
                        )
                        print_skip_reason(
                            f"Account-cap policy [{buy_risk_cap.policy_name}] headroom blocked BUY",
                            buy_risk_cap.reason,
                        )
                        log_trade_skip(
                            logger,
                            side=signal.side,
                            condition_id=signal.condition_id,
                            asset=signal.token_id,
                            size=float(signal.size) if signal.size else None,
                            price=live_price,
                            usdc_value=float(signal.usdc_size),
                            skip_code=buy_risk_cap.reason,
                            reason=reason,
                            event_slug=signal.event_slug,
                            meta={"user_id": context.user_id, "leader_id": leader_id},
                        )
                        metrics.inc(
                            "trades_skipped_total",
                            labels={**labels, "reason": buy_risk_cap.reason},
                        )
                        context.orders_repo.mark_processed_trade(
                            trade_key=trade_key,
                            user_id=context.user_id,
                            leader_id=leader_id,
                            tx_hash=signal.tx_hash,
                            side=signal.side,
                            asset=signal.token_id,
                            condition_id=signal.condition_id,
                            status=f"skipped_{buy_risk_cap.reason}",
                        )
                        results.append(
                            TradePipelineResult(
                                trade_key,
                                "skipped",
                                buy_risk_cap.reason,
                                None,
                            )
                        )
                        continue

                    max_notional_for_decision = min(
                        max_notional_for_decision,
                        buy_risk_cap.cap_notional_usdc,
                    )

                policy_enabled = _account_cap_policy_enabled(context)

                # --- Sizing audit: unified log showing both sizing sources ---
                policy_binding = (
                    buy_risk_cap is not None
                    and buy_risk_cap.cap_notional_usdc < target_notional
                )
                logger.info(
                    "sizing_audit | cond=%s leader=%s "
                    "leader_notional=%.2f strategy=%s sizing_target=%.4f "
                    "policy_enabled=%s policy_cap=%s policy_binding=%s "
                    "max_for_decision=%.4f | %s",
                    signal.condition_id[:12],
                    leader_id[:12],
                    float(signal.usdc_size),
                    sizing_result.strategy,
                    float(target_notional),
                    policy_enabled,
                    f"{float(buy_risk_cap.cap_notional_usdc):.4f}[{buy_risk_cap.policy_name}]"
                    if buy_risk_cap else "n/a",
                    policy_binding,
                    float(max_notional_for_decision),
                    sizing_result.reasoning,
                    extra={
                        "type": "sizing_audit",
                        "condition_id": signal.condition_id,
                        "leader_id": leader_id,
                        "leader_notional": float(signal.usdc_size),
                        "strategy": sizing_result.strategy,
                        "sizing_target": float(target_notional),
                        "policy_enabled": policy_enabled,
                        "policy_cap": float(buy_risk_cap.cap_notional_usdc) if buy_risk_cap else None,
                        "policy_binding": policy_binding,
                        "max_for_decision": float(max_notional_for_decision),
                    },
                )

                # Run the copy decision gate
                decision = copy_decision_svc.decide_buy(
                    trade=raw,
                    leader_score=leader_score,
                    live_price=live_price,
                    base_notional_usdc=float(target_notional),
                    max_notional_usdc=float(max_notional_for_decision),
                    market_min_size=market_min_size,
                    tick_size=market_tick_size,
                )

                log_trade_decision(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    decision_score=decision.decision_score,
                    allow=decision.allow,
                    reason=decision.reason,
                    event_slug=signal.event_slug,
                    meta={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "leader_score": leader_score.score,
                        "trade_score": decision.trade_score,
                        "price_drift_pct": decision.price_drift_pct,
                        "edge_decay_pct": decision.edge_decay_pct,
                        "leader_notional": float(signal.usdc_size),
                        "tx_hash": signal.tx_hash,
                    },
                )

                if not decision.allow:
                    if decision.skip_code != "signal_too_old":
                        skip_title = (
                            "BUY blocked by execution minimums/risk cap"
                            if decision.skip_code in {"risk_cap_too_low_for_min_order", "buy_below_minimum_order_requirements", "buy_cap_blocks_upsize_to_venue_min"}
                            else "Decision gate rejected BUY"
                        )
                        print_skip_reason(skip_title, decision.reason)
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code=decision.skip_code,
                        reason=decision.reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": decision.skip_code})
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{decision.skip_code}",
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", decision.reason, None))
                    continue

                # Use decision-gated notional (may be adjusted by scoring)
                gated_notional = Decimal(str(decision.target_notional_usdc)) if decision.target_notional_usdc > 0 else target_notional

                # Log final execution sizing if decision gate changed the amount
                if float(gated_notional) != float(target_notional):
                    logger.info(
                        "sizing_final | cond=%s decision_gate adjusted %.4f → %.4f "
                        "(trust_mult=%.3f)",
                        signal.condition_id[:12],
                        float(target_notional),
                        float(gated_notional),
                        decision.trust_multiplier if hasattr(decision, "trust_multiplier") else 0,
                        extra={
                            "type": "sizing_final",
                            "condition_id": signal.condition_id,
                            "leader_id": leader_id,
                            "original_notional": float(target_notional),
                            "gated_notional": float(gated_notional),
                        },
                    )

                exec_req = ExecutionRequest(
                    user_id=context.user_id,
                    leader_id=leader_id,
                    token_id=signal.token_id,
                    side="BUY",
                    amount=gated_notional,
                    max_slippage_bps=int(float(context.extra.get("slippage_tolerance_bps", 200))),
                    reason="copy_trade",
                    signal_id=signal.tx_hash,
                    condition_id=signal.condition_id,
                )
            else:
                pos = context.positions_repo.get_position(context.user_id, signal.condition_id, token_id=signal.token_id)
                if pos is None:
                    # No exact (condition_id, token_id) match — check if we hold the *other* outcome token.
                    candidates = context.positions_repo.list_positions_for_condition(
                        context.user_id, signal.condition_id
                    )
                    wrong_token_held = any(p.token_id != signal.token_id for p in candidates)
                    if wrong_token_held:
                        skip_detail = f"wrong token held — leader sells {shorten_addr(signal.token_id, 8, 6)} but we hold a different outcome"
                        skip_code = "wrong_token_held"
                    else:
                        skip_detail = "never held — we never entered this market"
                        skip_code = "never_held"
                    logger.debug(
                        "sell_skip",
                        extra={
                            "reason": skip_code,
                            "condition_id": signal.condition_id,
                            "token_id": signal.token_id,
                            "leader_id": leader_id,
                        },
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=float(signal.price) if signal.price else None,
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_code,
                        reason=skip_detail,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{skip_code}",
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
                    continue

                if pos.size_tokens <= Decimal("0"):
                    skip_detail = "already closed — position size is zero"
                    skip_code = "already_closed"
                    logger.debug(
                        "sell_skip",
                        extra={
                            "reason": skip_code,
                            "condition_id": signal.condition_id,
                            "leader_id": leader_id,
                        },
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=float(signal.price) if signal.price else None,
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_code,
                        reason=skip_detail,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{skip_code}",
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
                    continue

                # Capture entry price before apply_fill mutates or closes/deletes the position.
                sell_entry_price = _to_optional_float(getattr(pos, "avg_entry_price", None))

                requested_sell_tokens = _derive_sell_tokens(signal, pos, raw)
                max_sell_tokens = pos.size_tokens

                live_sellable_tokens = await _fetch_live_position_size(
                    context,
                    token_id=signal.token_id,
                )
                if live_sellable_tokens is not None:
                    venue_sell_cap = live_sellable_tokens.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                    if venue_sell_cap <= Decimal("0") and live_sellable_tokens > Decimal("0"):
                        venue_sell_cap = live_sellable_tokens
                    if venue_sell_cap < max_sell_tokens:
                        logger.info(
                            "sell_size_clamped_to_live_balance | cond=%s leader=%s requested=%.4f live_balance=%.4f clamped=%.4f",
                            signal.condition_id[:12],
                            leader_id[:12],
                            float(requested_sell_tokens),
                            float(live_sellable_tokens),
                            float(venue_sell_cap),
                            extra={
                                "type": "sell_size_clamped_to_live_balance",
                                "condition_id": signal.condition_id,
                                "leader_id": leader_id,
                                "requested_sell_tokens": float(requested_sell_tokens),
                                "live_sellable_tokens": float(live_sellable_tokens),
                                "clamped_sell_tokens": float(venue_sell_cap),
                            },
                        )
                        if venue_sell_cap <= Decimal("0") and live_sellable_tokens <= Decimal("0"):
                            context.positions_repo.delete_position(context.user_id, signal.condition_id, token_id=signal.token_id)
                        max_sell_tokens = venue_sell_cap

                leader_sell_fraction = _extract_leader_sell_fraction(raw) or Decimal("1")
                sell_tokens = _resolve_leader_sell_tokens(
                    requested_sell_tokens=requested_sell_tokens,
                    max_sell_tokens=max_sell_tokens,
                    market_min_size=market_min_size,
                )

                if sell_tokens <= Decimal("0"):
                    reason = "SELL size is zero"
                    print_skip_reason("SELL skipped", reason)
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(sell_tokens),
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code="sell_size_zero",
                        reason=reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": "sell_size_zero"})
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status="skipped_sell_size_zero",
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", reason, None))
                    continue

                if requested_sell_tokens + Decimal("1e-12") < sell_tokens:
                    logger.info(
                        "sell_size_bumped_to_min | requested=%.4f bumped_to=%.4f held=%.4f",
                        float(requested_sell_tokens),
                        float(sell_tokens),
                        float(pos.size_tokens),
                    )

                logger.info(
                    "sell_ratio_applied | cond=%s leader=%s held=%.4f leader_fraction=%.6f sell_tokens=%.4f",
                    signal.condition_id[:12],
                    leader_id[:12],
                    float(pos.size_tokens),
                    float(leader_sell_fraction),
                    float(sell_tokens),
                    extra={
                        "type": "sell_ratio_applied",
                        "condition_id": signal.condition_id,
                        "leader_id": leader_id,
                        "position_size": float(pos.size_tokens),
                        "leader_sell_fraction": float(leader_sell_fraction),
                        "requested_sell_tokens": float(requested_sell_tokens),
                        "max_sell_tokens": float(max_sell_tokens),
                        "market_min_size": float(market_min_size),
                        "sell_tokens": float(sell_tokens),
                    },
                )

                exec_req = ExecutionRequest(
                    user_id=context.user_id,
                    leader_id=leader_id,
                    token_id=signal.token_id,
                    side="SELL",
                    amount=sell_tokens,
                    max_slippage_bps=int(float(context.extra.get("slippage_tolerance_bps", 200))),
                    reason="leader_sell",
                    signal_id=signal.tx_hash,
                    condition_id=signal.condition_id,
                )

            print_executing_strategy(signal.side)
            exec_result = await context.execution_service.execute(exec_req)

            if exec_result.status == "duplicate_ignored":
                metrics.inc("duplicate_order_prevented_total", labels=labels)
                context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="duplicate_ignored",
                )
                results.append(
                    TradePipelineResult(
                        trade_key=trade_key,
                        status="skipped",
                        reason="duplicate_ignored",
                        order_id=exec_result.order_id,
                    )
                )
                continue

            if exec_result.accepted and exec_result.filled_amount <= 0:
                context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="submitted_unconfirmed",
                    order_id=exec_result.order_id,
                )
                log_trade_submitted(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    size=float(signal.size) if signal.size else None,
                    price=float(signal.price) if signal.price else None,
                    usdc_value=float(signal.usdc_size),
                    event_slug=signal.event_slug,
                    meta={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "submissionState": "submitted_unconfirmed",
                        "copyOrderId": exec_result.order_id,
                    },
                )
                results.append(
                    TradePipelineResult(
                        trade_key=trade_key,
                        status="submitted_unconfirmed",
                        reason="accepted_no_fill_confirmed",
                        order_id=exec_result.order_id,
                    )
                )
                continue

            if not exec_result.accepted:
                metrics.inc("execution_failures_total", labels={**labels, "reason": exec_result.status})
                log_trade_skip(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    size=float(signal.size) if signal.size else None,
                    price=float(signal.price) if signal.price else None,
                    usdc_value=float(signal.usdc_size),
                    skip_code=exec_result.status,
                    reason=exec_result.error or exec_result.status,
                    status="failed",
                    event_slug=signal.event_slug,
                    meta={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "order_id": exec_result.order_id,
                    },
                )
                is_terminal = exec_result.status == "rejected"
                max_non_terminal_attempts = _max_non_terminal_failure_attempts(context)
                prev_trade = context.orders_repo.get_processed_trade(trade_key) or {}
                prev_retry_attempt = _retry_attempt_from_status(
                    prev_trade.get("status"), prev_trade.get("retry_attempt")
                )
                next_retry_attempt = prev_retry_attempt + 1
                non_terminal_retry_exhausted = (
                    not is_terminal and next_retry_attempt >= max_non_terminal_attempts
                )
                status_to_persist = (
                    "terminal_failed"
                    if is_terminal
                    else (
                        "terminal_failed"
                        if non_terminal_retry_exhausted
                        else _RETRYABLE_FAILED_STATUS
                    )
                )
                context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status=status_to_persist,
                    retry_attempt=next_retry_attempt,
                )
                fail_reason = exec_result.error or exec_result.status
                live_synced_size: Decimal | None = None
                if signal.side == "SELL" and "insufficient_balance" in str(fail_reason).lower():
                    existing = context.positions_repo.get_position(
                        context.user_id,
                        signal.condition_id,
                        token_id=signal.token_id,
                    )
                    previous_local_size = getattr(existing, "size_tokens", Decimal("0")) if existing else Decimal("0")
                    
                    live_synced_size = await _sync_sell_position_from_live_holdings(
                        context,
                        leader_id=leader_id,
                        condition_id=signal.condition_id,
                        token_id=signal.token_id,
                    )
                    if live_synced_size is not None:
                        fail_reason = (
                            f"{fail_reason} | local position synced to live holdings={float(live_synced_size):.4f}"
                        )
                        audit = getattr(context, "audit_logger", None)
                        if audit is not None:
                            try:
                                action = "deleted" if live_synced_size <= Decimal("1e-12") else "shrunk"
                                audit.log_event(
                                    "manual_position_drift_reconciled",
                                    {
                                        "user_id": context.user_id,
                                        "leader_id": leader_id,
                                        "condition_id": signal.condition_id,
                                        "token_id": signal.token_id,
                                        "previous_local_size": str(previous_local_size),
                                        "live_size": str(live_synced_size),
                                        "action": action,
                                        "source": "trade_pipeline",
                                    },
                                )
                            except Exception:
                                pass
                if non_terminal_retry_exhausted:
                    fail_reason = (
                        f"{fail_reason} "
                        f"(retry budget exhausted after {next_retry_attempt} attempts)"
                    )
                print_trade_result(
                    success=False,
                    side=signal.side,
                    reason=fail_reason,
                    title=signal.title,
                    submarket=signal.submarket,
                    outcome=signal.outcome,
                )
                logger.warning(
                    "trade_copy_failed",
                    extra={
                        "type": "trade_copy_failed",
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "condition_id": signal.condition_id,
                        "side": signal.side,
                        "status": exec_result.status,
                        "error": exec_result.error,
                        "title": signal.title,
                        "submarket": signal.submarket,
                        "outcome": signal.outcome,
                        "terminal": is_terminal,
                        "retry_exhausted": non_terminal_retry_exhausted,
                        "retry_attempt": next_retry_attempt,
                        "max_non_terminal_attempts": max_non_terminal_attempts,
                        "persisted_status": status_to_persist,
                    },
                )
                results.append(
                    TradePipelineResult(
                        trade_key=trade_key,
                        status="failed",
                        reason=fail_reason,
                        order_id=exec_result.order_id,
                    )
                )
                continue

            fill_price = exec_result.avg_price or signal.price
            fill_size = Decimal(str(exec_result.filled_amount))
            fee_paid = Decimal(str(exec_result.fees_paid or 0))
            try:
                context.fills_repo.append_fill(
                    FillRecord(
                        user_id=context.user_id,
                        leader_id=leader_id,
                        order_id=exec_result.order_id,
                        condition_id=signal.condition_id,
                        token_id=signal.token_id,
                        side=signal.side,
                        size_tokens=fill_size,
                        price=Decimal(str(fill_price or 0)),
                        fee=fee_paid,
                        fill_ts=int(now_ts),
                    ),
                    raw_payload={
                        "signal_tx_hash": signal.tx_hash,
                        "copy_order_id": exec_result.order_id,
                        "status": exec_result.status,
                    },
                )
                updated_position = context.positions_repo.apply_fill(
                    user_id=context.user_id,
                    leader_id=leader_id,
                    condition_id=signal.condition_id,
                    token_id=signal.token_id,
                    side=signal.side,
                    fill_size=fill_size,
                    fill_price=Decimal(str(fill_price or 0)),
                )
            except sqlite3.IntegrityError as ie:
                error_msg = str(ie)
                if "idx_fills_unique" in error_msg or "UNIQUE constraint failed: fills" in error_msg:
                    # Duplicate fill — idempotent, skip gracefully
                    logger.info(
                        "trade_pipeline_duplicate_fill_ignored",
                        extra={
                            "user_id": context.user_id,
                            "leader_id": leader_id,
                            "condition_id": signal.condition_id,
                            "order_id": exec_result.order_id,
                        },
                    )
                    context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status="duplicate_ignored",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key=trade_key,
                            status="skipped",
                            reason="duplicate_ignored",
                            order_id=exec_result.order_id,
                        )
                    )
                    continue
                # Non-duplicate integrity error — log and re-raise
                logger.error(
                    "trade_pipeline_integrity_error",
                    extra={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "condition_id": signal.condition_id,
                        "signal_tx_hash": signal.tx_hash,
                        "order_id": exec_result.order_id,
                        "traceback": _tb.format_exc(),
                    },
                )
                raise

            if signal.side == "BUY":
                if context.pnl_tracker is not None:
                    context.pnl_tracker.record_buy(
                        condition_id=signal.condition_id,
                        price=float(fill_price or 0),
                        size_tokens=float(fill_size),
                        usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                        leader_wallet=leader_id,
                        event_slug=signal.event_slug,
                        title=signal.title,
                    )
            else:
                updated_entry_price = _to_optional_float(
                    getattr(updated_position, "avg_entry_price", None) if updated_position is not None else None
                )
                signal_entry_price = _to_optional_float(signal.price)
                entry_price = next(
                    (
                        candidate
                        for candidate in (sell_entry_price, updated_entry_price, signal_entry_price)
                        if candidate is not None
                    ),
                    0.0,
                )
                if entry_price == 0.0 and all(
                    candidate is None for candidate in (sell_entry_price, updated_entry_price, signal_entry_price)
                ):
                    logger.warning(
                        "trade_sell_entry_price_missing",
                        extra={
                            "user_id": context.user_id,
                            "leader_id": leader_id,
                            "condition_id": signal.condition_id,
                            "order_id": exec_result.order_id,
                        },
                    )
                realized = (float(fill_price or 0) - entry_price) * float(fill_size)
                if context.pnl_tracker is not None:
                    context.pnl_tracker.record_sell(
                        condition_id=signal.condition_id,
                        price=float(fill_price or 0),
                        size_tokens=float(fill_size),
                        usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                        entry_price=entry_price,
                        realized_pnl=realized,
                        exit_reason="leader_sell",
                        leader_wallet=leader_id,
                        event_slug=signal.event_slug,
                        title=signal.title,
                    )

            metrics.inc("trades_copied_total", labels=labels)

            log_trade_submitted(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(fill_size),
                price=float(fill_price or 0),
                usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                event_slug=signal.event_slug,
                meta={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "copyOrderId": exec_result.order_id,
                    "order_id": exec_result.order_id,
                    "fee_paid": float(fee_paid),
                    "title": signal.title,
                    "submarket": signal.submarket,
                    "outcome": signal.outcome,
                },
            )
            log_trade_execution(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(fill_size),
                price=float(fill_price or 0),
                usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                tx_hash=signal.tx_hash,
                event_slug=signal.event_slug,
                meta={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "copyOrderId": exec_result.order_id,
                    "order_id": exec_result.order_id,
                    "fee_paid": float(fee_paid),
                    "status": exec_result.status,
                    "title": signal.title,
                    "submarket": signal.submarket,
                    "outcome": signal.outcome,
                },
            )
            print_trade_result(
                success=True,
                side=signal.side,
                leader_tx=signal.tx_hash,
                copy_tx=exec_result.order_id,
                amount=float(fill_size * Decimal(str(fill_price or 0))),
                price=float(fill_price or 0),
                title=signal.title,
                submarket=signal.submarket,
                outcome=signal.outcome,
            )

            context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status="executed",
            )

            tx_hashes = raw.get("_txHashes") or raw.get("txHashes") or []
            if isinstance(tx_hashes, list):
                for txh in tx_hashes:
                    tx_key = build_trade_key(
                        context.user_id,
                        leader_id,
                        {
                            "transactionHash": txh,
                            "side": signal.side,
                            "asset": signal.token_id,
                        },
                    )
                    context.orders_repo.mark_processed_trade(
                        trade_key=tx_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=str(txh),
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status="executed",
                    )

            context.readiness.update(last_successful_order_ts=int(time.time()))
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="executed",
                    reason=None,
                    order_id=exec_result.order_id,
                )
            )

    return results
