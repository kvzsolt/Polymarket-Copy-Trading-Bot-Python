"""Signal normalization, trade keys, SELL token sizing, balance caps, and affordability helpers."""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pmcopy.application.runtime_state import (
    resolve_account_execution_state,
)
from pmcopy.application.trading_context import TradingContext
from pmcopy.config.app_config import AppConfig
from pmcopy.domain.models import TradeSignal
from pmcopy.domain.performance import PerformanceUnits
from pmcopy.domain.source_time import SourceTimestampParser
from pmcopy.domain.units import BASIS_POINTS_PER_UNIT
from pmcopy.orchestrators.pipeline.pipeline_market_data import (
    _COPY_TRADE_IDENTITY,
    _LEADER_SELL_MINIMUM_POLICY,
    _pipeline_facade_attr,
)
from pmcopy.services.risk.account_snapshot import AccountRiskSnapshot, AccountRiskSnapshotBuilder
from pmcopy.services.risk.risk_engine import headroom_for_buy
from pmcopy.utils import is_valid_clob_token_id
from pmcopy.utils.logger import get_logger, print_warning, shorten_addr
from pmcopy.utils.wallet_balance import get_my_balance

logger = get_logger("TradePipeline")


@dataclass
class TradePipelineResult:
    """Describe one leader-signal pipeline outcome."""

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


async def _resolve_leader_tier(context: TradingContext, leader_id: str) -> str:
    """Look up the follow tier for a leader.

    P0-2: fail-closed. An absent repo, a lookup error, or an unknown/missing
    tier resolves to ``watchlist`` (the non-live, no-submission tier), never
    ``live_follow``. A DB glitch or a misconfiguration must not auto-promote a
    leader to live copy trading. (The global LIVE_TRADING master switch already
    forces paper unless explicitly enabled; this is defense-in-depth within
    live mode.)
    """
    if not hasattr(context, "leaders_repo") or context.leaders_repo is None:
        return "watchlist"
    try:
        rec = await context.leaders_repo.get_tier(leader_id)
        tier = rec.follow_tier if rec and rec.follow_tier else None
    except Exception:
        return "watchlist"
    if not tier or tier not in ("watchlist", "paper_follow", "live_follow"):
        return "watchlist"
    return tier


def build_trade_key(user_id: str, leader_id: str, trade: dict[str, Any]) -> str:
    """Delegate canonical economic identity construction."""

    return _COPY_TRADE_IDENTITY.build(user_id, leader_id, trade)


def all_tx_hashes_for_trade(trade: dict[str, Any]) -> list[str]:
    """Return every tx hash associated with a trade, deduped, order-preserved.

    Phase 2.10 (H6) — aggregated signals carry constituent hashes in
    ``_txHashes`` / ``txHashes``. Dedup must consider ALL of them, not just
    the primary ``transactionHash``, so a second detection channel (e.g.
    on-chain WS) cannot re-trigger an already-processed aggregated signal.
    The primary hash is first; constituents follow in their list order.
    """
    primary = (
        trade.get("transactionHash")
        or trade.get("txHash")
        or trade.get("transaction_hash")
        or trade.get("tx_hash")
        or ""
    )
    seen: set[str] = set()
    out: list[str] = []
    if primary:
        out.append(str(primary))
        seen.add(str(primary))
    constituents = trade.get("_txHashes") or trade.get("txHashes") or []
    if isinstance(constituents, (list, tuple)):
        for txh in constituents:
            if not txh:
                continue
            h = str(txh)
            if h not in seen:
                out.append(h)
                seen.add(h)
    return out


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def seed_position_pct_cap_snapshot(
    context: TradingContext,
    balance_usdc: Any,
    *,
    timestamp: float | None = None,
) -> None:
    """Seed the MAX_POSITION_PCT cap basis from a wallet balance snapshot.

    The position percentage cap is intentionally based on a coarse wallet
    snapshot: startup balance, then a refresh at most once per 24 hours. Other
    balance policies may still use live free cash.
    """
    balance = max(Decimal("0"), _to_decimal(balance_usdc))
    resolve_account_execution_state(context).position_pct_cap_snapshot.seed(
        balance,
        wallet=_funder_wallet_address(context),
        captured_at=timestamp,
    )


def _position_pct_cap_snapshot_is_fresh(
    context: TradingContext,
    *,
    now: float | None = None,
) -> bool:
    return resolve_account_execution_state(context).position_pct_cap_snapshot.is_fresh(
        wallet=_funder_wallet_address(context),
        now=now,
    )


def _seed_position_pct_cap_snapshot_if_stale(
    context: TradingContext,
    balance_usdc: Any,
) -> None:
    if _position_pct_cap_snapshot_is_fresh(context):
        return
    seed_position_pct_cap_snapshot(context, balance_usdc)


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


def _funder_wallet_address(context: TradingContext) -> str:
    config = getattr(context, "config", None)
    configured = str(getattr(getattr(config, "wallet", None), "funder_address", "") or "").strip()
    return configured or str(getattr(context, "wallet_address", "") or "").strip()


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


def _derive_sell_tokens(
    signal: TradeSignal,
    position: Any,
    raw_trade: dict[str, Any],
    *,
    missing_policy: str = "skip",
) -> Decimal | None:
    """Derive the copied sell size from the leader's sell fraction.

    Preferred rule: sell the same fraction of *our* held shares as the leader
    sold from *their* position using the trade monitor's
    ``_leader_sell_fraction`` annotation.

    When the annotation is missing (legacy feed, or the leader positions
    fetch failed — see ``_leader_sell_fraction_unavailable``), the configured
    ``execution.sell_fraction_missing_policy`` decides:
    ``skip`` returns ``None`` (fail closed; caller must treat it as a
    retryable skip), ``full_close`` preserves the legacy behaviour of
    dumping the whole held position.
    """
    leader_fraction = _extract_leader_sell_fraction(raw_trade)
    if leader_fraction is None:
        if missing_policy != "full_close":
            logger.warning(
                "sell_fraction_missing_skipped | cond=%s leader=%s unavailable=%s "
                "— skipping copy sell (fail closed)",
                signal.condition_id[:12],
                raw_trade.get("_leader_id", "")[:12],
                bool(raw_trade.get("_leader_sell_fraction_unavailable")),
                extra={
                    "type": "sell_fraction_missing_skipped",
                    "condition_id": signal.condition_id,
                },
            )
            return None
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
    market_min_size: Decimal | float,
    live_price: Decimal = Decimal("1"),
    sub_min_sell_policy: str = "round_up",
) -> Decimal:
    """Resolve leader-follow SELL size without decision-gate scoring.

    Leader SELLs are authoritative exits: copy the same fraction of our held
    position, capped by what we can actually sell. The executable minimum is
    the larger of the dynamic share minimum and the shares worth $1 at the
    copied price. ``sub_min_sell_policy`` controls sub-minimum fractions:

    - ``round_up`` (legacy): sell the effective minimum when holdings cover
      it; otherwise fail closed without submitting a known-invalid order.
    - ``skip`` (fail-closed, Phase 2.8): drop the copy sell (return 0) rather
      than overshoot the leader's intent. The signal is retried on the next
      poll when the leader's remaining fraction crosses the minimum.

    Risk-management exits may still use their own logic, but leader SELLs
    should not be blocked by copy-decision scoring.
    """
    max_sell_tokens = max(Decimal("0"), max_sell_tokens)
    target = min(max(Decimal("0"), requested_sell_tokens), max_sell_tokens)
    if target <= 0:
        return Decimal("0")

    min_tokens = _LEADER_SELL_MINIMUM_POLICY.required_shares(
        price=live_price,
        market_min_size=market_min_size,
    )
    if min_tokens > 0 and target < min_tokens:
        if str(sub_min_sell_policy).strip().lower() == "skip":
            # Fail-closed: do not overshoot a sub-minimum proportional sell.
            return Decimal("0")
        return min_tokens if max_sell_tokens >= min_tokens else Decimal("0")
    return target


def _explicit_copier_exposure_cap(context: TradingContext) -> Decimal | None:
    """The copier-level open-exposure cap, but ONLY when explicitly configured.

    Schema v2 ``limits.max_open_exposure_quote`` (and an explicit v1 follow
    field) mark the cap as operator policy; every legacy/default-derived
    value stays informational and returns ``None`` here so behavior is
    exactly as before the cap existed.
    """
    if not bool(getattr(context, "market_exposure_cap_explicit", False)):
        return None
    try:
        cap = Decimal(str(getattr(context, "max_market_exposure_usdc", 0)))
    except Exception:
        return None
    return cap if cap > 0 else None


def copier_open_exposure_headroom(
    *,
    cap: Decimal,
    open_exposure: Decimal | None,
    reserved_quote: Decimal,
) -> Decimal | None:
    """Headroom for one more copied entry under a copier open-exposure cap.

    The exposure basis is the existing cost-basis accounting
    (``AccountRiskSnapshot.total_exposure_usdc`` — shares x entry price over
    open positions; the same valuation the account-level risk policy uses).
    ``reserved_quote`` adds the account's ACTIVE BUY reservations so
    in-flight entries count toward the cap before their fills confirm.
    Unknown open exposure (``None``) is never treated as zero: the result is
    ``None`` and the caller must fail closed.
    """
    if open_exposure is None:
        return None
    return cap - open_exposure - reserved_quote


async def _fetch_balance_snapshot(
    context: TradingContext,
    condition_id: str,
    leader_id: str,
) -> AccountRiskSnapshot | None:
    """Fetch cash and scoped inventory once; keep missing marks and cost distinct."""
    balance_usdc = await _pipeline_facade_attr("get_my_balance", get_my_balance)(_funder_wallet_address(context))
    try:
        free_cash = PerformanceUnits.finite(balance_usdc)
    except ValueError:
        return None
    if free_cash is None or free_cash < 0:
        return None
    _seed_position_pct_cap_snapshot_if_stale(context, free_cash)
    positions = await context.positions_repo.list_held_positions(context.user_id)
    return AccountRiskSnapshotBuilder(
        context.market_quotes,
        max_age_seconds=_trade_config(context).market_data.executable_quote_max_age_seconds,
    ).build(
        account=context.user_id,
        free_cash=free_cash,
        positions=positions,
        condition_id=condition_id,
        leader_id=leader_id,
    )


async def _position_pct_cap_balance_usdc(context: TradingContext) -> Decimal | None:
    now = time.time()
    snapshot = resolve_account_execution_state(context).position_pct_cap_snapshot
    if _position_pct_cap_snapshot_is_fresh(context, now=now) and snapshot.balance_usdc is not None:
        return snapshot.balance_usdc

    wallet = _funder_wallet_address(context)
    refreshed = await _pipeline_facade_attr("get_my_balance", get_my_balance)(wallet)
    if refreshed is None:
        return None
    seed_position_pct_cap_snapshot(context, refreshed, timestamp=now)
    return _to_decimal(refreshed)


async def _resolve_position_pct_cap(
    context: TradingContext,
    signal: TradeSignal,
    snapshot: AccountRiskSnapshot,
) -> _BuyRiskCap:
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
            cap_notional_usdc=snapshot.free_cash_usdc,
        )

    cap_balance_usdc = await _position_pct_cap_balance_usdc(context)
    if cap_balance_usdc is None:
        return _BuyRiskCap(
            allow=False,
            reason="position_pct_cap_balance_unavailable",
            policy_name="max_position_pct",
            cap_notional_usdc=Decimal("0"),
        )

    # Coarse wallet-snapshot per-BUY cap. Existing position exposure is
    # enforced by the market/account exposure policies; MAX_POSITION_PCT only
    # limits how much of the startup/24h wallet snapshot this new BUY may
    # consume.
    cap_notional_usdc = max(Decimal("0"), cap_balance_usdc * max_position_pct)
    if cap_notional_usdc <= 0:
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
        cap_notional_usdc=cap_notional_usdc,
    )


async def _resolve_buy_risk_cap(
    context: TradingContext,
    leader_id: str,
    signal: TradeSignal,
    snapshot: AccountRiskSnapshot,
) -> _BuyRiskCap:
    equity_usdc = snapshot.equity_usdc
    free_cash_usdc = snapshot.free_cash_usdc
    missing_reason = None
    if equity_usdc is None:
        missing_reason = "marked_equity_unavailable"
    elif (
        snapshot.total_exposure_usdc is None
        or snapshot.market_exposure_usdc is None
        or snapshot.leader_exposure_usdc is None
    ):
        missing_reason = "position_cost_basis_unavailable"
    day_start_equity = Decimal(str(_trade_config(context).risk.day_start_equity_usdc))
    if missing_reason is None and day_start_equity <= 0:
        missing_reason = "account_day_start_equity_unavailable"
    if missing_reason is not None:
        return _BuyRiskCap(
            allow=False,
            reason=missing_reason,
            policy_name="account_cap",
            cap_notional_usdc=Decimal(0),
        )
    assert equity_usdc is not None
    assert snapshot.market_exposure_usdc is not None
    assert snapshot.leader_exposure_usdc is not None
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


def _buy_required_cash_with_fees(
    notional_usdc: Decimal,
    fee_bps: int,
) -> Decimal:
    """Cash a BUY actually consumes once venue fees are added (P2-e).

    A BUY that is affordable on notional alone can still be unaffordable once
    fees are charged on top. ``required_cash = notional * (1 + fee_bps/10_000)``
    in Decimal so the headroom/affordability comparison never silently lets a
    BUY through that the wallet cannot cover net-of-fees. Fees affect ONLY this
    affordability gate — never order sizing or the effective-idempotency key.
    """
    if notional_usdc <= 0:
        return Decimal("0")
    fee_multiplier = Decimal("1") + (Decimal(int(fee_bps)) / BASIS_POINTS_PER_UNIT)
    return notional_usdc * fee_multiplier


async def _buy_fee_bps_or_none(context: TradingContext, token_id: str) -> int | None:
    """Best-effort venue fee in bps for the BUY affordability gate.

    Returns ``None`` when no fee source is wired (configless/legacy/test paths)
    so the caller can keep the pre-existing no-fee cash gates authoritative
    instead of regressing previously-allowed trades. ``FeeService.get_fee_bps``
    is itself cached and never raises (it falls back to its default bps), so a
    non-None result is always a usable integer.
    """
    exec_service = getattr(context, "execution_service", None)
    fee_service = getattr(exec_service, "fee_service", None)
    getter = getattr(fee_service, "get_fee_bps", None)
    if getter is None:
        return None
    try:
        fee_bps = await getter(token_id)
    except Exception as exc:  # defensive — get_fee_bps should not raise
        logger.warning(
            "buy_fee_lookup_failed | token=%s error=%s",
            shorten_addr(token_id, 8, 6),
            repr(exc),
        )
        return None
    try:
        return int(fee_bps)
    except (TypeError, ValueError):
        return None


def normalize_signals(
    raw_signals: list[dict[str, Any]], leader_id: str
) -> list[tuple[TradeSignal, dict[str, Any]]]:
    """Normalize raw leader rows into immutable execution signals."""
    out: list[tuple[TradeSignal, dict[str, Any]]] = []
    for raw in raw_signals:
        side = str(raw.get("side") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            continue

        condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "")
        token_id = str(raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or "")
        tx_hash = str(
            raw.get("transactionHash")
            or raw.get("txHash")
            or raw.get("transaction_hash")
            or raw.get("tx_hash")
            or ""
        )
        if not condition_id or not token_id or not tx_hash:
            # A detected TRADE that is missing core market identifiers cannot be
            # copied. Surface a concise line so the operator is not left with a
            # silent "NEW TRADE DETECTED" followed by nothing.
            print_warning(
                f"Ignored — missing market identifiers "
                f"({side} {raw.get('title') or raw.get('slug') or 'unknown market'})"
            )
            continue
        if not is_valid_clob_token_id(token_id):
            logger.warning("normalize_signals: skipping signal with invalid token_id=%r", token_id)
            continue

        size = _to_decimal(raw.get("size") or 0)
        price = _to_decimal(raw.get("price") or 0)
        usdc_size = _to_decimal(raw.get("usdcSize") or 0)
        if usdc_size <= 0 and size > 0 and price > 0:
            usdc_size = size * price

        try:
            source_time = SourceTimestampParser.parse(
                raw.get("timestamp"), unit=raw.get("timestamp_unit")
            )
        except ValueError:
            source_time = None
        ts = None if source_time is None else int(source_time.timestamp())

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
            source_action_id=raw.get("source_action_id"),
        )
        out.append((signal, raw))
    return out


