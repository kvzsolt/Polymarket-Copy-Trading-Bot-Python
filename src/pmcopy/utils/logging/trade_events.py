"""Structured JSONL audit loggers for trades, orders, drift, risk, and heartbeats."""

from __future__ import annotations

import logging
from typing import Any

from pmcopy.utils.logging.formatters import (
    _ORDER_LIFECYCLE_LAST,
    _lifecycle_dedup_key,
    _lifecycle_should_emit,
)
from pmcopy.utils.logging.printers import (
    _response_shape,
    event_link,
    format_usdc,
    redact_error_detail,
    safe_error_detail,
    shorten_addr,
    tx_link,
)


def _log_trade(
    logger: logging.Logger,
    *,
    label: str,
    trade_type: str,
    side: str,
    condition_id: str | None,
    event_slug: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    tx_hash: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    def _f(v, fmt=".4f"):
        return format(float(v), fmt) if v is not None else "0"

    parts = [
        f"{label} {side.upper()}",
        f"cond={condition_id}",
        f"asset={asset}",
        f"size={_f(size)}",
        f"price={_f(price)}",
        format_usdc(usdc_value),
    ]
    tx_url = tx_link(tx_hash)
    ev_url = event_link(event_slug)
    if tx_hash:
        parts.append(f"tx={tx_hash}")
    if ev_url:
        parts.append(ev_url)

    extras: dict[str, Any] = {
        "type": trade_type,
        "side": side.upper(),
        "conditionId": condition_id,
        "asset": asset,
        "size": size,
        "price": price,
        "usdcValue": usdc_value,
    }
    if tx_hash:
        extras["txHash"] = tx_hash
        extras["txUrl"] = tx_url
    if ev_url:
        extras["eventUrl"] = ev_url
    if meta:
        extras.update(meta)

    logger.info(" | ".join(parts), extra=extras)


def log_api_error(
    logger: logging.Logger,
    *,
    side: str,
    error: str | dict | None = None,
    condition_id: str | None = None,
    asset: str | None = None,
    retry: int = 0,
    max_retries: int = 3,
    meta: dict[str, Any] | None = None,
) -> None:
    # ``error_detail`` lands in the JSONL audit log via the ``extra``
    # payload below. Pre-fix it carried ``str(error)`` verbatim, which
    # for aiohttp / CLOB SDK exceptions can embed the signed Authorization
    # header, the HMAC signature, or the full RPC URL with embedded API
    # key. Route through ``safe_error_detail`` to get a category-only
    # representation (matches the structured ``error`` field above) so
    # the JSONL line can never carry credentials.
    """Record api error."""
    redacted_message = redact_error_detail(error)
    redacted_detail = safe_error_detail(error) if error else "Unknown error"
    payload = {
        "type": "api_error",
        "side": side.upper(),
        "error": redacted_message,
        "error_detail": redacted_detail,
        "conditionId": condition_id,
        "asset": asset,
        "retry": retry,
        "maxRetries": max_retries,
        **(meta or {}),
    }
    logger.error(
        f"API ERROR {side.upper()} | {redacted_message} | cond={condition_id}",
        extra=payload,
    )


def log_trade_execution(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    tx_hash: str | None = None,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Record trade execution."""
    _log_trade(
        logger,
        label="EXECUTED",
        trade_type="trade_execution",
        side=side,
        condition_id=condition_id,
        event_slug=event_slug,
        asset=asset,
        size=size,
        price=price,
        usdc_value=usdc_value,
        tx_hash=tx_hash,
        meta=meta,
    )


def log_trade_detected(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    tx_hash: str | None = None,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Record trade detected."""
    _log_trade(
        logger,
        label="DETECTED",
        trade_type="trade_detected",
        side=side,
        condition_id=condition_id,
        event_slug=event_slug,
        asset=asset,
        size=size,
        price=price,
        usdc_value=usdc_value,
        tx_hash=tx_hash,
        meta=meta,
    )


def log_trade_batch_summary(
    logger: logging.Logger,
    *,
    leader_wallet: str,
    trades: list[dict[str, Any]],
) -> None:
    """Record trade batch summary."""
    buy_count = 0
    sell_count = 0
    gross_usdc = 0.0
    merged_fills = 0
    for trade in trades:
        side = str(trade.get("side") or "").upper()
        if side == "BUY":
            buy_count += 1
        elif side == "SELL":
            sell_count += 1
        gross_usdc += float(trade.get("usdcSize") or 0)
        merged_fills += max(0, int(trade.get("_aggregatedCount") or 1) - 1)

    logger.info(
        "trade_batch leader=%s trades=%d buy=%d sell=%d gross_usdc=%.2f merged_fills=%d",
        shorten_addr(leader_wallet),
        len(trades),
        buy_count,
        sell_count,
        gross_usdc,
        merged_fills,
        extra={
            "type": "trade_batch_summary",
            "leader": leader_wallet,
            "trades": len(trades),
            "buyCount": buy_count,
            "sellCount": sell_count,
            "grossUsdc": gross_usdc,
            "mergedFills": merged_fills,
        },
    )


def log_trade_submitted(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Record trade submitted."""
    _log_trade(
        logger,
        label="SUBMIT",
        trade_type="trade_submitted",
        side=side,
        condition_id=condition_id,
        event_slug=event_slug,
        asset=asset,
        size=size,
        price=price,
        usdc_value=usdc_value,
        meta=meta,
    )


def log_order_lifecycle(
    logger: logging.Logger,
    *,
    status: str,
    side: str,
    condition_id: str | None,
    asset: str | None,
    order_id: str | None,
    size: float | None = None,
    price: float | None = None,
    usdc_value: float | None = None,
    venue_status: str | None = None,
    submission_state: str | None = None,
    tx_hash: str | None = None,
    event_slug: str | None = None,
    signal_id: str | None = None,
    account: str | None = None,
    meta: dict[str, Any] | None = None,
    force: bool = False,
) -> None:
    """Emit a transition-only structured log for order lifecycle progress.

    The handler renders this as a Rich panel (status → color/icon defined in
    _ORDER_LIFECYCLE_PANEL). Repeat calls with the same dedup key + status are
    suppressed unless ``force=True``.

    ``account`` is an optional stable account/user identifier that scopes the
    dedup key when no venue order id exists, so two copiers reacting to the
    same signal/condition keep independent lifecycle streams. Omitted
    (``None``), the legacy single-copier key and payload are produced exactly.
    """
    canonical = (status or "").strip().lower()
    key = _lifecycle_dedup_key(order_id, signal_id, condition_id, account)
    if not force and not _lifecycle_should_emit(key, canonical):
        return

    payload: dict[str, Any] = {
        "type": "order_lifecycle",
        "status": canonical,
        "side": (side or "").upper(),
        "venueStatus": venue_status,
        "submissionState": submission_state,
        "conditionId": condition_id,
        "asset": asset,
        "copyOrderId": order_id,
        "size": size,
        "price": price,
        "usdcValue": usdc_value,
        "txHash": tx_hash,
        "signalId": signal_id,
    }
    if account:
        payload["account"] = account
    ev_url = event_link(event_slug)
    if ev_url:
        payload["eventUrl"] = ev_url
    if meta:
        payload.update(meta)

    def _f(v, fmt=".4f"):
        return format(float(v), fmt) if v is not None else "0"

    msg = (
        f"LIFECYCLE {canonical} | side={(side or '').upper()} | "
        f"cond={condition_id} | asset={asset} | order_id={order_id} | "
        f"size={_f(size)} price={_f(price)} {format_usdc(usdc_value)}"
    )
    logger.info(msg, extra=payload)


def _reset_lifecycle_dedup_for_tests() -> None:
    """Test helper — clears the per-key dedup map."""
    _ORDER_LIFECYCLE_LAST.clear()


def log_position_drift(
    logger: logging.Logger,
    *,
    status: str,
    action: str,
    user_id: str,
    condition_id: str | None,
    asset: str | None,
    local_size: float | None = None,
    venue_size: float | None = None,
    drift_size: float | None = None,
    reason: str | None = None,
    reservation_ids: list[str] | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Emit a structured ``position_drift`` log (PR7).

    Renders via ``RichConsoleHandler._render_position_drift`` and writes a
    JSONL record. No dedup — every drift observation is operator-visible.
    """
    payload: dict[str, Any] = {
        "type": "position_drift",
        "status": (status or "").lower(),
        "action": (action or "").lower(),
        "userId": user_id,
        "conditionId": condition_id,
        "asset": asset,
        "localSize": local_size,
        "venueSize": venue_size,
        "driftSize": drift_size,
        "reason": reason,
        "reservationIds": reservation_ids or [],
    }
    if meta:
        payload.update(meta)
    logger.info(
        "POSITION_DRIFT %s/%s | user=%s | cond=%s | asset=%s | local=%s venue=%s drift=%s | %s",
        payload["status"],
        payload["action"],
        user_id,
        condition_id,
        asset,
        local_size,
        venue_size,
        drift_size,
        reason or "",
        extra=payload,
    )


def log_trade_decision(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    decision_score: float,
    allow: bool,
    reason: str,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Record trade decision."""
    payload: dict[str, Any] = {
        "type": "trade_decision",
        "side": side.upper(),
        "conditionId": condition_id,
        "asset": asset,
        "decisionScore": decision_score,
        "allow": allow,
        "reason": reason,
    }
    ev_url = event_link(event_slug)
    if ev_url:
        payload["eventUrl"] = ev_url
    if meta:
        payload.update(meta)
    logger.info(
        "DECISION %s | side=%s | cond=%s | asset=%s | score=%.4f | reason=%s",
        "ALLOW" if allow else "SKIP",
        side.upper(),
        condition_id,
        asset,
        float(decision_score),
        reason,
        extra=payload,
    )


def log_trade_skip(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    skip_code: str,
    reason: str,
    status: str = "skipped",
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Record trade skip."""
    _log_trade(
        logger,
        label=status.upper(),
        trade_type="trade_skip",
        side=side,
        condition_id=condition_id,
        event_slug=event_slug,
        asset=asset,
        size=size,
        price=price,
        usdc_value=usdc_value,
        meta={
            "skipCode": skip_code,
            "skipReason": reason,
            "status": status,
            **(meta or {}),
        },
    )


def log_risk_exit(
    logger: logging.Logger,
    *,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    reason: str,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Record risk exit."""
    _log_trade(
        logger,
        label="RISK_EXIT",
        trade_type="risk_exit",
        side="SELL",
        condition_id=condition_id,
        event_slug=event_slug,
        asset=asset,
        size=size,
        price=price,
        usdc_value=usdc_value,
        meta={"riskReason": reason, **(meta or {})},
    )


def log_monitor_heartbeat(
    logger: logging.Logger,
    *,
    interval_s: int,
    checked_activities: int,
    leader_wallet: str | None = None,
    monitored_leader_count: int | None = None,
    symbol: str = "🟢",
) -> None:
    """Record monitor heartbeat."""
    leader_label = shorten_addr(leader_wallet) if leader_wallet else "multi-leader"
    leader_summary = (
        f"leaders={monitored_leader_count} total | current={leader_label}"
        if monitored_leader_count and monitored_leader_count > 1
        else f"leader={leader_label}"
    )
    logger.info(
        f"{symbol} Trade Monitor is running and healthy | {leader_summary} | "
        f"no new trades in last {interval_s}s | checked={checked_activities}",
        extra={
            "type": "monitor_heartbeat",
            "intervalSeconds": interval_s,
            "checkedActivities": checked_activities,
            "leaderWallet": leader_wallet,
            "monitoredLeaderCount": monitored_leader_count,
            "status": "healthy",
            "symbol": symbol,
        },
    )


def log_api_response(
    logger: logging.Logger,
    *,
    method: str,
    url: str,
    status: int,
    data: Any = None,
    error: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Write a structured JSONL entry for every external API response without raw payloads."""
    response_shape = _response_shape(data)

    ok = 200 <= status < 400
    msg = f"API {method} {status} {'OK' if ok else 'ERR'} {url}"

    extras: dict[str, Any] = {
        "type": "api_response",
        "httpMethod": method,
        "url": url,
        "httpStatus": status,
    }
    if response_shape is not None:
        extras["response_shape"] = response_shape

    # Restore rich data_summary for safe fields (v5 behavior)
    _SAFE_DATA_FIELDS = (
        "success",
        "orderID",
        "order_id",
        "status",
        "error",
        "price",
        "size",
        "min_order_size",
        "transactionHash",
    )
    if isinstance(data, dict):
        summary = {k: data[k] for k in _SAFE_DATA_FIELDS if k in data}
        if summary:
            extras["data_summary"] = summary

    if error:
        extras["error"] = redact_error_detail(error)
        # Never write the raw error string into the audit log: provider
        # error bodies can echo signed URLs, Authorization headers, or key
        # material. Route through the redacting safe detail helper.
        extras["error_detail"] = safe_error_detail(error)
    if meta:
        extras.update(meta)

    if ok:
        logger.info(msg, extra=extras)
    else:
        logger.warning(msg, extra=extras)
