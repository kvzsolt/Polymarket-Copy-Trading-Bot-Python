from __future__ import annotations

from typing import Any

_RISK_QUARANTINE_PREFIX = "risk_quarantine"


def market_risk_quarantine_key(user_id: str, condition_id: str) -> str:
    return f"{_RISK_QUARANTINE_PREFIX}|{str(user_id or '').strip().lower()}|{str(condition_id or '').strip().lower()}"


def get_market_risk_quarantine(orders_repo: Any, *, user_id: str, condition_id: str) -> dict[str, Any] | None:
    key = market_risk_quarantine_key(user_id, condition_id)
    if not hasattr(orders_repo, "get_processed_trade"):
        return None
    return orders_repo.get_processed_trade(key)


def mark_market_risk_quarantined(
    orders_repo: Any,
    *,
    user_id: str,
    leader_id: str | None,
    condition_id: str,
    asset: str | None,
    reason: str,
) -> None:
    key = market_risk_quarantine_key(user_id, condition_id)
    orders_repo.mark_processed_trade(
        trade_key=key,
        user_id=user_id,
        leader_id=leader_id,
        tx_hash=None,
        side="SYSTEM",
        asset=asset,
        condition_id=condition_id,
        status=f"risk_quarantined_{reason}",
    )
