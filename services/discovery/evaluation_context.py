from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any


def build_evaluation_context(config: Any, *, balance_usdc: Decimal | str | int | float) -> dict[str, Any]:
    balance = str(Decimal(str(balance_usdc or "0")))
    wallet = getattr(config, "wallet", None)
    sizing = getattr(config, "sizing", None)
    execution = getattr(config, "execution", None)
    risk = getattr(config, "risk", None)
    leader = getattr(config, "leader", None)
    discovery = getattr(config, "discovery", None)
    return {
        "wallet_address": str(getattr(wallet, "funder_address", "") or "").lower(),
        "balance_usdc": balance,
        "sizing_mode": str(getattr(sizing, "mode", "") or ""),
        "sizing_percentage": _value(sizing, "percentage"),
        "fixed_amount": _value(sizing, "fixed_amount"),
        "buy_min_amount": _value(sizing, "buy_min_amount"),
        "buy_max_amount": _value(sizing, "buy_max_amount"),
        "trade_multiplier": _value(sizing, "trade_multiplier"),
        "max_position_size_usdc": _value(execution, "max_position_size_usdc"),
        "min_balance_headroom_usdc": _value(execution, "min_balance_headroom_usdc"),
        "max_open_positions": _value(risk, "max_open_positions"),
        "auto_discover_min_score": _value(leader, "auto_discover_min_score"),
        "discovery_min_group_notional_usdc": _value(
            discovery, "discovery_min_group_notional_usdc"
        ),
        "discovery_min_copyable_grouped_signal_ratio": _value(
            discovery, "discovery_min_copyable_grouped_signal_ratio"
        ),
        "discovery_min_copyable_notional_ratio": _value(
            discovery, "discovery_min_copyable_notional_ratio"
        ),
    }


def hash_evaluation_context(context: dict[str, Any]) -> str:
    encoded = json.dumps(context, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_evaluation_context_hash(
    config: Any, *, balance_usdc: Decimal | str | int | float
) -> tuple[str, dict[str, Any]]:
    context = build_evaluation_context(config, balance_usdc=balance_usdc)
    return hash_evaluation_context(context), context


def _value(section: Any, name: str) -> str:
    return str(getattr(section, name, ""))
