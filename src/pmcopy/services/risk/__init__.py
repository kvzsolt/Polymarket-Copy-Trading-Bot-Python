"""Risk management services — engine, exit policy, monitoring."""

from __future__ import annotations

from typing import TYPE_CHECKING

# Eagerly import lightweight modules
from pmcopy.services.risk.exit_policy import ExitDecision, evaluate_exit

if TYPE_CHECKING:
    from pmcopy.services.risk.risk_engine import (
        AccountCapPolicy,
        PortfolioSnapshot,
        ProposedTrade,
        RiskDecision,
        RiskEngine,
        RiskHeadroom,
        RiskPolicy,
        active_policy,
        get_active_policy,
        get_policy,
        headroom_for_buy,
        init_risk_engine,
        policy_from_config,
        policy_name_for_equity,
    )
    from pmcopy.services.risk.risk_monitor import (
        CachedRiskPrice,
        RiskPriceCache,
        VenueCircuitBreaker,
        reconcile_unconfirmed_risk_orders,
        rehydrate_position_manager,
        risk_monitor_loop_for_contexts,
        run_risk_check_for_context,
    )


# Lazy imports for modules that depend on Web3 or venue clients.
def __getattr__(name: str):
    _lazy_map = {
        # risk_engine
        "AccountCapPolicy": "pmcopy.services.risk.risk_engine",
        "PortfolioSnapshot": "pmcopy.services.risk.risk_engine",
        "ProposedTrade": "pmcopy.services.risk.risk_engine",
        "RiskDecision": "pmcopy.services.risk.risk_engine",
        "RiskEngine": "pmcopy.services.risk.risk_engine",
        "RiskHeadroom": "pmcopy.services.risk.risk_engine",
        "RiskPolicy": "pmcopy.services.risk.risk_engine",
        "active_policy": "pmcopy.services.risk.risk_engine",
        "get_active_policy": "pmcopy.services.risk.risk_engine",
        "get_policy": "pmcopy.services.risk.risk_engine",
        "headroom_for_buy": "pmcopy.services.risk.risk_engine",
        "init_risk_engine": "pmcopy.services.risk.risk_engine",
        "policy_from_config": "pmcopy.services.risk.risk_engine",
        "policy_name_for_equity": "pmcopy.services.risk.risk_engine",
        # risk_monitor
        "CachedRiskPrice": "pmcopy.services.risk.risk_monitor",
        "RiskPriceCache": "pmcopy.services.risk.risk_monitor",
        "VenueCircuitBreaker": "pmcopy.services.risk.risk_monitor",
        "reconcile_unconfirmed_risk_orders": "pmcopy.services.risk.risk_monitor",
        "rehydrate_position_manager": "pmcopy.services.risk.risk_monitor",
        "run_risk_check_for_context": "pmcopy.services.risk.risk_monitor",
        "risk_monitor_loop_for_contexts": "pmcopy.services.risk.risk_monitor",
    }
    if name in _lazy_map:
        import importlib

        mod = importlib.import_module(_lazy_map[name])
        value = getattr(mod, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AccountCapPolicy",
    "CachedRiskPrice",
    "ExitDecision",
    "PortfolioSnapshot",
    "ProposedTrade",
    "RiskDecision",
    "RiskEngine",
    "RiskHeadroom",
    "RiskPriceCache",
    "RiskPolicy",
    "VenueCircuitBreaker",
    "active_policy",
    "evaluate_exit",
    "get_active_policy",
    "get_policy",
    "headroom_for_buy",
    "init_risk_engine",
    "policy_from_config",
    "policy_name_for_equity",
    "reconcile_unconfirmed_risk_orders",
    "rehydrate_position_manager",
    "run_risk_check_for_context",
    "risk_monitor_loop_for_contexts",
]
