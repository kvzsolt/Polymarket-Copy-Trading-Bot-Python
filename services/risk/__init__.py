"""Risk management services — engine, exit policy, monitoring, price cache."""
from __future__ import annotations

# Eagerly import lightweight modules
from services.risk.exit_policy import ExitDecision, evaluate_exit

# Lazy imports for modules that depend on web3/py_clob_client_v2
def __getattr__(name: str):
    _lazy_map = {
        # risk_engine
        "AccountCapPolicy": "services.risk.risk_engine",
        "PortfolioSnapshot": "services.risk.risk_engine",
        "ProposedTrade": "services.risk.risk_engine",
        "RiskDecision": "services.risk.risk_engine",
        "RiskEngine": "services.risk.risk_engine",
        "RiskHeadroom": "services.risk.risk_engine",
        "RiskPolicy": "services.risk.risk_engine",
        "active_policy": "services.risk.risk_engine",
        "headroom_for_buy": "services.risk.risk_engine",
        "init_risk_engine": "services.risk.risk_engine",
        "policy_from_config": "services.risk.risk_engine",
        # risk_monitor
        "CachedRiskPrice": "services.risk.risk_monitor",
        "RiskPriceCache": "services.risk.risk_monitor",
        "VenueCircuitBreaker": "services.risk.risk_monitor",
        "reconcile_unconfirmed_risk_orders": "services.risk.risk_monitor",
        "risk_monitor_loop": "services.risk.risk_monitor",
        "run_risk_check_for_context": "services.risk.risk_monitor",
        # price_cache
        "PositionPriceCache": "services.risk.price_cache",
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
    "PositionPriceCache",
    "ProposedTrade",
    "RiskDecision",
    "RiskEngine",
    "RiskHeadroom",
    "RiskPriceCache",
    "RiskPolicy",
    "VenueCircuitBreaker",
    "active_policy",
    "evaluate_exit",
    "headroom_for_buy",
    "init_risk_engine",
    "policy_from_config",
    "reconcile_unconfirmed_risk_orders",
    "risk_monitor_loop",
    "run_risk_check_for_context",
]
