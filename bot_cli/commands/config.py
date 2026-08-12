"""bot config — print, validate, and explain configuration."""

from __future__ import annotations

import argparse
import json

from config.app_config import load_config, AppConfig, EnvValidationError


# Keys that should be redacted when printing config
_SECRET_KEYS = frozenset(
    {
        "PRIVATE_KEY",
        "private_key",
        "CLOB_API_KEY",
        "CLOB_SECRET",
        "CLOB_PASSPHRASE",
        "DATABASE_URL",
        "api_key",
        "api_secret",
        "api_passphrase",
        "api_token",
        "token",
        "dsn",
    }
)

_REDACTED = "***REDACTED***"


def _redact_value(key: str, value: object) -> object:
    """Redact secret values for safe display."""
    key_lower = key.lower()
    for secret in _SECRET_KEYS:
        if secret.lower() in key_lower:
            return _REDACTED
    return value


def _config_to_dict(cfg: AppConfig, *, redacted: bool = True) -> dict:
    """Recursively convert an AppConfig to a dict, optionally redacting secrets."""
    import dataclasses
    from decimal import Decimal
    from pydantic import BaseModel

    def _convert(obj: object) -> object:
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            result = {}
            for f in dataclasses.fields(obj):
                val = getattr(obj, f.name)
                display_key = f.name
                display_val = _convert(val)
                if redacted:
                    display_val = _redact_value(display_key, display_val)
                result[display_key] = display_val
            return result
        if isinstance(obj, BaseModel):
            return _convert(obj.model_dump(mode="python"))
        if isinstance(obj, (list, tuple)):
            return [_convert(item) for item in obj]
        if isinstance(obj, dict):
            result_dict: dict = {}
            for k, v in obj.items():
                converted = _convert(v)
                if redacted and isinstance(k, str):
                    converted = _redact_value(k, converted)
                result_dict[k] = converted
            return result_dict
        if isinstance(obj, Decimal):
            return str(obj)
        return obj

    converted = _convert(cfg)
    if not isinstance(converted, dict):
        raise TypeError("application configuration must serialize to an object")
    return converted


# ── Config key explanations ──────────────────────────────────────────────────

_CONFIG_EXPLANATIONS: dict[str, str] = {
    "EXECUTION.TRADING_MODE": "Immutable startup selector: paper or live.",
    "LEADER.WALLETS": "Configured Ethereum leader addresses; discovery roster leaders are added automatically.",
    "WALLET.FUNDER_ADDRESS": "Your Polymarket proxy-wallet address.",
    "CHAIN_ID": "Polygon chain ID: 137 (mainnet) or 80002 (testnet).",
    "SIZING_MODE": "Trade sizing strategy: 'proportional' (copy % of leader), 'fixed' (fixed USDC), or 'adaptive'.",
    "PERCENTAGE": "When SIZING_MODE=proportional, the % of leader trade size to copy.",
    "FIXED_AMOUNT": "When SIZING_MODE=fixed, the USDC amount per trade.",
    "BUY_MIN_AMOUNT": "Minimum USDC size for a BUY order.",
    "BUY_MAX_AMOUNT": "Maximum USDC size for a BUY order.",
    "MAX_POSITION_PCT": "Max fraction of balance to risk on a single position (0.0–1.0).",
    "MAX_MARKET_EXPOSURE_USDC": "Maximum total USDC exposure per market.",
    "MAX_LEADER_EXPOSURE_USDC": "Maximum total USDC exposure per leader.",
    "MAX_OPEN_POSITIONS": "Maximum number of concurrent open positions.",
    "MAX_DAILY_DRAWDOWN_PCT": "Max daily drawdown percentage before risk engine halts trading.",
    "KILL_SWITCH": "Emergency switch — if true, the bot will not trade.",
    "ENABLE_RISK_MGMT": "Enable the risk management monitor loop.",
    "SLIPPAGE_TOLERANCE": "Max acceptable slippage % before rejecting an order.",
    "MIN_SHARE_PRICE": "Minimum acceptable BUY price (0 disables).",
    "MAX_SHARE_PRICE": "Maximum acceptable BUY price (0 disables).",
    "FETCH_INTERVAL": "Seconds between leader trade polling cycles.",
    "RISK_CHECK_INTERVAL": "Seconds between risk monitor evaluations.",
    "RECONCILE_INTERVAL_SECONDS": "Seconds between reconciliation passes.",
    "PENDING_SUBMISSION_TTL_SECONDS": "Seconds before a pending order is considered stale.",
    "ENFORCE_GEOBLOCK": "If true, check geoblock compliance before trading.",
    "DISCOVERY.SCHEDULER.ENABLED": (
        "Runs the configured Heisenberg funnel, longitudinal ranker, and hysteresis roster scheduler."
    ),
    "DISCOVERY.HYSTERESIS.PROMOTE_SCORE": "Minimum configured score required for consecutive promotion cycles.",
    "DISCOVERY.HYSTERESIS.DEMOTE_SCORE": "Score below which an active leader starts a demotion streak.",
    "COPY_DECISION_MIN_SCORE": "Minimum leader score (0–1) required to copy a trade.",
    "ENABLE_COPY_DECISION_GATE": "If true, gate trades on the leader's copy-decision score.",
    "ENABLE_NEAR_RESOLUTION_GATE": "If true, reject BUY signals near market resolution.",
    "NEAR_RESOLUTION_GATE_PRICE": "Price threshold for near-resolution gating.",
    "BOOK_SPREAD_MODE": "Spread gate mode: 'absolute_bps', 'relative_bps', or 'ticks'.",
    "MAX_BOOK_SPREAD_BPS": "Max book spread in basis points (0 = disabled).",
    "CIRCUIT_FAILURE_THRESHOLD": "Consecutive failures before circuit breaker trips.",
    "CIRCUIT_COOLDOWN_SECONDS": "Seconds to wait after circuit breaker trips.",
    "NETWORK.RPC_URL": "Polygon RPC endpoint URL.",
    "CLOB.HTTP_URL": "Polymarket CLOB HTTP API URL.",
}


def register_config_command(subparsers: argparse._SubParsersAction) -> None:
    """Register the configuration command with the CLI parser."""
    parser = subparsers.add_parser("config", help="Print, validate, and explain configuration")
    config_sub = parser.add_subparsers(dest="config_subcommand", required=True)

    print_parser = config_sub.add_parser("print", help="Print current config")
    print_parser.add_argument(
        "--redacted",
        action="store_true",
        default=True,
        help="Secrets are always redacted",
    )

    config_sub.add_parser("validate", help="Validate config and report errors")

    explain_parser = config_sub.add_parser("explain", help="Explain what a config key does")
    explain_parser.add_argument("key", help="Config key to explain")


def _run_config_print(args: argparse.Namespace) -> int:
    try:
        config = load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return 1

    output = _config_to_dict(config, redacted=True)
    # Remove raw env from output (too noisy)
    output.pop("raw", None)
    print(json.dumps(output, indent=2, default=str))
    return 0


def _run_config_validate(_args: argparse.Namespace) -> int:
    try:
        load_config()
    except EnvValidationError as exc:
        errors = str(exc).split("\n")
        output = {"valid": False, "errors": errors}
        print(json.dumps(output, indent=2))
        return 1

    output = {"valid": True, "errors": []}
    print(json.dumps(output, indent=2))
    return 0


def _run_config_explain(args: argparse.Namespace) -> int:
    key = args.key.upper()
    explanation = _CONFIG_EXPLANATIONS.get(key)
    if explanation is None:
        # Try matching case-insensitively on partial key
        matches = [(k, v) for k, v in _CONFIG_EXPLANATIONS.items() if key in k.upper()]
        if matches:
            output = {
                "key": args.key,
                "matches": [{k: v} for k, v in matches],
            }
        else:
            output = {
                "key": args.key,
                "explanation": None,
                "hint": "No explanation found. Check the config/<service>.yml files.",
            }
    else:
        output = {"key": args.key, "explanation": explanation}

    print(json.dumps(output, indent=2))
    return 0


def run_config(args: argparse.Namespace) -> int:
    """Run the validated configuration CLI command."""
    if args.config_subcommand == "print":
        return _run_config_print(args)
    elif args.config_subcommand == "validate":
        return _run_config_validate(args)
    elif args.config_subcommand == "explain":
        return _run_config_explain(args)
    return 1
