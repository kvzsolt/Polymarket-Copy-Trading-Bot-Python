"""Legacy environment configuration module.

.. deprecated:: Phase 6 (Clean Architecture migration)
    This module's ``ENV`` dict is the legacy runtime config surface. New code
    should depend on :class:`config.app_config.AppConfig` and receive it via
    constructor injection (see ``bootstrap.build_app``). ``ENV`` is retained
    only for:

    * The test suite, which patches it extensively.
    * Transitional call sites inside ``services/`` and ``orchestrators/``
      that have not yet been migrated to consume ``AppConfig``.

    Do not introduce new ``from config.env import ENV`` imports in production
    code — thread ``AppConfig`` (or the specific sub-config) through the
    constructor instead.
"""

import os
import re
import sys
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path

from config.addresses import (
    POLYGON_USDCE_ADDRESS,
    POLYMARKET_CTF_COLLATERAL_ADAPTER_ADDRESS,
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    POLYMARKET_PUSD_ADDRESS,
)
from config.runtime_env import load_runtime_env

# NOTE: Do NOT call load_runtime_env() at import time.
# Import-time side effects make the test suite non-deterministic because a local
# `.env` file (developer machine / CI workspace) can silently override defaults.
# Runtime entrypoints (e.g. app.py) should call load_runtime_env() explicitly.

_ETH_ADDR_RE = re.compile(r'^0x[0-9a-fA-F]{40}$')
_HEX_PRIVATE_KEY_RE = re.compile(r'^0x[0-9a-fA-F]{64}$')


class EnvValidationError(ValueError):
    """Raised when runtime environment configuration is malformed."""


class SignatureType(IntEnum):
    EOA = 0
    POLY_PROXY = 1
    GNOSIS_SAFE = 2


@dataclass(frozen=True)
class WalletConfig:
    private_key: str
    chain_id: int
    funder_address: str
    polygon_rpc_url: str
    signature_type: SignatureType | int


def _raw(key: str, default: str = '') -> str:
    return str(os.getenv(key, default) or '')


def _has_inline_comment(raw: str) -> bool:
    # Treat "VALUE # comment" as malformed to avoid ambiguous parsing.
    return (' #' in raw) or ('\t#' in raw)


def _ensure_no_inline_comment(key: str, raw: str) -> str:
    if _has_inline_comment(raw):
        raise EnvValidationError(
            f'{key} contains an inline comment. Use pure values only; '
            f'place comments on separate lines.'
        )
    return raw.strip()


def _configured_if_valid(key: str) -> bool:
    try:
        return bool(_ensure_no_inline_comment(key, _raw(key, '')))
    except EnvValidationError:
        return False


def _float(key: str, default: str) -> float:
    raw = _ensure_no_inline_comment(key, _raw(key, default))
    return float(raw) if raw else float(default)


def _float_or_auto(key: str, default: str) -> float | None:
    raw = _ensure_no_inline_comment(key, _raw(key, default))
    if not raw or raw.lower() == 'auto' or raw.startswith('#'):
        return None
    try:
        return float(raw)
    except ValueError as exc:
        raise EnvValidationError(
            f"{key} must be a valid float or 'auto', got: {raw!r}"
        ) from exc


def _int(key: str, default: str) -> int:
    raw = _ensure_no_inline_comment(key, _raw(key, default))
    return int(raw) if raw else int(default)


def _bool(key: str, default: bool = False) -> bool:
    raw_default = 'true' if default else 'false'
    raw = _ensure_no_inline_comment(key, _raw(key, raw_default))
    return raw.strip().lower() in ('1', 'true', 'yes')


def _runtime_python_supported() -> bool:
    return (sys.version_info.major == 3) and (11 <= sys.version_info.minor < 13)


def _validate_python_runtime() -> None:
    if _runtime_python_supported():
        return
    raise EnvValidationError(
        f"Unsupported Python runtime {sys.version_info.major}.{sys.version_info.minor}. "
        "Use Python 3.11.x or 3.12.x."
    )


def is_valid_eth_address(addr: str) -> bool:
    """Return True if *addr* is a well-formed 42-char hex Ethereum address."""
    return bool(_ETH_ADDR_RE.match(addr))


def _leader_wallets() -> list[str]:
    """Parse LEADER_WALLETS (comma-separated) with USER_ADDRESS as fallback.

    Validates each address is a proper 42-character Ethereum address
    (required by the Polymarket data API).
    """
    raw = _ensure_no_inline_comment('LEADER_WALLETS', _raw('LEADER_WALLETS', ''))
    if raw:
        wallets = [w.strip() for w in raw.split(',') if w.strip()]
    else:
        single = _ensure_no_inline_comment('USER_ADDRESS', _raw('USER_ADDRESS', ''))
        wallets = [single] if single else []

    invalid = [w for w in wallets if not is_valid_eth_address(w)]
    if invalid:
        import warnings
        for w in invalid:
            warnings.warn(
                f"Invalid Ethereum address '{w}' in LEADER_WALLETS/USER_ADDRESS — "
                f"must be 42-char hex (0x + 40 hex digits). "
                f"The Polymarket API will reject this address."
            )
    return wallets


def _load_secret_from_file(path_str: str, env_name: str) -> str:
    path = Path(path_str).expanduser()
    if not path.exists():
        raise EnvValidationError(f'{env_name}_FILE path does not exist: {path}')
    if not path.is_file():
        raise EnvValidationError(f'{env_name}_FILE must point to a file: {path}')

    try:
        mode = path.stat().st_mode & 0o777
        if mode != 0o600:
            raise EnvValidationError(
                f'{env_name}_FILE must have permissions 0600 exactly (chmod 600): {path}'
            )
    except OSError as exc:
        raise EnvValidationError(f'Unable to read permissions for {env_name}_FILE: {exc}') from exc

    try:
        secret = path.read_text(encoding='utf-8').strip()
    except OSError as exc:
        raise EnvValidationError(f'Unable to read {env_name}_FILE: {exc}') from exc

    if not secret:
        raise EnvValidationError(f'{env_name}_FILE is empty: {path}')
    return secret


def get_private_key() -> str:
    """Load PRIVATE_KEY from env or PRIVATE_KEY_FILE with strict validation."""
    key_file = _ensure_no_inline_comment('PRIVATE_KEY_FILE', _raw('PRIVATE_KEY_FILE', ''))
    if key_file:
        private_key = _load_secret_from_file(key_file, 'PRIVATE_KEY')
    else:
        private_key = _ensure_no_inline_comment('PRIVATE_KEY', _raw('PRIVATE_KEY', ''))

    if not private_key:
        return ''
    if not _HEX_PRIVATE_KEY_RE.match(private_key):
        raise EnvValidationError(
            'PRIVATE_KEY must be a 66-char hex key (0x + 64 hex digits).'
        )
    return private_key


def get_chain_id() -> int:
    return _int('CHAIN_ID', '137')


def get_funder_address() -> str:
    funder = _ensure_no_inline_comment('FUNDER_ADDRESS', _raw('FUNDER_ADDRESS', ''))
    if funder:
        return funder
    return _ensure_no_inline_comment('PROXY_WALLET', _raw('PROXY_WALLET', ''))


def _derive_signer_address_sync(private_key: str) -> str | None:
    try:
        from web3 import Web3
        return str(Web3().eth.account.from_key(private_key).address)
    except Exception:
        return None


def get_signature_type() -> SignatureType:
    """Return a synchronous best-guess signature type without RPC probing.

    For production use, prefer ``detect_wallet_signature_type()`` which
    performs RPC contract-code detection.  This fallback is kept for
    config building where async is not available.
    """
    from services.wallet_signature_detector import _classify_wallet_signature_type

    pk = get_private_key()
    signer = _derive_signer_address_sync(pk) if pk else None
    funder = get_funder_address()
    st, _label = _classify_wallet_signature_type(
        signer_address=signer,
        funder_address=funder,
        funder_has_code=False,
    )
    return st


def get_metrics_bind_host() -> str:
    return _ensure_no_inline_comment('METRICS_BIND_HOST', _raw('METRICS_BIND_HOST', '127.0.0.1'))


def get_fallback_rpc_url() -> str:
    """Return the fallback RPC URL if configured, otherwise empty string."""
    return _ensure_no_inline_comment('FALLBACK_RPC_URL', _raw('FALLBACK_RPC_URL', ''))


def build_wallet_config() -> WalletConfig:
    return WalletConfig(
        private_key=get_private_key(),
        chain_id=get_chain_id(),
        funder_address=get_funder_address(),
        polygon_rpc_url=_ensure_no_inline_comment('RPC_URL', _raw('RPC_URL', '')),
        signature_type=get_signature_type(),
    )


def validate_wallet_config(cfg: WalletConfig) -> None:
    if not cfg.private_key:
        raise EnvValidationError('PRIVATE_KEY is required.')
    if not cfg.funder_address:
        raise EnvValidationError('FUNDER_ADDRESS is required.')
    if not is_valid_eth_address(cfg.funder_address):
        raise EnvValidationError(
            f'Invalid FUNDER_ADDRESS "{cfg.funder_address}" (expected 42-char Ethereum address).'
        )
    if cfg.chain_id not in (137, 80002):
        raise EnvValidationError(f'Unsupported CHAIN_ID "{cfg.chain_id}". Use 137 or 80002.')
    if not cfg.polygon_rpc_url:
        raise EnvValidationError('RPC_URL is required.')


def validate_runtime_env() -> None:
    """Fail fast on malformed runtime config before the bot starts trading."""
    errors: list[str] = []

    try:
        _validate_python_runtime()
    except EnvValidationError as exc:
        errors.append(str(exc))

    # Inline-comment ambiguity checks for commonly used keys.
    for key in (
        'USER_ADDRESS',
        'LEADER_WALLETS',
        'LEADER_AUTO',
        'LEADER_AUTO_COUNT',
        'AUTO_DISCOVER_LEADERS',
        'AUTO_DISCOVER_TOP_N',
        'DISCOVERY_PROMOTE_TO_ACTIVE',
        'ENABLE_TRADER_DISCOVERY',
        'FUNDER_ADDRESS',
        'SIGNATURE_TYPE',
        'CHAIN_ID',
        'MULTI_USER_CONFIG_FILE',
        'RPC_URL',
        'FALLBACK_RPC_URL',
        'PNL_LOG_FILE',
        'CLOB_HTTP_URL',
        'METRICS_BIND_HOST',
    ):
        try:
            _ensure_no_inline_comment(key, _raw(key, ''))
        except EnvValidationError as exc:
            errors.append(str(exc))

    # Private key validation
    try:
        get_private_key()
    except EnvValidationError as exc:
        errors.append(str(exc))

    # Address validation
    wallets = _leader_wallets()
    for wallet in wallets:
        if not is_valid_eth_address(wallet):
            errors.append(
                f'Invalid leader wallet "{wallet}" (expected 42-char Ethereum address).'
            )
    funder_address = get_funder_address()
    if funder_address and not is_valid_eth_address(funder_address):
        errors.append(
            f'Invalid FUNDER_ADDRESS "{funder_address}" (expected 42-char Ethereum address).'
        )

    # Wallet model validation
    try:
        validate_wallet_config(build_wallet_config())
    except EnvValidationError as exc:
        errors.append(str(exc))

    # Basic guardrails for risk-critical numeric settings.
    if not (0 < ENV.get('MAX_POSITION_PCT', 0.0) <= 1.0):
        errors.append('MAX_POSITION_PCT must be in (0, 1].')
    if ENV.get('TRADE_MULTIPLIER', 1.0) < 0:
        errors.append('TRADE_MULTIPLIER must be non-negative.')
    adaptive_basis = str(ENV.get('ADAPTIVE_SIZING_BASIS', 'trade_size') or 'trade_size').strip().lower()
    if adaptive_basis not in ('trade_size', 'performance'):
        errors.append('ADAPTIVE_SIZING_BASIS must be one of: trade_size, performance.')
    if ENV.get('ADAPTIVE_MIN_PERCENT', 0.0) < 0:
        errors.append('ADAPTIVE_MIN_PERCENT must be non-negative.')
    if ENV.get('ADAPTIVE_MAX_PERCENT', 0.0) < 0:
        errors.append('ADAPTIVE_MAX_PERCENT must be non-negative.')
    if ENV.get('ADAPTIVE_THRESHOLD_USDC', 0.0) <= 0:
        errors.append('ADAPTIVE_THRESHOLD_USDC must be > 0.')
    if (
        ENV.get('ADAPTIVE_MIN_PERCENT', 0.0) > 0
        and ENV.get('ADAPTIVE_MAX_PERCENT', 0.0) > 0
        and ENV.get('ADAPTIVE_MIN_PERCENT', 0.0) > ENV.get('ADAPTIVE_MAX_PERCENT', 0.0)
    ):
        errors.append('ADAPTIVE_MIN_PERCENT must be <= ADAPTIVE_MAX_PERCENT.')
    if ENV.get('SLIPPAGE_TOLERANCE', 0.0) < 0:
        errors.append('SLIPPAGE_TOLERANCE must be non-negative.')
    if ENV.get('MIN_SHARE_PRICE', 0.0) < 0:
        errors.append('MIN_SHARE_PRICE must be non-negative.')
    if ENV.get('MIN_SHARE_PRICE', 0.0) > 1:
        errors.append('MIN_SHARE_PRICE must be <= 1.0 for binary markets.')
    if ENV.get('MAX_SHARE_PRICE', 0.0) < 0:
        errors.append('MAX_SHARE_PRICE must be non-negative.')
    if ENV.get('MAX_SHARE_PRICE', 0.0) > 1:
        errors.append('MAX_SHARE_PRICE must be <= 1.0 for binary markets.')
    if ENV.get('MAX_SHARE_PRICE', 0.0) > 0 and ENV.get('MAX_SHARE_PRICE', 0.0) < ENV.get('MIN_SHARE_PRICE', 0.0):
        errors.append('MAX_SHARE_PRICE must be >= MIN_SHARE_PRICE when enabled.')
    if ENV.get('TRANSACTION_FEE_BPS', 0.0) < 0:
        errors.append('TRANSACTION_FEE_BPS must be non-negative.')
    if ENV.get('GAS_COST_USDC', 0.0) < 0:
        errors.append('GAS_COST_USDC must be non-negative.')
    if ENV.get('RISK_BUFFER_PCT', 0.0) < 0:
        errors.append('RISK_BUFFER_PCT must be non-negative.')
    if not (0 <= ENV.get('COPY_DECISION_MIN_SCORE', 0.0) <= 1):
        errors.append('COPY_DECISION_MIN_SCORE must be in [0, 1].')
    if not (0 <= ENV.get('TINY_LEADER_HIGH_CONFIDENCE_SCORE', 0.0) <= 1):
        errors.append('TINY_LEADER_HIGH_CONFIDENCE_SCORE must be in [0, 1].')
    if ENV.get('COPY_MAX_PRICE_DRIFT_PCT', 0.0) < 0:
        errors.append('COPY_MAX_PRICE_DRIFT_PCT must be non-negative.')
    if ENV.get('COPY_MAX_EDGE_DECAY_PCT', 0.0) < 0:
        errors.append('COPY_MAX_EDGE_DECAY_PCT must be non-negative.')
    if ENV.get('COPY_MAX_SIGNAL_AGE_SECONDS', 0.0) < 0:
        errors.append('COPY_MAX_SIGNAL_AGE_SECONDS must be non-negative.')
    if ENV.get('COPY_LIQUIDITY_TARGET_USDC', 0.0) <= 0:
        errors.append('COPY_LIQUIDITY_TARGET_USDC must be > 0.')
    if ENV.get('TRUST_SIZE_MULTIPLIER_MIN', 0.0) <= 0:
        errors.append('TRUST_SIZE_MULTIPLIER_MIN must be > 0.')
    if ENV.get('TRUST_SIZE_MULTIPLIER_MAX', 0.0) <= 0:
        errors.append('TRUST_SIZE_MULTIPLIER_MAX must be > 0.')
    if ENV.get('TRUST_SIZE_MULTIPLIER_MIN', 0.0) > ENV.get('TRUST_SIZE_MULTIPLIER_MAX', 0.0):
        errors.append('TRUST_SIZE_MULTIPLIER_MIN must be <= TRUST_SIZE_MULTIPLIER_MAX.')
    if ENV.get('LEADER_SCORE_BALANCE_TARGET_USDC', 0.0) <= 0:
        errors.append('LEADER_SCORE_BALANCE_TARGET_USDC must be > 0.')
    if ENV.get('LEADER_SCORE_ACTIVITY_TARGET', 0) <= 0:
        errors.append('LEADER_SCORE_ACTIVITY_TARGET must be > 0.')
    if not (0 <= ENV.get('LEADER_SCORE_FLOOR', 0.0) <= 1):
        errors.append('LEADER_SCORE_FLOOR must be in [0, 1].')
    if ENV.get('LEADER_SCORE_WEIGHT_BALANCE', 0.0) < 0:
        errors.append('LEADER_SCORE_WEIGHT_BALANCE must be non-negative.')
    if ENV.get('LEADER_SCORE_WEIGHT_ANALYTICS', 0.0) < 0:
        errors.append('LEADER_SCORE_WEIGHT_ANALYTICS must be non-negative.')
    if ENV.get('LEADER_SCORE_WEIGHT_ACTIVITY', 0.0) < 0:
        errors.append('LEADER_SCORE_WEIGHT_ACTIVITY must be non-negative.')
    if (
        ENV.get('LEADER_SCORE_WEIGHT_BALANCE', 0.0)
        + ENV.get('LEADER_SCORE_WEIGHT_ANALYTICS', 0.0)
        + ENV.get('LEADER_SCORE_WEIGHT_ACTIVITY', 0.0)
    ) <= 0:
        errors.append('LEADER_SCORE_WEIGHT_* values must sum to > 0.')
    if ENV.get('PROFIT_LOCK_TRIGGER_PCT', 0.0) <= 0:
        errors.append('PROFIT_LOCK_TRIGGER_PCT must be > 0.')
    if not (0 <= ENV.get('NEAR_MAX_PRICE_WARN_TRIM_RATIO', 0.0) <= 1):
        errors.append('NEAR_MAX_PRICE_WARN_TRIM_RATIO must be in [0, 1].')
    if not (0 <= ENV.get('NEAR_MAX_PRICE_WARN', 0.0) <= 1):
        errors.append('NEAR_MAX_PRICE_WARN must be in [0, 1].')
    if not (0 <= ENV.get('NEAR_MAX_PRICE_EXIT', 0.0) <= 1):
        errors.append('NEAR_MAX_PRICE_EXIT must be in [0, 1].')
    if ENV.get('NEAR_MAX_PRICE_WARN', 0.0) > ENV.get('NEAR_MAX_PRICE_EXIT', 0.0):
        errors.append('NEAR_MAX_PRICE_WARN must be <= NEAR_MAX_PRICE_EXIT.')
    if ENV.get('MAX_MARKET_EXPOSURE_USDC', 0.0) <= 0:
        errors.append('MAX_MARKET_EXPOSURE_USDC must be > 0.')
    if ENV.get('MAX_CATEGORY_EXPOSURE_USDC', 0.0) <= 0:
        errors.append('MAX_CATEGORY_EXPOSURE_USDC must be > 0.')
    if ENV.get('MAX_LEADER_EXPOSURE_USDC', 0.0) <= 0:
        errors.append('MAX_LEADER_EXPOSURE_USDC must be > 0.')
    if ENV.get('MAX_OPEN_POSITIONS', 0) <= 0:
        errors.append('MAX_OPEN_POSITIONS must be > 0.')
    if ENV.get('MAX_DAILY_DRAWDOWN_PCT', 0.0) <= 0:
        errors.append('MAX_DAILY_DRAWDOWN_PCT must be > 0.')
    if ENV.get('PENDING_SUBMISSION_TTL_SECONDS', 0) <= 0:
        errors.append('PENDING_SUBMISSION_TTL_SECONDS must be > 0.')
    if ENV.get('AUTO_DISCOVER_TOP_N', 0) <= 0:
        errors.append('AUTO_DISCOVER_TOP_N must be > 0.')
    if ENV.get('DISCOVERY_MAX_RESULTS', 0) <= 0:
        errors.append('DISCOVERY_MAX_RESULTS must be > 0.')

    if errors:
        raise EnvValidationError('Invalid runtime environment:\n- ' + '\n- '.join(errors))


# IMPORTANT: This snapshot is populated at import time from os.environ.
# ``runtime_env.load_runtime_env()`` MUST be called before ``config.env`` is first
# imported in runtime entrypoints (app.py does this). Tests can call ``rehydrate_env()``
# after patching ``os.environ`` to rebuild this snapshot in place.
def _build_env_snapshot() -> dict[str, object]:
    """Return a parsed environment snapshot from current ``os.environ``."""
    configured_auto_discover = _bool('AUTO_DISCOVER_LEADERS', False)
    configured_trader_discovery = _bool('ENABLE_TRADER_DISCOVERY', False)
    configured_promote_to_active = _bool('DISCOVERY_PROMOTE_TO_ACTIVE', False)
    leader_auto = _bool(
        'LEADER_AUTO',
        configured_auto_discover or configured_promote_to_active,
    )
    auto_discover_leaders = configured_auto_discover or leader_auto
    enable_trader_discovery = configured_trader_discovery or leader_auto or auto_discover_leaders
    discovery_promote_to_active = configured_promote_to_active or leader_auto
    legacy_auto_count = _int('AUTO_DISCOVER_TOP_N', '5')
    leader_auto_count_raw = _ensure_no_inline_comment('LEADER_AUTO_COUNT', _raw('LEADER_AUTO_COUNT', ''))
    if leader_auto_count_raw:
        try:
            auto_discover_top_n = int(leader_auto_count_raw)
        except ValueError as exc:
            raise EnvValidationError(
                f'LEADER_AUTO_COUNT must be a valid integer, got: {leader_auto_count_raw!r}'
            ) from exc
    else:
        auto_discover_top_n = legacy_auto_count

    private_key_file_configured = _configured_if_valid('PRIVATE_KEY_FILE')
    private_key_configured = private_key_file_configured or _configured_if_valid('PRIVATE_KEY')
    # CLOB credentials are equally sensitive as PRIVATE_KEY — they sign
    # API requests. Mirror the REDACTED treatment so an ENV-dump
    # cannot leak them.
    clob_api_key_configured = bool(os.getenv('CLOB_API_KEY'))
    clob_secret_configured = bool(os.getenv('CLOB_SECRET'))
    clob_passphrase_configured = bool(os.getenv('CLOB_PASSPHRASE'))

    return {
    # --- Wallet / auth ---
    'USER_ADDRESS': os.getenv('USER_ADDRESS', ''),
    'FUNDER_ADDRESS': get_funder_address(),
    # Keep secrets and secret-file metadata out of ENV snapshot; call get_private_key() where needed.
    'PRIVATE_KEY': '***REDACTED***' if private_key_configured else '',
    'PRIVATE_KEY_FILE': '***REDACTED***' if private_key_file_configured else '',
        'CHAIN_ID': _int('CHAIN_ID', '137'),
    'LEADER_WALLETS': _leader_wallets(),

    # --- CLOB ---
    'CLOB_HTTP_URL': os.getenv('CLOB_HTTP_URL', 'https://clob.polymarket.com/'),
    # CLOB credentials redacted in the in-memory snapshot — retrieve via
    # os.getenv() at the actual call site (signing path) instead of from
    # the cached ENV dict.
    'CLOB_API_KEY': '***REDACTED***' if clob_api_key_configured else '',
    'CLOB_SECRET': '***REDACTED***' if clob_secret_configured else '',
    'CLOB_PASSPHRASE': '***REDACTED***' if clob_passphrase_configured else '',

    # --- Polling ---
    'FETCH_INTERVAL': _float('FETCH_INTERVAL', '0.25'),
    'LEADER_AUTO': leader_auto,
    'LEADER_AUTO_COUNT': auto_discover_top_n,
    'AUTO_DISCOVER_LEADERS': auto_discover_leaders,
    'AUTO_DISCOVER_MAX_LEADERS': max(_int('AUTO_DISCOVER_MAX_LEADERS', '10'), auto_discover_top_n),
    'AUTO_DISCOVER_TOP_N': auto_discover_top_n,
    'AUTO_DISCOVER_MIN_SCORE': _float('AUTO_DISCOVER_MIN_SCORE', '0.45'),
    'AGGREGATE_SIGNAL_WINDOW_SECONDS': _float('AGGREGATE_SIGNAL_WINDOW_SECONDS', '0.1'),

    # --- Sizing ---
    # SIZING_MODE: "proportional" (default), "fixed", "adaptive"
    #   proportional = copy PERCENTAGE% of leader trade size
    #   fixed         = always use FIXED_AMOUNT USDC per trade
    #   adaptive      = optionally scale by trade size (runtime) or performance (legacy path)
    'SIZING_MODE': os.getenv('SIZING_MODE', 'proportional').strip().lower(),
    'PERCENTAGE': _float('PERCENTAGE', '2'),
    'FIXED_AMOUNT': _float('FIXED_AMOUNT', '3'),
    'TRADE_MULTIPLIER': _float('TRADE_MULTIPLIER', '1.0'),
    'TIERED_TRADE_MULTIPLIERS': os.getenv('TIERED_TRADE_MULTIPLIERS', '').strip(),
    'ADAPTIVE_SIZING_BASIS': os.getenv('ADAPTIVE_SIZING_BASIS', 'trade_size').strip().lower(),
    'ADAPTIVE_MIN_PERCENT': _float('ADAPTIVE_MIN_PERCENT', '0'),
    'ADAPTIVE_MAX_PERCENT': _float('ADAPTIVE_MAX_PERCENT', '0'),
    'ADAPTIVE_THRESHOLD_USDC': _float('ADAPTIVE_THRESHOLD_USDC', '500'),
    'BUY_MIN_AMOUNT': _float('BUY_MIN_AMOUNT', '6'),
    'BUY_MAX_AMOUNT': _float('BUY_MAX_AMOUNT', '15'),
    'POSITION_THRESHOLD': _int('POSITION_THRESHOLD', '0'),
    # Max fraction of our balance to risk on a single trade (0.0–1.0)
    'MAX_POSITION_PCT': _float('MAX_POSITION_PCT', '0.45'),
    'ENABLE_TRUST_SIZE_MULTIPLIERS': os.getenv('ENABLE_TRUST_SIZE_MULTIPLIERS', 'true').strip().lower() in ('1', 'true', 'yes'),
    # Decision-first sizing floor controls
    'TRUST_SIZE_MULTIPLIER_MIN': _float('TRUST_SIZE_MULTIPLIER_MIN', '0.50'),
    'TRUST_SIZE_MULTIPLIER_MAX': _float('TRUST_SIZE_MULTIPLIER_MAX', '1.25'),
    'COPY_DECISION_MIN_SCORE': _float('COPY_DECISION_MIN_SCORE', '0.15'),
    'TINY_LEADER_HIGH_CONFIDENCE_SCORE': _float('TINY_LEADER_HIGH_CONFIDENCE_SCORE', '0.75'),
    'COPY_MAX_PRICE_DRIFT_PCT': _float('COPY_MAX_PRICE_DRIFT_PCT', '15'),
    'COPY_MAX_EDGE_DECAY_PCT': _float('COPY_MAX_EDGE_DECAY_PCT', '90'),
    'COPY_MAX_SIGNAL_AGE_SECONDS': _float('COPY_MAX_SIGNAL_AGE_SECONDS', '100'),
    'COPY_LIQUIDITY_TARGET_USDC': _float('COPY_LIQUIDITY_TARGET_USDC', '50'),
    'MIN_LEADER_TRADE_NOTIONAL_USDC': _float('MIN_LEADER_TRADE_NOTIONAL_USDC', '1'),
    'LEADER_SCORE_BALANCE_TARGET_USDC': _float('LEADER_SCORE_BALANCE_TARGET_USDC', '500'),
    'LEADER_SCORE_PNL_BONUS_CAP': _float('LEADER_SCORE_PNL_BONUS_CAP', '0.10'),
    'LEADER_SCORE_ACTIVITY_TARGET': _int('LEADER_SCORE_ACTIVITY_TARGET', '8'),
    'LEADER_SCORE_WEIGHT_BALANCE': _float('LEADER_SCORE_WEIGHT_BALANCE', '0.35'),
    'LEADER_SCORE_WEIGHT_ANALYTICS': _float('LEADER_SCORE_WEIGHT_ANALYTICS', '0.45'),
    'LEADER_SCORE_WEIGHT_ACTIVITY': _float('LEADER_SCORE_WEIGHT_ACTIVITY', '0.20'),
    'LEADER_SCORE_FLOOR': _float('LEADER_SCORE_FLOOR', '0.10'),

    # --- Risk management ---
    'ENABLE_RISK_MGMT': os.getenv('ENABLE_RISK_MGMT', 'false').strip().lower() in ('1', 'true', 'yes'),
    # How often (seconds) the risk manager checks open positions. Supports sub-second floats.
    'RISK_CHECK_INTERVAL': _float('RISK_CHECK_INTERVAL', '10'),
    'MAX_MARKET_EXPOSURE_USDC': _float('MAX_MARKET_EXPOSURE_USDC', '300'),
    'MAX_CATEGORY_EXPOSURE_USDC': _float('MAX_CATEGORY_EXPOSURE_USDC', '600'),
    'MAX_LEADER_EXPOSURE_USDC': _float('MAX_LEADER_EXPOSURE_USDC', '500'),
    'MAX_OPEN_POSITIONS': _int('MAX_OPEN_POSITIONS', '40'),
    'MAX_DAILY_DRAWDOWN_PCT': _float('MAX_DAILY_DRAWDOWN_PCT', '20'),
    'KILL_SWITCH': os.getenv('KILL_SWITCH', 'false').strip().lower() in ('1', 'true', 'yes'),

    # Account-cap policy (single unified policy)
    'ENABLE_BALANCE_TIERED_POLICY': os.getenv('ENABLE_BALANCE_TIERED_POLICY', 'false').strip().lower() in ('1', 'true', 'yes'),
    'BALANCE_POLICY_RESERVE_PCT': _float('BALANCE_POLICY_RESERVE_PCT', '0.12'),
    'BALANCE_POLICY_RESERVE_FLOOR': _float('BALANCE_POLICY_RESERVE_FLOOR', '7'),
    'BALANCE_POLICY_HARD_CAP_PCT': _float('BALANCE_POLICY_HARD_CAP_PCT', '0.15'),
    'BALANCE_POLICY_HARD_CAP_CEILING': _float('BALANCE_POLICY_HARD_CAP_CEILING', '8'),
    'BALANCE_POLICY_MAX_OPEN_POSITIONS': _int('BALANCE_POLICY_MAX_OPEN_POSITIONS', '12'),
    'BALANCE_POLICY_ALLOW_PARTIAL_EXITS': os.getenv('BALANCE_POLICY_ALLOW_PARTIAL_EXITS', 'true').strip().lower() in ('1', 'true', 'yes'),
    'BALANCE_POLICY_ALLOW_PYRAMIDING': os.getenv('BALANCE_POLICY_ALLOW_PYRAMIDING', 'false').strip().lower() in ('1', 'true', 'yes'),
    'BALANCE_POLICY_MAX_ADDS_PER_POSITION': _int('BALANCE_POLICY_MAX_ADDS_PER_POSITION', '1'),
    'BALANCE_POLICY_BASE_MIN_SCORE': _float('BALANCE_POLICY_BASE_MIN_SCORE', '0.22'),
    'BALANCE_POLICY_MIN_NOTIONAL_USDC': _float('BALANCE_POLICY_MIN_NOTIONAL_USDC', '0.00001'),
    'BALANCE_POLICY_DYNAMIC_THRESHOLD_RAISE': os.getenv('BALANCE_POLICY_DYNAMIC_THRESHOLD_RAISE', 'false').strip().lower() in ('1', 'true', 'yes'),
    'DAY_START_EQUITY_USDC': _float('DAY_START_EQUITY_USDC', '50'),

    # Extended exit intelligence
    'ENABLE_PROFIT_LOCK': os.getenv('ENABLE_PROFIT_LOCK', 'false').strip().lower() in ('1', 'true', 'yes'),
    'PROFIT_LOCK_TRIGGER_PCT': _float('PROFIT_LOCK_TRIGGER_PCT', '70'),
    'PROFIT_LOCK_SELL_FRACTION': _float('PROFIT_LOCK_SELL_FRACTION', '0.50'),

    # Near-max price exits (default 0.95 = near 95c)
    'ENABLE_NEAR_MAX_EXIT': os.getenv('ENABLE_NEAR_MAX_EXIT', 'false').strip().lower() in ('1', 'true', 'yes'),
    'NEAR_MAX_PRICE_WARN': _float('NEAR_MAX_PRICE_WARN', '0.95'),
    'NEAR_MAX_PRICE_WARN_TRIM_RATIO': _float('NEAR_MAX_PRICE_WARN_TRIM_RATIO', '0.50'),
    'NEAR_MAX_PRICE_EXIT': _float('NEAR_MAX_PRICE_EXIT', '0.95'),

    # Trailing stop
    'ENABLE_TRAILING_STOP': os.getenv('ENABLE_TRAILING_STOP', 'false').strip().lower() in ('1', 'true', 'yes'),
    'TRAILING_ACTIVATION_PCT': _float_or_auto('TRAILING_ACTIVATION_PCT', '20'),
    'TRAILING_DISTANCE': _float('TRAILING_DISTANCE', '0.05'),

    # Staged take-profit (comma-separated levels and fractions)
    'ENABLE_STAGED_TP': os.getenv('ENABLE_STAGED_TP', 'false').strip().lower() in ('1', 'true', 'yes'),
    'STAGED_TP_LEVELS': os.getenv('STAGED_TP_LEVELS', '30,50'),
    'STAGED_TP_FRACTIONS': os.getenv('STAGED_TP_FRACTIONS', '0.25,0.35'),

    # Leader divergence exit
    'ENABLE_LEADER_DIVERGENCE_EXIT': os.getenv('ENABLE_LEADER_DIVERGENCE_EXIT', 'false').strip().lower() in ('1', 'true', 'yes'),

    # Stop-loss exit (absolute price floor, grace-period protected)
    'ENABLE_STOP_LOSS': _bool('ENABLE_STOP_LOSS', False),
    'STOP_LOSS_THRESHOLD': _float('STOP_LOSS_THRESHOLD', '0.15'),
    'STOP_LOSS_GRACE_PERIOD_SECONDS': _float('STOP_LOSS_GRACE_PERIOD_SECONDS', '180'),

    # Trader discovery via polymarketanalytics.com
    'ENABLE_TRADER_DISCOVERY': enable_trader_discovery,
    'DISCOVERY_INTERVAL_SECONDS': _int('DISCOVERY_INTERVAL_SECONDS', '3600'),
    'DISCOVERY_PROMOTE_TO_ACTIVE': discovery_promote_to_active,
    'MIN_LEADER_PNL': _float('MIN_LEADER_PNL', '1000'),
    'MIN_LEADER_WIN_RATE': _float('MIN_LEADER_WIN_RATE', '0.55'),
    'MIN_LEADER_POSITIONS': _int('MIN_LEADER_POSITIONS', '20'),
    'DISCOVERY_MAX_RESULTS': _int('DISCOVERY_MAX_RESULTS', '50'),

    # --- Near-one exit (replaces on-chain redemption) ---
    'ENABLE_NEAR_ONE_EXIT': os.getenv('ENABLE_NEAR_ONE_EXIT', 'false').strip().lower() in ('1', 'true', 'yes'),
    'NEAR_ONE_EXIT_THRESHOLD': os.getenv('NEAR_ONE_EXIT_THRESHOLD', '0.99'),
    'NEAR_ONE_EXIT_DWELL_SECONDS': _int('NEAR_ONE_EXIT_DWELL_SECONDS', '60'),
    'NEAR_ONE_EXIT_CHECK_INTERVAL': _int('NEAR_ONE_EXIT_CHECK_INTERVAL', '5'),

    # --- Execution quality ---
    # Max acceptable slippage (%) before rejecting order
    'SLIPPAGE_TOLERANCE': _float('SLIPPAGE_TOLERANCE', '2.0'),
    # Minimum allowed BUY quote price; 0 disables this gate.
    'MIN_SHARE_PRICE': _float('MIN_SHARE_PRICE', '0.03'),
    # Maximum allowed BUY quote price; 0 disables this gate.
    'MAX_SHARE_PRICE': _float('MAX_SHARE_PRICE', '0'),
    'MAX_RETRIES': _int('MAX_RETRIES', '3'),
    'NON_TERMINAL_FAILURE_MAX_ATTEMPTS': _int('NON_TERMINAL_FAILURE_MAX_ATTEMPTS', '3'),
    'ENABLE_TIER_GATE_WATCHLIST': os.getenv('ENABLE_TIER_GATE_WATCHLIST', 'false').strip().lower() in ('1', 'true', 'yes'),
    'ENABLE_TIER_GATE_WATCHLIST_LOG': os.getenv('ENABLE_TIER_GATE_WATCHLIST_LOG', 'false').strip().lower() in ('1', 'true', 'yes'),
    # Global switch for copy decision gating (trade scoring / trust gating / decision rejections).
    'ENABLE_COPY_DECISION_GATE': os.getenv('ENABLE_COPY_DECISION_GATE', 'false').strip().lower() in ('1', 'true', 'yes'),
    # Global paper-only override: if true, all live_follow leaders are treated as paper_follow.
    # Must be false in production. Use true for dry-run / staging verification.
    'PAPER_TRADE_ONLY': os.getenv('PAPER_TRADE_ONLY', 'false').strip().lower() in ('1', 'true', 'yes'),
    # Near-resolution gate for BUY signals: reject stale signals when live_price >= this threshold.
    'ENABLE_NEAR_RESOLUTION_GATE': os.getenv('ENABLE_NEAR_RESOLUTION_GATE', 'false').strip().lower() in ('1', 'true', 'yes'),
    'NEAR_RESOLUTION_GATE_PRICE': _float('NEAR_RESOLUTION_GATE_PRICE', '0.95'),
    'NEAR_RESOLUTION_MAX_AGE_SECONDS': _float('NEAR_RESOLUTION_MAX_AGE_SECONDS', '15'),
    # Expire stale in-flight idempotency locks so retries are not blocked forever.
    'PENDING_SUBMISSION_TTL_SECONDS': _int('PENDING_SUBMISSION_TTL_SECONDS', '120'),
    # Exponential backoff base (seconds) between retries
    'RETRY_BACKOFF_BASE': _float('RETRY_BACKOFF_BASE', '1.0'),
    # Transaction-cost model for pre-trade gating
    'TRANSACTION_FEE_BPS': _float('TRANSACTION_FEE_BPS', '20'),
    'GAS_COST_USDC': _float('GAS_COST_USDC', '0.03'),
    'RISK_BUFFER_PCT': _float('RISK_BUFFER_PCT', '0.10'),

    # --- Infrastructure ---
    'MULTI_USER_CONFIG_FILE': os.getenv('MULTI_USER_CONFIG_FILE', ''),
    'RPC_URL': os.getenv('RPC_URL', 'https://polygon-bor-rpc.publicnode.com'),
    'FALLBACK_RPC_URL': os.getenv('FALLBACK_RPC_URL', ''),
    'USDC_CONTRACT_ADDRESS': os.getenv('USDC_CONTRACT_ADDRESS', POLYGON_USDCE_ADDRESS),
    'PUSD_CONTRACT_ADDRESS': os.getenv('PUSD_CONTRACT_ADDRESS', POLYMARKET_PUSD_ADDRESS),
    'CTF_COLLATERAL_ADAPTER_ADDRESS': os.getenv('CTF_COLLATERAL_ADAPTER_ADDRESS', POLYMARKET_CTF_COLLATERAL_ADAPTER_ADDRESS),
    'CTF_EXCHANGE_V2_ADDRESS': os.getenv('CTF_EXCHANGE_V2_ADDRESS', POLYMARKET_CTF_EXCHANGE_V2_ADDRESS),
    'NEG_RISK_EXCHANGE_V2_ADDRESS': os.getenv('NEG_RISK_EXCHANGE_V2_ADDRESS', POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS),
    'MIN_PUSD_BALANCE': _float('MIN_PUSD_BALANCE', '1.0'),
    'BOT_STATE_DB': os.getenv('BOT_STATE_DB', 'data/bot_state.db'),
    'ENABLE_METRICS_SERVER': os.getenv('ENABLE_METRICS_SERVER', 'false').strip().lower() in ('1', 'true', 'yes'),
    'METRICS_PORT': _int('METRICS_PORT', '9102'),
    'METRICS_BIND_HOST': os.getenv('METRICS_BIND_HOST', '127.0.0.1'),
    'ENFORCE_GEOBLOCK': os.getenv('ENFORCE_GEOBLOCK', 'true').strip().lower() in ('1', 'true', 'yes'),

    # --- Execution quality gates ---
    # Spread gate mode:
    # - absolute_bps: (ask-bid) * 10_000
    # - relative_bps: ((ask-bid)/bid) * 10_000
    # - ticks: (ask-bid) / tick_size
    'BOOK_SPREAD_MODE': os.getenv('BOOK_SPREAD_MODE', 'absolute_bps').strip().lower(),
    # Maximum acceptable book spread in bps for absolute_bps/relative_bps (0 = disabled). Default: 500.
    'MAX_BOOK_SPREAD_BPS': _int('MAX_BOOK_SPREAD_BPS', '500'),
    # Maximum acceptable spread in ticks when BOOK_SPREAD_MODE=ticks (0 = disabled).
    'MAX_BOOK_SPREAD_TICKS': _int('MAX_BOOK_SPREAD_TICKS', '0'),
    # Require fresh orderbook fetch for execution-time spread checks.
    'EXECUTION_REQUIRE_FRESH_BOOK': os.getenv('EXECUTION_REQUIRE_FRESH_BOOK', 'true').strip().lower() in ('1', 'true', 'yes'),
    # CLOB circuit breaker settings.
    'CIRCUIT_FAILURE_THRESHOLD': _int('CIRCUIT_FAILURE_THRESHOLD', '5'),
    'CIRCUIT_COOLDOWN_SECONDS': _float('CIRCUIT_COOLDOWN_SECONDS', '60'),
    # Market metadata cache TTLs (seconds): good books cached longer than thin books.
    'META_CACHE_TTL_GOOD': _float('META_CACHE_TTL_GOOD', '300'),
    'META_CACHE_TTL_THIN': _float('META_CACHE_TTL_THIN', '30'),

    # --- Leader tier promotion / demotion thresholds ---
    # Defaults must match the hardcoded fallbacks in orchestrators/leader_registry.py.
    'TIER_PROMOTE_TO_PAPER': _float('TIER_PROMOTE_TO_PAPER', '0.40'),
    'TIER_PROMOTE_TO_LIVE': _float('TIER_PROMOTE_TO_LIVE', '0.55'),
    'TIER_MIN_PAPER_SECONDS': _float('TIER_MIN_PAPER_SECONDS', '86400'),
    'TIER_DEMOTE_TO_PAPER': _float('TIER_DEMOTE_TO_PAPER', '0.35'),
    'TIER_DEMOTE_TO_WATCHLIST': _float('TIER_DEMOTE_TO_WATCHLIST', '0.25'),

    # --- Logging / P&L tracking ---
    'POLYMARKET_LOG_LEVEL': os.getenv('POLYMARKET_LOG_LEVEL', 'INFO').strip().upper(),
    'LOG_DIR': os.getenv('LOG_DIR', 'log'),
    'APP_LOG_JSONL_FILE': os.getenv('APP_LOG_JSONL_FILE', 'log/PolymarketCopyTradingBot.jsonl'),
    'LOG_MAX_BYTES': _int('LOG_MAX_BYTES', '5242880'),
    'LOG_BACKUP_COUNT': _int('LOG_BACKUP_COUNT', '3'),
    'PNL_LOG_FILE': os.getenv('PNL_LOG_FILE', 'log/pnl.jsonl'),
    'DISCOVERY_REFRESH_INTERVAL_SECONDS': _int('DISCOVERY_REFRESH_INTERVAL_SECONDS', '300'),

    # --- V2 Migration readiness ---
    # Set to true once smoke tests pass and pUSD balance/allowance confirmed.
    # When false, a startup warning is logged. This is a safety signal, not a feature toggle.
    'CLOB_V2_READY': _bool('CLOB_V2_READY', False),
}


ENV = _build_env_snapshot()


def rehydrate_env(*, load_dotenv_file: bool = False, dotenv_path: str | os.PathLike[str] | None = None, override: bool = False) -> dict[str, object]:
    """Rebuild the module-level ``ENV`` snapshot from live ``os.environ``.

    This mutates ``ENV`` in place so modules that imported ``ENV`` keep pointing
    at the refreshed values. It is primarily intended for tests that patch
    ``os.environ`` after ``config.env`` has already been imported.

    Args:
        load_dotenv_file: When True, load dotenv values before rebuilding.
        dotenv_path: Optional explicit dotenv path (defaults to standard lookup).
        override: Forwarded to the secrets `.env` load when ``load_dotenv_file`` is True.
    """
    if load_dotenv_file:
        load_runtime_env(secrets_path=dotenv_path, override_secrets=override)

    ENV.clear()
    ENV.update(_build_env_snapshot())
    return ENV
