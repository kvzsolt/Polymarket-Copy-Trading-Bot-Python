import os
import re
import sys
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from dotenv import load_dotenv

# NOTE: Do NOT call load_dotenv() at import time.
# Import-time side effects make the test suite non-deterministic because a local
# `.env` file (developer machine / CI workspace) can silently override defaults.
# Runtime entrypoints (e.g. index.py) should call load_dotenv() explicitly.

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
    signature_type: SignatureType
    funder_address: str
    polygon_rpc_url: str


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


def _float(key: str, default: str) -> float:
    raw = _ensure_no_inline_comment(key, _raw(key, default))
    return float(raw) if raw else float(default)


def _int(key: str, default: str) -> int:
    raw = _ensure_no_inline_comment(key, _raw(key, default))
    return int(raw) if raw else int(default)


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
        if mode & 0o077:
            raise EnvValidationError(
                f'{env_name}_FILE must not be group/world readable (chmod 600): {path}'
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


def get_signature_type() -> SignatureType:
    raw = _ensure_no_inline_comment('SIGNATURE_TYPE', _raw('SIGNATURE_TYPE', '0'))
    try:
        value = int(raw)
    except ValueError as exc:
        raise EnvValidationError("SIGNATURE_TYPE must be one of: 0, 1, 2.") from exc

    try:
        return SignatureType(value)
    except ValueError as exc:
        raise EnvValidationError("SIGNATURE_TYPE must be one of: 0 (EOA), 1 (POLY_PROXY), 2 (GNOSIS_SAFE).") from exc


def get_funder_address() -> str:
    funder = _ensure_no_inline_comment('FUNDER_ADDRESS', _raw('FUNDER_ADDRESS', ''))
    if funder:
        return funder
    # Backward-compatible fallback
    return _ensure_no_inline_comment('PROXY_WALLET', _raw('PROXY_WALLET', ''))


def build_wallet_config() -> WalletConfig:
    return WalletConfig(
        private_key=get_private_key(),
        chain_id=get_chain_id(),
        signature_type=get_signature_type(),
        funder_address=get_funder_address(),
        polygon_rpc_url=_ensure_no_inline_comment('RPC_URL', _raw('RPC_URL', '')),
    )


def validate_wallet_config(cfg: WalletConfig) -> None:
    if not cfg.private_key:
        raise EnvValidationError('PRIVATE_KEY is required.')
    if not cfg.funder_address:
        raise EnvValidationError('FUNDER_ADDRESS is required (or set PROXY_WALLET as fallback).')
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
        'FUNDER_ADDRESS',
        'PROXY_WALLET',
        'SIGNATURE_TYPE',
        'CHAIN_ID',
        'MULTI_USER_CONFIG_FILE',
        'RPC_URL',
        'PNL_LOG_FILE',
        'CLOB_HTTP_URL',
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
    proxy_wallet = _ensure_no_inline_comment('PROXY_WALLET', _raw('PROXY_WALLET', ''))
    if proxy_wallet and not is_valid_eth_address(proxy_wallet):
        errors.append(
            f'Invalid PROXY_WALLET "{proxy_wallet}" (expected 42-char Ethereum address).'
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
    if ENV.get('SLIPPAGE_TOLERANCE', 0.0) < 0:
        errors.append('SLIPPAGE_TOLERANCE must be non-negative.')
    if ENV.get('MIN_SHARE_PRICE', 0.0) < 0:
        errors.append('MIN_SHARE_PRICE must be non-negative.')
    if ENV.get('MIN_SHARE_PRICE', 0.0) > 1:
        errors.append('MIN_SHARE_PRICE must be <= 1.0 for binary markets.')
    if ENV.get('TRANSACTION_FEE_BPS', 0.0) < 0:
        errors.append('TRANSACTION_FEE_BPS must be non-negative.')
    if ENV.get('GAS_COST_USDC', 0.0) < 0:
        errors.append('GAS_COST_USDC must be non-negative.')
    if ENV.get('RISK_BUFFER_PCT', 0.0) < 0:
        errors.append('RISK_BUFFER_PCT must be non-negative.')
    if ENV.get('COPY_MIN_SHARES', 0.0) < 0:
        errors.append('COPY_MIN_SHARES must be >= 0.')
    if ENV.get('COPY_MIN_NOTIONAL_USDC', 0.0) <= 0:
        errors.append('COPY_MIN_NOTIONAL_USDC must be > 0.')
    if ENV.get('COPY_SIZE_DECIMALS', 0) < 0:
        errors.append('COPY_SIZE_DECIMALS must be >= 0.')
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

    if errors:
        raise EnvValidationError('Invalid runtime environment:\n- ' + '\n- '.join(errors))


# IMPORTANT: This snapshot is populated at import time from os.environ.
# ``dotenv.load_dotenv()`` MUST be called before ``config.env`` is first imported
# in runtime entrypoints (index.py does this). Tests can call ``rehydrate_env()``
# after patching ``os.environ`` to rebuild this snapshot in place.
def _build_env_snapshot() -> dict[str, object]:
    """Return a parsed environment snapshot from current ``os.environ``."""
    return {
    # --- Wallet / auth ---
    'USER_ADDRESS': os.getenv('USER_ADDRESS', ''),
    'PROXY_WALLET': os.getenv('PROXY_WALLET', ''),
    'FUNDER_ADDRESS': os.getenv('FUNDER_ADDRESS', ''),
    'PRIVATE_KEY': os.getenv('PRIVATE_KEY', ''),
    'PRIVATE_KEY_FILE': os.getenv('PRIVATE_KEY_FILE', ''),
    'SIGNATURE_TYPE': _int('SIGNATURE_TYPE', '0'),
    'CHAIN_ID': _int('CHAIN_ID', '137'),
    'LEADER_WALLETS': _leader_wallets(),

    # --- CLOB ---
    'CLOB_HTTP_URL': os.getenv('CLOB_HTTP_URL', 'https://clob.polymarket.com/'),
    'CLOB_API_KEY': os.getenv('CLOB_API_KEY', ''),

    # --- Polling ---
    'FETCH_INTERVAL': _int('FETCH_INTERVAL', '1'),
    'AUTO_DISCOVER_LEADERS': os.getenv('AUTO_DISCOVER_LEADERS', 'false').strip().lower() in ('1', 'true', 'yes'),
    'AUTO_DISCOVER_MAX_LEADERS': _int('AUTO_DISCOVER_MAX_LEADERS', '10'),
    'AUTO_DISCOVER_TOP_N': _int('AUTO_DISCOVER_TOP_N', '5'),
    'AUTO_DISCOVER_MIN_SCORE': _float('AUTO_DISCOVER_MIN_SCORE', '0.45'),
    'AGGREGATE_SIGNAL_WINDOW_SECONDS': _float('AGGREGATE_SIGNAL_WINDOW_SECONDS', '2.0'),

    # --- Sizing ---
    # SIZING_MODE: "proportional" (default), "fixed", "adaptive"
    #   proportional = copy PERCENTAGE% of leader trade size
    #   fixed         = always use FIXED_AMOUNT USDC per trade
    #   adaptive      = scale percentage by leader's recent win rate
    'SIZING_MODE': os.getenv('SIZING_MODE', 'proportional').strip().lower(),
    'PERCENTAGE': _float('PERCENTAGE', '5'),
    'FIXED_AMOUNT': _float('FIXED_AMOUNT', '10'),
    'BUY_MIN_AMOUNT': _float('BUY_MIN_AMOUNT', os.getenv('MIN_AMOUNT', '1')),
    'BUY_MAX_AMOUNT': _float('BUY_MAX_AMOUNT', os.getenv('MAX_AMOUNT', '100')),
    'POSITION_THRESHOLD': _int('POSITION_THRESHOLD', '0'),
    # Max fraction of our balance to risk on a single trade (0.0–1.0)
    'MAX_POSITION_PCT': _float('MAX_POSITION_PCT', '0.25'),
    # Decision-first sizing floor controls
    'COPY_MIN_SHARES': _float('COPY_MIN_SHARES', '5'),
    'COPY_MIN_NOTIONAL_USDC': _float('COPY_MIN_NOTIONAL_USDC', '1'),
    'COPY_SIZE_DECIMALS': _int('COPY_SIZE_DECIMALS', '4'),
    'TRUST_SIZE_MULTIPLIER_MIN': _float('TRUST_SIZE_MULTIPLIER_MIN', '0.70'),
    'TRUST_SIZE_MULTIPLIER_MAX': _float('TRUST_SIZE_MULTIPLIER_MAX', '1.25'),
    'COPY_DECISION_MIN_SCORE': _float('COPY_DECISION_MIN_SCORE', '0.28'),
    'TINY_LEADER_HIGH_CONFIDENCE_SCORE': _float('TINY_LEADER_HIGH_CONFIDENCE_SCORE', '0.75'),
    'COPY_MAX_PRICE_DRIFT_PCT': _float('COPY_MAX_PRICE_DRIFT_PCT', '6'),
    'COPY_MAX_EDGE_DECAY_PCT': _float('COPY_MAX_EDGE_DECAY_PCT', '70'),
    'COPY_MAX_SIGNAL_AGE_SECONDS': _float('COPY_MAX_SIGNAL_AGE_SECONDS', '120'),
    'COPY_LIQUIDITY_TARGET_USDC': _float('COPY_LIQUIDITY_TARGET_USDC', '50'),
    'MIN_LEADER_TRADE_NOTIONAL_USDC': _float('MIN_LEADER_TRADE_NOTIONAL_USDC', '3'),
    'LEADER_SCORE_BALANCE_TARGET_USDC': _float('LEADER_SCORE_BALANCE_TARGET_USDC', '500'),
    'LEADER_SCORE_PNL_BONUS_CAP': _float('LEADER_SCORE_PNL_BONUS_CAP', '0.10'),
    'LEADER_SCORE_ACTIVITY_TARGET': _int('LEADER_SCORE_ACTIVITY_TARGET', '8'),
    'LEADER_SCORE_WEIGHT_BALANCE': _float('LEADER_SCORE_WEIGHT_BALANCE', '0.35'),
    'LEADER_SCORE_WEIGHT_ANALYTICS': _float('LEADER_SCORE_WEIGHT_ANALYTICS', '0.45'),
    'LEADER_SCORE_WEIGHT_ACTIVITY': _float('LEADER_SCORE_WEIGHT_ACTIVITY', '0.20'),
    'LEADER_SCORE_FLOOR': _float('LEADER_SCORE_FLOOR', '0.10'),

    # --- Risk management ---
    'ENABLE_RISK_MGMT': os.getenv('ENABLE_RISK_MGMT', 'true').strip().lower() in ('1', 'true', 'yes'),
    'STOP_LOSS_PCT': _float('STOP_LOSS_PCT', '20'),
    'TAKE_PROFIT_PCT': _float('TAKE_PROFIT_PCT', '60'),
    # How often (seconds) the risk manager checks open positions
    'RISK_CHECK_INTERVAL': _int('RISK_CHECK_INTERVAL', '15'),
    'MAX_MARKET_EXPOSURE_USDC': _float('MAX_MARKET_EXPOSURE_USDC', '300'),
    'MAX_CATEGORY_EXPOSURE_USDC': _float('MAX_CATEGORY_EXPOSURE_USDC', '600'),
    'MAX_LEADER_EXPOSURE_USDC': _float('MAX_LEADER_EXPOSURE_USDC', '500'),
    'MAX_OPEN_POSITIONS': _int('MAX_OPEN_POSITIONS', '20'),
    'MAX_DAILY_DRAWDOWN_PCT': _float('MAX_DAILY_DRAWDOWN_PCT', '20'),
    'KILL_SWITCH': os.getenv('KILL_SWITCH', 'false').strip().lower() in ('1', 'true', 'yes'),

    # Balance-tiered regime system
    'ENABLE_BALANCE_TIERED_POLICY': os.getenv('ENABLE_BALANCE_TIERED_POLICY', 'false').strip().lower() in ('1', 'true', 'yes'),
    'BALANCE_REGIME_MICRO_MAX': _float('BALANCE_REGIME_MICRO_MAX', '75'),
    'BALANCE_REGIME_SMALL_MAX': _float('BALANCE_REGIME_SMALL_MAX', '250'),
    'BALANCE_REGIME_MEDIUM_MAX': _float('BALANCE_REGIME_MEDIUM_MAX', '1000'),
    'DAY_START_EQUITY_USDC': _float('DAY_START_EQUITY_USDC', '0'),

    # Neg-risk arbitrage scanner
    'ENABLE_NEG_RISK_ARB': os.getenv('ENABLE_NEG_RISK_ARB', 'false').strip().lower() in ('1', 'true', 'yes'),
    'ARB_MIN_PROFIT_BPS': _int('ARB_MIN_PROFIT_BPS', '30'),
    'ARB_MAX_COST_USDC': _float('ARB_MAX_COST_USDC', '10'),
    'ARB_MIN_ASK_DEPTH': _float('ARB_MIN_ASK_DEPTH', '20'),
    'ARB_SCAN_INTERVAL': _int('ARB_SCAN_INTERVAL', '30'),
    'ARB_MAX_CONCURRENT': _int('ARB_MAX_CONCURRENT', '2'),
    # Extended exit intelligence
    'ENABLE_PROFIT_LOCK': os.getenv('ENABLE_PROFIT_LOCK', 'true').strip().lower() in ('1', 'true', 'yes'),
    'PROFIT_LOCK_TRIGGER_PCT': _float('PROFIT_LOCK_TRIGGER_PCT', '70'),
    'PROFIT_LOCK_SELL_FRACTION': _float('PROFIT_LOCK_SELL_FRACTION', '0.50'),
    'PROFIT_LOCK_MIN_PROFIT_PCT': _float('PROFIT_LOCK_MIN_PROFIT_PCT', '35'),
    'PROFIT_LOCK_DRAWDOWN_PCT': _float('PROFIT_LOCK_DRAWDOWN_PCT', '15'),

    # Near-max price exits (default 0.95 = near 95c)
    'ENABLE_NEAR_MAX_EXIT': os.getenv('ENABLE_NEAR_MAX_EXIT', 'true').strip().lower() in ('1', 'true', 'yes'),
    'NEAR_MAX_PRICE_WARN': _float('NEAR_MAX_PRICE_WARN', '0.92'),
    'NEAR_MAX_PRICE_WARN_TRIM_RATIO': _float('NEAR_MAX_PRICE_WARN_TRIM_RATIO', '0.50'),
    'NEAR_MAX_PRICE_EXIT': _float('NEAR_MAX_PRICE_EXIT', '0.95'),

    # Trailing stop
    'ENABLE_TRAILING_STOP': os.getenv('ENABLE_TRAILING_STOP', 'true').strip().lower() in ('1', 'true', 'yes'),
    'TRAILING_ACTIVATION_PCT': _float('TRAILING_ACTIVATION_PCT', '20'),
    'TRAILING_DISTANCE': _float('TRAILING_DISTANCE', '0.05'),

    # Staged take-profit (comma-separated levels and fractions)
    'ENABLE_STAGED_TP': os.getenv('ENABLE_STAGED_TP', 'true').strip().lower() in ('1', 'true', 'yes'),
    'STAGED_TP_LEVELS': os.getenv('STAGED_TP_LEVELS', '30,50'),
    'STAGED_TP_FRACTIONS': os.getenv('STAGED_TP_FRACTIONS', '0.25,0.35'),

    # Leader divergence exit
    'ENABLE_LEADER_DIVERGENCE_EXIT': os.getenv('ENABLE_LEADER_DIVERGENCE_EXIT', 'true').strip().lower() in ('1', 'true', 'yes'),

    # Trader discovery via polymarketanalytics.com
    'ENABLE_TRADER_DISCOVERY': os.getenv('ENABLE_TRADER_DISCOVERY', 'false').strip().lower() in ('1', 'true', 'yes'),
    'DISCOVERY_INTERVAL_SECONDS': _int('DISCOVERY_INTERVAL_SECONDS', '3600'),
    'DISCOVERY_PROMOTE_TO_ACTIVE': os.getenv('DISCOVERY_PROMOTE_TO_ACTIVE', 'false').strip().lower() in ('1', 'true', 'yes'),
    'MIN_LEADER_PNL': _float('MIN_LEADER_PNL', '1000'),
    'MIN_LEADER_WIN_RATE': _float('MIN_LEADER_WIN_RATE', '0.55'),
    'MIN_LEADER_POSITIONS': _int('MIN_LEADER_POSITIONS', '20'),
    'DISCOVERY_MAX_RESULTS': _int('DISCOVERY_MAX_RESULTS', '50'),

    # --- Auto-redeem ---
    'ENABLE_AUTO_REDEEM': os.getenv('ENABLE_AUTO_REDEEM', 'true').strip().lower() in ('1', 'true', 'yes'),
    'REDEEM_CHECK_INTERVAL': _int('REDEEM_CHECK_INTERVAL', '60'),

    # --- Execution quality ---
    # Max acceptable slippage (%) before rejecting order
    'SLIPPAGE_TOLERANCE': _float('SLIPPAGE_TOLERANCE', '2.0'),
    # Minimum allowed BUY quote price; 0 disables this gate.
    'MIN_SHARE_PRICE': _float('MIN_SHARE_PRICE', '0'),
    'MAX_RETRIES': _int('MAX_RETRIES', '3'),
    'NON_TERMINAL_FAILURE_MAX_ATTEMPTS': _int('NON_TERMINAL_FAILURE_MAX_ATTEMPTS', '3'),
    # Expire stale in-flight idempotency locks so retries are not blocked forever.
    'PENDING_SUBMISSION_TTL_SECONDS': _int('PENDING_SUBMISSION_TTL_SECONDS', '120'),
    # Exponential backoff base (seconds) between retries
    'RETRY_BACKOFF_BASE': _float('RETRY_BACKOFF_BASE', '1.0'),
    # Transaction-cost model for pre-trade gating
    'TRANSACTION_FEE_BPS': _float('TRANSACTION_FEE_BPS', '20'),
    'GAS_COST_USDC': _float('GAS_COST_USDC', '0.03'),
    'RISK_BUFFER_PCT': _float('RISK_BUFFER_PCT', '0.5'),

    # --- Infrastructure ---
    'MULTI_USER_CONFIG_FILE': os.getenv('MULTI_USER_CONFIG_FILE', ''),
    'RPC_URL': os.getenv('RPC_URL', 'https://polygon-bor-rpc.publicnode.com'),
    'USDC_CONTRACT_ADDRESS': os.getenv('USDC_CONTRACT_ADDRESS', '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'),
    'MIN_ORDER_SIZE': _float('MIN_ORDER_SIZE', '0.00001'),
    'BOT_STATE_DB': os.getenv('BOT_STATE_DB', 'data/bot_state.db'),
    'ENABLE_METRICS_SERVER': os.getenv('ENABLE_METRICS_SERVER', 'false').strip().lower() in ('1', 'true', 'yes'),
    'METRICS_PORT': _int('METRICS_PORT', '9102'),
    'ENFORCE_GEOBLOCK': os.getenv('ENFORCE_GEOBLOCK', 'true').strip().lower() in ('1', 'true', 'yes'),

    # --- P&L tracking ---
    'PNL_LOG_FILE': os.getenv('PNL_LOG_FILE', 'log/pnl.jsonl'),
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
        override: Forwarded to ``load_dotenv`` when ``load_dotenv_file`` is True.
    """
    if load_dotenv_file:
        load_dotenv(dotenv_path=dotenv_path, override=override)

    ENV.clear()
    ENV.update(_build_env_snapshot())
    return ENV
