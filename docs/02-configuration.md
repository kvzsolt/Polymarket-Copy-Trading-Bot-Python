# Configuration Reference

> Complete reference for all configuration options in the Polymarket Copy Trading Bot.

---

## Configuration Architecture

The bot uses a **layered, immutable configuration system** built on frozen dataclasses. All configuration flows through a single `AppConfig` object created by `load_config()`, which parses environment variables and validates them as a batch.

Key design principles:
- **Immutable after construction** — `AppConfig` is a `@dataclass(frozen=True)`. No runtime mutation is possible.
- **Fail-fast validation** — All parsing errors are aggregated and raised together in a single `EnvValidationError`.
- **No inline comments** — `.env` files must not contain inline comments (`VALUE # comment`). The parser detects and rejects them.
- **Secret redaction** — The `PRIVATE_KEY` and `PRIVATE_KEY_FILE` values are never stored in the `ENV` snapshot; they appear as `***REDACTED***`.

---

## Runtime File Hierarchy

Configuration is loaded from multiple files in a specific order (later files override earlier ones):

| File | Purpose | Git-ignored? |
|---|---|---|
| `config/runtime.env` | Default runtime configuration (committed) | No |
| `config/runtime.local.env` | Local overrides (not committed) | Yes |
| `secrets.env` | Sensitive credentials (private key, RPC URL) | Yes |

### Loading Order

1. `load_runtime_env()` is called at startup (in `app.py`)
2. It loads `config/runtime.env` first (shared defaults)
3. Then `config/runtime.local.env` (operator-specific overrides)
4. Then `secrets.env` (sensitive values, overrides everything)
5. Finally, `os.environ` variables take precedence over all files

---

## AppConfig Structure

The root `AppConfig` contains the following sub-configs:

```
AppConfig
├── database: DatabaseConfig
├── wallet: WalletConfig
├── clob: ClobConfig
├── polling: PollingConfig
├── sizing: SizingConfig
├── risk: RiskConfig
├── execution: ExecutionConfig
├── leader: LeaderConfig
├── logging: LoggingConfig
├── network: NetworkConfig
├── redemption: RedemptionConfig
├── reconciliation: ReconciliationConfig
├── discovery: DiscoveryConfig
├── features: FeatureFlagConfig
└── raw: Mapping[str, Any]  (transitional backwards compat)
```

---

## DatabaseConfig

Controls SQLite persistence.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `BOT_STATE_DB` | string | `data/bot_state.db` | Path to the SQLite database file |
| `BOT_STATE_DB_ENABLED` | bool | `true` | Enable/disable database persistence |
| `DB_ENABLED` | bool | `true` | Legacy alias (fallback if `BOT_STATE_DB_ENABLED` not set) |

---

## WalletConfig

Core wallet and blockchain configuration.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `PRIVATE_KEY` | string | *(required for live)* | Hex-encoded private key (0x + 64 hex digits) |
| `PRIVATE_KEY_FILE` | string | *(empty)* | Path to file containing private key (must have 0600 perms) |
| `FUNDER_ADDRESS` | string | *(required)* | Your funder/proxy wallet address |
| `PROXY_WALLET` | string | *(empty)* | Legacy alias for `FUNDER_ADDRESS` |
| `CHAIN_ID` | int | `137` | Polygon chain ID (137 = mainnet, 80002 = testnet) |
| `RPC_URL` | string | `https://polygon-bor-rpc.publicnode.com` | Polygon JSON-RPC endpoint |

> **Validation:** `PRIVATE_KEY` must be a 66-character hex string. `FUNDER_ADDRESS` must be a 42-character Ethereum address. `CHAIN_ID` must be 137 or 80002.

---

## ClobConfig

Polymarket CLOB API connection.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `CLOB_HTTP_URL` | string | `https://clob.polymarket.com/` | CLOB API base URL |
| `CLOB_API_KEY` | string | *(empty)* | Optional CLOB API key |

---

## PollingConfig

Cadence and timing for various bot loops.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `FETCH_INTERVAL` | float | `0.25` | Seconds between leader activity fetches |
| `AGGREGATE_SIGNAL_WINDOW_SECONDS` | float | `0.1` | Window for aggregating signals |
| `RISK_CHECK_INTERVAL` | float | `15` | Seconds between risk position scans |
| `REDEEM_CHECK_INTERVAL` | int | `60` | Seconds between redemption scans |
| `DISCOVERY_REFRESH_INTERVAL_SECONDS` | int | `300` | Seconds between leader discovery refreshes |
| `LEADER_POSITIONS_MIN_FETCH_SPACING_MS` | int | `250` | Minimum ms between leader position fetches |
| `LEADER_ACTIVITY_MIN_FETCH_SPACING_MS` | int | `250` | Minimum ms between leader activity fetches |
| `RECONCILE_INTERVAL_SECONDS` | int | `300` | Seconds between order reconciliation passes |

---

## SizingConfig

Position sizing strategy and limits.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `SIZING_MODE` | string | `proportional` | Sizing mode: `proportional`, `fixed`, or `adaptive` |
| `PERCENTAGE` | float | `2` | For `proportional` mode: percentage of leader trade size |
| `FIXED_AMOUNT` | float | `3` | For `fixed` mode: USDC amount per trade |
| `TRADE_MULTIPLIER` | float | `1.0` | Global trade size multiplier (non-negative) |
| `TIERED_TRADE_MULTIPLIERS` | string | *(empty)* | Comma-separated tier multipliers |
| `ADAPTIVE_SIZING_BASIS` | string | `trade_size` | Adaptive basis: `trade_size` or `performance` |
| `ADAPTIVE_MIN_PERCENT` | float | `0` | Minimum adaptive scaling percentage |
| `ADAPTIVE_MAX_PERCENT` | float | `0` | Maximum adaptive scaling percentage |
| `ADAPTIVE_THRESHOLD_USDC` | float | `500` | USDC threshold for adaptive scaling |
| `BUY_MIN_AMOUNT` | float | `6` | Minimum order size in USDC |
| `BUY_MAX_AMOUNT` | float | `15` | Maximum order size in USDC |
| `POSITION_THRESHOLD` | int | `0` | Position threshold for sizing adjustments |
| `MAX_POSITION_PCT` | float | `0.45` | Maximum fraction of balance per trade (0, 1] |
| `ENABLE_TRUST_SIZE_MULTIPLIERS` | bool | `true` | Scale position size by leader trust score |
| `TRUST_SIZE_MULTIPLIER_MIN` | float | `0.50` | Minimum trust-based size multiplier |
| `TRUST_SIZE_MULTIPLIER_MAX` | float | `1.25` | Maximum trust-based size multiplier |

---

## RiskConfig

Risk management parameters. See [05-risk-controls.md](05-risk-controls.md) for detailed explanations.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `ENABLE_RISK_MGMT` | bool | `false` | Master switch for risk management |
| `MAX_MARKET_EXPOSURE_USDC` | float | `300` | Maximum USDC exposure per market |
| `MAX_CATEGORY_EXPOSURE_USDC` | float | `600` | Maximum USDC exposure per category |
| `MAX_LEADER_EXPOSURE_USDC` | float | `500` | Maximum USDC exposure per leader |
| `MAX_OPEN_POSITIONS` | int | `40` | Maximum number of open positions |
| `MAX_DAILY_DRAWDOWN_PCT` | float | `20` | Maximum daily drawdown percentage |
| `KILL_SWITCH` | bool | `false` | Emergency kill switch (blocks all trading) |
| `ENABLE_BALANCE_TIERED_POLICY` | bool | `false` | Enable account cap policy |
| `BALANCE_POLICY_RESERVE_PCT` | float | `0.12` | Reserve percentage of equity |
| `BALANCE_POLICY_RESERVE_FLOOR` | float | `7` | Minimum reserve in USDC |
| `BALANCE_POLICY_HARD_CAP_PCT` | float | `0.15` | Hard cap as percentage of equity |
| `BALANCE_POLICY_HARD_CAP_CEILING` | float | `8` | Maximum hard cap in USDC |
| `BALANCE_POLICY_MAX_OPEN_POSITIONS` | int | `12` | Position limit under balance policy |
| `BALANCE_POLICY_ALLOW_PARTIAL_EXITS` | bool | `true` | Allow partial position exits |
| `BALANCE_POLICY_ALLOW_PYRAMIDING` | bool | `false` | Allow adding to existing positions |
| `BALANCE_POLICY_MAX_ADDS_PER_POSITION` | int | `1` | Maximum pyramid adds per position |
| `BALANCE_POLICY_BASE_MIN_SCORE` | float | `0.22` | Minimum decision score for balance policy |
| `BALANCE_POLICY_MIN_NOTIONAL_USDC` | float | `0.00001` | Minimum notional for strategy entries |
| `BALANCE_POLICY_DYNAMIC_THRESHOLD_RAISE` | bool | `false` | Dynamically raise thresholds with equity |
| `DAY_START_EQUITY_USDC` | float | `50` | Starting equity for drawdown calculation |
| `NEAR_MAX_PRICE_WARN` | float | `0.95` | Price level for near-max warning trim |
| `NEAR_MAX_PRICE_WARN_TRIM_RATIO` | float | `0.50` | Fraction to trim at warning level |
| `NEAR_MAX_PRICE_EXIT` | float | `0.95` | Price level for full near-max exit |
| `PROFIT_LOCK_TRIGGER_PCT` | float | `70` | Profit % to trigger profit lock |
| `PROFIT_LOCK_SELL_FRACTION` | float | `0.50` | Fraction of position to sell at lock |
| `TRAILING_ACTIVATION_PCT` | float | `20` | Profit % to activate trailing stop |
| `TRAILING_DISTANCE` | float | `0.05` | Trailing stop distance as peak-drop percentage (`0.10` or `10` = 10%) |
| `STAGED_TP_LEVELS` | string | `30,50` | Comma-separated take-profit levels |
| `STAGED_TP_FRACTIONS` | string | `0.25,0.35` | Comma-separated TP sell fractions |
| `STOP_LOSS_THRESHOLD` | float | `0.15` | Price floor for stop-loss exit |
| `STOP_LOSS_GRACE_PERIOD_SECONDS` | float | `180` | Grace period before stop-loss activates |

---

## ExecutionConfig

Order execution quality and decision gates.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `SLIPPAGE_TOLERANCE` | float | `2.0` | Maximum acceptable slippage (%) |
| `MIN_SHARE_PRICE` | float | `0.03` | Minimum buy price; 0 disables |
| `MAX_SHARE_PRICE` | float | `0` | Maximum buy price; 0 disables |
| `MAX_RETRIES` | int | `3` | Maximum order submission retries |
| `NON_TERMINAL_FAILURE_MAX_ATTEMPTS` | int | `3` | Max attempts for non-terminal failures |
| `COPY_FAK_MAX_ATTEMPTS` | int | `8` | Max FAK reposts to fill remaining BUY copy notional |
| `EXIT_FAK_MAX_ATTEMPTS` | int | `8` | Max FAK reposts for forced SELL exits |
| `PAPER_TRADE_ONLY` | bool | `false` | Force paper trading mode |
| `ENABLE_COPY_DECISION_GATE` | bool | `false` | Enable copy decision scoring gate |
| `ENABLE_NEAR_RESOLUTION_GATE` | bool | `false` | Reject stale signals near resolution |
| `NEAR_RESOLUTION_GATE_PRICE` | float | `0.95` | Price threshold for near-resolution gate |
| `NEAR_RESOLUTION_MAX_AGE_SECONDS` | float | `15` | Maximum signal age for near-resolution |
| `PENDING_SUBMISSION_TTL_SECONDS` | int | `120` | TTL for pending idempotency locks |
| `RETRY_BACKOFF_BASE` | float | `1.0` | Exponential backoff base (seconds) |
| `TRANSACTION_FEE_BPS` | float | `20` | Transaction fee in basis points |
| `GAS_COST_USDC` | float | `0.03` | Estimated gas cost in USDC |
| `RISK_BUFFER_PCT` | float | `0.10` | Risk buffer percentage |
| `BOOK_SPREAD_MODE` | string | `absolute_bps` | Spread gate: `absolute_bps`, `relative_bps`, or `ticks` |
| `MAX_BOOK_SPREAD_BPS` | int | `2500` | Maximum acceptable book spread (bps) |
| `MAX_BOOK_SPREAD_TICKS` | int | `0` | Maximum spread in ticks (0 = disabled) |
| `EXECUTION_REQUIRE_FRESH_BOOK` | bool | `true` | Require fresh orderbook for spread check |
| `CIRCUIT_FAILURE_THRESHOLD` | int | `15` | Failures before circuit breaker opens |
| `CIRCUIT_COOLDOWN_SECONDS` | float | `60` | Circuit breaker cooldown period |
| `META_CACHE_TTL_GOOD` | float | `300` | Cache TTL for good market metadata (seconds) |
| `META_CACHE_TTL_THIN` | float | `30` | Cache TTL for thin market metadata (seconds) |

---

## LeaderConfig

Leader selection, scoring, and tier management. See [06-leader-selection.md](06-leader-selection.md) for details.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `LEADER_WALLETS` | string | *(empty)* | Comma-separated leader wallet addresses |
| `USER_ADDRESS` | string | *(empty)* | Fallback single leader address |
| `LEADER_AUTO` | bool | *(derived)* | Auto-promote discovered leaders |
| `LEADER_AUTO_COUNT` | int | `5` | Number of leaders to auto-promote |
| `AUTO_DISCOVER_LEADERS` | bool | `false` | Enable leader auto-discovery |
| `AUTO_DISCOVER_MAX_LEADERS` | int | `10` | Maximum discovered leaders |
| `AUTO_DISCOVER_TOP_N` | int | `5` | Top N leaders to promote from discovery |
| `AUTO_DISCOVER_MIN_SCORE` | float | `0.45` | Minimum score for auto-discovered leaders |
| `COPY_DECISION_MIN_SCORE` | float | `0.15` | Minimum score to copy a signal |
| `TINY_LEADER_HIGH_CONFIDENCE_SCORE` | float | `0.75` | High-confidence score for small leaders |
| `COPY_MAX_PRICE_DRIFT_PCT` | float | `15` | Max price drift from signal reference |
| `COPY_MAX_EDGE_DECAY_PCT` | float | `90` | Max edge decay percentage |
| `COPY_MAX_SIGNAL_AGE_SECONDS` | float | `100` | Maximum age of a signal before rejection |
| `COPY_LIQUIDITY_TARGET_USDC` | float | `50` | Minimum liquidity target for copy execution |
| `MIN_LEADER_TRADE_NOTIONAL_USDC` | float | `1` | Minimum notional for a leader trade to qualify |
| `LEADER_SCORE_BALANCE_TARGET_USDC` | float | `500` | Balance target for scoring normalization |
| `LEADER_SCORE_PNL_BONUS_CAP` | float | `0.10` | PnL bonus cap in scoring |
| `LEADER_SCORE_ACTIVITY_TARGET` | int | `8` | Activity target for scoring normalization |
| `LEADER_SCORE_WEIGHT_BALANCE` | float | `0.35` | Weight for balance component of score |
| `LEADER_SCORE_WEIGHT_ANALYTICS` | float | `0.45` | Weight for analytics component of score |
| `LEADER_SCORE_WEIGHT_ACTIVITY` | float | `0.20` | Weight for activity component of score |
| `LEADER_SCORE_FLOOR` | float | `0.10` | Minimum possible leader score |
| `TIER_PROMOTE_TO_PAPER` | float | `0.40` | Score threshold: promote to paper_follow |
| `TIER_PROMOTE_TO_LIVE` | float | `0.55` | Score threshold: promote to live_follow |
| `TIER_MIN_PAPER_SECONDS` | float | `86400` | Minimum time in paper tier before live promotion |
| `TIER_DEMOTE_TO_PAPER` | float | `0.35` | Score threshold: demote to paper_follow |
| `TIER_DEMOTE_TO_WATCHLIST` | float | `0.25` | Score threshold: demote to watchlist |

---

## LoggingConfig

Structured logging configuration.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `POLYMARKET_LOG_LEVEL` | string | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |
| `LOG_DIR` | string | `log` | Directory for log files |
| `APP_LOG_JSONL_FILE` | string | `log/PolymarketCopyTradingBot.jsonl` | JSONL audit log file path |
| `LOG_MAX_BYTES` | int | `5242880` | Maximum log file size (5 MB) |
| `LOG_BACKUP_COUNT` | int | `3` | Number of rotated log files to keep |
| `PNL_LOG_FILE` | string | `log/pnl.jsonl` | PnL tracking log file path |

---

## NetworkConfig

Blockchain network and metrics server.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `RPC_URL` | string | *(see WalletConfig)* | Polygon RPC endpoint |
| `ENFORCE_GEOBLOCK` | bool | `true` | Enable geoblock compliance check |
| `ENABLE_METRICS_SERVER` | bool | `false` | Enable Prometheus-style metrics endpoint |
| `METRICS_PORT` | int | `9102` | Port for metrics server |
| `METRICS_BIND_HOST` | string | `127.0.0.1` | Bind address for metrics server |
| `USDC_CONTRACT_ADDRESS` | string | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` | USDC (bridged) contract on Polygon |
| `PUSD_CONTRACT_ADDRESS` | string | `0x5731a8987bC70324e2C14d22D95a9cF0E84b687E` | pUSD contract on Polygon |

---

## ReconciliationConfig

| Env Var | Type | Default | Description |
|---|---|---|---|
| `RECONCILE_STALE_SECONDS` | float | `3600` | Seconds before an order is considered stale |
| `RECONCILE_AUTO_CANCEL` | bool | `false` | Automatically cancel stale orders |

---

## RedemptionConfig

| Env Var | Type | Default | Description |
|---|---|---|---|
| `ENABLE_AUTO_REDEEM` | bool | `false` | Enable automatic position redemption |

---

## DiscoveryConfig

| Env Var | Type | Default | Description |
|---|---|---|---|
| `ENABLE_TRADER_DISCOVERY` | bool | *(derived)* | Enable trader discovery service |
| `ALLOW_AUTO_TRADER_DETECTION` | bool | `false` | When no manual leaders are configured, allow startup-only DB/discovery leader selection |
| `MAX_AUTO_TRADERS_COUNT` | int | `2` | Maximum startup auto-selected leaders, clamped by `AUTO_DISCOVER_MAX_LEADERS` |
| `DISCOVERY_INTERVAL_SECONDS` | int | `3600` | Seconds between discovery scans |
| `DISCOVERY_PROMOTE_TO_ACTIVE` | bool | *(derived)* | Promote discovered leaders to active |
| `MIN_LEADER_PNL` | float | `1000` | Minimum PnL for discovered leaders |
| `MIN_LEADER_WIN_RATE` | float | `0.55` | Minimum win rate for discovered leaders |
| `MIN_LEADER_POSITIONS` | int | `20` | Minimum positions for discovered leaders |
| `MIN_ACTIVE_POSITIONS` | int | `1` | Minimum currently active positions |
| `DISCOVERY_MAX_RESULTS` | int | `50` | Maximum results from discovery API |
| `DISCOVERY_CACHE_TTL_SECONDS` | float | `3600` | Discovery cache TTL |
| `MULTI_USER_CONFIG_FILE` | string | *(empty)* | Path to multi-user configuration file |

---

## FeatureFlagConfig

Feature toggles for advanced exit strategies.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `ENABLE_PROFIT_LOCK` | bool | `false` | Enable profit lock exit |
| `ENABLE_NEAR_MAX_EXIT` | bool | `true` | Enable near-max price exit |
| `ENABLE_TRAILING_STOP` | bool | `false` | Enable trailing stop |
| `ENABLE_STAGED_TP` | bool | `false` | Enable staged take-profit |
| `ENABLE_LEADER_DIVERGENCE_EXIT` | bool | `false` | Enable leader divergence exit |
| `ENABLE_STOP_LOSS` | bool | `false` | Enable stop-loss exit |
| `ENABLE_TIER_GATE_WATCHLIST` | bool | `false` | Gate trades on watchlist tier |
| `ENABLE_TIER_GATE_WATCHLIST_LOG` | bool | `false` | Log-only mode for tier gate |

---

## Paper vs Live Mode Differences

| Aspect | Paper Mode | Live Mode |
|---|---|---|
| Order submission | Simulated (no real orders) | Real CLOB orders |
| Fill tracking | Simulated fills at signal price | Real venue fills |
| `PRIVATE_KEY` required | No | Yes |
| Geoblock enforced | If `ENFORCE_GEOBLOCK=true` | Always enforced |
| Redemption | Not applicable | Active if `ENABLE_AUTO_REDEEM=true` |
| Risk blocks | Tracked but not enforced | Fully enforced |
| `bot doctor` | `--paper` (relaxed checks) | `--live` (strict checks) |

---

## Config Validation Rules

The `load_config()` function enforces the following invariants at startup:

1. Python runtime must be 3.11.x or 3.12.x
2. `PRIVATE_KEY` must be a 66-char hex string (or loaded from `PRIVATE_KEY_FILE`)
3. `FUNDER_ADDRESS` must be a valid 42-char Ethereum address
4. `CHAIN_ID` must be 137 (mainnet) or 80002 (testnet)
5. `RPC_URL` must be non-empty
6. `MAX_POSITION_PCT` must be in (0, 1]
7. `TRADE_MULTIPLIER` must be non-negative
8. `ADAPTIVE_SIZING_BASIS` must be `trade_size` or `performance`
9. `NEAR_MAX_PRICE_WARN` must be ≤ `NEAR_MAX_PRICE_EXIT`
10. All leader score weights must be non-negative and sum to > 0
11. All exposure limits, position limits, and drawdown percentages must be positive
12. No inline comments in `.env` values

If any validation fails, the bot prints all errors and exits immediately. No partial configuration is accepted.

---

## Next Steps

- See [03-paper-trading.md](03-paper-trading.md) for configuring paper mode.
- See [05-risk-controls.md](05-risk-controls.md) for detailed risk parameter explanations.
- See [06-leader-selection.md](06-leader-selection.md) for leader configuration guidance.
