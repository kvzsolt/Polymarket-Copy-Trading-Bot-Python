# 02 - Configuration Reference

The bot uses a strictly typed configuration system (`config.app_config.AppConfig`). It fails closed if required variables are missing.

## File Structure
- `secrets.env`: Holds sensitive keys (ignored by git).
- `config/runtime.local.env`: Holds your operational overrides (leaders, sizing, limits).

## Core Variables

### Identity & Funding (`secrets.env` & `runtime.local.env`)
- `PRIVATE_KEY`: Your Polygon wallet private key. (Required for live trading).
- `FUNDER_ADDRESS`: The public address of the wallet funding the trades.
- `USER_ADDRESS`: Fallback leader wallet to follow (if explicit list is empty).
- `LEADER_WALLETS`: Comma-separated list of wallets to strictly follow.
- `FALCON_API_TOKEN`: Used by the Discovery engine to fetch wallet history.

### Risk & Sizing (`runtime.local.env`)
- `DAILY_DRAWDOWN_LIMIT_PCT`: Max percentage of portfolio you are willing to lose in 24h before the kill switch triggers.
- `MAX_POSITION_SIZE_USDC`: Hard cap on how much USDC can be allocated to a single position, regardless of the leader's sizing.
- `CIRCUIT_BREAKER_ENABLED`: Set to `true` to enable global trading halts on anomalous errors.

### Auto Discovery Engine
- `FALCON_MIN_WIN_RATE_15D`: Minimum win rate (e.g., 0.55) to consider a candidate.
- `FALCON_MIN_PNL_15D`: Minimum profit in the last 15 days.
- `DISCOVERY_DYNAMIC_MONITORING_ENABLED`: Set `true` to let the bot auto-rotate leaders.

## Multi-User / Allocator Mode
If running for multiple sub-accounts, you can specify `MULTI_USER_CONFIG_FILE`. Keep this blank for standard single-wallet operations.