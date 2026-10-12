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

## Follow Whitelists (who gets copied)

Per-copier `follows` configuration is an **explicit whitelist** and the source
of truth. A leader wallet with no follow entry for a copier is **not** copied —
there is no implicit union of the global/discovered leader set into every
account. This applies at startup, to newly discovered leaders, and to the CLI.

**Legacy single-account migration.** When exactly ONE user account exists AND
its follows did not come from explicit configuration — i.e. no
`MULTI_USER_CONFIG_FILE` is set and the account is the config-less fallback —
the historical default still applies: that lone account auto-follows every
global leader (sized by the global `sizing`/`risk` configuration). The
default keys on the absence of explicit follows *configuration*, not on the
in-memory follows map being empty, so it applies at startup AND at every
leader refresh: the config-less lone account keeps auto-following newly
discovered/promoted leaders, exactly like the pre-whitelist behavior
(including `--auto-trader` startup). The moment the account is loaded from a
multi-user configuration — any follow entry (enabled or disabled), or even an
explicitly empty follows list — or a second account exists, the migration no
longer applies and absence of an entry means not-following.

An empty whitelist (or disabling all entries) stops **new** copy entries only.
Existing open positions keep their protective exits, reconciliation, and risk
monitoring: the runtime builds risk-only contexts for accounts with open
positions, and all of it runs through the same account-owned coordination
state (one position lock map and one intent registry per follower account,
shared by every leader binding and the execution coordinator).