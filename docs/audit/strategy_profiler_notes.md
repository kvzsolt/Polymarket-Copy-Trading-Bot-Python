# Strategy profiler + env tuner — implemented in PR3

PR3 delivered the strategy profiler, archetype classifier, env tuner, and a
dedicated atomic env-patch writer. Dry-run by default; `--apply` required to
mutate `config/runtime.local.env`; protected keys (secrets, RPC, leaders)
never mutated.

## Module layout (shipped)
```
services/strategy_profile/
  __init__.py
  leader_history_loader.py    # fresh Polymarket Data API fetch
  strategy_profiler.py        # 17 features
  strategy_classifier.py      # 6 StrategyArchetype thresholds
  strategy_env_tuner.py       # archetype -> EnvPatch list
  env_patch_writer.py         # dedicated atomic writer + secret allowlist
models/
  strategy_profile.py         # StrategyProfile + StrategyArchetype
  env_patch.py                # EnvPatch frozen dataclass
bot_cli/commands/
  profile_leader.py           # 'bot.py profile-leader'
  tune_strategy.py            # 'bot.py tune-strategy'
```

## Data source
Fresh fetch from `https://data-api.polymarket.com` (`/activity`, `/trades`,
`/closed-positions`) via `aiohttp`. The walk is bounded by
`POLYMARKET_ACTIVITY_MAX_OFFSET`. Network errors degrade gracefully (empty
sections, no crash). Verified endpoints via Firecrawl against
https://docs.polymarket.com/market-data/overview.

## Features (17)
`trade_count_7d`, `trade_count_30d`, `median_notional_usdc`,
`avg_entry_price`, `low_price_entry_ratio`, `near_certainty_entry_ratio`,
`crypto_fast_signal_ratio`, `avg_hold_time_minutes`, `grouped_signal_ratio`,
`maker_fill_ratio`, `taker_fill_ratio`, `win_rate_7d`, `roi_7d`,
`pnl_30d_usdc`, `max_drawdown_30d`, `market_concentration_hhi`,
`near_resolution_ratio`, `near_max_exit_ratio`. Each degrades to `None`
when the underlying data is insufficient.

## Archetypes (6)
`FAST_CRYPTO_SCALPER`, `LOW_PRICE_LOTTERY`, `NEAR_CERTAINTY_SWEEPER`,
`EVENT_SPECIALIST`, `BALANCED_SWING`, `NOISY_UNCOPYABLE`. Too-thin (<5
trades in 30d) and extreme-concentration guards fall back conservatively to
`NOISY_UNCOPYABLE`, which produces an empty patch list (do not auto-tune).

## Env patches (per archetype, from the report)
- `FAST_CRYPTO_SCALPER`: `ENABLE_ONCHAIN_PENDING_DETECTION=true`,
  `PENDING_TX_MAX_SIGNAL_AGE_MS=500`, `COPY_MAX_SIGNAL_AGE_SECONDS=2`,
  `COPY_MAX_PRICE_DRIFT_PCT=3`, `BUY_MAX_AMOUNT=3`, `MAX_POSITION_PCT=0.05`,
  `MARKET_DATA_PRIMARY_SOURCE=rest_snapshot`,
  `MARKET_DATA_REST_SNAPSHOT_MAX_AGE_MS=300`.
- `LOW_PRICE_LOTTERY`: `MIN_SHARE_PRICE=0.03`, `MAX_SHARE_PRICE=0.35`,
  `BUY_MAX_AMOUNT=2`, `MAX_POSITION_PCT=0.03`, `MAX_OPEN_POSITIONS=80`,
  `ENABLE_STOP_LOSS=false`, `ENABLE_NEAR_MAX_EXIT=true`.
- `NEAR_CERTAINTY_SWEEPER`: `MIN_SHARE_PRICE=0.75`, `MAX_SHARE_PRICE=0.95`,
  `ENABLE_NEAR_RESOLUTION_GATE=true`, `NEAR_RESOLUTION_GATE_PRICE=0.95`,
  `ENABLE_NEAR_MAX_EXIT=true`, `NEAR_MAX_PRICE_WARN=0.95`,
  `NEAR_MAX_PRICE_EXIT=0.99`, `BUY_MAX_AMOUNT=6`.
- `EVENT_SPECIALIST` / `BALANCED_SWING`: moderate conservative patches.
- `NOISY_UNCOPYABLE`: empty patch list.

## Safety
- Never modifies secrets (`PRIVATE_KEY`, `CLOB_*`, `RPC_*`, `FUNDER_ADDRESS`,
  `DATABASE_URL`, etc.) — protected-key allowlist (exact + substring).
- Never auto-rewrites `LEADER_WALLETS` unless `--allow-leader-rewrite` is
  explicitly passed AND `--apply` is set.
- Writes only `config/runtime.local.env` (never `runtime.env` or the example).
- Dry-run default; `--apply` required for mutation.
- Atomic write: `.bak.<ts>` backup + `.tmp` then `os.replace`.
- Secret values redacted in the patch explanation.
- The tuner never produces patches for protected keys (tested).