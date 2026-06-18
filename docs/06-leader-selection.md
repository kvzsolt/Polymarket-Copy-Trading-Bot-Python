# Leader Selection

> How to choose, configure, and manage leader wallets for copy trading.

---

## Overview

The bot's performance is fundamentally determined by which leaders you follow. A leader is any Polymarket trader whose wallet activity you monitor and potentially copy. The bot supports **manual configuration**, **startup-only auto trader detection**, and **manual rotation** from the discovery database.

Regardless of how leaders are selected, the bot applies a consistent evaluation pipeline:
1. **Signal detection** — Monitor the leader's wallet for new trades
2. **Copy decision scoring** — Score the signal's quality and relevance
3. **Trust multiplier** — Scale position sizes based on leader trust
4. **Tier management** — Promote or demote leaders through trust tiers

---

## Manual Leader Configuration

### Setting LEADER_WALLETS

The simplest way to specify leaders is via the `LEADER_WALLETS` environment variable:

```env
LEADER_WALLETS=0xABC123...DEF456,0x789GHI...012JKL
```

This is a comma-separated list of 42-character Ethereum addresses. Each address must be a valid hex string starting with `0x`.

### Fallback: USER_ADDRESS

In manual mode, if `LEADER_WALLETS` is not set, the bot falls back to `USER_ADDRESS` as a single leader:

```env
USER_ADDRESS=0xABC123...DEF456
```

This is a legacy compatibility option. Prefer `LEADER_WALLETS` for multi-leader support. When startup auto leader selection is enabled through `LEADER_SOURCE=auto`, `AUTO_LEADER_ENABLED=true`, or `ALLOW_AUTO_TRADER_DETECTION=true`, empty `LEADER_WALLETS` means no manual leaders and does not fall back to `USER_ADDRESS`.

### Validation

The bot validates all leader addresses at startup:
- Each address must be exactly 42 characters (0x + 40 hex digits)
- Invalid addresses trigger a warning and are excluded from monitoring
- If no valid leaders are configured and auto-discovery is disabled, the bot will not start

---

## Startup Auto Trader Detection

Startup auto trader detection is opt-in and only applies when no manual leaders are configured. Manual `LEADER_WALLETS` always win.

```env
LEADER_WALLETS=
USER_ADDRESS=
LEADER_SOURCE=auto
AUTO_LEADER_ENABLED=true
AUTO_LEADER_COUNT=2
ENABLE_TRADER_DISCOVERY=true
FALCON_SORT_MODES=h_score,pnl,sharpe,roi,win_rate
LEADER_SCORE_VERSION=leader_score_v2
DISCOVERY_AUTO_WRITE_LEADERS=false
DISCOVERY_DYNAMIC_MONITORING_ENABLED=false
DISCOVERY_REJECT_STALE_EVALUATIONS=true
DISCOVERY_EVALUATION_TTL_SECONDS=3600
AUTO_DISCOVER_MIN_SCORE=0.55
```

At startup the bot checks existing live-eligible discovery evaluations in the database. If none exist, it runs one synchronous discovery bootstrap before CLOB clients, user runtime state, market data, user-data websockets, risk monitors, or trade monitors are started. If eligible candidates are still unavailable after that bootstrap, live runtime startup is blocked cleanly.

The selector checks the DB first, then runs one synchronous discovery bootstrap, then checks the DB again. It does not write `LEADER_WALLETS`, does not edit runtime env files, does not lower discovery thresholds, and does not hot-add leaders while the bot is already running unless `DISCOVERY_DYNAMIC_MONITORING_ENABLED=true`.

| Env Var | Default | Effect |
|---|---|---|
| `LEADER_SOURCE` | `manual` | `auto` enables startup auto-selection and disables `USER_ADDRESS` fallback when `LEADER_WALLETS` is empty |
| `AUTO_LEADER_ENABLED` | `false` | Alias for startup auto-selection |
| `AUTO_LEADER_COUNT` | `2` | Startup-selected leader count |
| `ALLOW_AUTO_TRADER_DETECTION` | `false` | Legacy alias for startup-only DB/discovery selection |
| `MAX_AUTO_TRADERS_COUNT` | `2` | Legacy startup-selected leader count, clamped by `AUTO_DISCOVER_MAX_LEADERS` |
| `AUTO_LEADER_REQUIRE_SCORE_VERSION` | `true` | Only select DB rows matching `LEADER_SCORE_VERSION` |

---

## Auto-Discovery Mode

Auto-discovery fetches and scores candidate traders, then stores evaluations in the database. The database is the canonical source of truth for ranked discovered leaders. Normal runtime discovery is DB-only by default: it does not rewrite env files and does not start live monitors by itself.

### Enabling Auto-Discovery

There are several interrelated flags that control auto-discovery. The bot derives the effective settings using a cascade:

| Flag | Default | Effect |
|---|---|---|
| `AUTO_DISCOVER_LEADERS` | `false` | Directly enables auto-discovery |
| `DISCOVERY_PROMOTE_TO_ACTIVE` | `false` | Discovered leaders become active monitored |
| `ENABLE_TRADER_DISCOVERY` | `false` | Enables the discovery service |
| `LEADER_AUTO` | *(derived)* | Shorthand: enables both discovery and promotion |

The cascade logic:
```
LEADER_AUTO = AUTO_DISCOVER_LEADERS OR DISCOVERY_PROMOTE_TO_ACTIVE
auto_discover_leaders = AUTO_DISCOVER_LEADERS OR LEADER_AUTO
enable_trader_discovery = ENABLE_TRADER_DISCOVERY OR LEADER_AUTO OR auto_discover_leaders
discovery_promote_to_active = DISCOVERY_PROMOTE_TO_ACTIVE OR LEADER_AUTO
```

### Simple Configuration

For most operators, just set:

```env
LEADER_AUTO=true
LEADER_AUTO_COUNT=5
```

This enables discovery, promotion, and trader discovery settings. Discovery will:
1. Query Falcon leaderboard candidates across configured sort modes
2. Score each candidate
3. Store candidate evaluations for rotation or startup auto trader detection

### Discovery Filters

| Parameter | Default | Description |
|---|---|---|
| `MIN_LEADER_PNL` | `1000` | Minimum lifetime PnL (USD) |
| `MIN_LEADER_WIN_RATE` | `0.55` | Minimum win rate (0–1) |
| `MIN_LEADER_POSITIONS` | `20` | Minimum total positions traded |
| `MIN_ACTIVE_POSITIONS` | `1` | Minimum currently open positions |
| `DISCOVERY_MAX_RESULTS` | `50` | Maximum API results to evaluate |
| `AUTO_DISCOVER_MIN_SCORE` | `0.45` | Minimum composite score for promotion |
| `FALCON_SORT_MODES` | `h_score,pnl,sharpe,roi,win_rate` | Leaderboard sort modes to query and merge |
| `FALCON_DEDUPE_CANDIDATES` | `true` | Deduplicate candidates by normalized wallet |
| `FALCON_MAX_CANDIDATES` | `250` | Maximum Falcon candidates per discovery run |

### Discovery Cadence

| Parameter | Default | Description |
|---|---|---|
| `DISCOVERY_INTERVAL_SECONDS` | `3600` | Full discovery scan interval (1 hour) |
| `DISCOVERY_REFRESH_INTERVAL_SECONDS` | `300` | Quick refresh interval (5 minutes) |
| `DISCOVERY_CACHE_TTL_SECONDS` | `3600` | Cache TTL for discovery results |

---

## Leader Scoring System

Every leader (manual or discovered) is assigned a **composite score** between 0 and 1. This score drives the copy decision gate, the trust size multiplier, and the tier promotion/demotion system.

Discovery evaluations store `score_version` and `score_breakdown_json`. The default discovery formula is `leader_score_v2`; `leader_score_v1` remains supported for compatibility.

### Composite Score Components

For Falcon discovery v2:

| Component | Weight |
|---|---|
| Falcon quality | `0.30` |
| Copyability replay | `0.25` |
| Recent consistency | `0.15` |
| Market fit | `0.10` |
| 15-day volume fit | `0.10` |
| Leaderboard rank quality | `0.05` |
| Risk penalty inverse | `0.05` |

Key v2 controls:

| Parameter | Default | Description |
|---|---|---|
| `LEADER_SCORE_VERSION` | `leader_score_v2` | Discovery scoring formula version |
| `DISCOVERY_VOLUME_TARGET_15D` | `10000` | Volume normalization target |
| `DISCOVERY_MIN_VOLUME_15D` | `0` | Volume floor |
| `DISCOVERY_RANK_TARGET` | `100` | Rank normalization target |
| `DISCOVERY_RISK_PENALTY_ENABLED` | `true` | Penalize very low activity, narrow market coverage, suspicious win-rate/trade-count combinations, declining trajectory, and unknown volume |

Legacy leader scoring still uses:

The composite score is a weighted sum of three components:

| Component | Weight (Default) | Description |
|---|---|---|
| Balance | `0.35` | Leader's account balance relative to target |
| Analytics | `0.45` | Win rate, PnL, and ROI metrics |
| Activity | `0.20` | Trading frequency relative to target |

### Score Calculation

```
balance_score = min(1.0, leader_balance / BALANCE_TARGET_USDC)
analytics_score = derived from win_rate, pnl, and roi
activity_score = min(1.0, leader_trades / ACTIVITY_TARGET)

composite = (weight_balance × balance_score
           + weight_analytics × analytics_score
           + weight_activity × activity_score)
           / (weight_balance + weight_analytics + weight_activity)

composite = max(composite, LEADER_SCORE_FLOOR)
```

### Key Scoring Parameters

| Parameter | Default | Description |
|---|---|---|
| `LEADER_SCORE_BALANCE_TARGET_USDC` | `500` | Balance normalization target |
| `LEADER_SCORE_PNL_BONUS_CAP` | `0.10` | Maximum PnL bonus added to score |
| `LEADER_SCORE_ACTIVITY_TARGET` | `8` | Activity normalization target (trades) |
| `LEADER_SCORE_WEIGHT_BALANCE` | `0.35` | Weight for balance component |
| `LEADER_SCORE_WEIGHT_ANALYTICS` | `0.45` | Weight for analytics component |
| `LEADER_SCORE_WEIGHT_ACTIVITY` | `0.20` | Weight for activity component |
| `LEADER_SCORE_FLOOR` | `0.10` | Minimum possible score |

### Validation Rules

- All weights must be non-negative
- Weights must sum to > 0
- `LEADER_SCORE_FLOOR` must be in [0, 1]

---

## Copy Decision Gate

When `ENABLE_COPY_DECISION_GATE=true`, every signal is evaluated through the copy decision gate before execution. The gate checks:

### Decision Gate Parameters

| Parameter | Default | Description |
|---|---|---|
| `COPY_DECISION_MIN_SCORE` | `0.15` | Minimum leader score to copy a signal |
| `TINY_LEADER_HIGH_CONFIDENCE_SCORE` | `0.75` | High-confidence score override for small leaders |
| `COPY_MAX_PRICE_DRIFT_PCT` | `15` | Maximum price drift from signal reference |
| `COPY_MAX_EDGE_DECAY_PCT` | `90` | Maximum edge decay from original signal |
| `COPY_MAX_SIGNAL_AGE_SECONDS` | `100` | Maximum signal age before rejection |
| `COPY_LIQUIDITY_TARGET_USDC` | `50` | Minimum liquidity in the orderbook |
| `MIN_LEADER_TRADE_NOTIONAL_USDC` | `1` | Minimum leader trade size to qualify |

### Gate Logic

1. **Score check** — Leader's composite score must be ≥ `COPY_DECISION_MIN_SCORE`
2. **Price drift** — Current price must not have drifted more than `COPY_MAX_PRICE_DRIFT_PCT` from the signal price
3. **Edge decay** — Remaining edge must be ≥ (100% - `COPY_MAX_EDGE_DECAY_PCT`)
4. **Signal freshness** — Signal must be younger than `COPY_MAX_SIGNAL_AGE_SECONDS`
5. **Liquidity** — Orderbook must have ≥ `COPY_LIQUIDITY_TARGET_USDC` available

If any check fails, the signal is rejected with a descriptive reason logged in the audit trail.

---

## Trust Multiplier

When `ENABLE_TRUST_SIZE_MULTIPLIERS=true` (default), the position size is scaled by the leader's trust score:

```
trust_multiplier = TRUST_SIZE_MULTIPLIER_MIN
                 + (TRUST_SIZE_MULTIPLIER_MAX - TRUST_SIZE_MULTIPLIER_MIN)
                 × score_normalized

adjusted_size = base_size × trust_multiplier
```

| Parameter | Default | Description |
|---|---|---|
| `ENABLE_TRUST_SIZE_MULTIPLIERS` | `true` | Enable trust-based sizing |
| `TRUST_SIZE_MULTIPLIER_MIN` | `0.50` | Multiplier for lowest-scored leaders |
| `TRUST_SIZE_MULTIPLIER_MAX` | `1.25` | Multiplier for highest-scored leaders |

A leader with a perfect score (1.0) gets a 1.25× multiplier, while a leader at the minimum threshold gets 0.50×. This naturally allocates more capital to higher-quality signals.

---

## Tier System

The tier system manages leader trust progression through a permission ladder:

### Tiers

| Tier | Description |
|---|---|
| `watchlist` | Leader is tracked but no trades are copied |
| `paper_follow` | Leader's signals are copied in paper mode only |
| `live_follow` | Leader's signals are copied with real orders |

### Promotion Thresholds

| Parameter | Default | Description |
|---|---|---|
| `TIER_PROMOTE_TO_PAPER` | `0.40` | Score to promote from watchlist → paper_follow |
| `TIER_PROMOTE_TO_LIVE` | `0.55` | Score to promote from paper_follow → live_follow |
| `TIER_MIN_PAPER_SECONDS` | `86400` | Minimum 24 hours in paper before live promotion |

### Demotion Thresholds

| Parameter | Default | Description |
|---|---|---|
| `TIER_DEMOTE_TO_PAPER` | `0.35` | Score to demote from live_follow → paper_follow |
| `TIER_DEMOTE_TO_WATCHLIST` | `0.25` | Score to demote from paper_follow → watchlist |

### Time Requirement

A leader must spend at least `TIER_MIN_PAPER_SECONDS` (default: 24 hours) in the `paper_follow` tier before being eligible for promotion to `live_follow`. This prevents rapid promotion of untested leaders based on short-term score spikes.

### Tier Evaluation

The `LeaderRegistry` evaluates tiers on every discovery refresh. The `evaluate_tier` function from `services/discovery/permission_ladder.py` considers:
- Current tier and time in tier
- Current composite score
- Whether the leader has sufficient data coverage
- Promotion and demotion thresholds from configuration

---

## What Makes a Good Leader

There is no guaranteed formula, but historically good leaders tend to share these characteristics:

1. **Consistent PnL** — Positive lifetime PnL over many trades, not just a few lucky wins
2. **High win rate** — Above 55% suggests the leader has a genuine edge
3. **Sufficient activity** — At least 20+ total positions shows the track record is not a fluke
4. **Large balance** — A leader with significant capital at risk has skin in the game
5. **Diversified positions** — Not concentrated in a single market category
6. **Reasonable trade sizes** — Very small trades may indicate testing, not conviction

### Red Flags

- Very few positions (under 10) — Insufficient data
- All gains from a single market — Likely luck, not skill
- Rapid position turnover — May be market-making, not directional trading
- Declining win rate — Edge may be deteriorating
- Very recent account — No track record to evaluate

---

## Risk of Following Leaders

- **Correlation risk** — If all your leaders trade the same markets, you're effectively concentrated in one thesis
- **Survivorship bias** — You only see leaders who are currently profitable; past leaders who blew up are invisible
- **Timing risk** — You may copy a leader's entry late due to polling latency, getting a worse price
- **Size mismatch** — A leader's position size may be appropriate for their $100K account but not for your $500 account
- **Strategy drift** — Leaders may change their strategy without warning
- **Exit divergence** — A leader may exit quickly while your copy order is still pending

---

## Leader Registry Modes

The `LeaderRegistry` refresh path remains DB/scoring-focused. Live runtime leader selection is decided before CLOB startup by manual `LEADER_WALLETS` or the startup auto trader bootstrap.

| Mode | Condition | Behavior |
|---|---|---|
| `configured` | `LEADER_WALLETS` set | Only manually configured leaders are monitored |
| `no_configured_leaders` | No manual leaders | Discovery refresh scores DB candidates; live runtime starts only if startup auto trader detection selected eligible leaders |

Dynamic monitoring can hot-add newly selected DB leaders after startup only when `DISCOVERY_DYNAMIC_MONITORING_ENABLED=true`. It is disabled by default. When enabled, the existing startup reconciliation safety path runs before new monitor tasks start.

---

## Config File Placement

Runtime defaults are loaded from `config/runtime.env`, local public overrides from `config/runtime.local.env`, and sensitive values from `secrets.env`. Use `config/runtime.local.example` as the local operator template for leader discovery settings. Keep `FALCON_API_TOKEN` in `secrets.env`; keep leader-selection knobs, wallet lists, discovery thresholds, score controls, and dynamic-monitoring flags out of `secrets.env`.

---

## Next Steps

- See [05-risk-controls.md](05-risk-controls.md) for how leader scores affect position sizing via the soft cap.
- See [07-execution-lifecycle.md](07-execution-lifecycle.md) for how copy decisions flow into order execution.
