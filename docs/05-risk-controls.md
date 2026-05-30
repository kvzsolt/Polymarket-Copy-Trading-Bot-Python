# Risk Controls

> Comprehensive guide to the bot's risk management system, including kill switch, account cap policy, position limits, exit strategies, and circuit breaker.

---

## Overview

The risk management system is the bot's safety net. It operates at multiple layers to prevent catastrophic losses:

1. **Kill switch** — Emergency stop that blocks all new entries
2. **Account cap policy** — Reserve, hard cap, and soft cap based on equity
3. **Exposure limits** — Per-market, per-category, and per-leader caps
4. **Position limits** — Maximum open positions and per-position sizing
5. **Daily drawdown** — Prevents trading after hitting daily loss limit
6. **Exit strategies** — Profit lock, trailing stop, stop-loss, near-max exit
7. **Circuit breaker** — Pauses execution during venue outages
8. **Risk blocks** — Prevents re-entry after risk rule violations

All risk controls are configured via `RiskConfig` and `FeatureFlagConfig`. See [02-configuration.md](02-configuration.md) for the full parameter reference.

---

## Kill Switch

The kill switch (`KILL_SWITCH=true`) is the master emergency stop.

### Behavior When Active
- **All BUY intents are rejected** with reason `kill_switch_enabled`
- **SELL intents still execute** — Existing positions can be closed
- **The bot continues monitoring** — Leaders are still tracked, signals are still scored
- **Risk blocks are recorded** — Every rejected BUY is logged with the kill switch reason

### When to Use It
- You suspect a configuration error
- A leader is behaving erratically
- You want to pause new entries without stopping the bot
- Market conditions are unusual and you want to go flat

### How to Activate

```env
KILL_SWITCH=true
```

Then restart the bot. The `bot doctor --live` check will report `kill_switch_active` as a blocking issue.

### How to Deactivate

```env
KILL_SWITCH=false
```

Restart the bot. The kill switch clears immediately.

---

## Account Cap Policy

The account cap policy (`AccountCapPolicy`) is a unified risk framework that manages capital allocation based on your equity. Enable it with `ENABLE_BALANCE_TIERED_POLICY=true`.

### Reserve

The reserve ensures you always keep a minimum cash buffer:

```
reserve = max(reserve_floor, equity × reserve_pct)
```

| Parameter | Default | Description |
|---|---|---|
| `BALANCE_POLICY_RESERVE_PCT` | `0.12` | 12% of equity reserved |
| `BALANCE_POLICY_RESERVE_FLOOR` | `7` | At least $7 USDC reserved |

The reserve is subtracted from your free cash before any buy headroom is calculated. If `free_cash - reserve <= 0`, no buys are allowed.

### Hard Cap

The hard cap limits the maximum notional for a single trade:

```
hard_cap = min(tradable_cash, equity × hard_cap_pct, hard_cap_ceiling)
```

| Parameter | Default | Description |
|---|---|---|
| `BALANCE_POLICY_HARD_CAP_PCT` | `0.15` | 15% of equity as max trade |
| `BALANCE_POLICY_HARD_CAP_CEILING` | `8` | Absolute ceiling of $8 USDC per trade |

The hard cap ensures no single trade can risk more than a fraction of your equity, with an absolute dollar ceiling as a safety bound.

### Soft Cap

The soft cap is the actual trade size, scaled by the decision score:

```
score_norm = (decision_score - base_min_score) / (1 - base_min_score)
soft_cap = hard_cap × (0.55 + 0.45 × score_norm)
```

| Parameter | Default | Description |
|---|---|---|
| `BALANCE_POLICY_BASE_MIN_SCORE` | `0.22` | Minimum score to get any allocation |

Higher decision scores result in larger trade sizes (up to `hard_cap`). Lower scores get smaller allocations. This creates a natural quality-weighted sizing system.

### Position Management

| Parameter | Default | Description |
|---|---|---|
| `BALANCE_POLICY_MAX_OPEN_POSITIONS` | `12` | Maximum simultaneous open positions |
| `BALANCE_POLICY_ALLOW_PARTIAL_EXITS` | `true` | Allow selling part of a position |
| `BALANCE_POLICY_ALLOW_PYRAMIDING` | `false` | Allow adding to existing positions |
| `BALANCE_POLICY_MAX_ADDS_PER_POSITION` | `1` | Maximum adds per position |

### Dynamic Threshold Raise

When `BALANCE_POLICY_DYNAMIC_THRESHOLD_RAISE=true`, the bot dynamically increases the base minimum score threshold as equity grows. This makes it progressively harder for low-quality signals to trade with larger account sizes, providing natural risk scaling.

---

## Exposure Limits

Exposure limits cap how much USDC can be allocated to specific dimensions:

| Limit | Parameter | Default | Description |
|---|---|---|---|
| Per-market | `MAX_MARKET_EXPOSURE_USDC` | `300` | Maximum USDC in any single market |
| Per-category | `MAX_CATEGORY_EXPOSURE_USDC` | `600` | Maximum USDC in any market category |
| Per-leader | `MAX_LEADER_EXPOSURE_USDC` | `500` | Maximum USDC following any single leader |

### How Headroom Is Calculated

For a BUY intent, the risk engine calculates:

```
market_headroom    = max(0, MAX_MARKET_EXPOSURE - current_market_exposure)
category_headroom  = max(0, MAX_CATEGORY_EXPOSURE - current_category_exposure)
leader_headroom    = max(0, MAX_LEADER_EXPOSURE - current_leader_exposure)
daily_headroom     = max(0, max_daily_loss - current_daily_loss)
```

The final trade cap is the minimum of `hard_cap`, `market_headroom`, `category_headroom`, `leader_headroom`, and `daily_headroom`. If any headroom is zero, the BUY is rejected.

---

## Position Limits

| Parameter | Default | Description |
|---|---|---|
| `MAX_OPEN_POSITIONS` | `40` | Maximum number of open positions across all markets |
| `MAX_POSITION_PCT` | `0.45` | Maximum fraction of balance per single trade |
| `BUY_MIN_AMOUNT` | `6` | Minimum order size in USDC |
| `BUY_MAX_AMOUNT` | `15` | Maximum order size in USDC |

If the number of open positions reaches `MAX_OPEN_POSITIONS`, all BUY intents are rejected with reason `max_open_positions_reached`.

`MAX_POSITION_PCT` caps **per-market exposure** at this fraction of your current **free cash** (not total equity). When a BUY would push a market's exposure past `free_cash × MAX_POSITION_PCT`, the trade is skipped with reason `max_position_pct_reached` and a `⚠️ COPY NOT EXECUTED` panel is printed to the terminal. Raise `MAX_POSITION_PCT` (up to `1.0`) if you want larger single-market concentration; lower it to spread copies across more markets. This is the most common reason a detected leader trade is not copied while the bot is otherwise healthy.

---

## Reservation TTL

| Parameter | Default | Description |
|---|---|---|
| `RESERVATION_TTL_SECONDS` | `120` | How long a pre-submit balance/share reservation stays active before it expires and is reclaimed by reconciliation / the risk-loop reaper |

When a copy is submitted, the bot reserves the required quote (BUY) or shares (SELL) so concurrent intents cannot over-commit the same funds. The reservation is released on fill confirmation, rejection, cancel, or terminal reconciliation; if none of those happen it expires after `RESERVATION_TTL_SECONDS`. Shorten this window if pending orders are blocking subsequent same-scope copies for too long; it must be `> 0`.

**Deployment note (reservation key format):** the reservation key now includes the source trade id so two different leader trades on the same market reserve independently. This changes the stored `order_intent_key` format, so a reservation created by an older build is not matched by the new lookup. The effect is bounded and fail-safe — reservations self-heal within `RESERVATION_TTL_SECONDS` (reclaimed by reconciliation / the risk-loop reaper), and duplicate *orders* are still prevented by the tx-keyed processed-trade claim and coordinator idempotency. For a zero-transient rollout, deploy during a quiescent window so no reservations are active at cutover.

---

## Daily Drawdown Limits

| Parameter | Default | Description |
|---|---|---|
| `MAX_DAILY_DRAWDOWN_PCT` | `20` | Maximum daily loss as percentage of day-start equity |
| `DAY_START_EQUITY_USDC` | `50` | Starting equity for drawdown calculation |

The daily drawdown check prevents trading after you've lost too much in a single day:

```
max_daily_loss = max(2.5, day_start_equity × max_daily_drawdown_pct / 100)
daily_headroom = max(0, max_daily_loss - current_daily_loss)
```

If `DAY_START_EQUITY_USDC` is set to 0 (default when not explicitly configured), the bot uses the current equity as the day-start value. Set it explicitly if you want a fixed reference point.

---

## Exit Policies

Exit strategies are controlled by feature flags. Enable only the exits you need.

### Stop Loss

| Parameter | Default | Description |
|---|---|---|
| `ENABLE_STOP_LOSS` | `false` | Enable stop-loss exit |
| `STOP_LOSS_THRESHOLD` | `0.15` | Price floor — exit if share price drops below this |
| `STOP_LOSS_GRACE_PERIOD_SECONDS` | `180` | Grace period after entry before stop-loss activates |

The stop-loss exits a position if the current share price falls below `STOP_LOSS_THRESHOLD`. The grace period prevents premature exits on volatile entries — the stop-loss does not trigger during the first N seconds after the position was opened.

### Profit Lock

| Parameter | Default | Description |
|---|---|---|
| `ENABLE_PROFIT_LOCK` | `false` | Enable profit lock exit |
| `PROFIT_LOCK_TRIGGER_PCT` | `70` | Profit percentage to trigger lock |
| `PROFIT_LOCK_SELL_FRACTION` | `0.50` | Fraction of position to sell at lock |

When a position's unrealized profit exceeds `PROFIT_LOCK_TRIGGER_PCT`, the bot sells `PROFIT_LOCK_SELL_FRACTION` of the position to lock in gains. The remaining position continues to run.

### Trailing Stop

| Parameter | Default | Description |
|---|---|---|
| `ENABLE_TRAILING_STOP` | `false` | Enable trailing stop |
| `TRAILING_ACTIVATION_PCT` | `20` | Profit percentage to activate trailing |
| `TRAILING_DISTANCE` | `0.05` | Trailing distance as peak-drop percentage (`0.10` or `10` = 10%) |

The trailing stop activates when unrealized profit exceeds `TRAILING_ACTIVATION_PCT`. Once active, it tracks the highest share price seen and exits the position if the price falls by `TRAILING_DISTANCE` percent from that peak.

### Near-Max Exit

| Parameter | Default | Description |
|---|---|---|
| `ENABLE_NEAR_MAX_EXIT` | `true` | Enable near-max price exit (default on) |
| `NEAR_MAX_PRICE_WARN` | `0.95` | Price level for warning trim |
| `NEAR_MAX_PRICE_WARN_TRIM_RATIO` | `0.50` | Fraction to trim at warning level |
| `NEAR_MAX_PRICE_EXIT` | `0.95` | Price level for full exit |

When a share price approaches resolution (close to $1.00 for YES or $0.00 for NO), the bot:
1. **At `NEAR_MAX_PRICE_WARN`**: Sells `NEAR_MAX_PRICE_WARN_TRIM_RATIO` of the position
2. **At `NEAR_MAX_PRICE_EXIT`**: Sells the remaining position

This protects gains from resolution uncertainty and avoids liquidity traps near market resolution.

### Staged Take-Profit

| Parameter | Default | Description |
|---|---|---|
| `ENABLE_STAGED_TP` | `false` | Enable staged take-profit |
| `STAGED_TP_LEVELS` | `30,50` | Comma-separated profit percentage levels |
| `STAGED_TP_FRACTIONS` | `0.25,0.35` | Comma-separated sell fractions at each level |

The staged take-profit sells incrementally at each level. For the default configuration:
- At 30% profit: Sell 25% of the position
- At 50% profit: Sell 35% of the remaining position

### Leader Divergence Exit

| Parameter | Default | Description |
|---|---|---|
| `ENABLE_LEADER_DIVERGENCE_EXIT` | `false` | Enable exit when leader diverts from position |

When the leader exits a position you're still holding, this exit strategy can trigger a sell. This ensures you're not left holding positions that the leader has abandoned.

---

## Circuit Breaker

The circuit breaker (`CircuitBreaker`) protects against venue outages and API failures. It operates in three states:

### States

| State | Behavior |
|---|---|
| `CLOSED` | Normal operation — all requests pass through |
| `OPEN` | Too many failures — all requests are rejected immediately |
| `HALF_OPEN` | After cooldown — allows a limited number of probe requests |

### Configuration

| Parameter | Default | Description |
|---|---|---|
| `CIRCUIT_FAILURE_THRESHOLD` | `15` | Consecutive failures before opening the circuit |
| `CIRCUIT_COOLDOWN_SECONDS` | `60` | Seconds before transitioning from OPEN to HALF_OPEN |

### State Transitions

```
CLOSED ──(failure_count >= threshold)──→ OPEN
OPEN ──(cooldown elapsed)──→ HALF_OPEN
HALF_OPEN ──(probe succeeds)──→ CLOSED
HALF_OPEN ──(probe fails)──→ OPEN
```

When the circuit is OPEN, all execution requests are rejected with a `CircuitOpen` exception. This prevents the bot from spamming a down API and accumulating unconfirmed orders.

---

## How Risk Blocks Prevent Re-Entry

When a risk rule rejects a BUY intent (e.g., max exposure reached, daily drawdown hit, kill switch active), the rejection is recorded in the `risk_blocks` database table. This prevents the same signal from being re-evaluated on the next tick.

### Risk Block Lifecycle

1. **Signal detected** — Leader makes a trade
2. **Risk evaluation** — Risk engine calculates headroom
3. **Rejection** — Headroom is zero or a limit is hit
4. **Block recorded** — The signal key + rejection reason is persisted
5. **Next tick** — The same signal is skipped (idempotency check)
6. **Block expiry** — Blocks expire based on the signal's TTL or a configurable timeout

This prevents the bot from repeatedly evaluating and logging the same rejected signal, which would be noisy and wasteful.

---

## Putting It All Together: Risk Evaluation Flow

For every BUY intent, the risk engine follows this decision tree:

```
1. Is KILL_SWITCH=true?         → REJECT: kill_switch_enabled
2. Is daily drawdown hit?       → REJECT: daily_drawdown_limit_hit
3. Max open positions reached?  → REJECT: max_open_positions_reached
4. No tradable cash after reserve? → REJECT: no_tradable_cash_after_reserve
5. Calculate hard cap from equity and policy
6. Check market headroom        → REJECT if zero: max_market_exposure_exceeded
7. Check category headroom      → REJECT if zero: max_category_exposure_exceeded
8. Check leader headroom        → REJECT if zero: max_leader_exposure_exceeded
9. Calculate soft cap from decision score
10. Final cap = min(hard_cap, soft_cap, all headrooms)
11. If cap > 0                  → APPROVE with adjusted notional
12. If cap <= 0                 → REJECT: no_buy_headroom
```

For SELL intents, the risk engine always approves (reason: `ok_sell_exit`), since exiting a position reduces risk.

---

## Next Steps

- See [06-leader-selection.md](06-leader-selection.md) for how leader scoring affects the soft cap calculation.
- See [07-execution-lifecycle.md](07-execution-lifecycle.md) for how risk decisions flow into the execution pipeline.
