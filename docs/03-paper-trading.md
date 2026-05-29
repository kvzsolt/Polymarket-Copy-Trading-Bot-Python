# Paper Trading Mode

> How to use paper trading mode for safe, risk-free testing before going live.

---

## Overview

Paper trading mode (`PAPER_TRADE_ONLY=true`) is the bot's dry-run sandbox. It runs the entire signal detection, scoring, sizing, and risk evaluation pipeline — but **substitutes simulated fills for real order submissions**. No real orders touch the Polymarket CLOB. No real capital is at risk.

Paper mode is not an afterthought — it is the **mandatory first step** for every operator. It is the only way to validate that your configuration, leader selection, risk parameters, and execution logic are working correctly before committing real funds.

---

## How Paper Mode Works

When `PAPER_TRADE_ONLY=true` is set, the following behavioral changes take effect:

### Signal Detection — Unchanged
The bot monitors leader wallets exactly as it would in live mode. It detects new trades, partial exits, and position changes with the same polling cadence (`FETCH_INTERVAL`). Signal detection is indistinguishable from live mode.

### Copy Decision Scoring — Unchanged
Every signal is scored through the copy decision engine. The scoring system evaluates leader trust, price drift, edge decay, signal age, and liquidity. Signals that fail the decision gate are rejected with the same reasons as in live mode. This ensures your scoring thresholds are correct before going live.

### Risk Evaluation — Unchanged
The risk engine evaluates every signal against your configured limits (market exposure, category exposure, leader exposure, daily drawdown, position caps). Signals that fail risk checks are rejected. This validates that your risk parameters behave as expected.

### Order Submission — Simulated
Instead of submitting a real order to the Polymarket CLOB, the bot **simulates a fill at the signal price**. The simulated fill is recorded in the database and the position is tracked as if the trade executed. This gives you a realistic view of what would have happened.

### Exit Processing — Simulated
Exit signals (profit lock, trailing stop, stop-loss, near-max exit, leader divergence) are evaluated and simulated fills are generated for exits as well. This tests your entire exit strategy configuration.

---

## Configuration for Paper Mode

### Minimal Paper Mode Setup

```env
# In config/runtime.local.env or secrets.env:
PAPER_TRADE_ONLY=true
LEADER_WALLETS=0xLeaderAddress1,0xLeaderAddress2
ENABLE_RISK_MGMT=true
```

You do **not** need a valid `PRIVATE_KEY` for paper mode. The `bot doctor --paper` check will pass even without a private key configured.

### Recommended Paper Mode Configuration

For the most realistic simulation, enable all features you plan to use in live mode:

```env
PAPER_TRADE_ONLY=true
LEADER_WALLETS=0xLeaderAddress1,0xLeaderAddress2
ENABLE_RISK_MGMT=true
ENABLE_BALANCE_TIERED_POLICY=true
ENABLE_COPY_DECISION_GATE=true
ENABLE_PROFIT_LOCK=true
ENABLE_NEAR_MAX_EXIT=true
ENABLE_TRAILING_STOP=true
ENABLE_STOP_LOSS=true
ENABLE_AUTO_REDEEM=false   # Not relevant in paper mode
```

---

## What Gets Logged and Tracked

In paper mode, the bot produces the same structured audit logs as live mode:

### JSONL Audit Log
Every decision is logged to `log/PolymarketCopyTradingBot.jsonl` with:
- Signal detection events (leader trade identified)
- Copy decision results (approved / rejected with reason)
- Risk evaluation results (headroom calculation, policy application)
- Simulated fills (price, size, market, leader)
- Exit evaluations (triggered / not triggered with reason)

### PnL Log
Simulated profit and loss is tracked in `log/pnl.jsonl`, giving you a realistic P&L picture of what would have happened with real trades.

### Database
All simulated orders, fills, and positions are persisted in SQLite (`data/bot_state.db`). You can query these tables to analyze hypothetical performance:

```sql
-- Review all paper trades
SELECT * FROM orders WHERE created_at > datetime('now', '-1 day');

-- Check simulated fills
SELECT * FROM fills;

-- Review position tracking
SELECT * FROM positions;
```

### Metrics Server
If `ENABLE_METRICS_SERVER=true`, the Prometheus-style metrics endpoint exposes the same gauges and counters as live mode on `http://127.0.0.1:9102/metrics`.

---

## Safety Guarantees in Paper Mode

1. **No real orders** — The CLOB client is never invoked for order submission. This is enforced at the execution layer, not just the configuration layer.
2. **No on-chain transactions** — No transactions are signed or broadcast. The private key is not used even if configured.
3. **No redemption** — Auto-redeem is irrelevant in paper mode since no real positions exist.
4. **No real USDC at risk** — Even if `PRIVATE_KEY` points to a funded wallet, paper mode will not touch those funds.
5. **Database isolation** — Paper mode uses the same database path, but you can configure a separate `BOT_STATE_DB` for paper testing to avoid mixing simulated and real data.

---

## Switching from Paper to Live

Before switching, complete every item on this checklist:

1. **Paper mode has run for at least one full monitoring cycle** (multiple hours) without errors
2. **`bot doctor --live` passes** with status `ok`
3. **Risk parameters are tuned** — Review your PnL log and adjust exposure limits, drawdown thresholds, and position sizes based on paper results
4. **Exit strategies are tested** — Confirm that profit lock, trailing stop, and stop-loss triggers fire at the expected price levels
5. **`PRIVATE_KEY` is configured** — With proper file permissions (0600) or in `secrets.env`
6. **`FUNDER_ADDRESS` matches your signer** — Run `bot doctor --live` to validate the redemption wallet check
7. **Geoblock check passes** — You are not in a restricted region
8. **Kill switch is off** — `KILL_SWITCH=false`

To switch:

```env
# Change in config/runtime.local.env or secrets.env:
PAPER_TRADE_ONLY=false
```

Then restart the bot:

```bash
python app.py
```

See [04-live-trading.md](04-live-trading.md) for the full live trading guide.

---

## Interpreting Paper Mode Results

### Expected Discrepancies

Paper fills are simulated at the signal price. In live mode, you may experience:
- **Slippage** — Real fills may execute at a worse price than the signal
- **Partial fills** — The orderbook may not have enough liquidity for a full fill
- **Rejected orders** — Thin orderbooks, spread violations, or CLOB errors may prevent execution
- **Timing differences** — Network latency may cause delayed fills

These discrepancies are normal and expected. Paper mode gives you a **best-case scenario** estimate. Adjust your expectations for live trading accordingly.

### Metrics to Watch

- **Win rate** — Are your leaders actually profitable?
- **Average fill price vs. signal price** — How much slippage would you expect?
- **Risk rejection rate** — Are too many signals being rejected by risk controls?
- **Position concentration** — Is exposure concentrated in a few markets or leaders?
- **Drawdown simulation** — What would your maximum drawdown have been?

---

## Next Steps

- See [04-live-trading.md](04-live-trading.md) for the live trading transition guide.
- See [05-risk-controls.md](05-risk-controls.md) to tune risk parameters based on paper results.
