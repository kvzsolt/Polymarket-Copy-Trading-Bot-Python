# ⚙️ Usage & Configuration Guide

This document breaks down the various systems, execution gateways, and configurable variables inside the Polymarket Copy Trading Bot. Use this guide to fine-tune the bot's risk tolerance, sizing mechanics, and automated exits.

---

## 🛡️ Execution Gateways (Guardrails)

Before a copied `BUY` signal is sent to the exchange, it must survive a gauntlet of quality gates. If any gate fails, the trade is skipped and logged.

### 1. The Price Drift Gate
Prevents buying if the price has surged since the leader executed their trade.
*   **`COPY_MAX_PRICE_DRIFT_PCT`**: Max allowed percentage difference between the leader's entry price and the current market price. (e.g., `15` means if the leader bought at $0.10, we will not buy if the current price is above $0.115).

### 2. The Edge Decay Gate
Prevents buying if the theoretical "edge" of the trade has decayed too much.
*   **`COPY_MAX_EDGE_DECAY_PCT`**: (e.g., `90`) If the price has moved more than 90% of the way toward $1.00 since the leader bought, the trade is rejected.

### 3. The Book Quality Gate (Spread Check)
Refuses to execute in manipulated, thin, or wide orderbooks to protect against massive slippage.
*   **`EXECUTION_REQUIRE_FRESH_BOOK`**: (`true`/`false`) Forces the bot to pull a live orderbook snapshot right before executing.
*   **`BOOK_SPREAD_MODE`**: Determines how spread is calculated (`absolute_bps` or `ticks`).
*   **`MAX_BOOK_SPREAD_BPS`**: (e.g., `2500` = 25%) The maximum allowed spread between the Best Bid and Best Ask. If the spread is wider, the book is considered broken/unsafe and the trade is skipped.

### 4. The Near-Resolution Gate
Prevents the bot from entering markets that are effectively over and locked.
*   **`ENABLE_NEAR_RESOLUTION_GATE`**: (`true`/`false`) Activates the gate.
*   **`NEAR_RESOLUTION_GATE_PRICE`**: (e.g., `0.95`) If the current price is >= $0.95, the bot will not copy new BUYs, assuming the market is essentially resolved.

---

## 💰 Position Sizing Systems

How the bot decides how many tokens to buy.

### Proportional vs. Fixed Sizing
*   **`SIZING_MODE`**:
    *   `proportional`: Sizes trades as a percentage of your total portfolio equity.
    *   `fixed`: Sizes every trade with a hardcoded USDC amount.
*   **`PERCENTAGE`**: (e.g., `2` = 2%) Used if mode is proportional.
*   **`FIXED_AMOUNT`**: (e.g., `3` = 3 USDC) Used if mode is fixed.
*   **`BUY_MIN_AMOUNT`** / **`BUY_MAX_AMOUNT`**: Absolute USDC floors and ceilings for any single trade, regardless of the sizing mode.

### Balance Tiered Policy (Advanced)
A dynamic risk engine that adjusts your sizing based on your daily drawdown and current available capital.
*   **`ENABLE_BALANCE_TIERED_POLICY`**: (`true`/`false`) Overrides standard sizing with portfolio-aware constraints.
*   **`BALANCE_POLICY_RESERVE_PCT`**: Forces the bot to always keep a certain percentage of your bankroll in cash.
*   **`BALANCE_POLICY_MAX_OPEN_POSITIONS`**: Caps the total number of parallel bets.

---

## 🚪 Exits & Automated Risk Management

How the bot protects profits and cuts losses.

### 1. Stop Loss & Take Profit
*   **`ENABLE_RISK_MGMT`**: (`true`/`false`) Master switch for automated exits.
*   **`STOP_LOSS_PCT`**: (e.g., `20` = 20%) Auto-sells if the position drops 20% from entry.
*   **`TAKE_PROFIT_PCT`**: (e.g., `70` = 70%) Auto-sells if the position gains 70%.

### 2. Profit Lock
A dynamic floor that rises once a position becomes highly profitable.
*   **`ENABLE_PROFIT_LOCK`**: (`true`/`false`)
*   **`PROFIT_LOCK_TRIGGER_PCT`**: (e.g., `70`) Activates the lock when the position is up 70%.
*   **`PROFIT_LOCK_MIN_PROFIT_PCT`**: (e.g., `35`) Once triggered, if the price crashes, the bot force-sells to guarantee at least a 35% profit.

### 3. Near-Max Trim (Resolution Exit)
Takes risk off the table right before a market resolves to avoid last-second oracle rug pulls.
*   **`ENABLE_NEAR_MAX_EXIT`**: (`true`/`false`)
*   **`NEAR_MAX_PRICE_WARN`**: (e.g., `0.95`) Starts trimming the position.
*   **`NEAR_MAX_PRICE_WARN_TRIM_RATIO`**: (e.g., `0.50`) Sells 50% of the position at the warning level.
*   **`NEAR_MAX_PRICE_EXIT`**: (e.g., `0.99`) Dumps the rest of the position.

### 4. Auto-Redeemer
Automatically claims USDC from winning positions after a market resolves.
*   **`ENABLE_AUTO_REDEEM`**: (`true`/`false`) Periodically scans your open positions for fully resolved markets (price >= $0.99 or <= $0.01 with `redeemable=true`).
*   **`REDEEM_CHECK_INTERVAL`**: (e.g., `60`) How often to scan for redeemable payouts (in seconds).
*   *(Note: Auto-redeem costs Polygon gas, but saves you from doing it manually on the UI).*

---

## 🧭 Leader Discovery & Scoring

If you don't want to hardcode leader wallets, the bot can find them for you.

*   **`ENABLE_TRADER_DISCOVERY`**: (`true`/`false`) Enables the discovery background loop.
*   **`AUTO_DISCOVER_MIN_SCORE`**: (e.g., `0.45`) Minimum internal trust score required before a random profitable trader is considered a "leader."
*   **`DISCOVERY_MAX_RESULTS`**: How many top traders the engine is allowed to track at once.
*   **`LEADER_SCORE_WEIGHT_BALANCE` / `_ANALYTICS` / `_ACTIVITY`**: The internal weighting the bot uses to grade traders based on their bankroll, historical PnL, and recent trade frequency.
