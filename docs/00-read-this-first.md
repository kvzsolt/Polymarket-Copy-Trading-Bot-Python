# 00 - Read This First

Welcome to the Polymarket Copy Trading Engine. Before you start funding wallets and turning on live execution, you need to understand the philosophy and constraints of this system.

## What This Bot IS
- **A systematic follower:** It monitors profitable wallets, scores them, and decides if their trades are safe to copy based on *your* risk limits.
- **A portfolio reconstructor:** It does not blindly copy "buy $100". It calculates the leader's portfolio percentage and applies that ratio to your budget, mimicking partial exits and complex scaling.
- **A defensive engine:** It is built with Postgres-backed idempotency. If the process crashes mid-order, it recovers safely.
- **Automated Discovery:** The Discovery engine finds profitable wallets via Falcon & Polymarket History, validates their copyability against your capital, and rotates them into action.

## What This Bot IS NOT
- **A guaranteed money printer:** You can and will lose money if you copy the wrong leaders or if the market shifts unpredictably.
- **A high-frequency trading (HFT) bot:** It operates on CLOB V2 but is bound by block times and leader execution delays. It is a follower, not an arbitrageur.
- **A naive mirroring script:** It will actively *refuse* to copy a leader if the spread is too wide, the liquidity is too thin, or if the daily drawdown limit is hit.

## The Operator's Duty
You are an operator, not a passenger. Your job is to:
1. Define strict risk limits in the configuration.
2. Monitor the `pmcopy doctor` and `pmcopy drift` outputs.
3. Understand why the bot rejected a trade (read the structured logs!).

Read the configuration guide next.