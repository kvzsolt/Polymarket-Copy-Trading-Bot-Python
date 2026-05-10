# Read This First

> **Start here before doing anything else with the Polymarket Copy Trading Bot.**

---

## What This Bot Is

The Polymarket Copy Trading Bot is a **self-hosted, open-source automation engine** that monitors leader wallets on the Polymarket prediction market, scores their trading signals, applies portfolio-aware risk controls, and executes copy trades through the Polymarket CLOB (Central Limit Order Book). It is designed for operators who want serious, production-grade copy trading without the chaos of naive mirror-trading tools.

Core capabilities include:

- **Trader Discovery & Ranking** — Automatically discovers profitable leaders, ranks them via a composite scoring system, and optionally promotes them through a tiered permission ladder (watchlist → paper → live).
- **Copy Decision Engine** — Reconstructs leader positions, calculates exact percentage sell-offs, and mimics partial exits with precision.
- **Portfolio-Aware Risk Sizing** — Adapts order sizes dynamically based on available USDC, open exposure, daily drawdown limits, and position caps.
- **Orderbook Quality Gates** — Refuses to execute in thin, manipulated, or spread-gapped orderbooks.
- **Profit Lock & Trailing Stops** — Dynamically protects unrealized gains and secures near-max resolution trims.
- **Idempotency & Reconciliation** — SQLite-backed persistence ensures the bot never double-copies a signal, even if the process restarts mid-tick.
- **Structured Audit Logging** — Every decision, skip, fill, and rejection is logged in JSONL format for post-hoc analysis.

---

## What This Bot Is NOT

| This bot is **NOT** | Why it matters |
|---|---|
| Financial advice | The bot executes trades based on other traders' activity. It does not analyze markets, predict outcomes, or recommend positions. |
| A guaranteed profit machine | Copy trading carries real risk of loss. Leaders can and do lose money. Past performance does not guarantee future results. |
| A custodial service | You hold your own keys. The bot signs transactions locally using your private key. No third party ever takes custody of your funds. |
| A Polymarket product | This is independent, community-built software. Polymarket does not endorse, warranty, or support it. |
| A set-and-forget system | Markets change, leaders change, and software has bugs. You must actively monitor and adjust. |

---

## Key Safety Principles

The bot is built on three non-negotiable safety principles:

### 1. Capital Safety
Every code path is designed to **err on the side of caution**. When the bot encounters uncertainty — a missing API response, an ambiguous order status, a network timeout — it **fails closed**: no trade is executed, no fill is assumed, and no position is created. The reconciliation system (see [07-execution-lifecycle.md](07-execution-lifecycle.md)) never fabricates a fill from missing data.

### 2. Fail-Closed Compliance
The geoblock check (see [10-security.md](10-security.md)) operates on a fail-closed model. If the geoblock API is unreachable, returns malformed data, or is missing the `blocked` field, the bot treats the operator as blocked. It will **not** proceed to trade. This prevents accidental trading from restricted jurisdictions.

### 3. No Duplicate Execution
The reservation system and idempotency layer (see [07-execution-lifecycle.md](07-execution-lifecycle.md)) guarantee that a single trading signal is never executed twice. The `OrderIntentKey.stable_key()` creates a unique fingerprint for each signal, and the `ReservationService` rejects any intent that already has an active reservation.

---

## Quick Start

1. **Install the bot** — Follow the step-by-step guide in [01-installation.md](01-installation.md).
2. **Configure for paper trading** — Set `PAPER_TRADE_ONLY=true` and run `bot doctor --paper` to verify your setup.
3. **Watch it operate** — Monitor logs, observe signal scoring, and validate that the risk engine behaves as expected.
4. **Switch to live (when ready)** — Follow the checklist in [04-live-trading.md](04-live-trading.md).

> ⚠️ **Never skip paper trading.** Paper mode exists precisely to surface configuration mistakes, logic errors, and unexpected behavior before real capital is at risk.

---

## Documentation Index

| Document | Description |
|---|---|
| [01-installation.md](01-installation.md) | System requirements, setup steps, Docker option |
| [02-configuration.md](02-configuration.md) | Full configuration reference for all env vars and sub-configs |
| [03-paper-trading.md](03-paper-trading.md) | How paper mode works, what gets tracked, switching to live |
| [04-live-trading.md](04-live-trading.md) | Live trading prerequisites, checklist, monitoring, emergency stop |
| [05-risk-controls.md](05-risk-controls.md) | Kill switch, account cap, position limits, exit policies, circuit breaker |
| [06-leader-selection.md](06-leader-selection.md) | Manual and auto-discovery leader configuration, scoring, tiers |
| [07-execution-lifecycle.md](07-execution-lifecycle.md) | Order states, reservation system, reconciliation |
| [08-redemption.md](08-redemption.md) | Auto-redeem, wallet validation, troubleshooting |
| [09-troubleshooting.md](09-troubleshooting.md) | Common problems and solutions |
| [10-security.md](10-security.md) | Secret management, API security, audit logging |
| [11-legal-disclaimer.md](11-legal-disclaimer.md) | Legal disclaimers and risk disclosures |
| [12-release-notes.md](12-release-notes.md) | Version history and upgrade instructions |

---

## Technology Stack

- **Language:** Python 3.11+ (3.11.x or 3.12.x required)
- **Persistence:** SQLite3 (orders, fills, positions, reservations, risk blocks)
- **Exchange:** Polymarket CLOB API (V2 SDK via `py-clob-client-v2`)
- **Blockchain:** Polygon PoS (chain ID 137 for mainnet, 80002 for testnet)
- **Architecture:** Clean dependency injection, frozen dataclasses, explicit state machines

---

## Getting Help

- Review [09-troubleshooting.md](09-troubleshooting.md) for common issues.
- Run `python bot_cli/main.py doctor --paper` to diagnose configuration and connectivity problems.
- Check the JSONL audit logs in `log/PolymarketCopyTradingBot.jsonl` for detailed event records.
