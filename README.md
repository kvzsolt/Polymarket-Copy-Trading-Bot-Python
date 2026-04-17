# 🔮 Polymarket Copy Trading Bot 

A production-oriented Polymarket copy-trading engine built for operators who want more than a toy "mirror wallet" script.

This bot watches one or more leader wallets, scores their signals, applies portfolio-aware risk controls, executes copy trades through the Polymarket CLOB, manages exits, logs everything in structured form, and exposes health/metrics endpoints for real deployment.

In plain English: this repo is for running a serious copy-trading stack on Polymarket with nuclear-grade guardrails.

---

## 💥 Why this exists

Most copy-trading bots are spiritually identical to a raccoon with root access:
- They blindly mirror leader trades.
- They ignore live price drift.
- They size positions with zero context.
- They have weak idempotency and barely log anything.
- They become portfolio arsonists the second the market gets weird.

This project was built to stop the raccoon. The architecture reflects hard-earned production lessons:
- **Don’t blindly copy. Score first.**
- **Don’t size from vibes. Size from balance, liquidity, and trust.**
- **Don’t assume exits are simple. Manage the full lifecycle.**
- **Don’t run dark. Emit structured logs, metrics, health checks, and PnL analytics.**

---

## ⚡ Core Capabilities

- **Trader Discovery & Ranking:** Automatically discovers profitable leaders, ranks them, and optionally promotes them to active trading based on PnL/win-rate criteria.
- **Copy Decision Engine:** Reconstructs leader positions, calculates exact percentage sell-offs, and mimics partial exits flawlessly.
- **Portfolio-Aware Risk Sizing:** Adapts order sizes dynamically based on your available USDC, open exposure to the market/leader, daily drawdown limits, and max position caps.
- **Orderbook Quality Gates:** Refuses to execute in thin, manipulated, or spread-gapped orderbooks. 
- **Profit Lock & Trailing Stops:** Dynamically protects unrealized gains and secures near-max resolution trims.
- **Idempotency & Reconcilitation:** SQLite-backed persistence ensures we never double-copy a signal, even if the node restarts mid-tick.
- **Structured JSONL Audit Logging:** Every decision, skip, fill, and rejection is cleanly logged for analytics.

---

## 🛠️ Stack & Architecture

- **Language:** Python 3.11+
- **Persistence:** SQLite3 (State, fills, positions, orders)
- **Clients:** Polymarket CLOB API, Gamma API, Open-Meteo
- **Architecture:** Clean DI (Dependency Injection) via Composition Root. Everything is testable, decoupled, and explicit.

## 🚀 Getting Started

### 1. Setup Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure
Copy the config templates:
```bash
cp config/runtime.local.example config/runtime.local.env
cp config/secrets.env.example secrets.env
```

Add your keys to `secrets.env`:
```env
PRIVATE_KEY=0xYourHexPrivateKey
RPC_URL=https://polygon-mainnet.infura.io/v3/YourKey
```

### 3. Run
```bash
python3 app.py
```

### 4. Test
```bash
pytest -q
```

---
*Built for the degens who want to sleep at night.*
