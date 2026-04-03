# Repository Guidelines

## Project Structure & Module Organization

- **Entry point:** `index.py` (async orchestrator that validates env, initializes clients, and starts leader monitors).
- **Config & validation:** `config/` (`config/env.py` loads `.env` via `python-dotenv` and enforces fail-fast validation; `config/db.py` handles optional MongoDB connection).
- **Domain services:** `services/` (monitoring, decisioning/scoring, execution, risk management, redemption, metrics/health server).
- **Utilities:** `utils/` (CLOB client wrapper, HTTP fetch helpers, logging, idempotency store).
- **Persistence models:** `models/` (Mongo collection helpers such as `models/userHistory.py`).
- **Tests:** `tests/` (pytest suites; integration tests are opt-in via marker).
- **Runtime state/logs:** `data/` (SQLite state file default `data/bot_state.db`), `log/` and `logs/` (JSONL/console logs configured by `utils/logger.py`).

## Build, Test, and Development Commands

```bash
# Create and activate a virtualenv (example)
python -m venv .venv && source .venv/bin/activate

# Install runtime dependencies
pip install -r requirements.txt

# Run unit tests (integration tests excluded by default via pytest.ini)
pytest

# Run integration tests (requires live external infra such as network/DB/RPC)
pytest -m integration

# Run the bot
python index.py
```

## Coding Style & Naming Conventions

- **Indentation:** 4 spaces (Python).
- **File naming:** mixed `camelCase.py` in `services/` and `utils/` (e.g. `tradeExecutor.py`, `createClobClient.py`); follow the existing module naming in the folder you’re extending.
- **Function/variable naming:** mostly `snake_case` (e.g. `validate_runtime_env()` in `config/env.py`).
- **Async conventions:** core workflows are `async def` and use `asyncio` (`index.py`, `services/tradeMonitor.py`).
- **Formatting/linting:** no repo-enforced formatter/linter config is present (no `.editorconfig`, `ruff`, `black`, or `flake8`). Keep diffs consistent with surrounding code.

## Testing Guidelines

- **Framework:** `pytest` + `pytest-asyncio` (see `pyproject.toml`).
- **Test files:** `tests/test_*.py`.
- **Running tests:** `pytest` (defaults to `-m "not integration"` from `pytest.ini`).
- **Integration tests:** use `@pytest.mark.integration` and are skipped unless explicitly selected.

## Commit & Pull Request Guidelines

- **Commit format:** history uses short, imperative messages (e.g. `first commit`, `update to new repo`, `v5`).
- **PR process / branches:** no PR template, review policy, or branch naming conventions were found in-repo; follow your organization’s standard workflow.

---

# Repository Tour

## 🎯 What This Repository Does

Polymarket-Copy-Trading-Bot-Python is an async Polymarket copy-trading bot: it monitors leader wallets via Polymarket’s data API and places mirrored orders via the Polymarket CLOB API, with optional MongoDB persistence and a Prometheus-style `/metrics` endpoint.

**Key responsibilities:**
- Monitor leader activity and turn bursty fills into stable copy-trade “signals”.
- Apply sizing/decisioning/risk rules and submit orders via the CLOB client.
- Record idempotency locally (SQLite) and optionally persist orders/fills/results to MongoDB.

---

## 🏗️ Architecture Overview

### System Context

```text
Polymarket Data API (activity/positions) ─┐
Polymarket CLOB API (orders/orderbook)   ─┼─> This bot (index.py + services/*) ──> MongoDB (optional)
Polygon RPC (balance/redeem)             ─┘                 │
                                                         └─> SQLite (data/bot_state.db)
```

### Key Components

- **Startup & orchestration (`index.py`)**
  - Validates runtime config (`config/env.py::validate_runtime_env`).
  - Loads leader wallets (`ENV['LEADER_WALLETS']` with a `USER_ADDRESS` fallback).
  - Loads runtime users (`services/multiUserConfig.py`) and builds one CLOB client per runtime user (`utils/createClobClient.py`).
  - Starts one leader monitor per leader wallet; each leader monitor has a local async queue with a worker that calls `services/tradeExecutor.execute_trades()`.

- **Environment & safety validation (`config/env.py`)**
  - Loads `.env` (via `dotenv.load_dotenv()`).
  - Enforces strict parsing rules (notably: **inline comments like `VALUE # comment` are treated as invalid**).
  - Validates risk-critical values early (Python runtime version, key formats, address formats, numeric guardrails).
  - Supports secret loading from a file (`PRIVATE_KEY_FILE`) and enforces permissions (must not be group/world readable).

- **Leader monitoring (`services/tradeMonitor.py`)**
  - Polls `https://data-api.polymarket.com/activity?user=<leader>&...`.
  - Normalizes activity payloads and aggregates “bursty” fills into a single signal window (see `_aggregate_trades()` and `AGGREGATE_SIGNAL_WINDOW_SECONDS`).
  - Uses SQLite-backed idempotency to avoid replaying the same activity (`utils/idempotency.py::build_trade_key` / `has_processed`).

- **Trade execution pipeline (`services/tradeExecutor.py`)**
  - Computes copy sizing (`SIZING_MODE` includes `proportional`, `fixed`, `adaptive`).
  - Uses composed services for execution and decisioning: `ExecutionService`, `CopyDecisionService`, scoring (`LeaderScoringService`, `TradeScoringService`), fees (`FeeService`), and risk (`services/riskEngine.py`).
  - Records processed trades to SQLite (`utils/idempotency.py::mark_processed`).
  - When `MONGO_URI` is set, records trade results to Mongo (via `models/userHistory.py`, and order/fill persistence via `services/orderRepository.py`).

- **Persistence**
  - **SQLite (always):** `utils/idempotency.py` stores processed trades in `processed_trades` at `ENV['BOT_STATE_DB']` (default `data/bot_state.db`).
  - **MongoDB (optional):** `config/db.py` connects/pings Mongo; `services/orderRepository.py` writes `orders` and `fills` collections with an in-memory fallback when no DB is configured.

- **Observability & ops**
  - **Logging:** `utils/logger.py` provides structured JSONL logging and Rich console output.
  - **Health/Metrics:** `services/healthServer.py` serves `/health` and `/metrics` (Prometheus exposition format) when `ENABLE_METRICS_SERVER` is enabled.

### Data Flow

1. `index.py` runs `validate_runtime_env()` and loads `leader_wallets` (env + optional leader auto-discovery via `services/traderDiscoveryService.py`).
2. For each leader wallet, `services/tradeMonitor.trade_monitor()` polls the data API, aggregates fills, and emits a list of normalized trades.
3. The leader’s worker dequeues the aggregated trades and calls `services/tradeExecutor.execute_trades()` for each runtime user that follows that leader.
4. `tradeExecutor` applies decisioning, sizing, and risk gates, then submits orders via `ExecutionService` (which uses `utils/createClobClient.py` under the hood).
5. State is recorded: SQLite idempotency is updated; Mongo persistence is used when configured.

---

## 📁 Project Structure [Partial Directory Tree]

```text
project-root/
├── index.py
├── README.md
├── requirements.txt
├── pyproject.toml
├── pytest.ini
├── config/
│   ├── env.py
│   └── db.py
├── services/
│   ├── tradeMonitor.py
│   ├── tradeExecutor.py
│   ├── executionService.py
│   ├── copyDecisionService.py
│   ├── riskEngine.py
│   ├── riskMonitor.py
│   ├── autoRedeem.py
│   ├── redemptionService.py
│   ├── traderDiscoveryService.py
│   └── healthServer.py
├── utils/
│   ├── createClobClient.py
│   ├── fetchData.py
│   ├── idempotency.py
│   ├── logger.py
│   └── getMyBalance.py
├── models/
│   └── userHistory.py
├── tests/
│   └── test_*.py
├── data/
├── log/
└── logs/
```

### Key Files to Know

| File | Purpose | When You’d Touch It |
|------|---------|---------------------|
| `index.py` | Bootstraps the bot; starts leader monitors and worker queues | Adding/removing background loops, wiring new services |
| `config/env.py` | Runtime env parsing + strict validation | Adding new config keys or tightening guardrails |
| `config/db.py` | Mongo connect/ping + `get_database()` | Changing DB defaults/behavior |
| `services/tradeMonitor.py` | Polls activity feed; aggregates and dedupes signals | Tuning polling/aggregation/idempotency behaviors |
| `services/tradeExecutor.py` | Main copy-trade execution pipeline | Adjusting sizing, decisioning, risk, or persistence |
| `services/multiUserConfig.py` | Multi-user follow config via `MULTI_USER_CONFIG_FILE` | Introducing new per-user knobs/limits |
| `utils/createClobClient.py` | Wraps `py-clob-client` + HTTP fallbacks | Modifying CLOB API usage/resiliency |
| `utils/idempotency.py` | SQLite dedupe of processed trades | Changing trade key semantics or state location |
| `services/healthServer.py` | `/health` + `/metrics` aiohttp server | Adding metrics, changing endpoints |

---

## 🔧 Technology Stack

### Core Technologies
- **Language:** Python `>=3.11,<3.13` (see `pyproject.toml` and `.python-version`).
- **Async runtime / HTTP:** `asyncio` + `aiohttp`.
- **Polymarket trading:** `py-clob-client==0.34.6`.
- **MongoDB (optional):** `pymongo==4.16.0`.
- **Web3 / signing:** `web3==7.14.1`, `eth-account==0.13.7`.

### Key Libraries
- **`python-dotenv==1.2.2`** — loads `.env` on startup via `config/env.py`.
- **`rich==14.3.3`** — Rich console output (see `utils/logger.py`).

### Development Tools
- **`pytest==9.0.2`**, **`pytest-asyncio==1.3.0`** (optional dev dependencies in `pyproject.toml`).

---

## 🌐 External Dependencies

### Required Services
- **Polymarket data API** (`data-api.polymarket.com`) for leader activity and positions (see `services/tradeMonitor.py`, `services/tradeExecutor.py`).
- **Polymarket CLOB HTTP API** (defaults to `https://clob.polymarket.com/`, see `utils/createClobClient.py`).
- **Polygon RPC** for balance checks and redemption flows (`ENV['RPC_URL']`, see `utils/getMyBalance.py`, `services/redemptionService.py`).

### Optional Services
- **MongoDB** enabled via `MONGO_URI` (`config/db.py`).
- **polymarketanalytics.com** used by leader discovery when enabled (`services/traderDiscoveryService.py`).

---

### Environment Variables [Optional]

The bot uses a large set of env vars; `config/env.py` enforces strict parsing (notably: no inline comments) and validates sensitive values.

```bash
# Required for trading
LEADER_WALLETS=0x...[,0x...]   # comma-separated; falls back to USER_ADDRESS
USER_ADDRESS=0x...             # fallback leader address
FUNDER_ADDRESS=0x...           # wallet used for trading; PROXY_WALLET is legacy fallback
PROXY_WALLET=0x...             # fallback for FUNDER_ADDRESS
PRIVATE_KEY=0x...              # 66-char hex key (0x + 64 hex digits)
# or
PRIVATE_KEY_FILE=~/key.txt     # enforced chmod 600 (no group/world read)
RPC_URL=https://...            # required; Polygon RPC

# Optional infrastructure/features
MONGO_URI=mongodb://.../db
MULTI_USER_CONFIG_FILE=/path/to/config.json
CLOB_HTTP_URL=https://clob.polymarket.com
CLOB_API_KEY=...
ENABLE_METRICS_SERVER=false
METRICS_PORT=9102

# Execution & risk knobs (examples; see config/env.py for full list)
SIZING_MODE=proportional|fixed|adaptive
PERCENTAGE=5
FIXED_AMOUNT=10
BUY_MIN_AMOUNT=1
BUY_MAX_AMOUNT=100
SLIPPAGE_TOLERANCE=2.0
BOT_STATE_DB=data/bot_state.db
```

---

## 🚨 Things to Be Careful About

### 🔒 Security Considerations

- **Secret handling:** Prefer `PRIVATE_KEY_FILE`; `config/env.py` rejects insecure permissions (`chmod 600`).
- **No inline env comments:** `config/env.py` treats values like `MONGO_URI=mongodb://... # comment` as invalid and will fail fast.
- **Replay protection:** idempotency relies on the SQLite key format in `utils/idempotency.py::build_trade_key()`. Changing it impacts dedupe/replay behavior.

---

*Update to last commit: 32a94d2efbf66bdf8d9c8f046fbace7d47124df9*
