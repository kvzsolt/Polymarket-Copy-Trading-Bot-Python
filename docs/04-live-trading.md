# 04 - Live Trading Cutover

When you are ready to execute real trades on the Polymarket CLOB V2, follow this checklist to ensure a safe cutover.

## 1. Environment & Configuration Check

Before launching the bot in live mode, ensure your configuration files are set correctly.

**`config/runtime.local.env`**
Make sure the following flags and values are explicitly set:
- `LIVE_TRADING=enabled`
- `PAPER_TRADE_ONLY=false`
- `CLOB_V2_READY=true`
- `FUNDER_ADDRESS`: Your Polymarket funding wallet address.
- `LEADER_WALLETS`: The comma-separated list of wallets you want to copy (if not using auto-discovery).
- `DATABASE_URL`: Ensure your PostgreSQL connection string is correct (e.g., `postgresql://bot:bot@localhost:5432/polymarket_copytrading_python`).

**`secrets.env` (Secrets)**
Your `secrets.env` file must contain the sensitive keys required to sign transactions and interact with the blockchain. Ensure it includes:
- `PRIVATE_KEY=0xYourHexPrivateKey` (The private key corresponding to `FUNDER_ADDRESS`, starting with `0x`)
- `RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YourKey` (Your reliable Polygon Mainnet RPC)

## 2. Wallet Preparation

- **Polygon MATIC**: Ensure the `FUNDER_ADDRESS` has enough MATIC to cover gas for approvals (though Polymarket CLOB is largely gasless for limit orders, proxy approvals may require gas).
- **pUSD / USDC.e**: The bot requires collateral. Ensure you have unlocked and deposited pUSD on the Polymarket V2 contracts.
- **Approvals**: The bot's `CollateralReadiness` service should handle checking allowances, but verify your CTF Exchange V2 approvals are set.

## 3. Installation & Setup

If you haven't already, install the dependencies in a virtual environment. You can use either `uv` (recommended for speed) or standard `pip`.

**Using `uv` (Recommended):**
```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

**Using standard `pip`:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 4. Starting the Engine

You can run the bot natively via the CLI or using Docker.

### Option A: Native Execution (CLI)

Ensure your virtual environment is active, then start the bot using the `run` command with the `--live` flag to enforce live trading execution:

```bash
# Activate virtual environment if not already active
source .venv/bin/activate

# Launch the bot in live mode
python bot.py run --live
```

### Option B: Docker Execution

If you prefer containerized deployment, use Docker Compose to start the bot and its PostgreSQL database in detached mode:

```bash
docker compose up -d
```

*(Note: The default `docker-compose.yml` is configured with `PAPER_TRADE_ONLY: "true"`. You must change this to `"false"` in the `docker-compose.yml` file, or the bot will run in paper mode. The container entrypoint also defaults to `python bot.py run`; it will automatically trade live if `PAPER_TRADE_ONLY` is false and `LIVE_TRADING` is enabled in your env.)*

## 5. Immediate Monitoring

Watch the initialization logs:
1. **Reconciliation**: The `ReconciliationService` will run first. It pulls your on-chain positions and aligns them with the Postgres DB. Ensure it reports `0 drift`.
2. **WebSockets**: Ensure `market_ws` and `user_ws` connect cleanly and report the 5-second `PING` heartbeats.
3. **Execution Ready**: Look for the log indicating `ClobGateway` is armed and listening.

## 6. Ongoing Operations

Use the Operator CLI in a separate terminal to check the health and state of your bot while it runs:
```bash
source .venv/bin/activate
python bot.py doctor
python bot.py reservations
python bot.py drift
python bot.py status
```

If anything looks anomalous, `Ctrl+C` (or `docker compose stop`) immediately. The Postgres backend ensures you can restart safely without state corruption.
