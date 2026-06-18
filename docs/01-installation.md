# Installation Guide

> Complete setup instructions for the Polymarket Copy Trading Bot.

---

## System Requirements

| Requirement | Minimum | Recommended |
|---|---|---|
| Python | 3.11.x | 3.12.x |
| OS | Linux (Ubuntu 22.04+) | Linux |
| RAM | 512 MB | 1 GB+ |
| Disk | 100 MB (code + deps) | 1 GB+ (with logs and database) |
| Network | Stable internet | Low-latency connection to Polygon RPC |
| Database | SQLite 3 (bundled) | — |
| Docker | Optional (see below) | Docker 24+ with Compose |

**Python version note:** The bot explicitly requires Python 3.11 or 3.12. Python 3.10 and below are not supported, and Python 3.13+ is not yet tested. The startup validation (`validate_runtime_env`) will refuse to boot on unsupported runtimes.

---

## Step 1: Clone the Repository

```bash
git clone https://github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python.git
cd Polymarket-Copy-Trading-Bot-Python
```

---

## Step 2: Create a Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

> Always use a virtual environment. Installing dependencies system-wide can cause conflicts and makes it harder to debug issues.

---

## Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

For V2 CLOB client support (recommended for current deployments):

```bash
pip install -e ".[v2]"
# or: pip install py-clob-client-v2
```

Verify the V2 SDK is importable:

```bash
python -c "from py_clob_client_v2.client import ClobClient; print('V2 OK')"
```

---

## Step 4: Configure Environment Files

The bot uses a layered environment configuration system. You need to create your local configuration files from the provided templates.

### 4a. Runtime Configuration

```bash
cp config/runtime.local.example config/runtime.local.env
```

Edit `config/runtime.local.env` to set your non-sensitive runtime parameters. This file is git-ignored by default. Key settings to review:

- `LEADER_WALLETS` — Comma-separated list of Ethereum addresses to follow
- `SIZING_MODE` — `proportional`, `fixed`, or `adaptive`
- `PAPER_TRADE_ONLY` — Set to `true` for initial testing
- `ENABLE_RISK_MGMT` — Set to `true` to enable risk controls
- `ENFORCE_GEOBLOCK` — Set to `true` (default) for compliance

### 4b. Secrets Configuration

```bash
cp config/secrets.env.example secrets.env
chmod 600 secrets.env
```

Edit `secrets.env` and add your sensitive credentials:

```env
PRIVATE_KEY=0xYourHexPrivateKey
RPC_URL=https://polygon-mainnet.infura.io/v3/YourInfuraKey
FUNDER_ADDRESS=0xYourFunderAddress
```

> ⚠️ **Never commit `secrets.env` to version control.** The `.gitignore` includes it, but double-check before pushing. See [10-security.md](10-security.md) for full secret management practices.

### 4c. Alternative: Private Key File

For enhanced security, you can store your private key in a standalone file instead of `secrets.env`:

```bash
echo -n "0xYourHexPrivateKey" > ~/.polymarket-bot-key
chmod 600 ~/.polymarket-bot-key
```

Then set in your runtime config:

```env
PRIVATE_KEY_FILE=~/.polymarket-bot-key
```

The bot validates that the file has exactly `0600` permissions and will refuse to start if the permissions are too open.

---

## Step 5: Initialize the Database

The bot creates its SQLite database automatically on first run. The default path is `data/bot_state.db`, configured via `BOT_STATE_DB`.

Ensure the directory exists:

```bash
mkdir -p data
```

The database schema is created via migrations in `storage/sqlite/migrations.py`. No manual SQL execution is needed — the bot handles this on startup.

---

## Step 6: Verify with Bot Doctor

The `bot doctor` command runs a comprehensive diagnostic suite to verify your configuration before you start trading.

### Paper Mode Verification

```bash
python bot_cli/main.py doctor --paper
```

This checks:
- **Configuration validation** — All env vars parse correctly and pass semantic validation
- **Geoblock compliance** — Your region is not blocked by Polymarket (if `ENFORCE_GEOBLOCK=true`)
- **Database writability** — SQLite database is accessible and writable
- **RPC connectivity** — Your Polygon RPC endpoint is reachable
- **Kill switch** — Not accidentally enabled
- **Leaders configured** — At least one leader wallet or auto-discovery is set up
- **Order size sanity** — `BUY_MIN_AMOUNT` is reasonable

Expected output:

```json
{
  "mode": "paper",
  "status": "ok",
  "checks": {
    "config": {"status": "ok"},
    "geoblock": {"status": "ok"},
    "db": {"status": "ok"},
    "rpc": {"status": "ok"},
    "kill_switch": {"status": "ok"},
    "leaders": {"status": "ok"},
    "order_size": {"status": "ok"}
  }
}
```

If `status` is `"blocked"`, address the failing checks before proceeding.

### Live Mode Verification

```bash
python bot_cli/main.py doctor --live
```

This adds additional checks:
- **Secrets presence** — `PRIVATE_KEY` is configured and not a placeholder

See [04-live-trading.md](04-live-trading.md) for the full live trading checklist.

---

## Step 7: Start the Bot

### Paper Mode (recommended first run)

```bash
python app.py
```

With `PAPER_TRADE_ONLY=true` in your configuration, the bot will monitor leaders and score signals but will **not** submit real orders. All fills are simulated.

### Live Mode

```bash
python app.py --live
```

> Only use live mode after passing `bot doctor --live` and completing the checklist in [04-live-trading.md](04-live-trading.md).

---

## Docker Installation

The project includes a production Dockerfile for containerized deployment.

### Build the Image

```bash
docker build -t polymarket-copy-trader .
```

### Run in Paper Mode

```bash
docker run --env-file config/runtime.local.env \
           --env-file secrets.env \
           -v ./data:/app/data \
           -v ./log:/app/log \
           polymarket-copy-trader
```

### Run in Live Mode

```bash
docker run --env-file config/runtime.local.env \
           --env-file secrets.env \
           -v ./data:/app/data \
           -v ./log:/app/log \
           polymarket-copy-trader --live
```

### Docker Compose (Optional)

Create a `docker-compose.yml`:

```yaml
version: "3.8"
services:
  bot:
    build: .
    env_file:
      - config/runtime.local.env
      - secrets.env
    volumes:
      - ./data:/app/data
      - ./log:/app/log
    ports:
      - "9102:9102"  # Metrics server
    restart: unless-stopped
    command: ["--paper"]  # Change to ["--live"] when ready
```

### Docker Security Notes

- The container runs as root by default. For production, add a non-root user.
- Always use `--env-file` for secrets; never pass `PRIVATE_KEY` as a plain `-e` flag (it appears in `docker inspect`).
- Volume-mount `data/` and `log/` to persist state across container restarts.
- The Dockerfile includes a `HEALTHCHECK` that queries the `/health/ready` endpoint on port 9090.

See [10-security.md](10-security.md) for more Docker security practices.

---

## V2 CLOB Migration

If you are running the V2 CLOB migration branch, additional steps are required:

1. Install V2 dependencies:
   ```bash
   .venv/bin/pip install -e ".[v2]"
   ```

2. Run the V2 smoke test:
   ```bash
   .venv/bin/python scripts/smoke_v2.py
   ```

3. Run the V2 readiness test suite:
   ```bash
   .venv/bin/pytest \
     tests/test_clob_v1_compat_seam.py \
     tests/test_clob_v1_method_adapters.py \
     tests/test_clob_v2_market_meta.py \
     tests/test_clob_v2_runtime_smoke.py \
     tests/test_v2_readiness.py \
     tests/test_v2_metadata_and_order_options.py \
     -v --tb=short
   ```

4. Set the readiness flag:
   ```env
   CLOB_V2_READY=true
   ```

See [v2_operator_runbook.md](v2_operator_runbook.md) for full details.

---

## Next Steps

- Read [02-configuration.md](02-configuration.md) for the full configuration reference.
- Start with paper trading as described in [03-paper-trading.md](03-paper-trading.md).
