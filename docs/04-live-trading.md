# Live Trading Mode

> Prerequisites, configuration, monitoring, and emergency procedures for live trading.

---

## ⚠️ Risk Warning

**Live trading involves real financial risk.** You can lose your entire investment. The bot executes trades automatically based on leader activity and your configuration. There is no guarantee of profit. Only use funds you can afford to lose entirely.

Before enabling live mode, you **must** complete paper trading validation. See [03-paper-trading.md](03-paper-trading.md).

---

## Prerequisites

Before switching to live mode, every prerequisite below must be satisfied:

### 1. Bot Doctor — Live Must Pass

```bash
python bot_cli/main.py doctor --live
```

Expected output: `"status": "ok"` with no `"blocked"` checks. The live doctor performs these checks beyond what paper mode requires:

| Check | What It Verifies |
|---|---|
| Config validation | All env vars parse and pass semantic rules |
| Geoblock | Your region is not blocked by Polymarket |
| Database | SQLite is writable and operational |
| RPC | Polygon RPC endpoint is reachable |
| **Secrets presence** | `PRIVATE_KEY` is configured and valid (live-only) |
| Kill switch | `KILL_SWITCH=false` |
| Leaders | At least one leader is configured or discoverable |
| Order size | `BUY_MIN_AMOUNT` is reasonable |
| **Redemption wallet** | Signer/funder address compatibility (live-only when auto-redeem is on) |

### 2. Paper Mode Validation Complete

Paper trading should have run for **at least one full monitoring cycle** (ideally several hours to a full day) without errors. You should have reviewed the simulated PnL and confirmed that:

- Signal scoring produces expected results
- Risk controls reject inappropriate signals
- Exit strategies trigger at correct price levels
- Position sizing is within your comfort zone

### 3. Wallet Funded

Your funder wallet must have:
- Sufficient **USDC/pUSD** balance for the trades you intend to copy
- Sufficient **MATIC** for gas fees on Polygon
- Proper **allowances** set for the CTF Exchange and Neg Risk Exchange contracts

---

## Configuration Checklist

Complete every item before starting live mode:

```env
# === CRITICAL: Disable paper mode ===
PAPER_TRADE_ONLY=false

# === Wallet (REQUIRED for live) ===
PRIVATE_KEY=0xYourHexPrivateKey          # Or use PRIVATE_KEY_FILE
FUNDER_ADDRESS=0xYourFunderAddress
RPC_URL=https://polygon-mainnet.infura.io/v3/YourKey
CHAIN_ID=137

# === Leaders ===
LEADER_WALLETS=0xLeader1,0xLeader2       # Or enable AUTO_DISCOVER_LEADERS=true

# === Risk (STRONGLY RECOMMENDED) ===
ENABLE_RISK_MGMT=true
KILL_SWITCH=false
MAX_OPEN_POSITIONS=20                     # Adjust to your comfort
MAX_DAILY_DRAWDOWN_PCT=10                # Conservative for first live run
MAX_MARKET_EXPOSURE_USDC=200
MAX_LEADER_EXPOSURE_USDC=300

# === Account Cap Policy (RECOMMENDED) ===
ENABLE_BALANCE_TIERED_POLICY=true
BALANCE_POLICY_RESERVE_PCT=0.15          # Keep 15% in reserve
BALANCE_POLICY_RESERVE_FLOOR=10          # At least $10 reserve

# === Execution Quality ===
ENABLE_COPY_DECISION_GATE=true
SLIPPAGE_TOLERANCE=2.0
BOOK_SPREAD_MODE=absolute_bps
MAX_BOOK_SPREAD_BPS=500
CIRCUIT_FAILURE_THRESHOLD=5

# === Geoblock ===
ENFORCE_GEOBLOCK=true                    # NEVER disable this

# === Redemption (optional) ===
ENABLE_AUTO_REDEEM=false                 # Enable only after validation
```

---

## Geoblock Verification

The bot enforces Polymarket's geoblock policy by default. The compliance check:

1. Queries `https://polymarket.com/api/geoblock`
2. Checks the `blocked` field in the response
3. **Fails closed** — If the API is unreachable, returns malformed data, or is missing the `blocked` field, the bot treats you as blocked

This is a critical safety feature. **Do not disable `ENFORCE_GEOBLOCK`** unless you are certain of your legal right to trade on Polymarket from your jurisdiction.

The geoblock decision is cached for 15 minutes. After that, the bot re-checks automatically.

---

## Wallet Setup

### Private Key Configuration

**Option A: Environment Variable**

```env
PRIVATE_KEY=0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890
```

This must be a 66-character hex string (0x prefix + 64 hex digits).

**Option B: File-Based (Recommended)**

```bash
echo -n "0xabcdef..." > ~/.polymarket-bot-key
chmod 600 ~/.polymarket-bot-key
```

```env
PRIVATE_KEY_FILE=~/.polymarket-bot-key
```

The bot validates:
- The file exists and is a regular file
- File permissions are exactly `0600`
- The file contains a valid 66-char hex key

### Funder Address

The `FUNDER_ADDRESS` is the Polymarket proxy wallet address that holds your USDC and executes trades. For EOA (Externally Owned Account) wallets, the funder address is the same as the address derived from your private key.

If you use a Polymarket proxy wallet (Gnosis Safe or Polymarket's own proxy), the funder address will differ from your signer address. In this case:
- `FUNDER_ADDRESS` = the proxy wallet address
- `PRIVATE_KEY` = the key that controls the proxy

> **Important:** Auto-redeem currently only supports EOA wallets where signer equals funder. See [08-redemption.md](08-redemption.md) for details.

---

## Starting Live Mode

### From the Command Line

```bash
python app.py
```

With `PAPER_TRADE_ONLY=false` in your configuration, the bot starts in live mode.

### From Docker

```bash
docker run --env-file config/runtime.local.env \
           --env-file secrets.env \
           -v ./data:/app/data \
           -v ./log:/app/log \
           polymarket-copy-trader --live
```

### First Live Run Recommendations

1. **Start with conservative sizing** — Set `BUY_MIN_AMOUNT=5` and `BUY_MAX_AMOUNT=10` for your first session
2. **Limit open positions** — `MAX_OPEN_POSITIONS=5` or lower
3. **Tight drawdown limit** — `MAX_DAILY_DRAWDOWN_PCT=5` to catch problems early
4. **Watch the logs** — Monitor `log/PolymarketCopyTradingBot.jsonl` in real-time
5. **Stay near the kill switch** — Be ready to enable `KILL_SWITCH=true` if anything looks wrong

---

## Monitoring Live Trading

### Real-Time Log Monitoring

```bash
tail -f log/PolymarketCopyTradingBot.jsonl | python -m json.tool
```

Key events to watch:
- `copy_signal_detected` — Leader made a trade
- `copy_decision_approved` — Signal passed scoring gate
- `risk_headroom_ok` — Risk engine approved the trade
- `execution_filled` — Order was filled on the CLOB
- `execution_retryable_failed` — Order failed but will be retried
- `execution_terminal_failed` — Order failed permanently
- `risk_exit_triggered` — Exit strategy triggered a sell

### Metrics Server

If `ENABLE_METRICS_SERVER=true`:

```bash
curl http://127.0.0.1:9102/metrics
```

Key metrics:
- `monitored_leaders` — Number of leaders being tracked
- `open_positions` — Current open position count
- `daily_pnl_usdc` — Realized + unrealized PnL for the day

### Health Endpoint

The health server (port 9090 by default) exposes:

```
GET /health/ready    → 200 if bot is operational
GET /health/live     → 200 if process is alive
```

---

## Emergency Stop Procedures

### Option 1: Kill Switch (Recommended)

Set `KILL_SWITCH=true` in your environment and restart the bot:

```bash
# In config/runtime.local.env:
KILL_SWITCH=true

# Restart
python app.py
```

When the kill switch is active:
- **All new BUY signals are rejected** — No new positions are opened
- **Existing SELL signals still execute** — Positions can still be closed
- **The bot continues monitoring** — You can observe what would have happened

This is the safest emergency stop because it allows exits while blocking new entries.

### Option 2: Process Kill

```bash
# Find the bot process
ps aux | grep app.py

# Kill it
kill <PID>
```

> ⚠️ **Warning:** Killing the process mid-tick may leave orders in `submitted_unconfirmed` state. The reconciliation system will resolve these on next startup, but there may be a window where you have an open order on the CLOB that the bot is not tracking.

### Option 3: Docker Stop

```bash
docker stop <container_id>
```

Same caveats as process kill above.

### Option 4: Disable the Bot Entirely

```bash
# Prevent restart
docker update --restart=no <container_id>
docker stop <container_id>
```

### After Emergency Stop

1. Check the CLOB for any open orders: visit [Polymarket Portfolio](https://polymarket.com/portfolio)
2. Cancel any unwanted orders manually through the Polymarket UI
3. Review `log/PolymarketCopyTradingBot.jsonl` for the events leading up to the stop
4. Run `bot doctor --live` before restarting

---

## Risk of Loss Disclaimers

- **Market risk:** Prediction markets can resolve to zero. You can lose your entire position.
- **Leader risk:** Leaders can and do make losing trades. Following a leader does not guarantee profit.
- **Technical risk:** Software bugs, network outages, and exchange downtime can cause missed trades, incorrect trades, or inability to exit positions.
- **Liquidity risk:** Thin orderbooks may prevent execution at expected prices, or prevent execution entirely.
- **Smart contract risk:** The bot interacts with on-chain contracts. While Polymarket's contracts are audited, no smart contract is risk-free.

See [11-legal-disclaimer.md](11-legal-disclaimer.md) for the full legal disclaimer.

---

## Next Steps

- See [05-risk-controls.md](05-risk-controls.md) for detailed risk management configuration.
- See [09-troubleshooting.md](09-troubleshooting.md) if you encounter issues.
