# Live Trading Runbook

When to use: about to start the bot against real money on Polymarket. Read
`emergency_shutdown.md` and `recovery_playbook.md` first.

## Prerequisites

- Python 3.11 or 3.12 in a virtualenv.
- All dependencies installed (`pip install -e .[dev]`).
- A funded Polygon wallet and matching Polymarket account.
- Latest `pytest` green on this branch.
- `docs/live_canary_checklist.md` worked through end to end.

## Required environment variables

| ENV | Purpose |
| --- | --- |
| `PRIVATE_KEY` (or `PRIVATE_KEY_FILE`) | Signer key for the funder wallet |
| `FUNDER_ADDRESS` | 42-char hex EOA holding USDC + positions |
| `USER_ADDRESS` | Bot wallet (often same as funder for EOA mode) |
| `LEADER_WALLETS` | Comma-separated leader addresses or set `LEADER_AUTO=true` |
| `RPC_URL` | Polygon RPC endpoint |
| `CHAIN_ID` | `137` for mainnet |
| `PAPER_TRADE_ONLY` | Must be `false` for live |
| `CLOB_V2_READY` | Must be `true` to unlock the V2 SDK gate |
| `KILL_SWITCH` | Must be `false` to trade |
| `LIVE_CANARY_MODE` | `true` for the first run |
| `LIVE_CANARY_MAX_NOTIONAL_USDC` | Cap per submit (e.g. `1.00`) |
| `LIVE_CANARY_MAX_ORDERS` | Cap on total submitted orders (e.g. `1`) |
| `POSITION_DRIFT_CHECK_ON_STARTUP` | `true` |
| `ENABLE_METRICS_SERVER` | Recommended `true` for live observability |

## Step 1 — Paper mode dry run

Always confirm paper mode works against the current branch before live.

```bash
PAPER_TRADE_ONLY=true python -m bot_cli.main run
```

Watch for:
- `MarketDataRuntime` connect + `WSS` subscription.
- `startup_reconcile_unconfirmed` summary.
- `startup_drift_check` clean (drift_ok=True).

Stop with `Ctrl+C`.

## Step 2 — Live doctor

Run the full doctor before every live start.

```bash
python -m bot_cli.main doctor --live
```

Exit code `1` means blocked. The console summary prints `❌` on every blocker
with a `fix:` line. JSON dump follows for parsing.

Expected `status: ok` or `status: warning`. Treat any `blocked` line as a hard
stop.

## Step 3 — Live canary start

With canary env set, start the bot:

```bash
python -m bot_cli.main run
```

The coordinator will accept at most `LIVE_CANARY_MAX_ORDERS` submissions and
caps each at `LIVE_CANARY_MAX_NOTIONAL_USDC`. After the cap fires you'll see:

```
LIVE CANARY LIMIT REACHED submitted=1/1 ...
```

Skip code on subsequent attempts: `live_canary_limit`.

## Step 4 — Normal live start (after canary)

When the canary order completes cleanly:

1. Stop the bot (`Ctrl+C`).
2. Verify the trade in your wallet history + `data/bot_state.db` `fills` table.
3. Set `LIVE_CANARY_MODE=false`.
4. Restart with `python -m bot_cli.main run`.

## Reading order_lifecycle logs

Order lifecycle panels render with colour-coded titles:

- `📤 ORDER SUBMITTED — SIDE` — first POST acceptance.
- `⏳ ORDER UNCONFIRMED / DELAYED` — ambiguous POST or venue-side delay.
- `📘 ORDER LIVE` — venue acknowledged resting order.
- `🔄 ORDER MATCHED` — venue matched against book.
- `⛏️ TRADE MINED` — on-chain transaction mined.
- `✅ TRADE CONFIRMED` — terminal happy path.
- `❌ ORDER FAILED / CANCELLED` — terminal failure.
- `⚠️ ORDER AMBIGUOUS — TIMEOUT` — exceeded `ORDER_CONFIRMATION_MAX_WAIT_SECONDS`.
- `♻️ ORDER AUTO-RELEASED` — strict-grace auto-release fired.

Same data is in `log/PolymarketCopyTradingBot.jsonl` with `type="order_lifecycle"`.

## Reading position_drift logs

Drift panels:

- `✅ POSITION DRIFT CLEAN` — local == venue, nothing to do.
- `⚠️ POSITION DRIFT DETECTED` — sizes diverge or external position observed.
- `🛑 TRADING PAUSED FOR POSITION` — scope paused; submits return
  `skip_code="position_drift_unresolved"`.
- `🧹 LOCAL POSITION PRUNED` — auto-prune when venue == 0 and no reservations.

Same data in JSONL with `type="position_drift"`.

## Verifying readiness

Two ways:

```bash
curl http://127.0.0.1:9102/health/ready          # strict gate
curl http://127.0.0.1:9102/health/live           # liveness
```

Strict readiness requires `db_ok`, `api_ok`, `gateway_ok`, `wss_market_ok`,
`wss_user_ok`, `reconciler_ok`, `market_data_ready`, `drift_ok` AND
`circuit_breaker_state == "closed"`.

Any failed key short-circuits live execution: coordinator returns
`execution_coordinator_missing` or `position_drift_unresolved`.

## Stopping safely

```bash
# preferred — SIGTERM triggers graceful shutdown
kill -TERM <pid>
```

The runtime cancels monitor tasks, stops the market + user WSS runtimes, and
flushes JSONL. `Ctrl+C` (SIGINT) does the same.

Hard kill (`SIGKILL`) leaves open orders and live reservations untouched —
follow `emergency_shutdown.md` if that happens.
