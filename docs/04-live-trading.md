# 04 - Live Trading Cutover

When you are ready to execute real trades on the Polymarket CLOB V2, follow this checklist to ensure a safe cutover.

## 1. Wallet Preparation
- **Polygon MATIC**: Ensure the `FUNDER_ADDRESS` has enough MATIC to cover gas for approvals (though Polymarket CLOB is largely gasless for limit orders, proxy approvals may require gas).
- **pUSD / USDC.e**: The bot requires collateral. Ensure you have unlocked and deposited pUSD on the Polymarket V2 contracts.
- **Approvals**: The bot's `CollateralReadiness` service should handle checking allowances, but verify your CTF Exchange V2 approvals are set.

## 2. Configuration Finalization
- Set `PRIVATE_KEY` securely in `secrets.env`.
- Ensure `CLOB_V2_READY=true` is set.
- Double-check `MAX_POSITION_SIZE_USDC` and `DAILY_DRAWDOWN_LIMIT_PCT` are strictly enforced in `runtime.local.env`.

## 3. Starting the Engine
Run the bot natively or via Docker/Systemd:
```bash
python3 app.py
```

## 4. Immediate Monitoring
Watch the initialization logs:
1. **Reconciliation**: The `ReconciliationService` will run first. It pulls your on-chain positions and aligns them with the Postgres DB. Ensure it reports `0 drift`.
2. **WebSockets**: Ensure `market_ws` and `user_ws` connect cleanly and report the 5-second `PING` heartbeats.
3. **Execution Ready**: Look for the log indicating `ClobGateway` is armed and listening.

## 5. Ongoing Operations
Use the Operator CLI to check health:
```bash
python3 bot.py doctor
python3 bot.py reservations
python3 bot.py drift
```
If anything looks anomalous, `Ctrl+C` immediately. The Postgres backend ensures you can restart safely without state corruption.