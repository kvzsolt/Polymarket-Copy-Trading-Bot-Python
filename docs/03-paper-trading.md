# 03 - Paper Trading & Dry Runs

Before risking real capital, you must validate your configuration and ensure the Discovery and Copy Decision engines are behaving as expected.

## Phase 1: Pure Discovery (No Execution)
You can run the Discovery engine completely isolated from the execution layer. This allows you to build a database of profitable leaders.
1. Leave `PRIVATE_KEY` empty or invalid in `secrets.env`.
2. Ensure `FALCON_API_TOKEN` is set.
3. Run the bot. The system will ingest, score, and rank candidates into the PostgreSQL database.
4. Review the DB logs or use the CLI (`python3 bot.py rotate-traders`) to see who the bot *would* have picked.

## Phase 2: Copyability Replay
The `CopyabilityReplayService` is your best friend. It takes a discovered leader's historical signals and simulates them against your configured budget and risk limits.
- Did the leader buy 100k USDC of a token, but you only have 1k? The replay service will tell you if the resulting slippage/size ratio is actually copyable.

## Phase 3: Dry-Run / Paper Mode
*Note: Ensure your `ExecutionService` is configured to bypass actual CLOB order placement if you want a true dry run.*
Monitor the logs:
- Look for `RiskDecision` outputs. Ensure trades are being rejected/accepted appropriately.
- Check `PositionActionCoordinator` to see if intents are being formed correctly based on leader actions.

Once you are satisfied that the bot isn't sizing aggressively or tracking low-tier wallets, move to Live Trading.