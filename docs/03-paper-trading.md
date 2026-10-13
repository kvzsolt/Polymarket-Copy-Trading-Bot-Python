# 03 - Paper Trading & Dry Runs

Before risking real capital, you must validate your configuration and ensure the Discovery and Copy Decision engines are behaving as expected.

## Phase 1: Pure Discovery (No Execution)
You can run the Discovery engine completely isolated from the execution layer. This allows you to build a database of profitable leaders.
1. Leave `PMCOPY__WALLET__PRIVATE_KEY` empty in `secrets.env` (paper never demands signing secrets; `PMCOPY__WALLET__FUNDER_ADDRESS` remains required as the account identity).
2. Ensure `PMCOPY__DISCOVERY__API__TOKEN` is set.
3. Run the bot. The system will ingest, score, and rank candidates into the PostgreSQL database.
4. Review the DB logs or use the CLI (`pmcopy rotate-traders`) to see who the bot *would* have picked.

## Phase 2: Copyability Replay
The copyability replay takes a discovered leader's historical signals and simulates them against your configured budget and risk limits.
- Did the leader buy 100k USDC of a token, but you only have 1k? The replay will tell you if the resulting slippage/size ratio is actually copyable.

## Phase 3: Dry-Run / Paper Mode
Paper mode is structural, not a configuration flag on the executor: with
`execution.trading_mode=paper`, the runtime builds the `PaperExecutionGateway`
— CCXT still supplies live public market reads, but every order write goes to
a local simulator. A paper process never constructs an authenticated CCXT
client and can never invoke a private write method, even when live secrets
are present in the configuration.
Monitor the logs:
- Look for `RiskDecision` outputs. Ensure trades are being rejected/accepted appropriately.
- Check the position coordinator logs to see if intents are being formed correctly based on leader actions.

Once you are satisfied that the bot isn't sizing aggressively or tracking low-tier wallets, move to Live Trading.
