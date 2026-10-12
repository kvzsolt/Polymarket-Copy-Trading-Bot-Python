# Redeem resolved winning and losing positions

The standalone command scans the selected follower's Deposit Wallet, including
losing shares and dust. It groups both outcomes by condition, checks market
identity against Gamma, and uses Polygon contract balances and payout data to
establish whether redemption is possible. Unresolved and empty holdings are
skipped. Losing outcomes pay zero; redeeming them burns the worthless tokens.

Gamma's [market lookup](https://docs.polymarket.com/api-reference/markets/list-markets)
defaults to open markets. Redemption explicitly looks up each condition with
`closed=true`, then checks `closed=false` only if the first response is empty.
Closed status does not prove resolution: Polygon payout data remains authoritative.

```bash
pmcopy redeem                     # preview; no signature, database, or transaction
pmcopy redeem --json              # preview as JSON; exact amounts are strings
pmcopy redeem --execute           # submit gasless redemptions
pmcopy redeem --user-id FOLLOWER --execute
```

`--dry-run` explicitly selects the default preview behavior and cannot be combined
with `--execute`. When multiple followers are configured, `--user-id` must select
exactly one. The bot and automatic trader discovery do not start for this command.

## Configuration

Fill the following placeholders in local, ignored, mode-0600 `secrets.env`:

```dotenv
PMCOPY__REDEMPTION__RELAYER_API_KEY=
PMCOPY__REDEMPTION__RELAYER_API_KEY_ADDRESS=
```

Use a **Polymarket Relayer API key**. The address is the EOA that owns that key and
signs for the Deposit Wallet, not the Deposit Wallet address. CLOB credentials and
Builder credentials cannot substitute for this pair. The existing wallet private
key stays in its existing secret configuration. Redemption does not derive or
create a Relayer API key. Environment variables can override these values using
the same names; storing the key in tracked YAML is rejected.

The Relayer nonce request uses the signing owner's address and `type=WALLET`.
Its returned `address` is validated as an EVM address but is not an ownership
assertion. It need not equal the signer or Deposit Wallet. Credential-owner and
derived Deposit Wallet checks still run independently before signing.

Execution requires all of the following:

- `--execute` and `execution.trading_mode: live` in the existing configuration.
- An explicit `wallet.funder_address` (or selected follower's `funder_address`).
  It must be a deployed Polygon Deposit Wallet derived from the signing owner;
  both supported UUPS and beacon wallet generations are accepted.
- A Polygon RPC endpoint supporting finalized block reads and receipts.
- The configured PostgreSQL database and permission to apply the additive
  `20260916_09_redemption_attempts` migration.

Relayer credentials are optional for normal bot startup and previews. Setting
them never enables automatic redemption. Legacy Safe/proxy wallets, EOA funders,
session-key signers, and combo positions are outside this command's scope.

## Execution and recovery

Each resolved condition is submitted as one atomic wallet batch. Where needed,
the batch grants the official collateral adapter CTF approval, redeems both
outcomes, and revokes that temporary approval. Existing approval is left intact.
All proceeds stay in the same Deposit Wallet as pUSD. No exchange orders, direct
RPC transaction broadcasts, or local position deletions are performed. Normal bot
reconciliation subsequently observes the changed venue balances.

The command installs only its independent journal migration when executing. It
holds a PostgreSQL session advisory lock for the selected wallet, persists the
nonsecret authorization identity before submission, and makes exactly one POST
per attempt. It never stores wallet signatures or Relayer credentials in the
journal. A second command for that wallet fails without submitting.

A transaction is reported as confirmed only after Relayer confirmation and a
canonical, finalized Polygon receipt prove the wallet's token outflows, matching
burns, and expected pUSD credit. Receipt credit is distinct from projected payout.
The receipt reader follows the extra legacy-adapter hop for negative-risk tokens.

Confirmation polling is bounded to 120 seconds per attempt, with individually
bounded HTTP requests. If confirmation is pending, unavailable, or a submission
has an ambiguous response, the command stops further submissions and exits 1.
Rerun the same command to resume reconciliation. Known transaction references are
polled without another POST. For a missing reference, a new attempt is allowed
only after finalized chain time exceeds the old authorization deadline (normally
ten minutes), balances are reread, and a fresh nonce is fetched. Empty balances
then clear ownership without attributing an unverified payout. Do not delete the
journal to force a retry.

Exit codes: 0 for a complete successful preview/run, 1 for failed or pending work,
2 for invalid CLI arguments, and 130 for Ctrl-C. Invalid per-market metadata is
reported while other valid conditions proceed; account/authentication, incomplete
inventory, RPC, or storage failures stop the run. Public API access restrictions
and Relayer eligibility can only be verified against the operator's actual account.

Definite per-condition failures, including explicit submission rejection and a
confirmed reverted transaction, do not prevent later conditions from proceeding.
Confirmed successes remain visible when another condition fails. A rerun reads
fresh chain balances and skips already empty holdings even if the Data API still
lists them.

Both human and JSON reports include a summary of discovered, inspected, planned,
confirmed, failed, skipped, and pending conditions, plus uninspected counts and
condition IDs. `scan_complete` means every known condition was visited; it does
not mean every redemption succeeded. Inspected includes failed inspection attempts.
Recovery results count toward the known conditions; multiple results for the same
condition count once using its latest outcome. Totals remain unknown (`null` in
JSON) if discovery never completed. Eligible zero-payout holdings are explicitly
labelled as cleanup, distinct from claims expected to pay collateral.

Settlement RPC reads use a dedicated single-attempt HTTP transport. Each logical
RPC call gets at most two passes over the primary and configured fallback endpoint,
with a 45-second total budget and a 10-second cap per HTTP request. Connection
failures, timeouts, HTTP 408/429/5xx, and JSON-RPC internal/resource-unavailable/
limit errors allow recovery with backoff and jitter. `Retry-After` defers reuse of
that endpoint within the budget. All retries preserve the original request and
pinned block tag. Invalid identity, malformed responses, invalid requests, and
other RPC errors stop the run. Errors identify the method and endpoint role without
exposing the endpoint URL or provider response. These retries apply only to reads;
Relayer transaction submission remains a single attempt.

Metadata errors block new signing and submission for the affected condition:

| Reason | Meaning |
| --- | --- |
| `redemption_market_metadata_not_found` | Neither the closed nor open lookup returned the condition. |
| `redemption_market_metadata_invalid` | Gamma returned malformed, incomplete, or multiple market records. |
| `redemption_market_metadata_conflicting` | Gamma's condition, outcome order, or negative-risk flag conflicts with the inventory. |
| `redemption_market_metadata_unavailable` | The public metadata request failed. |

Invalid, conflicting, and unavailable responses never trigger the open-market
fallback or bypass identity checks. Changing Relayer credentials cannot fix these
public metadata failures. Review the error and rerun `pmcopy redeem` to
preview. The reconciliation reminder is shown only when the report contains an
actual pending attempt, including one whose transaction reference is unknown.

## Compatibility and validation

Settlement is isolated from the CCXT order adapter and uses the already installed
Web3/eth-account dependencies. Contract addresses follow the official
[contract directory](https://docs.polymarket.com/resources/contracts); signing and
Relayer schemas were checked against
[Polymarket py-sdk revision 579bb2e](https://github.com/Polymarket/py-sdk/tree/579bb2e56be9cc5d152546985870ee6ad795ec52).
See also [managing positions](https://docs.polymarket.com/trading/positions/manage).
No official trading SDK is added or imported.

```bash
.venv/bin/python -m pytest -q tests/test_redemption_*.py tests/test_cli_redeem.py
.venv/bin/python -m pytest -q -o addopts='' tests/test_redemption_postgres.py
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest -q -o addopts=''
```

The PostgreSQL tests use disposable Docker containers. The command tests inject
network, clocks, and persistence; they do not submit live transactions.
