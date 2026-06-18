# Redemption

> How winning positions are redeemed, wallet validation, and troubleshooting redemption failures.

---

## Overview

When a Polymarket prediction market resolves, winning positions can be redeemed for collateral value. On CLOB v2 that collateral is pUSD. The bot's auto-redeem system automates this process by scanning for redeemable positions and submitting on-chain redemption transactions.

Redemption is a separate lifecycle from trading — it involves on-chain transactions rather than CLOB orders, and it has its own safety checks and validation logic.

---

## How Auto-Redeem Works

### Prerequisites

- `ENABLE_AUTO_REDEEM=true` must be configured
- The bot must be in live mode (`PAPER_TRADE_ONLY=false`)
- The wallet must pass redemption wallet validation (see below)
- The `RPC_URL` must be configured and reachable

### Scan Cycle

The redemption service runs on a configurable interval (`REDEEM_CHECK_INTERVAL`, default: 60 seconds):

1. **Query positions** — Fetch all positions from the Polymarket Data API
2. **Filter redeemable** — Identify positions where:
   - `curPrice >= 0.99` (YES token at near-$1.00) or `curPrice <= 0.01` (NO token at near-$0.00)
   - The `redeemable` flag is `true` in the API response
   - Position size is above the zero threshold (> 0.0001)
3. **Submit redemption** — For each redeemable position, submit an on-chain `redeemPositions` transaction
4. **Confirm** — Wait for transaction receipt and verify success

### Redemption Transaction

The redemption transaction calls `redeemPositions` on the Polymarket v2 CTF collateral adapter. The collateral token argument is pUSD:

```python
collateral_adapter.functions.redeemPositions(
    pusd_contract_address,       # pUSD collateral token
    parent_collection_id,        # Zero bytes (0x00...00)
    condition_id,                # The resolved market's condition ID
    [1, 2],                      # Index sets for both outcome collections
)
```

Current Polygon v2 addresses:

```env
CTF_COLLATERAL_ADAPTER_ADDRESS=0xAdA100Db00Ca00073811820692005400218FcE1f
PUSD_CONTRACT_ADDRESS=0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB
```

The transaction:
- Is signed with the funder's private key
- Uses a 20% gas price buffer to ensure timely inclusion
- Has a gas limit of 500,000
- Is sent from the funder address

---

## Wallet Validation

Before any redemption transaction is submitted, the bot validates the wallet configuration for safety. This prevents cross-wallet redemption errors that could result in lost funds.

### Validation Rules

The `WalletValidator` in `services/redemption/wallet_validator.py` enforces these rules:

| Wallet Type | Rule | Rationale |
|---|---|---|
| EOA (default) | Signer address **must equal** funder address | EOA redemption requires the signer and funder to be the same address |
| POLY_PROXY | Currently **NOT supported** | Proxy redemption requires additional signature flows not yet implemented |
| GNOSIS_SAFE | Currently **NOT supported** | Gnosis Safe redemption requires multi-sig coordination |

### How Signer Address Is Derived

The signer address is derived from the `PRIVATE_KEY` using `eth_account`:

```python
from eth_account import Account
acct = Account.from_key(private_key_hex)
signer_address = acct.address  # Checksummed
```

### Validation Result

The `WalletValidationResult` includes:
- `allowed` — Whether redemption is permitted
- `reason` — Human-readable explanation
- `signer_address` — The address derived from the private key
- `funder_address` — The configured funder address
- `signature_type` — The detected wallet type (EOA, POLY_PROXY, GNOSIS_SAFE)

### Common Rejection Reasons

| Reason | Meaning |
|---|---|
| `signer_address_derivation_failed` | Private key is invalid or could not derive an address |
| `funder_address_missing` | No funder address is configured |
| `eoa_signer_funder_mismatch` | Signer address differs from funder address (most common) |
| `proxy_redemption_not_yet_supported_poly_proxy` | Polymarket proxy wallets cannot auto-redeem yet |
| `proxy_redemption_not_yet_supported_gnosis_safe` | Gnosis Safe wallets cannot auto-redeem yet |

---

## EOA vs Proxy Wallets

### EOA Wallets

An Externally Owned Account (EOA) is a standard Ethereum wallet controlled by a single private key. For EOA wallets:
- The signer (derived from `PRIVATE_KEY`) and funder must be the same address
- This is the simplest and most common wallet type on Polymarket
- Auto-redeem is fully supported

### Polymarket Proxy Wallets

Polymarket creates proxy wallets for some users (Gnosis Safe-based). These wallets:
- Have a separate funder address from the signer
- Require additional signature flows for on-chain transactions
- Are **not currently supported** for auto-redeem

If you have a proxy wallet, you must redeem manually through the Polymarket UI.

### Detecting Your Wallet Type

The `bot doctor --live` command automatically detects your wallet type:

```python
sig_type, _, _ = await detect_wallet_signature_type(config.wallet)
```

The detection checks whether the funder address matches a Polymarket proxy pattern or a Gnosis Safe pattern.

---

## Redemption Statuses

Each redemption attempt produces a `RedemptionResult`:

| Status | Description |
|---|---|
| `confirmed` | Transaction was mined and succeeded (receipt status = 1) |
| `reverted` | Transaction was mined but reverted (receipt status = 0) |
| `failed` | Transaction failed to submit or an exception occurred |
| `not_submitted` | Prerequisites not met (e.g., missing RPC URL) |

---

## Redemption Reconciler

The `RedemptionService.reconcile_pending()` method is a placeholder for tracking in-flight redemption transactions. Currently, it returns an empty list. Future versions will:

- Track pending redemption transactions by hash
- Re-check receipt status on restart
- Retry failed redemptions
- Record all redemption attempts in a `redemptions` database table

---

## Manual Redemption Steps

If auto-redeem is disabled or not supported for your wallet type, you can redeem manually:

### Through the Polymarket UI

1. Navigate to [polymarket.com/portfolio](https://polymarket.com/portfolio)
2. Find the resolved market in your positions
3. Click "Redeem" or "Claim Winnings"
4. Confirm the transaction in your wallet

### Through the Collateral Adapter Directly

If you need to redeem programmatically:

```python
from web3 import Web3

CTF_COLLATERAL_ADAPTER_ADDRESS = "0xAdA100Db00Ca00073811820692005400218FcE1f"
PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

w3 = Web3(Web3.HTTPProvider("YOUR_RPC_URL"))
collateral_adapter = w3.eth.contract(
    address=Web3.to_checksum_address(CTF_COLLATERAL_ADAPTER_ADDRESS),
    abi=CTF_REDEEM_ABI,
)

tx = collateral_adapter.functions.redeemPositions(
    Web3.to_checksum_address(PUSD_ADDRESS),
    b"\x00" * 32,  # Parent collection ID
    bytes.fromhex(condition_id[2:]),  # Condition ID (without 0x prefix)
    [1, 2],  # Index sets
).build_transaction({
    "from": Web3.to_checksum_address(your_address),
    "nonce": w3.eth.get_transaction_count(your_address),
    "gas": 500_000,
    "gasPrice": w3.eth.gas_price,
})

signed = w3.eth.account.sign_transaction(tx, private_key)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
```

---

## Troubleshooting Redemption Failures

### "eoa_signer_funder_mismatch"

**Problem:** The address derived from `PRIVATE_KEY` does not match `FUNDER_ADDRESS`.

**Solution:**
- If using a regular wallet, ensure `FUNDER_ADDRESS` matches the address derived from your private key
- If using a Polymarket proxy wallet, auto-redeem is not yet supported — use manual redemption

### "rpc_url_missing"

**Problem:** `RPC_URL` is not configured.

**Solution:** Set `RPC_URL` in your configuration:
```env
RPC_URL=https://polygon-mainnet.infura.io/v3/YourKey
```

### "invalid_condition_id_length"

**Problem:** The condition ID is not 32 bytes.

**Solution:** This usually indicates corrupted data. Check the `condition_id` in your positions table.

### "redeem_tx_reverted"

**Problem:** The on-chain transaction was mined but reverted.

**Possible causes:**
- The market has not actually resolved yet
- The position has already been redeemed
- The condition ID does not match a valid resolved market
- Insufficient gas

**Solution:** Verify the market status on Polymarket before retrying.

### Transaction Not Confirming

**Problem:** The redemption transaction is submitted but not confirming.

**Possible causes:**
- Gas price too low
- Network congestion on Polygon

**Solution:** The bot uses a 20% gas price buffer by default. If transactions are consistently slow, consider using a faster RPC endpoint or increasing the gas price multiplier in the code.

---

## Next Steps

- See [04-live-trading.md](04-live-trading.md) for enabling auto-redeem in live mode.
- See [09-troubleshooting.md](09-troubleshooting.md) for general troubleshooting guidance.
