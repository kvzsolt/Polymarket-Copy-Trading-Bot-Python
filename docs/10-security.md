# Security

> Security practices for operating the Polymarket Copy Trading Bot safely.

---

## Overview

The bot handles sensitive financial credentials — private keys, API keys, and wallet addresses. A security compromise can result in irreversible loss of funds on the blockchain. This document outlines the security practices you must follow to protect your capital and credentials.

---

## Secret Management

### Never Print Keys

The bot is designed to never expose secrets in logs, crash reports, or output:

- `PRIVATE_KEY` values in the `ENV` snapshot are replaced with `***REDACTED***`
- `PRIVATE_KEY_FILE` paths are also redacted in the snapshot
- The `bot doctor` output never includes the actual private key value
- Crash reports and exception handlers use the audit logger, which redacts known secret fields

**Your responsibility:** Do not add `print(PRIVATE_KEY)` or similar debugging statements to the code. Do not log wallet secrets in custom modifications.

### .env File Protection

```bash
# secrets.env must be readable only by the owner
chmod 600 secrets.env

# Verify permissions
ls -la secrets.env
# Expected: -rw------- 1 user user ... secrets.env
```

**Rules:**
1. Never commit `secrets.env` to version control
2. Never share `secrets.env` via messaging, email, or cloud storage
3. Never include secrets in Docker `--env` flags (visible in `docker inspect`)
4. Always use `--env-file` for Docker secret injection

### Private Key File

For enhanced security, store your private key in a separate file:

```bash
echo -n "0xYourHexPrivateKey" > ~/.polymarket-bot-key
chmod 600 ~/.polymarket-bot-key
```

The bot enforces:
- File must exist and be a regular file
- Permissions must be exactly `0600` — no group or world read access
- If permissions are wrong, the bot refuses to start with a clear error message

Configure the file path:
```env
PRIVATE_KEY_FILE=~/.polymarket-bot-key
```

---

## API Credential Security

### CLOB API Key

If you use a `CLOB_API_KEY`, store it in `secrets.env` (never in `runtime.env` or `runtime.local.env`):

```env
CLOB_API_KEY=your_api_key_here
```

The API key is used for authenticated CLOB endpoints. If compromised, an attacker could potentially:
- View your open orders
- Submit orders on your behalf (if the key has write permissions)

### RPC URL

Your RPC URL may contain an API key (e.g., Infura, Alchemy):

```env
RPC_URL=https://polygon-mainnet.infura.io/v3/YOUR_INFURA_KEY
```

Treat this as a secret. A compromised RPC key could be used to:
- Drain your API quota
- Submit malicious RPC requests through your account

---

## Wallet Security

### Private Key Best Practices

1. **Use a dedicated wallet** — Create a separate wallet specifically for the bot. Never use your main DeFi wallet or a wallet with significant holdings.

2. **Fund minimally** — Only deposit the USDC you intend to trade with. Keep the reserve balance low.

3. **Never reuse keys** — Do not use the same private key across multiple services or bots.

4. **Rotate regularly** — Consider generating a new wallet periodically, especially if you suspect any credential exposure.

5. **Use hardware wallets for main funds** — The bot cannot directly use hardware wallets (it needs the raw private key for signing), but your primary holdings should be on a hardware wallet.

### Funder Address Security

The `FUNDER_ADDRESS` is your Polymarket proxy wallet. While it's not a secret (it's visible on the blockchain), be aware that:
- Anyone who knows your funder address can view your positions on Polymarket
- The funder address is included in on-chain transactions and is publicly visible
- Do not confuse the funder address with the signer address (they may differ for proxy wallets)

---

## Geoblock Compliance

### Fail-Closed Model

The geoblock check operates on a **fail-closed** model:

```python
async def check_geoblock() -> ComplianceDecision:
    # If API is unreachable → blocked
    # If response is not a dict → blocked
    # If "blocked" field is missing → blocked
    # If "blocked" is True → blocked
    # Only if "blocked" is False → allowed
```

This means:
- Network errors are treated as "blocked"
- Malformed responses are treated as "blocked"
- Missing data is treated as "blocked"

**Never disable `ENFORCE_GEOBLOCK`** unless you have independently verified your legal right to trade on Polymarket from your jurisdiction. Disabling geoblock does not make trading legal from a restricted region.

### Geoblock Caching

The compliance decision is cached for 15 minutes:
- `expires_at = checked_at + timedelta(minutes=15)`
- During the cache window, no re-check is performed
- After expiry, the bot re-checks automatically

---

## Docker Security

### Container Isolation

```bash
# Run with limited capabilities
docker run --cap-drop=ALL \
           --security-opt=no-new-privileges \
           --env-file secrets.env \
           -v ./data:/app/data \
           polymarket-copy-trader
```

### Secret Injection

**Never** pass secrets via `-e` flags:
```bash
# BAD: visible in docker inspect
docker run -e PRIVATE_KEY=0x... polymarket-copy-trader

# GOOD: use env-file
docker run --env-file secrets.env polymarket-copy-trader
```

### Network Security

```bash
# Restrict metrics server to localhost only
docker run -p 127.0.0.1:9102:9102 polymarket-copy-trader
```

The default `METRICS_BIND_HOST=127.0.0.1` ensures the metrics server only listens on localhost. Never bind to `0.0.0.0` unless you have proper network isolation.

### Volume Mounts

Only mount the directories the bot needs:
- `./data:/app/data` — Database persistence
- `./log:/app/log` — Log persistence

Do not mount the entire project directory or any directory containing secrets.

---

## Git Security

### .gitignore Rules

The project's `.gitignore` includes:

```
secrets.env
*.key
*.pem
config/runtime.local.env
data/
log/
```

**Verify these are in place** before committing any changes. A single accidental commit of `secrets.env` can expose your private key permanently (even if deleted in a subsequent commit, it remains in git history).

### Pre-Commit Checklist

Before any `git push`:

1. `git diff --cached | rg -i "private_key|0x[0-9a-f]{64}"` — Check for accidental key inclusion
2. `git status` — Verify no secret files are staged
3. Review any new `.env` files to ensure they don't contain real credentials

### If Secrets Are Committed

If you accidentally commit a private key:

1. **Immediately transfer all funds** from the compromised wallet to a new wallet
2. **Generate a new private key** for the bot
3. **Rewrite git history** using `git filter-branch` or BFG Repo Cleaner
4. **Force-push** the cleaned history
5. **Notify anyone** who may have pulled the compromised repository

---

## Audit Logging

### What Gets Logged

The bot's audit logger (`services/observability/audit_logger.py`) records every significant event:

| Category | Events |
|---|---|
| Execution | `execution_filled`, `execution_retryable_failed`, `execution_terminal_failed`, `execution_submitted_unconfirmed` |
| Risk | Risk rejections, headroom calculations, kill switch activations |
| Reconciliation | `reconciliation_resolved_filled`, `reconciliation_resolved_terminal`, `reconciliation_unreachable` |
| Discovery | `discovery_refresh_started`, `discovery_refresh_succeeded`, `discovery_refresh_failed` |
| Leadership | `leader_tier_changed`, `configured_leader_stale` |
| Compliance | Geoblock check results |

### Log Format

All logs are in JSONL format with structured fields:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "event": "execution_filled",
  "user_id": "user123",
  "order_id": "ord_abc",
  "reservation_id": "res_xyz",
  "venue_order_id": "venue_123",
  "side": "BUY",
  "token_id": "0x456...",
  "leader_wallet": "0xabc..."
}
```

### Log Integrity

- Logs are rotated when they reach `LOG_MAX_BYTES` (default: 5 MB)
- `LOG_BACKUP_COUNT` (default: 3) previous log files are retained
- Logs are append-only — the bot never modifies or deletes log entries

---

## Secret Redaction in Logs and Crash Reports

### Automatic Redaction

The bot automatically redacts sensitive values before they enter logs:

- `PRIVATE_KEY` → `***REDACTED***` in the `ENV` snapshot
- `PRIVATE_KEY_FILE` → `***REDACTED***` when configured
- Wallet addresses are NOT redacted (they are public on the blockchain)

### Crash Reports

If the bot crashes with an unhandled exception:
- The exception traceback may contain variable names but not values
- The `ENV` snapshot used in the crash context has secrets already redacted
- Python's default traceback printing does not include local variable values unless `traceback` is explicitly configured

### Manual Redaction

When sharing logs for debugging:
1. Review the log output for any accidentally included secrets
2. Redact wallet addresses if you prefer anonymity (replace with `0xAAAA...BBBB`)
3. Never share the raw `secrets.env` file or private key values

---

## Security Checklist

Before deploying the bot in production:

- [ ] `secrets.env` has `0600` permissions
- [ ] `PRIVATE_KEY_FILE` (if used) has `0600` permissions
- [ ] `secrets.env` is not committed to git
- [ ] Docker uses `--env-file` instead of `-e` for secrets
- [ ] Metrics server binds to `127.0.0.1` only
- [ ] `ENFORCE_GEOBLOCK=true` (never disable in production)
- [ ] A dedicated wallet is used (not your main wallet)
- [ ] The wallet is funded with only the minimum needed capital
- [ ] Bot runs in paper mode first to validate configuration
- [ ] Kill switch is tested and functional
- [ ] Log files are reviewed for accidental secret exposure
- [ ] `.gitignore` includes `secrets.env` and `*.key`

---

## Next Steps

- See [11-legal-disclaimer.md](11-legal-disclaimer.md) for legal disclaimers.
- See [09-troubleshooting.md](09-troubleshooting.md) for diagnosing security-related issues.
