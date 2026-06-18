# Release Notes

> Version history, upgrade instructions, and how to read release notes for the Polymarket Copy Trading Bot.

---

## How to Read Release Notes

Each release entry follows this format:

```
## [VERSION] — DATE

### Breaking Changes
Changes that require operator action before upgrading. These may include:
- Removed environment variables
- Changed default values
- Modified database schemas
- Altered behavior that could affect trading

### New Features
New capabilities added in this version.

### Improvements
Enhancements to existing features (performance, reliability, accuracy).

### Bug Fixes
Resolved issues from previous versions.

### Deprecations
Features or configuration options that will be removed in a future release.

### Migration Notes
Step-by-step instructions for upgrading from the previous version.
```

### Severity Indicators

| Indicator | Meaning |
|---|---|
| 🔴 **BREAKING** | Requires operator action; bot may not start or trade correctly without migration |
| 🟡 **WARNING** | Behavior change that may affect trading outcomes; review recommended |
| 🟢 **SAFE** | No operator action required; backwards compatible |

---

## Current Version

**Version:** See `pyproject.toml` for the current version number.

```bash
python -c "import tomllib; print(tomllib.open('pyproject.toml')['project']['version'])"
```

---

## Version History

### [Unreleased]

#### New Features
- V2 CLOB client migration (`py-clob-client-v2`)
- Tiered leader permission ladder (watchlist → paper_follow → live_follow)
- Account cap policy with reserve, hard cap, and soft cap
- Circuit breaker for CLOB API outages
- Orderbook spread quality gates
- Copy decision gate with signal scoring
- Near-resolution gate for stale signals
- Staged take-profit exits
- Leader divergence exit
- Stop-loss with grace period
- Bot doctor diagnostic command (`bot doctor --paper` / `--live`)
- Structured audit logging (JSONL format)
- Metrics server (Prometheus-compatible)
- Secret redaction in ENV snapshot and logs
- Private key file support with permission validation

#### Improvements
- Immutable configuration via frozen dataclasses (`AppConfig`)
- Batch validation of all environment variables at startup
- State machine enforcement for order lifecycle transitions
- Reservation system to prevent duplicate execution
- Reconciliation service for unconfirmed orders
- Composite leader scoring (balance + analytics + activity weights)
- Dynamic threshold raise in balance policy
- Trust-based size multipliers for position sizing

#### Breaking Changes
- V1 CLOB SDK (`py-clob-client`) removed in favor of V2
- `CLOB_V2_READY=true` required to start the bot
- `PROXY_WALLET` deprecated in favor of `FUNDER_ADDRESS`
- Various legacy environment variable names deprecated (see migration notes)

#### Migration Notes
- Install V2 SDK: `pip install py-clob-client-v2`
- Run V2 smoke test: `python scripts/smoke_v2.py`
- Set `CLOB_V2_READY=true` in your environment
- Replace `PROXY_WALLET` with `FUNDER_ADDRESS` in your configuration
- Run `bot doctor --paper` to verify all checks pass
- Review [v2_operator_runbook.md](v2_operator_runbook.md) for detailed V2 migration steps

---

## Upgrade Instructions

### General Upgrade Process

1. **Read the release notes** — Check for breaking changes and migration notes
2. **Stop the bot** — Ensure no orders are in-flight
3. **Backup your data** — `cp data/bot_state.db data/bot_state.db.bak`
4. **Pull the latest code** — `git pull origin main`
5. **Install dependencies** — `pip install -r requirements.txt`
6. **Review configuration** — Check for new or changed environment variables
7. **Run bot doctor** — `python bot_cli/main.py doctor --paper`
8. **Start in paper mode** — Verify the bot operates correctly
9. **Switch to live** — Only after confirming paper mode works

### Upgrading Across Major Versions

Major versions (e.g., 1.x → 2.x) may include:
- Database schema changes that require migration
- Removed configuration options
- Changed default behavior

Always review the migration notes for the specific version you're upgrading to. When in doubt, start with a fresh database and reconfigure from scratch.

### Database Migrations

The bot applies database migrations automatically on startup. If a migration fails:
1. Check the error message for details
2. Restore from backup: `cp data/bot_state.db.bak data/bot_state.db`
3. Review the migration in `storage/sqlite/migrations.py`
4. If the migration is complex, consider starting with a fresh database

### Configuration Migrations

When environment variables are renamed or restructured:
1. The bot logs deprecation warnings for legacy variable names
2. Legacy variables continue to work for at least one release after deprecation
3. After the deprecation period, legacy variables are removed
4. Update your `.env` files as soon as deprecation warnings appear

Current deprecation mappings:

| Legacy Variable | New Variable | Status |
|---|---|---|
| `PROXY_WALLET` | `FUNDER_ADDRESS` | Deprecated — still functional |
| `USER_ADDRESS` | `LEADER_WALLETS` (single entry) | Deprecated — still functional |
| `DB_ENABLED` | `BOT_STATE_DB_ENABLED` | Deprecated — still functional |
| `STOP_LOSS_PCT` | `STOP_LOSS_THRESHOLD` | Deprecated — value semantics changed |
| `TAKE_PROFIT_PCT` | `STAGED_TP_LEVELS` / `PROFIT_LOCK_TRIGGER_PCT` | Deprecated — restructured |

---

## Release Cadence

The project follows a pragmatic release schedule:
- **Patch releases** (x.x.Z): Bug fixes, minor improvements — as needed
- **Minor releases** (x.Y.0): New features, backward-compatible changes — monthly or as needed
- **Major releases** (X.0.0): Breaking changes — only when necessary

---

## Changelog Sources

Release notes are compiled from:
- Git commit history (conventional commit format)
- Pull request descriptions
- Issue tracker resolutions
- Manual testing results

Operators are encouraged to test pre-release versions in paper mode before upgrading production deployments.

---

## Next Steps

- See [01-installation.md](01-installation.md) for fresh installation instructions.
- See [09-troubleshooting.md](09-troubleshooting.md) if upgrades cause issues.
