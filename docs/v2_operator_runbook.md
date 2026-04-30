# V2 Operator Runbook

## Quick Start

### 1. Install V2 dependencies

```bash
cd /home/anon/Bots/Polymarket-Copy-Trading-Bot-Python
.venv/bin/pip install -e ".[v2]"
# or: .venv/bin/pip install py-clob-client-v2
```

Verify the SDK is importable:

```bash
.venv/bin/python -c "from py_clob_client_v2.client import ClobClient; print('V2 OK')"
```

### 2. Run the smoke test

```bash
.venv/bin/python scripts/smoke_v2.py
```

Expected output: `ALL CHECKS PASSED ✅`

The smoke test verifies:
- CLOB version endpoint
- `get_tick_size` for a known token
- `get_neg_risk` for a known token
- `get_order_book` for a known token

**It does NOT place orders or touch funds.**

### 2b. Run V2 readiness test suite

```bash
.venv/bin/python -m pytest tests/test_clob_v2_market_meta.py tests/test_v2_readiness.py tests/test_clob_v2_runtime_smoke.py tests/test_v2_metadata_and_order_options.py -q
```

All tests must pass before setting the readiness flag below.

### 3. Set readiness flag

Once smoke tests pass, enable V2 readiness in your `.env` or `runtime.env`:

```
CLOB_V2_READY=true
```

Without this flag, the bot **will not start** — `create_clob_client()` raises a `RuntimeError` to prevent accidental deployment before operator sign-off.

## What "Ready" Means

- ✅ V2 SDK installed and importable
- ✅ Public endpoints (tick_size, neg_risk, order_book, version) respond
- ✅ `CLOB_V2_READY=true` set in environment
- ⚠️ **pUSD collateral / allowance checks** are a separate concern tracked in Issue #53
- ⚠️ **Live order placement** should be tested in paper-trade mode first (`PAPER_TRADE_ONLY=true`)

## Rollback Constraints

Once this branch (`clob-v2-migration`) is merged into `develop`:
- **V1 SDK is gone** — `py-clob-client` is no longer a dependency
- All imports go through `py_clob-client-v2`
- The compatibility seam (`utils/clob_v1_compat.py`) is V2-native
- To revert, you would need to restore V1 imports and the old `py-clob-client` dependency

## When Is Live Trading Safe?

Live trading is safe to re-enable when:
1. Smoke tests pass
2. Paper-trade mode runs for at least one full monitoring cycle without errors
3. pUSD collateral balance and allowance are verified (Issue #53)
4. `CLOB_V2_READY=true` is set

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `ModuleNotFoundError: py_clob_client_v2` | SDK not installed in venv | `.venv/bin/pip install py-clob-client-v2` |
| Smoke test 429 errors | Rate-limited on public endpoints | Wait and retry; not a config issue |
| `CLOB V2 migration not finalized` warning | `CLOB_V2_READY` not set | Set `CLOB_V2_READY=true` in env |
| Order submission fails with `tick_size_violation` | Price not quantized to tick | Check `quantize_to_tick()` in `createClobClient.py` |
