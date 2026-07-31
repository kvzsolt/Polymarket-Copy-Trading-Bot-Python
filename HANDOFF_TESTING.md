# Testing handoff

## Phase 0 — Baseline preservation

1. Create or switch to `feature/hybrid-ccxt-prediction` from a clean current `develop`.
2. Run `python -m pytest -q` before applying the overlay.
3. Save the baseline test count, failures, skips, Python version, and current commit SHA.
4. Do not attribute existing failures to this branch. The recorded baseline at
   `d7a81bd` is **2054 passed, 7 failed, 3 skipped** on Python 3.12.13; all 7
   failures are environment-dependent `tests/test_config.py` cases that predate
   this work. Any *additional* failure is a branch regression and blocks.

## Phase 1 — Red-first evidence

1. Apply only the new test files, without the new implementation modules or SDK regression patch.
2. Run each new file separately.
3. Confirm failures are import/behavior failures for the intended missing feature, not fixture or environment errors.
4. Save the command and failing assertion for every test file.

## Phase 2 — Precision and client factory

Run:

```bash
python -m pytest -q \
  tests/test_ccxt_precision.py \
  tests/test_ccxt_prediction_client.py
```

Required result: all tests pass. Repeat under Python 3.11 and 3.12.

## Phase 3 — Gateway contract and error semantics

Run:

```bash
python -m pytest -q \
  tests/test_ccxt_prediction_gateway_contract.py \
  tests/test_ccxt_prediction_gateway_errors.py \
  tests/test_ccxt_prediction_gateway_pagination.py
```

Required result: all protocol methods pass with arbitrary tick sizes, explicit price limits, BUY-cost/SELL-share semantics, exact maker attribution, terminal FOK/FAK handling, ambiguous POST classification, and complete private-history cursor pagination that fails closed on a repeated cursor.

## Phase 4 — SDK regression preservation

Run:

```bash
python -m pytest -q \
  tests/test_polymarket_unified_sdk.py \
  tests/test_sdk_gateway_regressions.py
```

Required result: both the existing official SDK contract suite and the new regressions pass.

## Phase 5 — Hybrid shadow safety

Run:

```bash
python -m pytest -q \
  tests/test_hybrid_polymarket_gateway.py \
  tests/test_runtime_polymarket_gateway.py \
  tests/test_runtime_bootstrap_venue_backend.py
```

Required result: SDK output remains authoritative, read mismatches are recorded, shadow errors are contained, no submit/cancel method is ever called on the shadow gateway, and the composition root still resolves to `sdk` by default without constructing any CCXT client.

## Phase 6 — Repository regression suite

Run:

```bash
python -m pytest -q
```

Required result: no new failures, no reduced test count, no unexpected skips, and no timing regression in execution/reconciliation tests.

## Phase 7 — Optional dependency matrix

Run the full suite twice:

1. without CCXT installed;
2. with `python -m pip install -r requirements.ccxt.txt`.

Required result: the default SDK path passes in both environments; only explicitly CCXT-dependent integration tests may skip when CCXT is absent.

## Phase 8 — Read-only live comparison

1. Start in paper mode with `PMCOPY_VENUE_BACKEND=sdk-shadow-ccxt`.
2. Do not enable live execution.
3. Compare order books, open orders, and fills for a representative set of standard and negative-risk markets.
4. Record mismatch rate, shadow exception rate, dropped comparison count, p50/p95/p99 primary read latency, and memory growth.
5. Require zero write calls from the shadow backend.

## Phase 9 — Paper soak

1. Run at least one complete strategy/reconciliation cycle with CCXT as paper primary.
2. Include restart recovery, empty/thin books, partial fills, cancel-after-fill, rate limiting, websocket loss, REST timeout, and stale metadata scenarios.
3. Require zero duplicate intents, zero blind POST retries, zero unknown un-reconciled orders, and zero key material in logs.

## Phase 10 — Explicit live micro-order gate

1. Keep the official SDK as live primary for the first live comparison.
2. Enable CCXT live primary only through the separate experimental gate.
3. Use one operator-approved minimum-size order on a liquid market.
4. Verify accepted order ID, CLOB state, fill attribution, balance/allowance, cancellation behavior, and startup reconciliation.
5. Disable the experimental gate immediately after the test.

## Phase 11 — Release decision

Promotion requires all previous phases green, a clean full-suite rerun at the release commit, no unresolved shadow mismatch affecting signed amounts or order state, and a documented rollback test proving `PMCOPY_VENUE_BACKEND=sdk` restores the original path without data migration.
