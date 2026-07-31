<!-- PMCOPY-HYBRID-CCXT-GUIDANCE -->
# Hybrid CCXT branch instructions

**Before planning, editing, reviewing, or running tests, read the repository-root `CLAUDE.md` in full.** Every delegated agent or subagent must do the same. If a nested `AGENTS.md` exists, read it after the root files and obey the narrower rules for that subtree.

## Non-negotiable architecture

- The official `polymarket-client` SDK remains the production-default execution backend.
- CCXT is an optional second adapter behind `interfaces.clob_gateway.ClobGateway`.
- Do not let CCXT models, dictionaries, exceptions, symbols, or credentials leak into domain, risk, execution, reconciliation, or persistence code.
- `sdk-shadow-ccxt` is read-only on the shadow side. Never mirror create, cancel, approval, redemption, split, merge, or any other mutation.
- Raw Polymarket outcome token IDs are the adapter boundary identity. Do not invent crypto-style symbols.
- Live mode must never be inferred from the presence of a private key. The existing explicit live controls (`execution.trading_mode`) remain authoritative.
- CCXT may become the live primary only with the separate `allow_experimental_ccxt_live` gate. Never weaken or remove that gate.

## Required workflow

For every behavior slice, use this order and preserve the evidence:

1. Characterize the current SDK behavior and relevant failure mode.
2. Add a new focused test file or focused new test that fails for the intended reason.
3. Run the smallest test command and record the red result.
4. Implement only enough code to make that slice green.
5. Run the focused test again.
6. Run the adapter contract group.
7. Run the complete repository suite before considering the slice complete.
8. Review for key leakage, duplicate submission risk, stale metadata, precision loss, and non-idempotent retries.

Never write production code before the corresponding failing test. Do not edit generated CCXT files or vendor code. Keep the arbitrary-tick compatibility patch instance-local and removable.

## Correctness gates

A change is incomplete until tests cover, where applicable:

- arbitrary tick sizes including `0.005` and `0.0025`;
- explicit market-order price limits;
- BUY cost semantics versus SELL share semantics;
- signature type 3 and verified Deposit Wallet funder handling;
- missing `tick_size`, `min_order_size`, or `neg_risk` failing closed;
- maker and taker fill attribution;
- pagination duplicate elimination;
- ambiguous POST failures not being retried blindly;
- cancel-after-fill and cancel-404 idempotency;
- shadow mode never invoking a write method;
- SDK remaining the default in paper and live configuration.

## Scope discipline

Do not rewrite the copy-decision, trader-discovery, risk, reconciliation, persistence, WSS detection, or execution-coordination subsystems for this migration. The branch adds an adapter, a safe selector, shadow comparison, regression fixes, and tests. Preserve all existing behavior unless a failing regression test demonstrates a bug.

## Verifying library behavior

Do not rely on memory for CCXT or SDK semantics. The installed source is the
authority: read `.venv/lib/python3.*/site-packages/ccxt/prediction/polymarket.py`
(and `ccxt/abstract/prediction/polymarket.py` for generated raw endpoints)
before asserting what a method does. Several defects this branch fixes were
only confirmed by reading that file — the hard-coded four-value tick table in
`polymarket_order_raw_amounts`, the single-page `fetch_open_orders`, the
cached-outcome-price fallback when `create_order` is called without a price,
and the `negRisk` default of `False`.
