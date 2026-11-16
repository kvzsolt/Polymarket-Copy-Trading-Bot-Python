<!-- PMCOPY-CCXT-ONLY-GUIDANCE -->
# CCXT-only Polymarket venue instructions

**Before planning, editing, reviewing, or running tests, read the repository-root `CLAUDE.md` in full.** Every delegated agent or subagent must do the same. If a nested `AGENTS.md` exists, read it after the root files and obey the narrower rules for that subtree.

## Non-negotiable architecture

- `ccxt.prediction.polymarket` is the only authenticated trading and order-execution transport. The official `polymarket-client` / `py-clob-client` SDK has been removed; do not reintroduce it.
- Official Polymarket APIs, WebSockets, Polygon RPC/WSS, and contracts remain in use for market data, account data, and leader detection, but must not become a second private order execution implementation. No direct private order creation or cancellation using `httpx`, `aiohttp`, `requests`, or custom signing code.
- Do not let CCXT models, dictionaries, exceptions, symbols, or credentials leak into domain, risk, execution, reconciliation, or persistence code.
- Raw Polymarket outcome token IDs are the adapter boundary identity. Do not invent crypto-style symbols.
- Live mode must never be inferred from the presence of a private key. The explicit live control (`execution.trading_mode`) remains authoritative.
- There is no venue backend selector, no hybrid gateway, no shadow mode, and no experimental CCXT live gate. Obsolete `venue_backend` / `allow_experimental_ccxt_live` / `shadow_ccxt` config keys are rejected fail-closed.

## Execution boundary

```text
Application
    ↓
ClobGateway
    ↓
CcxtPredictionGateway (live) / PaperExecutionGateway (paper)
    ↓
CcxtPredictionClient
    ↓
ccxt.prediction.polymarket
```

Paper mode uses CCXT for live public market reads and the local simulator for order writes. Paper mode must never call CCXT private write methods.

## Required workflow

For every behavior slice, use this order and preserve the evidence:

1. Characterize the current CCXT behavior and relevant failure mode.
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
- paper mode never invoking a CCXT private write method;
- CCXT being mandatory in paper and live configuration;
- no `0.01` tick-size fallback existing anywhere.

## Scope discipline

Do not rewrite the copy-decision, trader-discovery, risk, reconciliation, persistence, WSS detection, or execution-coordination subsystems. Preserve all existing behavior unless a failing regression test demonstrates a bug.

## Verifying library behavior

Do not rely on memory for CCXT semantics. The installed source is the
authority: read `.venv/lib/python3.*/site-packages/ccxt/prediction/polymarket.py`
(and `ccxt/abstract/prediction/polymarket.py` for generated raw endpoints)
before asserting what a method does. The local CCXT checkout at
`/home/anon/Work/coding/ccxt` is the upstream truth-of-source mirror; verify
pinned-release behavior against it when planning upgrades.

## Working in this repository (standalone system)

- Production code lives under `src/pmcopy/` (single import namespace,
  wheel-installable). There is no control plane, operator API, or dashboard;
  the `pmcopy` CLI (`src/pmcopy/bot_cli/`) is the only operator surface.
- Test tiers: default `python -m pytest -q` (integration + release_gate
  deselected); `python -m pytest -m integration -q` runs the Postgres tier
  against ONE shared container (`pmcopy-integration-pg`, fixture in
  `tests/integration_postgres.py`) — never reintroduce per-test
  testcontainers churn (contract pinned by
  `tests/test_integration_shared_postgres.py`); `python -m pytest -m
  release_gate -q` runs release gates. Dispatches touching
  integration-marked files must run them explicitly with `-m integration`.
- Dependency/packaging workflow: edit `pyproject.toml` → `uv lock` (uv
  0.11.31 pinned in CI) → `python scripts/generate_requirements.py` (the
  requirements exports are generated from `uv.lock`, never hand-edited) →
  `python scripts/check_requirements_drift.py` must pass. Wheel/sdist
  battery: `python scripts/verify_package.py`.
- Multi-copier configuration is the schema-v2 roster
  (`app.copiers_config_file`, example `src/pmcopy/config/copiers.example.yml`
  — secret-free by design, wallet fields are env-var references). The
  v1 JSON roster (`app.multi_user_config_file`) still exists; combining the
  two is a configuration error.
- Python 3.13 only (`>=3.13,<3.14`); ccxt is pinned `==4.5.92`. 3.14 is
  deliberately unsupported (the runtime gate rejects it fail-closed), which
  also keeps ccxt's `coincurve==21.0.0` in the lock — signing is the native
  `CoinCurveECCBackend`, not the pure-Python fallback.
- **Python 3.13 syntax policy**: multi-type `except` clauses MUST be
  parenthesized (`except (A, B):`). The parenthesis-free PEP 758 form is
  legal on 3.14 but a `SyntaxError` on 3.13 and must never re-enter. The
  migration parenthesized 16 sites across 4 modules — an `ast.parse` sweep
  reports only the first error per file, so the naive count of 4 understated
  it; `[tool.ruff] target-version = "py313"` keeps `ruff format` from
  re-adding parens-free clauses. The release-gate guard
  `tests/release_gate/test_source_parse_clean.py` `ast.parse`s every file
  under `src/` + `scripts/` with the running interpreter and fails with
  `file:line` if a PEP 758 construct reappears.

## Polymarket execution work

Before touching Polymarket execution, discovery, reconciliation, authentication, or market data:

1. Read the repository-root `CLAUDE.md` completely.
2. Read every applicable `AGENTS.md` from root to the target directory.
3. Keep CCXT as the only trading transport behind `ClobGateway`.
4. Keep custom CLOB/WebSocket detection in the latency-sensitive path.
5. Never retry an ambiguous order submission without reconciliation.
6. Never infer live mode from available credentials.
7. Use exact decimal arithmetic for money, shares, prices, tick sizes, and limits.
8. Write failing tests before changing behavior.

## Falcon discovery

Treat Falcon Api as an external analytics provider, not an execution authority.

- Rank wallets by bankroll-compatible copyability, not raw PnL or H-Score alone.
- Validate multi-window Wallet360 evidence, sample size, liquidity, capacity, drawdown, concentration, recency, and anomalies.
- Provider authentication/schema/config errors fail closed.
- Transient outage fallback is allowed only when explicitly configured.
- A successful provider response yielding zero safe wallets must remain zero; do not silently substitute weaker candidates.
- Never log API keys, bearer headers, private keys, CLOB credentials, or unredacted provider errors.

## TDD and commits

For each behavior slice:

1. add/update focused tests;
2. demonstrate RED for the intended reason;
3. commit tests separately when practical;
4. implement the smallest safe change;
5. run focused GREEN;
6. run relevant regressions;
7. do a money-path and secret-leak review before commit.