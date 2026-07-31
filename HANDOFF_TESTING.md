# Hybrid CCXT + Heisenberg handoff — testing phases only

This file contains verification phases only. It is not an implementation roadmap.

## Phase 0 — environment and baseline

Record:

- branch and commit SHA;
- Python version;
- operating system;
- dependency lock state;
- whether CCXT is installed;
- whether a Heisenberg API key is available;
- whether Polymarket credentials are available for read-only authenticated checks.

Run the repository's existing full suite before migration changes:

```bash
python -m pytest -q
```

Gate: unrelated baseline failures are documented and separated from migration failures.

## Phase 1 — RED evidence

Apply only the new/updated tests for each slice before the implementation for that slice.

Required RED groups:

1. CCXT arbitrary tick precision and price protection.
2. CCXT gateway contract and error classification.
3. Private pagination, maker/taker attribution, and deduplication.
4. Read-only hybrid shadow invariants.
5. Runtime backend selection and live gate.
6. Heisenberg HTTP/auth/pagination/schema handling.
7. Wallet360 normalization and multi-window evidence.
8. Bankroll compatibility and visible-capacity filtering.
9. Composite fallback and fail-closed behavior.
10. Policy validation for NaN, infinity, impossible ranges, and unsupported windows.

For every RED run save the command, exit code, failing tests, and why the failure is expected.

Gate: failures must be caused by missing behavior, not broken imports, malformed fixtures, accidental network access, or absent unrelated packages.

## Phase 2 — isolated unit GREEN

Run unit tests without external network access for:

- exact Decimal/tick arithmetic;
- outcome/token mapping;
- order request semantics;
- error translation;
- pagination loop bounds;
- fill-leg attribution;
- wallet metric normalization;
- bankroll scaling;
- score composition;
- policy validation;
- secret redaction.

Gate: all isolated unit tests pass with no secret-bearing output.

## Phase 3 — gateway contract GREEN

Run the complete `ClobGateway` contract against:

1. official SDK adapter;
2. CCXT adapter using deterministic test doubles.

Verify:

- order-book metadata;
- BUY cost and SELL share semantics;
- limit and marketable orders;
- batch operations where supported;
- order state and partial fills;
- open orders;
- account fills;
- cancel and cancel-after-fill behavior;
- ambiguous POST handling;
- idempotent close.

Gate: both adapters satisfy domain behavior without leaking vendor types.

## Phase 4 — real CCXT package compatibility

Install the exact optional pin:

```bash
python -m pip install -r requirements.ccxt.txt
```

Run adapter tests against the installed CCXT package with network calls mocked at the transport boundary—not by replacing the adapter itself.

Verify:

- import path and prediction namespace;
- method signatures;
- returned structure fields;
- available `has` flags;
- exception classes;
- private pagination endpoint shapes;
- compatibility patch scope and teardown.

Gate: no implementation assumption depends solely on hand-written doubles.

## Phase 5 — default dependency matrix

Test two clean environments.

### Matrix A — CCXT absent

- normal SDK startup works;
- default backend is SDK;
- paper and live configuration parsing works;
- only explicitly optional CCXT tests skip;
- discovery can use its configured non-CCXT capacity probe or fail closed as designed.

### Matrix B — CCXT installed

- SDK default remains unchanged;
- CCXT adapter tests pass;
- hybrid shadow tests pass;
- no authenticated CCXT client is created in paper mode.

Gate: adding the optional integration does not make CCXT a mandatory production dependency.

## Phase 6 — Heisenberg provider contract

With recorded HTTP fixtures or a sandbox/read-only key, validate:

- bearer authentication;
- request endpoint and agent IDs;
- leaderboard parsing;
- Wallet360 1/3/7/15-day windows;
- all-time statistics;
- trade, market, Market360, realized-PnL, and price-jump responses;
- UTC date boundaries;
- pagination termination;
- 429/5xx retry behavior;
- auth/schema/config fail-fast behavior;
- response and error redaction.

Gate: schema drift is detected explicitly; unknown fields may be tolerated, but missing required evidence cannot silently receive a safe score.

## Phase 7 — bankroll-ranking scenarios

Use deterministic wallets covering:

- profitable whale incompatible with a small bankroll;
- smaller stable wallet with high copyable-trade ratio;
- high H-Score wallet with severe recent drawdown;
- concentrated wallet trading one illiquid market;
- stale wallet with good historical PnL;
- wallet dominated by short-duration markets;
- wallet affected by price-jump anomalies;
- wallet with insufficient trade sample;
- wallet whose trades become sub-minimum after scaling;
- wallet whose copied size exceeds safe visible depth.

Run ranking for at least:

```text
$100
$500
$2,000
$10,000
```

Gate: the best wallet may change with bankroll; raw PnL alone never determines rank.

## Phase 8 — composition and shadow soak

Verify the real runtime composition root.

For `sdk-shadow-ccxt`:

- primary return latency is not blocked by shadow;
- shadow reads are bounded;
- dropped/failed shadow observations are measurable;
- shadow never calls mutation methods;
- primary and shadow order-book/position/fill differences are recorded safely;
- shutdown closes the optional backend once.

Run an extended paper/read-only soak and report p50/p95/p99 latency, error counts, task backlog, memory growth, and comparison mismatches.

Gate: no mutation and no material degradation of the SDK-primary path.

## Phase 9 — complete repository regression

Run all repository-defined checks:

```bash
python -m pytest -q
```

Also run current lint, formatting, typing, security, and packaging commands defined in repository instructions and CI.

Gate:

- full suite green or baseline-known failures only;
- no unexpected skips;
- no generated/vendor files edited;
- no secrets in logs, snapshots, fixtures, or artifacts.

## Phase 10 — authenticated read-only verification

When credentials are available, perform read-only checks only:

- SDK balance/open orders/fills/positions;
- CCXT balance/open orders/fills/positions;
- Deposit Wallet/funder identity;
- signature type diagnostics;
- pagination across an account with enough history;
- reconciliation comparison.

Gate: results are explainably equivalent or differences are documented before any write test.

## Phase 11 — paper trading soak

Run the full bot in paper mode with:

- SDK default;
- SDK + CCXT shadow;
- Heisenberg discovery enabled;
- provider outage and rate-limit injection;
- WebSocket reconnect and REST backfill injection;
- process restart and reconciliation;
- multi-user isolation if supported.

Gate: no duplicate intents/orders, no stale reservations, deterministic restart, stable wallet roster behavior, and safe provider fallback.

## Phase 12 — live canary, separately authorized only

Do not execute this phase without explicit operator authorization.

When authorized:

- use minimum permitted order size;
- one selected market;
- one order at a time;
- explicit cap/floor;
- immediate reconciliation;
- verify partial fill, cancel race, settlement visibility, and rollback to SDK;
- stop on any mismatch.

Gate: live CCXT primary remains experimental until multiple successful canaries and an agreed soak period.

## Final evidence required

Provide:

- exact commands and outputs;
- test counts and durations;
- environment matrix;
- real CCXT version;
- Heisenberg endpoint/schema evidence used;
- shadow mismatch summary;
- ranking scenarios and results;
- unresolved risks;
- rollback steps;
- explicit statement that no live mutations were performed, unless separately authorized and documented.
