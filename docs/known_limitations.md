# Known Limitations

The Polymarket copy-trading bot is fail-closed wherever a money-moving
decision is at stake, but the production path still has several
documented limitations that the operator needs to know before running it
live. Each item below was the subject of a deep-research review cycle and
is intentionally documented here (and scanned for by
`scripts/rc1_check.sh`'s test surface) so the bot does not silently
regress on any of them.

## 1. http polling for market data

The leader-market and order-book read path uses **http polling** of the
Polymarket gamma REST API and the venue's REST `/book` endpoint. Polling
configurable rate defaults match what the deep-research review found to
be safe under typical leader activity; aggressive operators tuning the
poll are responsible for the rate-limit cost. The bot does not silently
replace polling with WSS streaming — the WSS subscription is opt-in and
the code paths remain separate per the REST-primary execution contract
(see `tests/test_market_data_rest_primary_execution.py`).

Limitations:

- Polling latency is bounded by the REST round-trip + rate limit refresh;
  a slow gamma response can delay a leader fill's reflection in the
  funder wallet's strategy decisions. The bot's fail-closed book-quality
  gate (`tests/test_book_quality_gate.py`) catches widesp-read books
  before they can drive an off-mark order.
- No streaming tail-fallback: when the gamma API is down, the bot drops
  the leader-fill update rather than risking a stale snapshot. The next
  poll cycle is the sole recuperation path.
- http polling is the floor — the WSS subscription path is gated by the
  operator setting `MARKET_DATA_PRIMARY_SOURCE` to a streaming mode;
  default config keeps http polling as the executive source of truth.

## 2. the coordinator submit surface is the single live-entry surface

External code paths that want to suggest a trade MUST route through the
execution coordinator's `submit_copy_trade` / `submit_risk_exit` /
`submit_protective_exit` funnel (`src/pmcopy/services/execution/coordinator.py`). Any
other call to `execution_service.execute` is forbidden by
`scripts/rc1_check.sh`'s forbid scan (it Greps production
code for `execution_service.execute`, the `getattr` aliases, and rejects
any file that lacks the `execution_coordinator_missing` guard). The
single permitted call site is `src/pmcopy/services/execution/coordinator.py` — the
code-level single-entry-point invariant for live order submission. This
is a deliberate architectural limitation, not a missing feature.

Limitations:

- The single entry-point does not enforce policy (slippage, max sizing);
  that policy layer is upstream of the coordinator submit surface and must
  always be invoked before reaching this surface. The bot enforces this
  ordering via the orchestrator pipeline (`src/pmcopy/orchestrators/trade_pipeline.py`).
- The single entry-point also means recovery paths (rollback, canary,
  cancel-and-resubmit) MUST go through the coordinator submit surface —
  they cannot bypass it through a helper. The audit log surfaces this
  because each executed order carries the same canonical idempotency key.

## 3. external integrations

The bot depends on three external surfaces:

- The Polymarket gamma REST API (HTTP polling for leader discovery +
  order-book snapshots).
- The CLOB venue V2 REST API (order placement + order-status + fills).
- The Polygon RPC (HTTP/WebSocket for on-chain collateral, on-chain
  `OrderFilled` event detection in PR2, and Neg Risk exchange
  probes).

Each of these has documented limitations:

- Polymarket gamma: rate-limit bursts throttle the bot aggressively
  (Polymarket-side), and the gamma API does not document a backoff
  contract; the bot's polling cadence defaults to what was empirically
  safe + the deep-research gap analysis. The freeze on aggressive
  polling is enforced at typed-config level (`AppConfig.polling_interval`).
- CLOB venue V2: the venue's order endpoint does not return ack in the
  same response — the bot polls `/order/<id>` separately. Pending vs
  confirmed fill distinction is client-side; the order-status call is
  best-effort and the bot's risk pre-reconciliation compensates for late
  acks (PR #122).
- Polygon RPC: the bot must hold two distinct RPC URLs (primary +
  fallback) because Polygon-style RPC providers have a stale-block failure
  mode. The bot's collateral-readiness per-iteration probe reads primary
  first and falls back if the primary returns a stale block. On Pr2 the
  WebSocket subscription paths assume a fresh RPC.

The contract is: **the operator is responsible for keeping at minimum one
healthy RPC**; if all RPCs go dark, the bot goes fail-closed (collateral
readiness degrades; doctor blocks; live trading halts). This is the
intended behaviour.

## 4. Strategy profiler is dry-run by default

The Phase-11 strategy profiler (`src/pmcopy/services/strategy_profile/`) and the env
tuner (`src/pmcopy/services/strategy_profile/env_patch_writer.py`) are dry-run-by-
default. The bot does not auto-apply env patches; the operator must
invoke the env-patch writer with `apply=True` and review the proposed
patch explanation. PATCHES TO SECRET KEYS ARE NEVER MUTATED — see
`_PROTECTED_EXACT` and `_PROTECTED_SUBSTRINGS` in
`src/pmcopy/services/strategy_profile/env_patch_writer.py`; the writer's
idempotency-key set + lock in the on-chain dispatcher (`PR2 follow-up`)
do not interact with this surface.

## 5. lost-POST recovery needs a stored market identity

A submission whose POST response was lost is persisted as
`submitted_unconfirmed` with no venue order id, and the runtime resolves it
through bounded, read-only evidence scans (see
`docs/recovery_playbook.md` §4 and
`src/pmcopy/integrations/polymarket/ccxt_ambiguous_evidence.py`).

Limitations:

- Recovery needs the persisted `token_id` and `condition_id`. Legacy rows
  whose sanitized request payload lacks either fail closed on every cycle
  and stay duplicate-blocking until an operator resolves them with
  `pmcopy resolve`.
- Absence is only accepted after repeated *consecutive* complete scans; a
  venue outage, an incomplete page, or a concurrent copy trade that moves
  the token position resets that counter. Slow resolution is the intended
  fail-closed behaviour, not a stall.
- The scan is corroborative, not a second execution path: it never submits,
  cancels, or retries the ambiguous order. Pinned by
  `tests/test_ccxt_ambiguous_evidence.py`.

## 6. Market-exposure cap semantics (confirmed basis)

Two exposure ceilings are enforced, both fail-closed: the account-level
`risk.max_market_exposure_usdc` risk-policy cap, and the explicit copier
cap — schema-v2 `limits.max_open_exposure_quote` (or the explicit v1
follow field). The copier cap is enforced at TWO levels (admission gate
`62458e9`, transaction authority `c497bf5`):

- **BUY admission** (trade pipeline, copied entries only — protective
  exits can never be blocked by an exposure cap): headroom is computed as
  cap − confirmed open exposure − active reserved quote, failing closed
  (`copier_open_exposure_unavailable`) when either basis cannot be
  established.
- **The reservation transaction is authoritative**: inside the per-user
  advisory-lock checked BUY reservation, confirmed open exposure + the
  account's already-reserved quote + the new reservation must be ≤ cap
  (`CopierOpenExposureCapExceeded` → skip code `copier_open_exposure_cap`).
  In-flight BUY intents therefore count toward the cap through their
  reservations, and concurrent cross-scope BUY races on one account are
  serialized by the transaction — exactly one commits (proven by the real
  Postgres cross-scope race test in `tests/multicopier/`).

Two residual limitations remain (documented, bounded):

- **TOCTOU window**: the confirmed-exposure basis is snapshotted before
  the lock (resolved through an injected reader ahead of the transaction),
  so a fill landing between snapshot and lock can be missed by the check
  and a burst can drift slightly past the cap until its fills land — the
  same pre-lock TOCTOU shape as the wallet-balance handling; the
  reservation domain still bounds the concurrent blast radius by wallet
  balance, so this is cap drift, not double-spend.
- **Partial-fill remainders** of an already-accepted order can likewise
  complete past the cap (pre-existing settlement semantics).

## 7. Dependency and toolchain limitations (2026-10 standalone refactor)

- **multidict CVE-2026-104874 RESOLVED (2026-10-13)**: the ccxt bump to
  `==4.5.87` (polymarket module byte-identical to 4.5.85) re-pins
  `multidict==6.9.1`, above the affected range (6.7.0–6.9.0); the lock and
  both requirements exports carry 6.9.1.
- **Python 3.13-only toolchain (3.14 is deliberately unsupported)**: the
  project targets `>=3.13,<3.14` — enforced by the fail-closed runtime gate
  (`pyproject.toml`, `src/pmcopy/config/app_config.py`), the CI matrix
  (`['3.13']`), and the Docker base (`python:3.13.15-slim-bookworm`). This
  is what keeps **coincurve 21.0.0** in the lock (ccxt's own
  `python_version < "3.14"` marker): signing runs on the native
  `CoinCurveECCBackend`. Measured on the 3.13.15 venv: 0.12 ms/sign raw
  `sign_msg`, 0.24 ms/sign EIP-191 `sign_message` (20-iteration means).
  Provenance, not an open item: in the 3.13-unsupported 3.14 era (ended
  2026-10-13) coincurve was absent (no cp314 wheels) and signing used
  `eth_keys`' pure-Python `NativeECCBackend` — 3.08 / 5.32 ms/sign on the
  same method; those figures are superseded history.
- **PEP 758 syntax policy (3.13-only)**: multi-type `except` clauses MUST
  be parenthesized (`except (A, B):`). The parenthesis-free PEP 758 form is
  legal on 3.14 but a `SyntaxError` on 3.13, so it must never re-enter. The
  migration parenthesized **16 sites across 4 modules** — an `ast.parse`
  sweep only reports the first error per file, which is why the initial
  count of 4 understated the real work — and
  `tests/release_gate/test_source_parse_clean.py` now `ast.parse`s every
  file under `src/` and `scripts/` with the running interpreter, failing
  with `file:line` if one comes back (`ruff` is pinned to
  `target-version = "py313"`, and `ruff format --check` on the four
  affected modules reports them already formatted — it no longer rewrites
  the parenthesized form back).
- **mypy beyond the money-path spine is ratcheted, not clean**: CI blocks
  on the spine (adapter + venue runtime + HTTP client) being clean, and
  ratchets the remaining services/orchestrators/domain debt with a
  committed PER-FILE manifest (`scripts/mypy_baseline.json` — 12 erroring
  files / 23 errors, exact per-file equality via
  `scripts/check_mypy_ratchet.py`; deliberate moves only through
  `--update`). New code must not add to any file's count.
- **One process-wide trading mode**: every copier in a process must declare
  the same `mode`, matching `execution.trading_mode` — paper and live
  cannot be mixed in one process; run one deployment per mode.
- **Integration tier is Linux/Docker-bound**: the shared
  `pmcopy-integration-pg` fixture (one container for the whole tier) relies
  on Docker and a `/proc`-dependent stale-database GC helper; it is
  exercised on linux/amd64 only.

## 8. KNOWN_LIMITATIONS scan contract

The release contract (`scripts/rc1_check.sh`) does NOT scan for any of
the literals in this document; the integrity of these limitations is
pinned by the unit test suite (`tests/test_market_data_rest_primary_;
execution.py::test_rest_primary_executes_with_wss_down`,
`tests/test_execution_coordinator_production.py::*`). If you add a new
limitation here, add a corresponding regression test to the unit suite.

## 9. Related docs

- `docs/live_runbook.md` — daily operator workflow including the pre-
  flight gate that enforces these limitations.
- `docs/recovery_playbook.md` — how `auto_released_ambiguous` interacts
  with the reconciliation state machine (one of the few invariants that
  survives the external-integration surface).
- `docs/emergency_shutdown.md` — operator response when an external
  integration drops (kill-switch + doctor).
- `docs/audit/findings_table.md` — the deep-research audit findings
  matrix mapping each finding to the corresponding code fix / test.