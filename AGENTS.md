# Polymarket-Copy-Trading-Bot-Python engineering contract

## 1. System Topology & Repo Routing Table

Ship the standalone Python 3.13 pmcopy wheel, its paper or live venue runtime and account-isolated multi-copier
execution.

| Owner or entry point | Follow this boundary |
| --- | --- |
| src/pmcopy/bot_cli/ | Parse standalone pmcopy commands and route configuration or operator actions. |
| src/pmcopy/services/copierConfig.py | Resolve schema-v2 copier identities, secret references and explicit exposure limits. |
| src/pmcopy/services/multiUserConfig.py | Load legacy v1 runtime users and preserve single-account compatibility. |
| src/pmcopy/orchestrators/runtime_bootstrap.py | Build trading contexts and attach account-scoped gateways, repositories and cap settings. |
| src/pmcopy/orchestrators/runtime_monitor.py | Route leader notifications and coalesce saturation independently by leader. |
| src/pmcopy/interfaces/clob_gateway.py | Preserve typed asynchronous requests, cancellation semantics and venue read contracts. |
| src/pmcopy/integrations/polymarket/venue_runtime.py | Compose public-only paper reads or the authenticated CCXT runtime. |
| src/pmcopy/integrations/polymarket/ccxt_compat/tick_size.py | Apply removable per-instance precision handling for the pinned CCXT release. |
| src/pmcopy/services/execution/reservation_service.py | Enforce funds, ownership and an explicitly configured copier exposure cap within reservation. |
| src/pmcopy/services/execution/ambiguous_recovery.py | Recover a specific copier's ambiguous order using its own authoritative evidence. |
| src/pmcopy/storage/postgres/ | Persist money-path state and package migration resources with the wheel. |
| src/pmcopy/utils/log_redaction.py | Redact registered secret values across operator and diagnostic output. |
| tests/multicopier/ | Exercise independent queues, account isolation, ambiguous recovery, shutdown and the database path. |

Read the guide for the checkout you are changing. For a coordinated release, look up the component revision in the
infrastructure manifest before debugging a branch-specific capability.

## 2. Fast-Feedback Execution Commands

Run from the repository root unless the command changes directory. Use a disposable environment. Select the narrow check
first, then the CI group that covers the changed boundary.

**Environment (CI Python 3.13)**

```sh
python3.13 -m venv .venv && . .venv/bin/activate
```

**Locked editable install**

```sh
python -m pip install --no-cache-dir -r requirements.txt && python -m pip install --no-cache-dir --no-deps -e . && python -m pip check
```

**Lint/type tools used by CI**

```sh
python -m pip install ruff mypy
```

**Single paper transport test**

```sh
python -m pytest -q tests/test_paper_execution_gateway.py::test_paper_marketable_buy_never_reaches_the_transport
```

**Single saturation test**

```sh
python -m pytest -q tests/multicopier/test_queue_saturation.py::test_saturating_leader_a_queue_coalesces_without_stalling_leader_b
```

**Unit tier**

```sh
python -m pytest -m "not integration and not release_gate" --maxfail=5 -q
```

**Venue regression group**

```sh
python -m pytest -q tests/test_ccxt_precision.py tests/test_ccxt_prediction_gateway_contract.py tests/test_paper_execution_gateway.py
```

**Release-gate tier**

```sh
python -m pytest -m release_gate --maxfail=1 -q
```

**Integration tier (disposable services)**

```sh
python -m pytest -m integration -q
```

**Blocking CI Ruff**

```sh
ruff check --exclude=legacy,test.py,bootstrap.py .
```

**Blocking CI mypy scope**

```sh
mypy --install-types --non-interactive --explicit-package-bases --ignore-missing-imports --follow-imports=silent src/pmcopy/integrations/polymarket/ccxt_prediction_client.py src/pmcopy/integrations/polymarket/ccxt_prediction_gateway.py src/pmcopy/integrations/polymarket/venue_runtime.py src/pmcopy/clients/network/http_client.py
```

**Per-file mypy debt ratchet**

```sh
python scripts/check_mypy_ratchet.py
```

**Dependency drift**

```sh
python -m pip install uv==0.11.31 && python scripts/check_requirements_drift.py
```

**Distribution verification**

```sh
python scripts/verify_package.py
```

- **N/A · Rust** — This Python distribution has no in-repository Cargo workspace or maturin build target.

## 3. Architecture & OOP/SOLID Rules

- Trace the request from the public contract to its owner and its observable result before changing a shared method.
- Keep domain decisions independent of transport, storage, provider configuration and presentation. Pass immutable
  inputs into the decision and return an explicit outcome.
- Inject external clients, stores, clocks and executors at composition time through small interfaces. A test must be
  able to substitute the dependency without starting the application.
- Use a Python Protocol for structural substitution or an ABC with abstract methods for a genuine lifecycle contract.
  Use the local language equivalent on other surfaces.
- An implementation must preserve input units, output types, exception semantics, ordering, cancellation and ownership.
  Exercise the same contract against each implementation.
- Add a strategy behind the existing seam for a new provider. Introduce an interface with a single implementation only
  when it protects an immediate I/O, platform or testing boundary.
- Prefer composition. Keep application inheritance within three levels including the concrete type. A new subclass must
  contribute a distinct contract or shared, tested behavior.
- Reuse the owner’s tested retry, rate-limit, normalization, arithmetic and connection lifecycle logic. Before
  extracting a helper, compare the deadline, units, idempotency and failure semantics of every caller.
- Across independent repositories share versioned schemas and conformance fixtures. Keep operational policy in its
  owner. Do not copy implementations between services.
- Keep the hot path direct. A wrapper or abstraction that adds measurable latency or needless per-event allocations
  needs a simpler implementation before merge.

## 4. High-Performance & Bot Reliability Invariants

- Use the src/pmcopy package layout and pmcopy.bot_cli.main:main entry point. Preserve the >=3.13,<3.14 interpreter
  range.
- Keep CCXT 4.5.87 as the sole authenticated Polymarket transport. Check behavior against the installed ccxt prediction
  module when changing the pin.
- Use execution.trading_mode to choose paper or live; authenticated credentials alone cannot select live operation.
- Apply tick workarounds to the exchange instance only. A second independently constructed exchange must retain its own
  precision behavior.
- Separate quote-sized BUY requests from share-sized SELL requests at signing, accounting and recovery boundaries.
- Store config references to secrets and resolve the environment only in the configuration layer. Reject ambiguous
  simultaneous v1 and v2 config files.
- Carry the explicit exposure-cap flag into runtime construction. A configured cap requires readable confirmed exposure
  and transaction-safe headroom.
- Restrict private history scans, absence counters and ambiguous recovery decisions to the copier that issued the
  submission.
- Coalesce saturation for the affected leader while continuing to service other leaders. Observe dropped or coalesced
  work without leaking raw payloads.
- Close every account's client once and release its leases before shutting down the shared pool. Preserve unresolved
  reservations after cancellation.
- Preserve Falcon's bankroll-specific discovery criteria, empty qualifying results, fail-closed auth/schema errors and
  explicit transient fallback policy.
- Preserve package data for migrations and YAML configuration. Test installed artifacts outside the source tree when
  resource lookup changes.
- Keep raw outcome token IDs at the gateway. Reject missing tick/minimum/negative-risk fields; never fall back to 0.01.
  Test 0.005/0.0025 ticks and preserve the authorized price bound after rounding.
- Attribute maker and taker fills, check pagination overlap, cancel-404 and cancel-after-fill, verify type-3 Deposit
  Wallet funding, and prove paper mode cannot call a private CCXT write.
- Preserve 3.13-compatible exception syntax, parenthesized multi-type except clauses and the non-mixing rule for v1/v2
  copiers. Keep keys as environment references in shared rosters.
- The standalone checkout uses src/pmcopy and CCXT 4.5.87. The deployed predictrecon-bot uses a separate flat layout and
  pin.
- The short venue regression group is a local loop. The ccxt-runtime CI job enumerates the complete twelve-file matrix.
- The default pytest tier excludes integration and release_gate. Inspect tests/integration_postgres.py for
  shared-container isolation and any unavailable prerequisites.
- Keep no-xfail enforcement intact; a new xfail on a money-path case breaks the unit job even when other cases pass.

**Under burst, disconnect and shutdown**

- Keep synchronous HTTP, filesystem calls, blocking DB clients, time.sleep and CPU-heavy loops off async event loops.
  Use native async I/O or a bounded offload executor with an explicit lifecycle.
- Set deadlines for connect, acquire, read/write and total network or IPC work. Propagate the remaining deadline through
  retries. Pool clients and connections, then close them at the owning lifecycle boundary.
- Bound every queue, buffer, cache and in-flight collection fed by external traffic. Specify item and byte limits where
  payload sizes vary. Describe what the producer and consumer do when a limit is reached.
- Expose queue age, saturation, drops, timeouts and recovery on the existing telemetry surface. Avoid high-cardinality
  user IDs or event IDs as metric dimensions.
- Prefer durable handoff or explicit rejection for commands. Coalesce replaceable display snapshots only when the
  consumer can recover a fresh state.
- On shutdown stop admission, bound drain time, cancel and await owned work, then close transports. Persist accepted
  work and reconcile outcomes that cannot be determined.
- Minimize copies and repeated decoding on measured high-rate paths. Reuse compiled schemas and regexes. Consider
  slotted records and the existing fast JSON adapter when a representative benchmark shows improvement.
- For production Rust return typed Result errors instead of unwrap() or expect(). Document the invariant behind each
  unsafe block and exercise the error branch.
- If a real Python/Rust binding is introduced, validate and own input before detaching from the GIL. Use the pinned PyO3
  API for allow_threads or detach, and map Result failures at the boundary. For a subprocess, test IPC timeouts, exit
  status and truncation instead.

## 5. Code Style & Documentation Standards

- Follow PEP 8 for Python naming, imports and layout. Use small functions with one observable responsibility. Remove
  dead branches and commented-out implementations introduced by the change.
- Type every Python function and method signature and every class attribute, including constructors and callbacks.
  Preserve generics and unit-bearing types. Decode untyped provider data at the edge.
- Name thresholds, capacities, retry budgets and wire literals where they are owned. Record the unit and the rationale.
  Keep stable protocol constants distinct from deployment configuration.
- Give every public class, abstract interface and reusable method a concise Google-style docstring. Document side
  effects, guarantees and cost or concurrency caveats. Use Args, Returns and Raises sections when applicable.
- For Rust and TypeScript apply the same contract to public types and functions using native documentation and type
  syntax. Preserve an existing JavaScript module boundary in a focused change.
- Raise a specific exception or return the existing typed failure. Never discard an exception with except Exception:
  pass. Preserve cancellation and redact sensitive values from messages and logs.

Use this contract shape for a Python API. Replace the example text with the method’s actual guarantees.

```text
Reserve a quantity for one owner until settlement or release.

Args:
    quantity: Exact units requested by the caller.
Returns:
    The reservation identity after the write commits.
Raises:
    InsufficientCapacity: The requested quantity exceeds available units.
```

Start behavior fixes with a failing deterministic regression. Record the red result, make the smallest safe change,
rerun the same test and then the affected contract and full required tiers.

## 6. Do-Not-Touch Zones

| Protected surface | Approved change route |
| --- | --- |
| src/pmcopy/storage/postgres/migrations/ | Append migrations and ensure package data and installed-artifact checks include them. |
| uv.lock | Change dependencies in pyproject.toml first and regenerate with the pinned uv workflow. |
| requirements.txt | Regenerate the full development export with scripts/generate_requirements.py; compare it to uv.lock. |
| requirements.runtime.txt | Regenerate alongside the development export; keep runtime and test-only dependency selections distinct. |
| scripts/mypy_baseline.json | Ratchet down after proven fixes; do not raise per-file counts to mask a new type error. |
| src/pmcopy/config/copiers.example.yml | Keep synthetic identities and environment references in the shipped example. |
| tests/fixtures/market_wss/ | Preserve scrubbed provider payload shapes and capture intentional schema changes with new cases. |

Open secrets.env only if an authorized operation requires a specific key. Keep credentials out of prompts, diffs,
exception bodies, snapshots and command output. Use fake providers and disposable stores for routine verification.
