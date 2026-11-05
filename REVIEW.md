# Polymarket-Copy-Trading-Bot-Python — pull request review policy

## Explicit Ignore Rules (Noise Suppression)

Review changed behavior and its reachable blast radius. Scope each finding to this revision, a changed hunk
and a caller that can exercise it.
Ship the standalone Python 3.13 pmcopy wheel, its paper or live venue runtime and account-isolated
multi-copier execution.

- Never comment on formatting, line length, whitespace, import sorting or trivial type-hint edits already
  enforced by CI.
- Ignore personal naming preferences, praise, summaries of the diff and alternative patterns without a
  concrete defect.
- Do not request a new interface, base class, factory or dependency just to implement a style preference.
- Suppress duplicate findings from linters and equivalent comments on multiple lines; attach the strongest
  counterexample to the owning line.
- For generated files review the producer, compatibility and provenance. Flag manual drift only when it
  changes behavior, breaks regeneration or invalidates a checked digest.
- Treat fixtures and tests as evidence. Flag an altered fixture when it hides a real failure, changes units,
  drops a tenant or masks an unknown outcome.
- Keep historical debt outside the diff unless this PR newly exposes it or increases its impact. Show that
  causal link.
- If a repository has no configured type checker or lint job, do not claim CI verified it. Report semantic
  type or contract failures with an input that triggers them.

**Where to follow a changed hunk**

| Boundary | Evidence to inspect |
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

## Priority 1 — Bot Performance & High-Pressure Failure Modes

- Flag blocking HTTP, sleep, database or filesystem calls and unbounded CPU work on an event loop. Name the
  task that stalls and identify whether the blocking client is really on that path.
- Flag missing connect, read, write or total network/RPC/IPC deadlines. Show how repeated retries outlive the
  caller’s budget.
- Flag a fresh client, socket, signer or connection pool created per event when reuse is safe. Check that
  lifecycle ownership closes it on cancellation.
- Flag queues with no positive cap, accumulating futures, uncapped dicts/caches, unbounded history and
  byte-unbounded payloads on continuous inputs.
- For saturation, check the producer policy and consumer recovery. Losing an accepted command differs from
  coalescing a recoverable latest-state update.
- Flag admission races, double reservations, lost updates, lock inversion and locks held over remote I/O.
  Describe the two interleavings and resulting state.
- Flag background tasks without ownership or exception handling, cancellation swallowed by blanket handlers
  and shutdown that loses accepted work.
- For retries, determine whether the original operation may have executed. A retry of an ambiguous write needs
  a durable, reconciled identity.
- For a hot-path allocation finding identify the object, its lifetime and event rate. Request a representative
  benchmark when impact is plausible but unmeasured.
- Flag repeated serialization, large buffer copies or regex/schema compilation per event when an existing
  validated structure or compiled instance is available.
- Check error-path cardinality and payload size before recommending more telemetry. Credentials and unredacted
  bodies must stay out of logs and evidence.
- Reject production Rust unwrap() and expect(), including rare error branches. Replace them with typed Result
  propagation or an explicit recoverable outcome.
- Flag unsafe Rust without documented ownership, lifetime and aliasing invariants. Include a failing boundary
  case or verification for the unsafe contract.
- When reviewing an actual Python/Rust FFI path, flag held-GIL CPU work, borrowed Python objects used after
  GIL release, redundant copying and unmapped Result failures.
- When the boundary is IPC, trace serialized units, framing, size limits, timeout, EOF and restart instead. A
  separate executable has no Python GIL to release.

**Repository-specific failure probes**

Apply only the probes reached by the diff. Use them to derive a minimal test case.

- **Change paper routing**
  Exercise BUY, SELL and cancel with a RecordingMarketGateway. Fail on the first private venue call.
- **Change arbitrary ticks**
  Try 0.005 and 0.0025 in both directions and preserve the caller's signed BUY ceiling or SELL floor.
- **Change local compatibility patches**
  Attach the same patch twice to one exchange and inspect a second exchange for leaked behavior.
- **Change v2 config parsing**
  Load distinct synthetic signers and funders, then configure v1 and v2 paths together. Fail the conflicting
  setup before starting users.
- **Change exposure reservation**
  Exhaust an explicit cap with concurrent requests or make confirmed exposure unreadable. Verify the cap fails
  closed in the funded account.
- **Change multi-copier recovery**
  Submit numerically equal intents for two copiers. Only private history from the owning account can advance
  its absence counter.
- **Change side matching**
  Supply equal numeric BUY quote and SELL share aggregates. Reject the cross-unit match in the recovery
  service.
- **Change queue saturation**
  Fully occupy leader A's notification queue while publishing to leader B. Verify B continues to dispatch and
  A coalesces.
- **Change shutdown**
  Make one venue client's close raise. Verify the second still closes exactly once and ambiguous holds stay
  unresolved.
- **Change structured logging**
  Insert bounded synthetic secret canaries into simulated provider errors and verify absence across all
  captured sinks.
- **Change database tests**
  Use a fresh disposable database per fixture on the shared PostgreSQL server. Verify account isolation and
  teardown after both scopes run.
- **Change distribution contents**
  Verify a fresh wheel and sdist can resolve SQL migrations and canonical YAML after changing package data.
- **Change scoring fallback**
  Simulate Falcon's successful empty list and an authorization failure. Check the first stays empty and the
  second fails closed.

## Priority 2 — OOP, SOLID, Reusability & Anti-Over-Engineering

- Flag domain code that initializes a provider, opens a socket, starts a database session or picks concrete
  secrets internally. Identify the existing composition boundary to use.
- Flag an interface that forces unrelated callers to depend on mutation privileges or transport-only types.
  Show the smaller behavior required by the caller.
- Flag changes that break substitution through altered return types, units, error classes, async behavior,
  ordering, cancellation or idempotency.
- Flag duplicated retry, rate-limit, sizing, normalization or lifecycle rules when equivalent callers now
  disagree or have to be patched in several places.
- Provide the stable extraction site and a shared contract test. Similar-looking math with different units or
  authorization scopes requires distinct behavior.
- Flag inheritance beyond three application levels. Recommend composition and show which responsibility can
  move without breaking substitution.
- Flag speculative factories, wrappers and ports with one implementation and no present boundary need. A real
  external dependency or a test seam can justify a narrow interface.
- In a latency-sensitive loop, isolate the added dispatch, copy or allocation. Criticism of indirection alone
  is insufficient.
- Treat cross-repo reuse as a versioned contract. Flag new imports into another service’s private storage, or
  copied generated schemas with no owner or regeneration path.

## Priority 3 — Clean Code, PEP 8 & Google Docstrings

- For changed Python public classes, abstract interfaces and reusable methods, flag missing or malformed
  Google-style docstrings. Name the missing contractual detail and supply the corrected docstring.
- Check Args, Returns and Raises where applicable. Include units, resource ownership, externally visible side
  effects and cancellation or performance caveats when they affect the caller.
- Require complete signature and attribute types. Report a meaningful ambiguity or unsafe Any at a boundary
  once. Leave mechanical fixes to the configured tool.
- Apply native documentation and type syntax to Rust and TypeScript. Review public contracts in the language
  that actually owns them.
- Flag swallowed exceptions, lost cancellation, blanket fallbacks that fabricate data, and loss of a typed
  error across a process/API boundary.
- Enforce PEP 8 naming and single-purpose methods where a deviation conceals ownership, control flow or units.
  Suppress cosmetic noise.
- Flag dead/commented code introduced by the diff when it obscures the active path. Replace new repeated bare
  thresholds or protocol literals with the named value owned by the contract.
- For schema or migration changes identify producer, persisted representation, client versions and transition
  order. Be precise about omitted versus null versus measured zero.

## Output Format

Publish actionable Critical, High or Medium findings only. Do not add a greeting, PR recap, praise,
low-severity notes or a quota of comments. If nothing qualifies, emit zero findings.

| Severity | Use it when |
| --- | --- |
| Critical | A credible path allows unauthorized execution or disclosure, irreversible financial corruption or systemic loss of accepted work. |
| High | A reachable race, outage, replay bias, duplicate effect, event-loop stall, resource leak or incompatible rollout materially breaks intended behavior. |
| Medium | A local contract, missing public documentation, failure-handling gap or duplicated policy has a concrete, actionable consequence. |

For each finding supply these fields in this order:

1. Severity and a short title stating the broken behavior.
2. Path, symbol and changed line. Link a second path only when it establishes reachability or a contract.
3. Trigger and consequence. Include the exact input, ordering or failure state. Quantify only with evidence.
4. A concrete, minimal replacement code snippet or diff that compiles in the target language and uses the
   local abstractions.
5. A regression assertion or reproduction that fails before the fix. Include its repository and runnable gate
   when available.

Prefer one finding for the root cause over separate comments for its symptoms. Do not invent a failing test
result or a benchmark. Remove credentials and user-identifying payloads from snippets.
