@AGENTS.md

# Polymarket-Copy-Trading-Bot-Python — execution protocol

## 1. Inheritance

The import on line one loads the architecture and test contract. Use this file for search order, delegation,
execution and handoff.
Before editing, resolve the path-specific agent instructions at the files being changed. Respect existing
worktrees and uncommitted user changes.
Record the starting branch and revision. When a report mentions production, record the manifest pin separately
and investigate the right revision.

```sh
git status --short --branch
git rev-parse --show-toplevel
git rev-parse HEAD
```

Ship the standalone Python 3.13 pmcopy wheel, its paper or live venue runtime and account-isolated
multi-copier execution.

## 2. Context Window & Sub-Agent Delegation

**Initial reading order**

1. Open `pyproject.toml` and locate the entry point relevant to the task.
2. Trace the first caller that can observe the requested behavior. Write down the units, identity and error
   outcome at that boundary.
3. Open the nearest existing regression and the matching CI step. Load one representative fixture or one
   example response on demand.
4. Only widen the search after you have a candidate caller, producer or consumer that the first pass could not
   explain.

Additional source anchors for this checkout:

- `.github/workflows/ci.yml`
- `pytest.ini`
- `src/pmcopy/services/copierConfig.py`
- `src/pmcopy/services/execution/reservation_service.py`
- `tests/multicopier/test_queue_saturation.py`
- `tests/test_paper_execution_gateway.py`

Use tracked-file search for an initial inventory. Apply these exclusions to broad searches; open a specific
fixture or generated contract only when the test requires it.

```sh
rg --files \
  -g '!.git/**' \
  -g '!.venv/**' \
  -g '!target/**' \
  -g '!build/**' \
  -g '!node_modules/**' \
  -g '!dist/**' \
  -g '!__pycache__/**' \
  -g '!.pytest_cache/**' \
  -g '!.mypy_cache/**' \
  -g '!.ruff_cache/**' \
  -g '!tests/replay/fixtures/**'
```

Keep secrets and runtime data outside agent context. Do not delegate reading an environment file or querying a
live account when a scrubbed fixture proves the behavior.

**Parallel investigation**

- Use the Task/Agent sub-agent facility for multi-repository searches and broad impact analysis. Assign
  disjoint questions to workers. Keep a linear local change on the direct path.
- If the client has no delegation facility, run the same scoped read passes sequentially and state that
  limitation. The contract checks still apply.
- Give each worker a repo, revision, small starting path set, exact question, expected evidence and a stop
  condition. Read-only is the default for discovery.
- Split work by producer, consumer and verification. Give each editable path one owner. One coordinator owns
  the cross-repo contract decision and integration order.
- Have each worker return a path and symbol, the contract it observed, a failure scenario and one suggested
  test. Request short evidence excerpts instead of whole files.
- Keep raw payloads, giant fixture dumps and complete dependency graphs out of the shared context. Retain a
  pointer and hash when identity matters.
- Resolve conflicting reports at the actual code boundary and selected revision. Source descriptions and
  generated examples can drift.
- Before compaction record the last verified revision, the exact red test, touched paths, unresolved
  assumptions and the next runnable command.

## 3. Plan-First Protocol

Write a short impact plan before editing a shared abstraction, reusable method, wire schema, authorization
boundary or hot path. Keep it in the conversation unless the task asks for a design artifact.

1. State the observable change and the counterexample that is wrong today.
2. Name the owning symbol, upstream producer, downstream consumers and the targeted revision or release
   cohort.
3. Identify the smallest change, the state it can invalidate and what the old consumer will see during a
   rolling update.
4. Name the exact test, failure injection and data needed to prove the change. On hot paths include a
   representative workload and baseline.
5. Choose the compatibility, retry and rollback order before the first mutation that depends on them.

**Route the investigation by change**

- **Change paper routing**
  Ask the verification owner to demonstrate this case: Exercise BUY, SELL and cancel with a
  RecordingMarketGateway. Fail on the first private venue call.
- **Change arbitrary ticks**
  Ask the verification owner to demonstrate this case: Try 0.005 and 0.0025 in both directions and preserve
  the caller's signed BUY ceiling or SELL floor.
- **Change local compatibility patches**
  Ask the verification owner to demonstrate this case: Attach the same patch twice to one exchange and inspect
  a second exchange for leaked behavior.
- **Change v2 config parsing**
  Ask the verification owner to demonstrate this case: Load distinct synthetic signers and funders, then
  configure v1 and v2 paths together. Fail the conflicting setup before starting users.
- **Change exposure reservation**
  Ask the verification owner to demonstrate this case: Exhaust an explicit cap with concurrent requests or
  make confirmed exposure unreadable. Verify the cap fails closed in the funded account.
- **Change multi-copier recovery**
  Ask the verification owner to demonstrate this case: Submit numerically equal intents for two copiers. Only
  private history from the owning account can advance its absence counter.
- **Change side matching**
  Ask the verification owner to demonstrate this case: Supply equal numeric BUY quote and SELL share
  aggregates. Reject the cross-unit match in the recovery service.
- **Change queue saturation**
  Ask the verification owner to demonstrate this case: Fully occupy leader A's notification queue while
  publishing to leader B. Verify B continues to dispatch and A coalesces.
- **Change shutdown**
  Ask the verification owner to demonstrate this case: Make one venue client's close raise. Verify the second
  still closes exactly once and ambiguous holds stay unresolved.
- **Change structured logging**
  Ask the verification owner to demonstrate this case: Insert bounded synthetic secret canaries into simulated
  provider errors and verify absence across all captured sinks.
- **Change database tests**
  Ask the verification owner to demonstrate this case: Use a fresh disposable database per fixture on the
  shared PostgreSQL server. Verify account isolation and teardown after both scopes run.
- **Change distribution contents**
  Ask the verification owner to demonstrate this case: Verify a fresh wheel and sdist can resolve SQL
  migrations and canonical YAML after changing package data.
- **Change scoring fallback**
  Ask the verification owner to demonstrate this case: Simulate Falcon's successful empty list and an
  authorization failure. Check the first stays empty and the second fails closed.

Keep plan scope stable. If a dependency or data source differs from the assumption, revise just the affected
step and communicate what changed.

## 4. Mandatory Verification Loop

Use the exact checks and toolchains in AGENTS.md. Run commands in the owning checkout. An absent configured
tool is a capability gap, not a successful check.

1. Reproduce a failure using a real boundary and a synthetic input. Record the assertion and why it
   distinguishes the regression.
2. Implement one coherent change. Check the focused test before broad formatting, regenerating artifacts or
   switching branches.
3. Run the targeted linter, type checker and unit test for the changed scope. All configured gates must exit
   with zero errors before declaring completion.
4. Run the relevant contract, integration or native checks and the required full tier. Preserve test counts,
   deselections and prerequisites.
5. Exercise the error and restart path as well as the happy path. For an externally visible change check
   actual producer and consumer versions.
6. Compare any hot-path measurement to the same dataset, concurrency and machine baseline. Preserve tail
   latency, queue high-water mark, allocation counts or bytes as appropriate.
7. Review the final diff. Confirm no new private data appeared in fixtures, logs, snapshots or rendered pages,
   and no operational command ran as a side effect.

```sh
git diff --check
git status --short
git diff --stat
```

**Evidence to record**

- Commit or base SHA and the affected component, including a separate consumer SHA when a contract changes.
- Exact command, working directory and interpreter/tool version if it influences the result.
- The smallest failing example before the change and its result after the change.
- Which failing external dependency was replaced by a fake and which contract the fake actually covers.
- Which CI job covers the remaining environment-only tier and whether its status was observed.
- Any locally unavailable check and the concrete dependency needed to run it. Label existing failures
  separately from new regressions.
- The remaining operational decision, if any. An offline test result does not establish a real delivery,
  production deployment or live fill.
