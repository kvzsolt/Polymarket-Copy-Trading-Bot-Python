# RC1 Validation Checklist

This document is the operator-facing RC1 release-criteria checklist. It
mirrors what `scripts/rc1_check.sh` enforces at CI time and what an
operator should run manually before any release-candidate-1 deployment
flip. The goal: a single page that an operator can run through, top to
bottom, and conclude "RC1 ready" or "RC1 blocked".

## 1. Definition of RC1

RC1 is the first release candidate: production code reaches its final
scoped feature set, the typed-config schema is locked, the doctor
live-readiness gate is green, and the forbid scans catch every
legacy / unguarded code pattern. RC1 is *not* the first production
release — it is the candidate that an operator validates before cutting
production.

The validation contract below is enforced by `scripts/rc1_check.sh`, which:

1. Runs `python -m compileall` over the production Python tree.
2. Runs `python -m pytest -q --tb=line`.
3. Runs the forbid scans: legacy `services.price_cache`,
   `PositionPriceCache`, `MarketMetaService`; plus the unguarded
   `execution_service.execute` alias scan (the single permitted call site
   is `services/execution/coordinator.py`, which is deny-list-exempt
   because it carries the `execution_coordinator_missing` guard).
4. Runs the doctor live readiness gate (`doctor --live`) when `PRIVATE_KEY`
   and `FUNDER_ADDRESS` are exported.

## 2. Manual checklist (operator runs these top to bottom)

### Phase 1 — Bytecode verification
```bash
python -m compileall -q app.py bot_cli clients orchestrators services cache domain integrations interfaces storage utils
```
**Expected:** no output, exit 0. Syntax errors will surface here before
`pytest` does. Catches a partial edit that imports cleanly but doesn't
byte-compile (rare, but a single mis-indented block can cost you 30+
minutes in a build pipeline).

### Phase 2 — Test suite
```bash
python -m pytest -q --tb=line
```
**Expected:** green for the non-integration, non-release-gate tier.
**Note:** the full suite may include integration tests gated on real
secrets; in operator-only runs those tier marks are filtered out.

### Phase 3 — Forbid scans
The production Python source must NOT contain:

- `services.price_cache` — legacy cache symbol that was replaced in
  the PR2 strategy-profiling cycle; the forbid scan catches any
  lingering implementation.
- `PositionPriceCache` — same provenance as above; the forbid scan
  catches the legacy typed cache.
- `MarketMetaService` — same provenance; the forbid scan catches the
  legacy meta-service types.

Run:

```bash
archs=(app.py bot_cli clients orchestrators services cache domain integrations interfaces storage utils config)
for substr in services.price_cache PositionPriceCache MarketMetaService; do
    if grep -RIn --include='*.py' "$substr" "${archs[@]}" >/dev/null 2>&1; then
        echo "FAIL forbidden substring still present: $substr"
        grep -RIn --include='*.py' "$substr" "${archs[@]}" | head -5
    else
        echo "ok $substr"
    fi
done
```

Plus the **unguarded execution_service.execute scan**: outside
`services/execution/coordinator.py`, any production file containing
`execution_service.execute`, `getattr(execution_service, "execute"`,
or `getattr(execution_service, 'execute'` MUST also contain the literal
`execution_coordinator_missing` token (the guard presence string). The
automated scan verifies this for the operator; manual grep checks should
also see the same.

### Phase 4 — Doctor live readiness gate
```bash
python -m bot_cli.main doctor --live
```
**Expected:** structured doctor result with `status=ok`. The doctor
blocks on: zero pUSD balance, zero CTF exchange allowance, zero Neg
Risk exchange allowance (since PR1, the Neg Risk exchange V2 is part
of the readiness contract; see `services/collateral_readiness.py`),
RPC unavailability, WSS unreachable when the WSS mode is the active
source, or an unresolved reconciliation wedge. The doctor's non-zero
exit code is non-gradient — it returns blocked, period.

### Phase 5 — Operator confirm
Resume from RC1 validation per the live runbook
(`docs/live_runbook.md` §3).

## 3. Regression contract — files that must not regress

The following source artifacts are pinned by the test suite but deserve
explicit operator review because their absence breaks the RC1 release:

- `scripts/rc1_check.sh` — must exist + be executable + run all 4 phases.
- `scripts/rollback.sh` — must exist + be executable + parse `--dry-run` and
  `--canary` flags (separate test path).
- `docs/live_runbook.md`, `docs/emergency_shutdown.md`,
  `docs/recovery_playbook.md`, `docs/known_limitations.md`,
  `docs/rc1_validation.md`, `docs/runbooks/rollback.md` — must exist.
- The doctor module (`bot_cli/commands/doctor.py`) and the
  `_run_doctor_checks` entry must remain coroutine-shaped and the
  live-mode invocation must short-circuit before `build_app` (the
  startup ordering test in `tests/test_doctor_app_startup.py` pins this
  ordering).

## 4. Known limitations to communicate before release

- See `docs/known_limitations.md` for the full list — particularly
  http polling (the gamma API latency floor), the single
  `execute_intent` entry-point invariant, and the external integration
  contract.
- The Neg Risk exchange V2 readiness gate is fail-closed: operators must
  approve pUSD for BOTH the CTF and the Neg Risk exchanges before live
  trading, even if the leader wallet only ever trades on one of them.
  See `config/runtime.local.example` for the documented operator step.

## 5. Caveats

- If `PRIVATE_KEY` / `FUNDER_ADDRESS` are not exported, the rc1_check.sh
  **skips** phase 4 (doctor live) and logs "skipped" — this is
  intentional and the script still completes the other checks. Operators
  must run phase 4 manually before flipping `LIVE_TRADING_ENABLED`.
- The forbid scans cover production Python only; test code may reference
  legacy tokens (intentionally — used in documenting them).
- `pytest` here is the full default tier; CI additionally runs release-
  gate markers separately (`.github/workflows/ci.yml`).

## 6. Related docs

- `docs/live_runbook.md` — daily live-trading operator workflow.
- `docs/runbooks/rollback.md` — rollback procedure invoked after a
  failed RC1 gate.
- `docs/audit/findings_table.md` — deep-research audit findings matrix.