# SELF_REVIEW — PR1: stabilize `develop` (integration + deep-research audit fixes)

## 1. Summary

PR1 integrates the one live open PR (#122) and implements the **P0/P1 config and
safety** findings from `deep-research-report.md`, cross-checked against the
*current* develop code (which had evolved well past the report's uploaded
snapshot). Scope is deliberately the **stabilization slice**; the two large
greenfield modules (onchain detection, strategy profiler) are deferred to PR2/PR3
per the agreed staged-delivery plan.

Headline outcomes:
- **Risk pre-reconcile P0 resolved** on both axes: the config-parse crash
  (feature-aware `TRAILING_ACTIVATION_PCT` parser) and the runtime 5 s timeout
  (PR #122 async REST + develop's existing wait_for/backoff machinery).
- **AppConfig env-drift closed**: 4 keys promoted to typed config; schema-coverage
  + duplicate-key lint added; only `PYTHONUNBUFFERED` allowlisted.
- **Neg Risk CTF Exchange V2** typed and wired into the collateral allowance check.
- **REST-primary execution verified WSS-independent** (no production change needed).
- **Reconciliation invariants verified** already-enforced; fail-safe defaults pinned.
- **P0 config smoke gate** added to CI (dotenv env files → `load_config`).

## 2. PRs integrated
- **PR #122** `fix/risk-pre-reconcile-async-io` — merged `--no-ff` (async REST venue
  order-status/fills so risk pre-reconcile overlaps without threads).

## 3. Branches integrated / deferred
Only PR #122 was live. All other branches the original inventory listed had been
**deleted from the remote** by the owner before this run (the `--prune` cleaned the
stale local refs). See the matrix in §15.

## 4. Conflicts resolved
None. PR #122 merged cleanly (no file overlap with the integration branch at merge
time). The `main` WIP (a fail-closed parser attempt) was preserved on
`wip/config-parser-main`; its leading-`#` detection was folded into the
feature-aware parser, and its fail-closed `test_comment_only_value_rejected` was
intentionally **superseded** by the Phase-5 disabled-OK contract.

## 5. Files changed (functional)
`config/app_config.py`, `orchestrators/trade_pipeline.py`,
`services/executionService.py`, `services/collateral_readiness.py`,
`utils/createClobClient.py`, `config/runtime.local.example`,
`.github/workflows/ci.yml`, `greptile.json`, plus tests
(`test_config.py`, `test_env_schema_coverage.py`, `test_config_smoke.py`,
`test_market_data_rest_primary_execution.py`, `test_collateral_readiness.py`,
`test_book_quality_gate.py`) and docs (`docs/branch_merge_matrix.md`,
`docs/audit/*`).

## 6. Tests added
~25 new assertions across: Phase-5 trailing parser matrix (6), env-schema
coverage + dup-key lint (2), config smoke (2), REST-primary/WSS-down (3),
Neg Risk collateral coverage (2 + assertions), both-exchange config (2),
order-confirmation safe-default guard (1).

## 7. Commands run & results
- `ruff check --select=F,E ...` on every changed file → **all checks passed**.
- Targeted suites (config, collateral, market-data, gateway, book-gate) → **green**.
- Full unit tier (`pytest -m "not integration and not release_gate"`):
  **baseline 8 failed / 2465 passed** before changes; after changes the same 8
  pre-existing failures remain (release-doc *existence* tests:
  `test_issue103_rollback_script`, `test_rc1_release_artifacts` — unrelated to this
  PR) and the new tests pass. Final run: **8 failed / 2482 passed / 1 skipped**
  (same 8 pre-existing doc-existence failures; +17 new passing tests).
  **Net: no new failures.**

## 8. Known limitations
- `config/runtime.env` and `config/runtime.local.env` are **gitignored**
  (operator-supplied). The smoke + schema-coverage tests therefore degrade to the
  tracked `runtime.local.example` in CI; the full 195-key sweep runs only where the
  operator files exist. Recommend committing a sanitized `runtime.env.example`
  (follow-up).
- The pre-existing 8 release-doc failures are left as-is (out of audit scope).
- The Neg-Risk collateral check is fail-closed; operators must approve pUSD for
  **both** exchanges before live trading (documented in `runtime.local.example`).
- `(tx_hash, log_index)` mined-fill dedupe + pending→mined reconcile are **deferred
  to PR2** (the `OrderFilled` detector is what produces `log_index`).

## 9. External docs consulted
- **Firecrawl** → `docs.polymarket.com/resources/contracts`: confirmed Neg Risk CTF
  Exchange V2 = `0xe2222d279d744050d28e00520010520000310F59` (matches
  `config/addresses.py`).

## 10. Security review
- **Secrets not logged**: no new code logs `PRIVATE_KEY`/CLOB/RPC secrets; collateral
  errors continue to route through `redact_error_detail`. `config.raw` strips
  sensitive keys (unchanged). `git grep "PRIVATE_KEY="` / `CLOB_SECRET=` show only
  the gitignored env files + tests, never source.
- **Env mutation safe**: no `.env` writes added; strategy/env tuning (PR3) is
  dry-run-by-default by design.
- **Pending detection provisional**: not implemented in PR1; PR2 design note marks it
  provisional.
- **Final dedupe `(tx_hash, log_index)`**: deferred to PR2 (documented).
- **Fail-closed master switch preserved**: `LIVE_TRADING` doctor-or-die behaviour is
  unchanged — see the contradiction note in §14.

## 11. Reconciliation review
- **No TTL-only release**: `reconciliation_service._maybe_auto_release_ambiguous`
  uses TTL/grace only to *trigger* probes; release requires all probes clean.
- **Probes-clean gate**: probe errors (`> max_probe_errors`, default 0) or
  inconclusive (`require_all_probes_clean`, default True) → reservation held.
- **Duplicate submits prevented**: atomic submission lock → exactly-one submission
  per logical fill (existing, tested).
- New guard test pins the fail-safe defaults. Full notes: `docs/audit/reconcile_state_machine_notes.md`.

## 12. Rollback plan
Each concern is an isolated commit on top of develop; `git revert <sha>` removes any
one without touching the others. The whole PR can be dropped by not merging the
integration branch — develop is untouched. PR #122 can be independently reverted
(its own merge commit). No migrations, no schema changes, no env-file rewrites.

## 13. Self-diff audit (`git diff develop...HEAD`)
Checked for: raw `os.environ` in new production paths (none — all via typed config),
broad exception swallowing (none added), missing awaits / fire-and-forget tasks
(none added), websocket loops without backoff (none added), secret-ish log values
(none), duplicate order paths (none), config keys without schema coverage (covered
by `test_env_schema_coverage`), pending-as-authoritative (n/a), branch-conflict
leftovers (none), duplicate env keys (lint added; deduped). All ruff-format churn
from the editor hook was reverted so the diff is functional-only.

## 14. Deep research compliance checklist

| Deep research finding | Implementation status | Files changed | Tests added | Notes |
|---|---|---|---|---|
| BUG-1 `TRAILING_ACTIVATION_PCT` comment-only crash | implemented | config/app_config.py | test_config (trailing matrix) | develop partly handled it; hardened + feature-aware |
| BUG-2 disabled feature fatal-parses its knob | implemented | config/app_config.py | test_config | core fix: `optional_float_or_symbol`, flag read first |
| BUG-3 `auto` smuggled as float | implemented | config/app_config.py | test_config | explicit `trailing_activation_mode` Literal |
| BUG-5 AppConfig env drift (typed vs raw) | implemented | app_config.py, trade_pipeline.py, executionService.py | test_env_schema_coverage | 4 keys typed; `PYTHONUNBUFFERED` allowlisted |
| BUG-6 REST-primary blocks on WSS | not applicable (already correct) | — | test_market_data_rest_primary_execution | verified; "contradiction" was in gitignored runtime.env |
| BUG-7 missing Neg Risk exchange | implemented | app_config.py, collateral_readiness.py, createClobClient.py, runtime.local.example | test_config, test_collateral_readiness | typed field + dual-exchange allowance |
| BUG-8 ambiguous reservation release | implemented (verified) | — | test_config (defaults guard) | already enforced; pinned defaults; full notes in docs/audit |
| BUG-9 duplicate env keys | implemented | (runtime.local.env — gitignored) | test_env_schema_coverage (dup lint) | deduped locally; lint guards future |
| BUG-10 parser fails on 3.13 / CI matrix | not applicable | — | — | CI already on py3.11/3.12 |
| Config smoke test (P0) | implemented | .github/workflows/ci.yml | test_config_smoke | dotenv → load_config; CI-safe |
| Early onchain detection (Phase 10) | deferred to PR2 | — | — | design note: docs/audit/pending_detection_notes.md |
| Strategy profiler / env tuner (Phase 11) | deferred to PR3 | — | — | design note: docs/audit/strategy_profiler_notes.md |
| `(tx_hash, log_index)` mined-fill dedupe | deferred to PR2 | — | — | `log_index` produced by the OrderFilled detector |

## 15. Branch and PR integration matrix

| Branch/PR | Action | Conflicts | Resolution | Tests run | Final status |
|---|---|---|---|---|---|
| PR #122 `fix/risk-pre-reconcile-async-io` | merge `--no-ff` | none | clean merge | 63 gateway tests | integrated |
| PR #121 `fix/...executor-isolation` | none | n/a | superseded by #122; deleted upstream | — | documented |
| PR #116 `fix/pr115-review-fixes` | none | n/a | closed + deleted by owner; edits removed subsystem; not resurrected | — | documented (rpc/slippage fixes flagged as follow-up) |
| `feature/concurrency-hardening` | none | n/a | reaper already on develop; deleted upstream | — | documented |
| `chore/remove-stale-legacy-test` | none | n/a | legacy test already absent on develop; deleted upstream | — | documented |
| local WIP on `main` | preserve → fold | n/a | committed to `wip/config-parser-main`; leading-# folded; fail-closed test superseded | test_config | documented |

### LIVE_TRADING contradiction (deep research vs repo)
The report recommends *replacing* `LIVE_TRADING` with a typed dry-run/live mode
(fail-soft). The repo's owner-confirmed decision is a **fail-closed
`LIVE_TRADING` master switch** (doctor-or-die). Per the "prefer the more specific,
repo-grounded instruction" rule, **the fail-closed switch is kept**; `LIVE_TRADING`
is already a typed field driving `paper_trade_only`/`dry_run`, so schema coverage
passes without changing its semantics.
