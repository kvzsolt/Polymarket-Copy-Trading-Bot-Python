# SELF_REVIEW — PR1: stabilize `develop` (integration + deep-research audit fixes)

# SELF_REVIEW — PR1 + PR2 + PR3: stabilize `develop` (integration + full deep-research audit)

## 1. Summary

This review covers the **full deep-research audit** delivered in three staged
PRs on the `integration/merge-all-prs-and-audit-fixes` branch:

- **PR1 (#123)** — integrated the one live open PR (#122) + P0/P1 config and
  safety fixes (trailing parser, AppConfig env-drift, Neg Risk collateral,
  REST-primary WSS-independence verification, reconciliation probe-clean
  release verification, P0 config smoke gate). See the PR1 sections below.
- **PR2 (this run)** — early on-chain trader detection
  (`services/onchain_detection/` + `clients/onchain_ws.py` +
  `orchestrators/onchain_detection_runtime.py`) on both the CTF Exchange V2
  and the Neg Risk CTF Exchange V2, plus the Phase 9 `(tx_hash, log_index)`
  mined-fill dedupe hardening and the state-machine extension to cover the
  existing-but-unmapped lifecycle states. Pending detection is flag-gated
  (default off); mined detection is always safe.
- **PR3 (this run)** — strategy profiler + env tuner
  (`services/strategy_profile/` + `models/env_patch.py` +
  `models/strategy_profile.py` + two `bot_cli` commands). Fresh Polymarket
  Data API fetch, 17 features, 6 archetypes, dry-run-by-default env-patch
  writer with a protected-key allowlist.

Every deferred deep-research item is now **implemented**. See the compliance
checklist in §14 for the per-finding mapping.

## 1a. PR2 + PR3 headline outcomes

- **Phase 10 on-chain detection**: `DetectionConfidence` ladder
  (`market_unattributed` / `pending_probable` / `mined_authoritative` /
  `confirmed_final`); `matchOrders` calldata decoder (selector `0x6d0fcd6e`,
  verified V2 `Order` struct); `OrderFilled` log decoder (topic0
  `0xd543...d8ee`, verified event); pending-tx detector via Alchemy
  `alchemy_pendingTransactions` (both exchanges, `hashesOnly=false`);
  runtime dispatcher routes mined signals to `execute_trades` and gates
  pending signals behind `ENABLE_ONCHAIN_PENDING_DETECTION`.
- **Phase 9 dedupe hardening**: `MinedFillDeduplicator` keys mined fills on
  `(tx_hash, log_index)` so one settlement tx with multiple fill logs
  produces one logical event per log; pending signals are superseded by the
  mined signal that shares their `tx_hash` (no double dispatch).
  `services/execution/state_machine.py` `ALLOWED_TRANSITIONS` extended to
  cover `ORDER_LIVE`, `SUBMITTED_DELAYED`, `MATCHED_PENDING_CONFIRMATION`,
  `TRADE_MINED`, `TRADE_CONFIRMED`, `TRADE_RETRYING`, `AMBIGUOUS_TIMEOUT`,
  `AUTO_RELEASED_AMBIGUOUS`; `AUTO_RELEASED_AMBIGUOUS` + `TRADE_CONFIRMED`
  marked terminal; ambiguous cannot jump directly to live (probe-clean
  release only).
- **Phase 11 strategy profiler**: 17 features from fresh Polymarket Data API
  fetch; 6 `StrategyArchetype` values; archetype -> `EnvPatch` list using the
  report's example patches; dedicated `EnvPatchWriter` (atomic `.bak` +
  tmp-then-replace; protected-key allowlist never mutated; dry-run default;
  `--apply` required; writes only `runtime.local.env`; secret values
  redacted in the explanation). CLI: `bot.py profile-leader` and
  `bot.py tune-strategy`.

## 1. PR1 Summary

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
- **PR2 — proxy/safe wallet attribution**: the on-chain detectors match on
  `Order.maker` (the source-of-funds address) per the prompt. Polymarket proxy
  and Gnosis-Safe wallets where the EOA `signer` differs from `maker` are not
  attributed to the EOA. Future work: resolve signer->proxy/safe ownership via
  `PolyProxyLib`/`PolySafeLib` address derivation.
- **PR2 — mined-signal condition_id enrichment**: the mined `OrderFilled` log
  carries `tokenId` but not `conditionId`. The runtime resolves condition_id
  via an injectable `condition_id_resolver` (Gamma API
  `/markets?clob_token_ids={tokenId}` in production). If the resolver is absent
  or fails, `condition_id` is empty and `normalize_trade_signal` drops the
  signal — acceptable for an inert-by-default feature, but the resolver must
  be wired before enabling live copy on the mined path.
- **PR2 — decimal scaling**: `makerAmountFilled`/`takerAmountFilled` are scaled
  by 1e6 (USDC.e / CTF token decimals) in the detector. This is verified in
  unit tests against synthetic logs; a known-good mainnet fill should be
  replayed in a follow-up to confirm against a real `OrderFilled` before
  enabling live copy.
- **PR3 — feature degradation**: features that the Polymarket Data API does not
  expose (e.g. maker/taker attribution on `/activity` rows) degrade to `None`;
  the classifier treats `None` as "unknown" and falls back conservatively.

## 9. External docs consulted
- **Firecrawl** → `docs.polymarket.com/resources/contracts`: confirmed Neg Risk CTF
  Exchange V2 = `0xe2222d279d744050d28e00520010520000310F59` (matches
  `config/addresses.py`).

## 10. Security review
- **Secrets not logged**: no new code logs `PRIVATE_KEY`/CLOB/RPC secrets; collateral
  errors continue to route through `redact_error_detail`. `config.raw` strips
  sensitive keys (unchanged). `git grep "PRIVATE_KEY="` / `CLOB_SECRET=` show only
  the gitignored env files + tests, never source. PR3's `EnvPatchWriter` redacts
  protected-key old values in the explanation and rejects protected-key patches.
- **Env mutation safe**: PR3's `EnvPatchWriter` is dry-run by default; `--apply`
  required; writes only `runtime.local.env`; protected-key allowlist (exact +
  substring) never mutated; `LEADER_WALLETS` refused unless
  `--allow-leader-rewrite`; atomic `.bak` + tmp-then-replace.
- **Pending detection provisional**: PR2's `DetectionConfidence.PENDING_PROBABLE`
  signals are marked `provisional=True`; the dispatcher only routes them when
  `ENABLE_ONCHAIN_PENDING_DETECTION=true`; a mined signal with the same
  `tx_hash` supersedes the pending (no double dispatch).
- **Final dedupe `(tx_hash, log_index)`**: PR2's `MinedFillDeduplicator` keys
  mined fills on the composite `(tx_hash, log_index)`; one settlement tx with
  multiple fill logs produces one logical event per log.
- **Fail-closed master switch preserved**: `LIVE_TRADING` doctor-or-die behaviour is
  unchanged — see the contradiction note in §14.

## 11. Reconciliation review
- **No TTL-only release**: `reconciliation_service._maybe_auto_release_ambiguous`
  uses TTL/grace only to *trigger* probes; release requires all probes clean.
- **Probes-clean gate**: probe errors (`> max_probe_errors`, default 0) or
  inconclusive (`require_all_probes_clean`, default True) → reservation held.
- **Duplicate submits prevented**: atomic submission lock → exactly-one submission
  per logical fill (existing, tested).
- **State machine extended (PR2)**: `ALLOWED_TRANSITIONS` now covers every
  non-terminal `OrderLifecycleStatus` value; `AMBIGUOUS_TIMEOUT` can only
  release to `AUTO_RELEASED_AMBIGUOUS` / `TRADE_CONFIRMED` /
  `TERMINAL_FAILED` / `RECONCILIATION_REQUIRED` (never directly to a
  non-terminal live state). `AUTO_RELEASED_AMBIGUOUS` + `TRADE_CONFIRMED`
  are terminal. Pinned by `test_reconciliation_state_machine_phase9.py`.
- **(tx_hash, log_index) dedupe (PR2)**: `MinedFillDeduplicator` ensures a
  single settlement tx with multiple fill logs produces one logical event
  per log; the runtime's mined handler drops duplicates and supersedes the
  pending signal for the same tx.
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
| Early onchain detection (Phase 10) | implemented (PR2) | services/onchain_detection/*, clients/onchain_ws.py, orchestrators/onchain_detection_runtime.py, config/app_config.py | test_calldata_decoder, test_order_filled_detector, test_pending_tx_detector, test_onchain_both_exchanges, test_onchain_detection_runtime | mined OrderFilled sub always safe; pending sub gated by ENABLE_ONCHAIN_PENDING_DETECTION (default off); both exchanges covered; (tx_hash, log_index) dedupe |
| Strategy profiler / env tuner (Phase 11) | implemented (PR3) | services/strategy_profile/*, models/env_patch.py, models/strategy_profile.py, bot_cli/commands/profile_leader.py, bot_cli/commands/tune_strategy.py | test_env_patch_writer, test_strategy_profiler_features, test_strategy_classifier, test_strategy_env_tuner, test_leader_history_loader, test_cli_profile_tune | fresh Polymarket Data API fetch; 17 features; 6 archetypes; dry-run default; protected keys never mutated |
| `(tx_hash, log_index)` mined-fill dedupe | implemented (PR2) | services/onchain_detection/mined_fill_deduplicator.py, orchestrators/onchain_detection_runtime.py, services/execution/state_machine.py | test_mined_fill_deduplicator, test_reconciliation_state_machine_phase9, test_onchain_detection_runtime | composite key; same-tx multi-log each counted once; pending superseded by mined; state-machine extended to cover existing-but-unmapped lifecycle states |

## 15. Branch and PR integration matrix

| Branch/PR | Action | Conflicts | Resolution | Tests run | Final status |
|---|---|---|---|---|---|
| PR #122 `fix/risk-pre-reconcile-async-io` | merge `--no-ff` | none | clean merge | 63 gateway tests | integrated |
| PR #121 `fix/...executor-isolation` | none | n/a | superseded by #122; deleted upstream | — | documented |
| PR #116 `fix/pr115-review-fixes` | none | n/a | closed + deleted by owner; edits removed subsystem; not resurrected | — | documented (rpc/slippage fixes flagged as follow-up) |
| `feature/concurrency-hardening` | none | n/a | reaper already on develop; deleted upstream | — | documented |
| `chore/remove-stale-legacy-test` | none | n/a | legacy test already absent on develop; deleted upstream | — | documented |
| local WIP on `main` | preserve → fold | n/a | committed to `wip/config-parser-main`; leading-# folded; fail-closed test superseded | test_config | documented |
| PR2 `feature/onchain-detection` (this run) | new branch off integration | n/a | new module; no conflict with existing paths | 32 PR2 tests + state-machine regression | implemented on integration branch |
| PR3 `feature/strategy-profiler` (this run) | new branch off integration | n/a | new module; no conflict with existing paths | 51 PR3 tests | implemented on integration branch |

### LIVE_TRADING contradiction (deep research vs repo)
The report recommends *replacing* `LIVE_TRADING` with a typed dry-run/live mode
(fail-soft). The repo's owner-confirmed decision is a **fail-closed
`LIVE_TRADING` master switch** (doctor-or-die). Per the "prefer the more specific,
repo-grounded instruction" rule, **the fail-closed switch is kept**; `LIVE_TRADING`
is already a typed field driving `paper_trade_only`/`dry_run`, so schema coverage
passes without changing its semantics.
