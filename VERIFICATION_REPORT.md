# Final verification report — Hybrid CCXT + Heisenberg discovery branch

Branch: `feature/hybrid-ccxt-implementation`
Repository: `kvzsolt/Polymarket-Copy-Trading-Bot-Python`
Local repo: `/home/anon/Work/bots/Polymarket-Copy-Trading-Bot-Python`

## 1. Starting and ending SHA

- Starting SHA: `45663e8ce18bfd66f3ff4abef4710abe1016565e`
- Ending SHA:   `56e05c2ac77d38f05ca7f3dbde95e9d18b7e9544`
- Working tree: clean (no uncommitted changes)

## 2. Commits created (tests-first order preserved)

```
56e05c2 docs: add Heisenberg discovery operator docs, agent guidance, CI matrix
4a388da refactor: remove legacy Falcon funnel/scoring/longitudinal/backfill
1a08a48 feat: replace live discovery with Heisenberg-backed adapter
15a85d8 feat: add Heisenberg typed client and bankroll-aware discovery
196a68e test: define Heisenberg discovery contracts
```

The RED test commit (`196a68e`) contains no production implementation. Implementation
commits follow it. The red test commit was not squashed into the implementation.

## 3. Changed files

Added:
- `integrations/heisenberg/__init__.py`, `client.py`, `models.py`, `errors.py`
- `integrations/polymarket/ccxt_prediction_market_data.py` (read-only CCXT facade)
- `services/discovery/bankroll_compatibility.py`, `ccxt_capacity_probe.py`,
  `composite_candidate_service.py`, `discovery_models.py`, `discovery_runtime.py`,
  `heisenberg_candidate_service.py`, `heisenberg_discovery_service.py` (new adapter),
  `operator_cli.py`, `policy_config.py`, `ranking_report.py`
- `orchestrators/discovery_funds.py` (read-only bankroll resolver)
- `scripts/rank_copy_wallets.py` (operator ranking CLI)
- `tests/test_heisenberg_client.py`, `test_heisenberg_models.py`,
  `test_heisenberg_candidate_service.py`, `test_heisenberg_discovery_service.py`,
  `test_bankroll_compatibility.py`, `test_discovery_policy_validation.py`,
  `test_ccxt_prediction_market_data.py`
- `docs/discovery_bankroll_ranking.md`

Modified:
- `bootstrap.py` (wire `HeisenbergDiscoveryService` as the live discovery service)
- `app.py` (close optional `heisenberg_client` instead of removed `falcon_client`)
- `config/app_config.py` (add `HeisenbergDiscoveryConfig` + `HeisenbergBankrollConfig`)
- `config/trader_discovery.yaml` (add `discovery.heisenberg` policy section)
- `services/discovery/models.py` (add `DiscoveryRepository` Protocol; add `heisenberg`
  to `discovery_config_hash` decision sections)
- `services/discovery/__init__.py` (export new adapter; drop deleted funnel/scoring)
- `docs/trader_discovery.md` (rewrite for the new Heisenberg-backed pipeline)
- `HANDOFF_TESTING.md` (refresh to CCXT+Heisenberg phases)
- `AGENTS.md` (merge Heisenberg discovery guidance, no duplicate markers)
- `.github/workflows/ci.yml` (add `unit-ccxt` matrix lane)
- `tests/test_bootstrap.py` (assert `HeisenbergDiscoveryService` type)
- `tests/test_discovery_repo_contract.py` (import `DiscoveryRepository` from `models`)
- `tests/discovery/test_hysteresis.py` (docstrings reference new adapter protocol)

Deleted:
- `integrations/falcon/__init__.py`, `client.py`
- `services/discovery/funnel.py`, `scoring.py`, `longitudinal.py`, `backfill.py`
- `tests/discovery/test_falcon_client.py`, `test_funnel.py`, `test_longitudinal.py`

## 4. Bugs fixed

- CCXT side: the two pre-existing SDK gateway regressions (discarded fetched
  execution metadata; misleading average fill price) were already fixed on the
  branch before this session (commits `fed42e0`/`f240ca4`); this session did not
  touch them. The new `ccxt_prediction_market_data.py` facade is read-only and does
  not alter the execution gateway.
- Discovery side: the legacy Falcon/Heisenberg funnel is replaced by a
  bankroll-aware adapter that fails closed on a missing/non-positive bankroll,
  marks `last_cycle_degraded` on transport outage so the roster is not
  mass-demoted on missing evidence, and includes the Heisenberg policy in the
  decision-config hash so a policy change invalidates cached rescoring.

## 5. RED commands and intended failure summaries

RED commit `196a68e` — tests only, no production code. Focused run before any
implementation:

```
pytest tests/test_heisenberg_client.py tests/test_heisenberg_models.py \
  tests/test_heisenberg_candidate_service.py tests/test_bankroll_compatibility.py \
  tests/test_discovery_policy_validation.py tests/test_ccxt_prediction_market_data.py \
  tests/test_heisenberg_discovery_service.py -q
=> 7 errors during collection (exit 2)
   ModuleNotFoundError: integrations.heisenberg.*, services.discovery.heisenberg_candidate_service,
   services.discovery.discovery_models, services.discovery.bankroll_compatibility,
   integrations.polymarket.ccxt_prediction_market_data, services.discovery.heisenberg_discovery_service
```

All failures were caused by missing implementation modules — the intended reason.
No failures from broken imports, malformed fixtures, accidental network access, or
missing unrelated packages.

## 6. Focused GREEN results

CCXT focused (CCXT installed):
```
pytest tests/test_ccxt_precision.py tests/test_ccxt_prediction_client.py \
  tests/test_ccxt_prediction_gateway_contract.py tests/test_ccxt_prediction_gateway_errors.py \
  tests/test_ccxt_prediction_gateway_pagination.py tests/test_ccxt_prediction_market_data.py \
  tests/test_hybrid_polymarket_gateway.py tests/test_runtime_polymarket_gateway.py \
  tests/test_runtime_bootstrap_venue_backend.py tests/test_sdk_gateway_regressions.py \
  tests/test_ccxt_optional_dependency.py -q
=> 80 passed
```

Heisenberg/discovery focused:
```
pytest tests/test_heisenberg_client.py tests/test_heisenberg_models.py \
  tests/test_heisenberg_candidate_service.py tests/test_heisenberg_discovery_service.py \
  tests/test_bankroll_compatibility.py tests/test_discovery_policy_validation.py \
  tests/discovery tests/test_discovery_repo_contract.py -q
=> 78 passed
```

No unexpected skips; no warnings showing leaked secrets; no shadow write method
called; no test relied on network access.

## 7. Full-suite and optional-dependency matrix

Baseline (pre-migration, clean HEAD `45663e8`, CCXT installed):
```
pytest -q
=> 7 failed, 2129 passed, 3 skipped (22.92s)
```
The 7 failures are all `tests/test_config.py`, pre-existing, caused by the local
`secrets.env` containing live-mode credentials. Confirmed identical on a clean
stash of `AGENTS.md` — they are environmental, not branch/migration defects.

Matrix A — CCXT absent (`/tmp/venv-noccxt`, no `requirements.ccxt.txt`):
```
python -m venv /tmp/venv-noccxt && pip install -e ".[dev]"
pytest -q
=> 7 failed, 2149 passed, 2 skipped, 561 deselected (25.26s)
```
Same 7 pre-existing config failures. No CCXT-dependent test failed; default SDK +
Heisenberg discovery works with CCXT absent. CCXT is NOT a mandatory production
dependency.

Matrix B — CCXT installed (`.venv`, `ccxt==4.5.70`):
```
pytest -q
=> 7 failed, 2149 passed, 3 skipped, 556 deselected (22.72s)
```
Same 7 pre-existing config failures. Skips: obsolete py-clob-client-v2 factory
contract (x2) + `test_postgres_migration` (testcontainers not installed in `.venv`).

Both matrices: identical 2149 passed. CCXT presence changes only skip counts, not
pass/fail. The +20 passed over baseline (2129 -> 2149) are the new
Heisenberg/adapter/market_data tests. No migration-introduced failures.

Skipped tests and reasons:
- `tests/release_gate/test_postgres_migration.py` — `testcontainers` not installed
  in the local venvs; runs in CI (Docker available). Module-level `pytest.importorskip`.
- `tests/test_collateral_readiness.py` (x2) — obsolete py-clob-client-v2 factory
  contract; superseded by `test_polymarket_unified_sdk`.
- `tests/test_bootstrap.py` (7, async) — `_pg_container` testcontainer fixture
  `pytest.skip`s without Docker daemon access locally; runs in CI.

## 8. Lint / type / security results

ruff hard-fail (CI gating):
```
ruff check --select=F,E --ignore=E501,F401,F841,E402,E741,E731 --exclude=legacy,test.py,bootstrap.py .
=> All checks passed!
```
New/modified files re-checked: all clean.

ruff full scan (CI `continue-on-error: true`): 2767 pre-existing style errors,
unchanged by this work.

bandit (CI gating, medium+):
```
bandit -r services orchestrators clients config utils cache storage interfaces domain \
  integrations bot_cli app.py bootstrap.py -ll --skip B101,B311,B608,B108,B310
=> No issues identified (medium+). 40 Low (pre-existing, not gated).
```

mypy BLOCKING (money-path spine):
```
mypy --install-types --non-interactive --explicit-package-bases --ignore-missing-imports \
  --follow-imports=silent integrations/polymarket/clob_v2_gateway.py clients/network/http_client.py
=> Success: no issues found in 2 source files
```

mypy soft-fail (CI `continue-on-error`, services/orchestrators/domain): 89 errors,
ALL pre-existing in unrelated files (`rpc_fallback`, `executionService`,
`trade_pipeline`, `runtime_bootstrap`, `risk_monitor`, `wallet_signature_detector`).
Zero errors in any new file.

## 9. Composition-root wiring location

Venue backend (CCXT) — unchanged from the prior branch state:
`orchestrators/runtime_bootstrap.build_user_venue_backend` (called from
`app.py:1136`) -> `integrations.polymarket.runtime_gateway.build_runtime_polymarket_gateway`.
Default `sdk`; `sdk-shadow-ccxt` read-only; `ccxt` gated behind
`allow_experimental_ccxt_live`.

Discovery (Heisenberg) — new in this session:
`bootstrap.build_app` constructs `HeisenbergClient` (when
`discovery.heisenberg.enabled` and the API key is set) -> `HeisenbergCandidateService`
-> `HeisenbergDiscoveryService` (adapter exposing `run()`/`rescore()`/`config_hash`/
`last_cycle_degraded`), injected into `LeaderRegistry`. The funds callable reads the
first runtime user's funder-address USDC balance via the read-only
`utils.getMyBalance.get_my_balance` (on-chain `balanceOf`). The existing
`orchestrators/runtime_discovery.discovery_refresh_loop` and `app.py` startup/runtime
orchestration are unchanged — they already drive `registry.refresh`/`registry.rescore`
on the configured cadence and persist candidates to the existing DB schema via
`storage.postgres.discovery_repo.DiscoveryRepo`.

## 10. SDK remains production-default and custom WSS untouched

Confirmed. The official `polymarket-client` SDK remains the production execution
default (`clob_v2_gateway.ClobV2Gateway`). CCXT stays optional, lazy-loaded, behind
the `ClobGateway` boundary. Custom CLOB/WebSocket leader detection is untouched and
remains authoritative for latency-sensitive ingestion. No database migration was
introduced; the `Candidate` row shape, `DiscoveryRepo` contract, and DB tables
(`discovery_cycles`, `discovery_score_history`, leader roster) are unchanged.

## 11. No live write performed

Confirmed. No live order submission, cancellation, approval, redemption, split,
merge, or any other mutation was performed. The discovery path is read-only
(`get_my_balance` on-chain `balanceOf` + Heisenberg analytics reads). The CCXT
market-data facade is read-only. No live canary was run (requires separate explicit
operator authorization).

## 12. Remaining risks, skipped validations, and rollback

Unresolved risks:
- **Heisenberg API schema drift**: the typed client interprets the
  `narrative.agent.heisenberg.so` parameterized-agent response into typed
  snapshots. A schema change upstream could surface as missing evidence and
  fail-closed rejections. Mitigation: contract tests on upgrade; exact endpoint
  pin/retry policy in `HeisenbergClient`.
- **Single-funds global discovery**: multi-user mode ranks against the first
  runtime user's balance only. Documented in `orchestrators/discovery_funds.py`
  and `docs/trader_discovery.md`. Operators with heterogeneous per-user capital
  should note this.
- **rescore semantics changed**: the old funnel's rescore reused stored
  `base_score` + longitudinal history with decay-weighted recomputation; the new
  adapter's rescore re-sorts stored candidates by their persisted score with no
  recomputation (the Heisenberg service has no cached-rescore concept). This is a
  behavior change; hysteresis tenure is still not advanced on rescore.
- **CCXT batch gateway contract**: the bundle's `test_ccxt_prediction_gateway_extended.py`
  tested batch methods (`submit_limit_orders`, `cancel_orders`,
  `cancel_all_orders`, `submit_marketable_orders`) that are NOT part of the repo's
  `ClobGateway` domain contract and were intentionally removed from the repo's
  evolved gateway. That test was not added (would fail for the wrong reason —
  missing non-contract methods). Single-order FAK/FOK/limit/cancel behavior remains
  covered by `test_ccxt_prediction_gateway_contract.py` and
  `test_ccxt_prediction_gateway_errors.py`.

Skipped validations:
- **Live Heisenberg read-only checks**: `PMCOPY__DISCOVERY__HEISENBERG__API_KEY`
  was not set in this environment, so no live read-only provider call was made.
  Operator-gated; safe to run later without code changes.
- **Docker/testcontainer tests** (`test_bootstrap.py`, `test_storage.py`,
  release-gate postgres tests): Docker daemon not accessible locally; these
  skip locally and run in CI.

Rollback:
- Disable Heisenberg discovery without code changes: set
  `PMCOPY__DISCOVERY__HEISENBERG__ENABLED=false` (or unset the API key) and
  restart. The discovery cycle then fails closed (no candidates). NOTE: per the
  operator decision there is no legacy fallback — a disabled/missing key yields no
  candidates.
- Full rollback to the pre-session state: `git revert 196a68e..56e05c2` (or
  `git reset --hard 45663e8` on the local branch) and restart. No DB migration was
  introduced, so revert is a code revert + restart, not a data migration. The old
  Falcon funnel/scoring/longitudinal/backfill modules and their tests are in git
  history at `<=45663e8`.

## 13. Branch push status

The branch was NOT pushed and NO pull request was opened. All work is local on
`feature/hybrid-ccxt-implementation` (5 new commits ahead of `45663e8`), as the
operator did not explicitly request a push or PR.

## Real CCXT version tested

`ccxt==4.5.70` (from `requirements.ccxt.txt`), installed in the `.venv` used for
the CCXT-installed matrix.

## Live Heisenberg read-only checks ran?

No. `PMCOPY__DISCOVERY__HEISENBERG__API_KEY` not available in this environment;
provider contract is covered by mocked-fixture tests (`test_heisenberg_client.py`,
`test_heisenberg_candidate_service.py`, `test_bankroll_compatibility.py`,
`test_discovery_policy_validation.py`). A live read-only check is operator-gated.