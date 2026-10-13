# Follow-up fix slice — CCXT 4.5.87 + everything actionable from the handoff

Plan date: 2026-10-13. Base: `refactor/standalone-pmcopy` @ `a407e76` (the completed 26-task standalone refactor; suite 5165/0 default, 542 integration, 187 release-gate, fixtures 25/25). Companion plan: `docs/plans/2026-10-11-standalone-refactor.md` (its Global Constraints bind this slice too).

## Global Constraints

Same binding rules as the 2026-10-11 plan (read it): CCXT-only transport; paper never private-writes; Decimal money; ambiguity safety; characterization-first TDD (RED for the intended reason → minimal fix → focused GREEN → full suite); no live trading; secrets never printed (runtime-built canaries only; read `secrets.env.example` at most); commits through the Mimosa gate WITHOUT bypass; **ZERO Docker container churn** — the integration tier uses the ONE shared `pmcopy-integration-pg` container exclusively (`pytest -m integration -q`); historical migrations never deleted.

Verified facts (2026-10-13):
- CCXT 4.5.87 (PyPI 2026-10-13, requires-python >=3.10): `ccxt/prediction/polymarket.py` is byte-identical to 4.5.85 → zero adapter-behavior change. Re-pins `multidict==6.9.1` (fixes open CVE-2026-104874; OSV affected 6.7.0–6.9.0) and loosens orjson to `>=3.11.9,<4`. Nothing else in our lock moves.
- Precision patch STAYS: 4.5.87 still hard-codes the tick table in `polymarket_order_raw_amounts` (verified in-wheel; local mirror /home/anon/Work/coding/ccxt lags PyPI ~6 weeks). Only the version-coverage note updates.
- Out of scope (stays documented in handoff §10): coincurve-on-3.14 wheels; web3 8 / rich 15 / websockets 16+ (ccxt/web3 pins); CI-on-GH-runners first-run (comes with the integration decision); live-trading/arm64 verification; CLAUDE.md git-tracking (operator choice). Exposure-cap TOCTOU and partial-fill remainder drift remain documented known-limitations — NO settlement redesign in this slice.

## Task 1: CCXT 4.5.87 upgrade + multidict CVE remediation

RED-first: update `tests/test_ccxt_dependency_sync.py` literals (==4.5.85 → ==4.5.87, four sites + lock/root alignment lists) and run it RED. Then: `pyproject.toml` ccxt pin + comment, `requirements.ccxt.txt` pin, `uv lock` regen (verify multidict 6.9.1 + orjson loosening land; confirm nothing else moved except what 4.5.87 pins: certifi/typing_extensions/cryptography/aiohttp-fast-zlib/zlib-ng/winloop per its requires_dist — record actual lock delta), regenerate `requirements.txt` + `requirements.runtime.txt` via `scripts/generate_requirements.py`, update the precision-patch version-coverage note (`src/pmcopy/integrations/polymarket/ccxt_compat/precision.py`) to "as of CCXT 4.5.87 (2026-10-13): byte-identical to 4.5.85", update README + AGENTS.md version mentions, reinstall venv (`uv pip install -r requirements.txt` + `-e . --no-deps`) + `pip check`. Gates: dependency-sync + drift script green; adapter contract group (10 files) green; full default suite green; `uv audit` (or pip-audit) re-run recording the multidict CVE as RESOLVED (if the audit tool can't reach its DB, record UNVERIFIED + the lock-level proof that multidict==6.9.1 > affected range). Update `docs/refactor/verification-matrix.md` audit rows + `docs/refactor/handoff.md` (CVE open → resolved with the ccxt-bump provenance). Single coherent commit (or RED commit + fix commit).

## Task 2: Money-path hardening batch (each item RED-first, own focused tests)

1. `_fills_consistent_with_submission` (`src/pmcopy/integrations/polymarket/ccxt_ambiguous_evidence.py:~425`): replace the final `return _close(total_size, amount)` fallthrough with explicit `elif unit is SHARES … / else return False` — any unit that is not exactly QUOTE or SHARES never matches (fail closed). RED: a synthetic third/None unit currently falls through to shares comparison.
2. `effective_intent_key` shim (`src/pmcopy/services/copyDecisionService.py:29-56`): add the `environment` parameter and forward it, aligning the shim's signature with the real derivation in `src/pmcopy/services/idempotency.py:39-51` (RED: calling the shim with environment= currently TypeErrors/ignores).
3. Quarantine-count throttle (`src/pmcopy/storage/postgres/orders_repo.py:844-856, 1653-1670`): the legacy-row quarantine count query (`fetchval`) + WARNING currently run on EVERY scoped unconfirmed call. Throttle: count at most once per TTL interval (inject a monotonic clock; default e.g. 60s) or on first sight per process; never block or alter the query RESULTS — pure observability throttling. RED: two calls within the window issue exactly one count query.
4. Per-binding `position_drift_service` mutation (`src/pmcopy/orchestrators/runtime_bootstrap.py:1392-1400`): replace the post-construction `coord.position_drift_service = drift_svc` per-binding mutation with construct-once injection in `build_user_state` (the T10 pattern: ExecutionCoordinator born with its account-owned drift service; binding code stops touching shared coordinator attributes; remove the PR7-era comment). Verify the drift service is genuinely account-owned (one per user) before injecting. RED: identity test — the coordinator's drift service is the build_user_state instance and no binding mutates it.
5. Pre-lease retention purge (`src/pmcopy/app.py:~1777-1790` region): move `purge_old_processed_trades` (and any sibling pre-lease DB writes in that block) below the lease-acquisition loop, mirroring the a407e76 evidence-drain fix; verify shutdown/no-op semantics for the not-run case. RED: source-order test (lease acquired before purge), mirroring the drain's pin.

Gates: focused files → T6-T9/T24 regression files (unit ones; the Postgres ones with `-m integration` via the SHARED container) → full default suite → golden fixtures --check 25/25.

## Task 3: Redaction/logging batch (RED-first; runtime-built canaries; fixtures stay 25/25)

1. Plural credential keys (`src/pmcopy/utils/log_redaction.py` classifier): `api_keys`, `secrets`, `passwords`, `keys` (and sensible plurals of the existing singular set) now classified as credential keys — with the SAME market-identity exemptions (`*_id`/`*_ids` untouched). RED: extras dict with `api_keys: [<canary>]` currently passes unredacted.
2. `redact_object` depth cap + cycle guard (`log_redaction.py:~313`): bounded recursion depth (e.g. 8) and a seen-id set for dict/list cycles; over-deep or cyclic structures render as a safe placeholder instead of RecursionError-then-swallow (current filter swallows → record passes UNREDACTED — that's the fail-open to close). RED: self-referencing extras dict currently either raises-silently or leaks canary.
3. Console `stack_info` path (`utils/logger.py`): stdlib appends `formatStack` output after the message in the console default path — run it through the text redactor (JsonFormatter ignores stack_info; verify unchanged). RED: `logger.error(..., stack_info=True)` with a canary in a frame currently leaks to console.
4. `pmcopy config print` list-element redaction (`bot_cli/commands/config.py`): list values currently skip `redact_text` — run string elements through value redaction (key context unnecessary; pattern redaction). RED: `tags: ["https://…/v2/<canary>"]` currently prints cleartext.

Gates: focused redaction test files + `tests/test_logger.py` + config-print tests → full suite → fixtures --check 25/25 (non-secret events byte-identical).

## Task 4: Config/loader strictness batch (RED-first)

1. `chain_id` bool guard (`src/pmcopy/services/multiUserConfig.py:~363`): `int(True) == 1` today — reject bool inputs explicitly. RED: `chain_id: true` currently loads as 1.
2. Private-key duplicate compare whitespace normalization (`multiUserConfig._reject_duplicate_users`): compare `strip()`ed values so whitespace-only differences don't defeat duplicate-wallet detection. RED: two keys differing by surrounding whitespace currently count as distinct wallets.
3. Legacy `POSTGRES_PASSWORD` in secrets.env: the loader currently inert-skips it (`config/app_config.py` `_read_secret_overrides` ~:154-163) — reject with a migration hint ("moved to db.env; see docs/docker.md") instead of silent tolerance. RED test: secrets env containing the legacy key → EnvValidationError naming db.env.
4. version_guard unreachable branch (`src/pmcopy/integrations/polymarket/ccxt_compat/version_guard.py`): delete the dead `if not release: return None`; strengthen the two tests that assert bare `RuntimeError` for prerelease cases to pin the prerelease reason (match on the "stable" message like the 4.5.7rc3 test does).
5. Doctor code-gate scan in installed mode (`bot_cli/commands/doctor.py:~162+`): the scan is vacuous when the source tree is absent — make it a real check in installed mode (verify the installed package's importability/integrity of the modules it claims to gate) or an honest explicit skip with the reason printed. RED: installed-mode doctor currently reports a pass for modules it never scanned.

Gates: focused config/loader/version/doctor tests → full suite.

## Task 5: Test-infra batch (zero container churn preserved — prove it)

1. Shared-fixture container identity validation (`tests/integration_postgres.py:~100-113`): an EXISTING `pmcopy-integration-pg` container must be verified to run the pinned image (and expected port) before reuse — mismatch → clear fixture error telling the operator to remove the stale container; not silent reuse. RED: fake a name-matched/different-image container → fixture errors.
2. Dockerfile secret-name scan (`tests/release_gate/test_dockerfile_safety_defaults.py:~96-104`): extend the forbidden-name loop from `--from=builder` stage crossings to ALL runtime-stage COPY/ADD lines (case-insensitive instruction match). Compose test (`tests/release_gate/test_compose_secret_split.py:~142-147`): match `COPY`/`ADD` case-insensitively. RED: a synthetic runtime-stage `COPY secrets.env /x` line currently passes both guards.
3. mypy ratchet composition (`scripts/` or ci.yml): the ratchet currently pins TOTAL=34 only — pin PER-FILE counts (a small manifest, e.g. a file in the repo listing file→count, checked exactly; total derived). RED: swapping one file's errors for another's currently passes.
4. PG temp-dir cleanup in `tests/multicopier/test_multicopier_postgres.py` `_config()` (`tempfile.mkdtemp` → cleaned fixture).
5. Hygiene: remove the dead `# noqa: BLE001` in tests/integration_postgres.py:209; fix the stale testcontainers prose in the two docstrings (tests/test_trade_pipeline_skip_visibility.py:10, tests/test_reservation_expiry_no_id_release_postgres.py:7).

Gates: focused test files → full default suite → FULL integration tier via the shared container with the same zero-churn proof (stable container id, 0 new containers) → release-gate tier.

## Task 6: Docs/truth pass + close-out

1. Fix the STALE "follow-level cap inert" wording: `docs/known_limitations.md` §6 (~:138-143) and `docs/refactor/handoff.md` §3 (~:316-317) still say the explicit copier cap is not enforced — it IS since `62458e9` (transaction-authoritative since `c497bf5`). Rewrite both to the true semantics (explicit cap enforced incl. in-flight reservations; TOCTOU window + partial-fill remainder drift remain documented limitations).
2. README: add the direct local wheel-install command (`python -m build` / `uv build` + `pip install dist/pmcopy-*.whl`, matching the verify_package battery).
3. `docs/rc1_validation.md`: correct the stale skip-caveat (~:128 claims the check script skips when PRIVATE_KEY/FUNDER_ADDRESS unset — it does not; align prose with `scripts/rc1_check.sh` actual behavior).
4. `tests/replay/`: add a README noting the six JSON fixtures are replay inputs with no collected tests yet (deliberate; future replay suite), so the exit-5 "no tests ran" in the verification matrix is explained at the source.
5. Dependabot h2 PR: attempt to close it via `gh pr close` (superseded by the canonical re-lock; the deletion manifest records this) — if `gh` is not authenticated, record the exact close instruction in the handoff instead.
6. Refresh `docs/refactor/verification-matrix.md` + `docs/refactor/handoff.md` rows affected by Tasks 1-5 (audit rows, ratchet description, fixture/doctor notes); append this slice to the ledger's completion record.
7. Final controller-verified runs (record in matrix): default suite, integration via shared container, release-gate, golden fixtures --check.

## Verification gates (all tasks)

Focused tests → applicable regression files (integration ones via `-m integration` + shared container only) → full default suite → golden fixtures `--check` 25/25 where logging is touched → Mimosa gate passes normally on every commit → ledger entries per task.
