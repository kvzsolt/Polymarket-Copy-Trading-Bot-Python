# Full Repository Audit — Polymarket Copy-Trading Bot

**Repository:** `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`
**Branch:** `fix/develop-runtime-hardening` @ `0d01cec`
**Working tree:** 2 uncommitted files (`services/execution/intent_recovery.py`, `tests/test_issue70_reservations_intent_lifecycle.py`) — the audit judges the **working tree**; the HEAD-vs-worktree delta is flagged where it matters (DDC-01).
**Date:** 2026-06-13
**Method:** 12 module-batch audits (B1–B12) + 4 cross-cutting deep dives (DD-A concurrency, DD-B money-math, DD-C crash-recovery, DD-D Polymarket API contract via Context7) + adversarial verification of P0/P1 candidates. ~103k LOC, 414 Python files. Polymarket API claims verified against `/polymarket/py-clob-client-v2` and `/llmstxt/polymarket_llms-full_txt` (Context7) and the installed SDK source.

---

## Executive Summary

The codebase is **architecturally strong and unusually disciplined** on the things that normally sink trading bots: money math is `Decimal`-clean end-to-end (DD-B confirms float is confined to telemetry and the one sanctioned SDK serialization adapter), canonical `PositionKey(user, condition_id, token_id)` identity is enforced, the CLOB SDK is properly isolated behind a gateway protocol, per-scope `asyncio` locks + DB advisory locks + idempotency keys serialize the common in-process paths, and there is a large (2,288-test) suite with a release-gate tier. The hardening work recorded in recent commits is real and visible.

**However, the bot is NOT production-ready, and live trading must remain blocked.** The audit found **5 confirmed P0 issues**, all on the settlement / crash-recovery / live-gate axis — exactly the seams where a copy-trading bot loses money silently:

1. **Reconciler double-applies fills after a crash or a swallowed DB error** (P0-1). Settlement spans three independent transactions; the de-dup watermark is written last, wrapped in `except Exception: pass`, and the fills unique index keys on a wall-clock timestamp, so a re-apply in a later second is not caught. Confirmed independently by 4 agents + adversarial verification.
2. **The live-trading gate fails open.** `PAPER_TRADE_ONLY` (and `dry_run`) default to `False`, there is no master "enable live" switch, and the per-leader tier resolver defaults to `live_follow` on missing/error/unknown (P0-2). A bare or compose-overridden run with valid credentials trades live with no affirmative opt-in. (Docker image + compose set `true`, which mitigates container deploys only.)
3. **A transient market-data API failure at boot deletes the entire local position ledger** (P0-3): the fetch helper collapses failure into `[]`, and startup prune treats "no live positions" as "delete all persisted positions."
4. **Startup-reconcile failure does not stop trading** (P0-4): `reconciler_ok=False` is recorded but no monitor reads it; the bot trades on an un-reconciled snapshot.
5. **Startup order-status normalization deletes venue-accepted orders before reconciliation can see them** (P0-5): `normalize` purges `pending` rows (no venue lookup) and runs *before* `startup_reconcile_unconfirmed`, so an order that was accepted by the venue but not yet locally recorded becomes an untracked off-bot position.

Plus a confirmed duplicate-order path on HTTP-5xx-after-accept (the wrapper classifies venue errors by string, not status code), a manual-sync vs reconciler lost-update race, several fail-open risk/exit gaps, and a release gate whose two most important invariants (concurrent duplicate suppression, crash-recovery no-double-fill) are **skipped or AST-only** — i.e. the gate that is supposed to authorize live trading does not actually test the non-negotiables.

**Highest-priority fixes (in order):** (0) commit the pending `intent_recovery` `await` fix — at HEAD, DB-backed restart recovery is a silent no-op; (1) make reconciler fill-application idempotent on a stable key + atomic with the watermark; (2) default the live gate closed; (3) fail-closed on boot position-fetch failure; (4) gate trading on reconcile success; (5) reconcile before purge.

**Final risk rating: 🔴 RED — unsafe for production.** Justification at the end.

---

## Repository Architecture Summary

**Purpose.** Monitors "leader" wallets on Polymarket, scores their trades, applies portfolio/risk gates, and mirrors entries/exits through the Polymarket CLOB V2 venue, with protective stops and reconciliation-owned settlement. Async Python 3.11 (3.13 unsupported), PostgreSQL via `asyncpg`, `py-clob-client-v2==1.0.0`.

**Entrypoints.** `bot.py` → `bot_cli` (operator CLI: doctor/config/reservations/drift/rotate-traders/reconcile/orders). `bootstrap.py` is the pure composition root (no import-time I/O). `app.py` (1,711 lines) is the async runtime: loads config → builds the shared graph → per-user wiring (`orchestrators/runtime_bootstrap.py`) → `normalize_startup_order_statuses` → `recover_intents_from_db` → `startup_reconcile_unconfirmed` → manual sync → drift check → market/user WSS → spawns ~8 supervised loops (leader monitor, discovery, trade pipeline, reconciliation, market WSS, user WSS, risk monitor, health server).

**Core trading path.** leader signal (`services/tradeMonitor.py`) → copy decision (`services/copyDecisionService.py` + `services/copyStrategy.py`) → sizing (`services/order_sizing.py`) → `orchestrators/trade_pipeline.py` (2,875 lines, the largest file) → reservation + submission through `services/execution/coordinator.py` → `services/executionService.py` (1,562 lines) → `integrations/polymarket/clob_v2_gateway.py` (gateway protocol, `interfaces/clob_gateway.py`). Settlement is owned solely by `services/execution/reconciliation_service.py` (1,983 lines), which applies incremental matched-size deltas and releases reservations. State is persisted in `storage/postgres/*` (orders, reservations, positions, fills, risk-blocks repos).

**Market data.** `services/market_data_service.py` is the sole authority; REST snapshot is the default primary source (PR10.4) with WSS (`clients/market_ws.py`, `clients/user_ws.py`) secondary; fail-closed `StaleDataError` on stale/missing data.

**Config.** Typed `AppConfig` from `config/app_config.py` (1,499 lines) with `config/env.py` as a legacy parallel parser; `secrets.env` overrides only an allowlisted credential set (`config/runtime_env.py`).

**Runtime.** `Dockerfile` (python:3.11-slim, non-root `botuser`, `PAPER_TRADE_ONLY=true` baked, healthcheck `/health/ready:9102`), `docker-compose.yml` (postgres:16-alpine). CI (`.github/workflows/ci.yml`): unit matrix 3.11/3.12, release-gate job, lint-type-import job, docker-smoke.

**Legacy/dead surface (confirmed by B10).** `services/reconciliation.py` (legacy, CLI-only), `services/position_action_coordinator.py` (dead shim), `utils/idempotency.py` IO helpers (no prod caller), `test.py` at repo root (stale WSS-client copy, not a test), `services/accountingService.py` (wired but never invoked), `services/marketStructureService.py` and `services/backtest.py` (no runtime caller), `services/intentRegistry.py` (written only by startup recovery, never finalized in steady state).

---

## Commands Run

| Command | Result |
|---|---|
| `git rev-parse HEAD` | `0d01cecf2decb32e86612b8b577b43543fc75904` |
| `git status --short` | 2 modified: `intent_recovery.py`, `test_issue70_reservations_intent_lifecycle.py` |
| `python --version` | 3.11.14 |
| `pip freeze` (key) | aiohttp 3.13.5, asyncpg 0.31.0, web3 7.14.1, websockets 15.0.1, py-clob-client-v2 1.0.0, pytest 8.4.2 |
| `pytest -m "not integration and not release_gate"` | **2286 passed, 2 skipped, 205 deselected, 14m39s** (1 unclosed aiohttp session warning at teardown) |
| `python -m compileall …` | exit 0 (all packages compile) |
| `python scripts/check_requirements_drift.py` | OK — requirements.txt covers pyproject deps |
| `ruff check --select F,E …` (CI-hard parity) | **All checks passed** |
| `ruff check --select F401,F841,E402` (incl. bootstrap.py) | 48 errors (CI blind spot; unused imports/vars + module-level import placement) |
| `ruff check --select ASYNC,DTZ,B,RUF006,PLE` | 31 (incl. ASYNC250 blocking `input()` in rotation, RUF006 dangling tasks in test.py) |
| `ruff check --select S` (bandit substitute, prod) | 62 (40 S110 try-except-pass, 9 S608 SQL [all param-safe false positives], 8 S112, 3 S105, 2 S311) |
| `mypy` (scoped money path, `--explicit-package-bases`) | 110 errors in 16 files (CI runs mypy non-blocking `|| true`; 155 over services+orchestrators+domain) |
| `bandit` | not installed locally; substituted ruff `S` rules |

Targeted greps: 0 `time.sleep` in prod, 0 `requests.` lib, 0 unbounded `asyncio.Queue()`, 0 naive `datetime.now()/utcnow()` in services/storage/domain, 0 `.acquire()` outside `async with`, 0 live `near_one` references (module deleted; 6 remaining are comments). `services/idempotency.py` vs `utils/idempotency.py` diverged by design (effective-order key vs leader-trade key).

---

## P0 Findings

> Severity is judged for **live mode**; `PAPER_TRADE_ONLY` is recorded as a *mitigating control*, not a discount below P1. P0 = wrong/duplicate/oversized real orders, ledger corruption, broken crash-recovery, bypassable live gate, lost positions, secrets leak.

### P0-1 — Reconciler double-applies fills after crash or swallowed error (settlement is 3 non-atomic transactions; dedup keyed on wall-clock)

- **Severity:** P0 — position-ledger corruption and double-recorded realized PnL on the settlement path, reachable by a process crash **or** a single transient/​swallowed DB error (no crash required). Confirmed by B2-01, B3-01, DD-A (DDA-01), DD-C (DDC-03), and adversarial verification.
- **Location:** `services/execution/reconciliation_service.py:1354` (append_fill), `:1386` (apply_fill), `:1481-1488` (watermark write, `except Exception: pass`), read at `:956-968` (`_previous_matched_for_order` → `Decimal(0)` on read error); `fill_ts=int(time.time())` `:1365`; dedup index `storage/postgres/migrations.py:83-84` `(user_id, order_id, condition_id, side, fill_ts)`; SELL parity `:1717/:1819`.
- **Evidence:**
  ```python
  # reconcile_trade_record — three independent transactions, no enclosing conn.transaction():
  1354  await self.fills_repo.append_fill(...)              # tx1 (own pool conn, commits)
  1365      fill_ts=int(time.time()),                       # wall-clock → dedup key varies per second
  1386  await self.positions_repo.apply_fill(..., delta)    # tx2 (own conn, commits the ledger delta)
  1481  try:
  1482      await self.orders_repo.update_submission_status(order_id, status, response_patch=fill_info)  # tx3 = the ONLY watermark write
  1487  except Exception:
  1488      pass                                            # de-dup watermark advance is best-effort
  ```
- **Trigger:** Order in `submitted_unconfirmed` with venue cumulative matched `M`. Pass N reads `previous_matched` from `orders.response_json`, computes `delta = M − previous`, commits the fill row, commits `apply_fill` (+delta to positions). Then the process is killed (OOM/SIGKILL/deploy) **or** `update_submission_status` raises and is swallowed at `:1488`. Pass N+1 (any of the 4 reconcile drivers: WSS-wake, risk loop ~15s, execute_trades preamble, manual-sync) reads the stale watermark, recomputes the same positive delta, and `apply_fill` adds it again. The fills `ON CONFLICT DO NOTHING` does **not** save it because `fill_ts` is a fresh wall-clock second → different unique key → second fill row inserted. BUY inflates the position; SELL double-records realized PnL. Reachable in paper and live (same ledger code); the live consequence is corrupted exposure feeding sizing and protective exits.
- **Recommended fix:** Make fill application idempotent on a **stable** key (`order_id` + cumulative-matched watermark, or venue trade id) instead of wall-clock `fill_ts`, and write the matched watermark in the **same transaction** as `apply_fill` (the atomic primitive `fills_repo.append_fill_and_apply` at `fills_repo.py:96` already exists and is used by submitter paths — route the reconciler through a connection-aware variant that also advances the watermark). Remove the `except Exception: pass` so a failed watermark write surfaces. Gate `apply_fill` on whether `append_fill` actually inserted (its return id is currently ignored).
- **Effort:** Medium–High (new connection-aware atomic repo method + dedup-key migration). **Must not be rushed** — this is the most safety-critical file.
- **Confidence:** High.
- **Regression test:** Mock `orders_repo.update_submission_status` to raise after `positions_repo.apply_fill` succeeds; re-invoke `reconcile_trade_record` with unchanged `current_matched`; assert `apply_fill` runs exactly once / position size applied once. Restart variant: persist position at size N with stale `response_json` matched=0, reconcile with venue matched=N twice across a 1-second boundary, assert final size N (not 2N) and one fill row.

### P0-2 — Live-trading gate fails open: `PAPER_TRADE_ONLY`/`dry_run` default False, no master switch, leader tier defaults `live_follow`

- **Severity:** P0 — "fail-closed when config is missing" (CLAUDE.md non-negotiable) is violated; a deploy without the env, or with a compose/env-file override, submits real orders with no affirmative opt-in. Confirmed by B1-05, B8-04, B11-01, and direct verification.
- **Location:** `config/app_config.py:1127` (`paper_trade_only=p.bool_("PAPER_TRADE_ONLY", False)`), `:1488` (`dry_run=p.bool_("PAPER_TRADE_ONLY", False)`); `services/execution/coordinator.py:90` (`is_live_mode`); `orchestrators/trade_pipeline.py:818-827` (`_resolve_leader_tier` returns `live_follow` on no-repo/exception/unknown); no `ENABLE_LIVE`/`LIVE_TRADING` switch exists (grep empty).
- **Evidence:**
  ```python
  paper_trade_only=p.bool_("PAPER_TRADE_ONLY", False),   # absent/malformed → False → LIVE
  dry_run=p.bool_("PAPER_TRADE_ONLY", False),            # same var, dual-bound
  # _resolve_leader_tier: return "live_follow" on (no repo | except | unknown tier)
  ```
  `bool_` recognizes only `1/true/yes` as true; `KILL_SWITCH=on`, `ENABLE_RISK_MGMT=enabled` silently parse to the (unsafe) default (B11-03). Risk-exit dispatch (`services/risk/risk_monitor.py:711-773`) and `ExecutionService.execute` have **no** independent paper guard (B1-05, B8-04, B7-04) — so a paper soak with `ENABLE_RISK_MGMT=true` on a wallet holding real positions fires real protective SELLs.
- **Trigger:** Operator sets credentials, omits `PAPER_TRADE_ONLY` (or `docker run --env-file` carries `PAPER_TRADE_ONLY=false`, B12-04), leader-tier table empty/unreachable → real BUY/SELL. Mitigated for the standard container path (Dockerfile `ENV PAPER_TRADE_ONLY=true`, compose `"true"`) but **not** for bare `python app.py`, `bot run`, tests, or env-file overrides.
- **Recommended fix:** Invert to fail-closed: default `paper_trade_only=True` (and derive `dry_run` from it, single source); require an explicit affirmative `LIVE_TRADING_ENABLED=true` to go live; make `_resolve_leader_tier` fall back to `watchlist`/`paper_follow`, never `live_follow`; add a terminal paper guard inside the gateway submit methods (single bypass-proof choke point, B7-04); validate boolean tokens and reject unknown ones.
- **Effort:** Low (the change), Medium (test fallout — many tests construct config without the env and assume live).
- **Confidence:** High.
- **Regression test:** `load_config({...valid creds, no PAPER_TRADE_ONLY...})` → assert `is_live_mode(cfg) is False`; `_resolve_leader_tier` with no repo → non-live tier; gateway `submit_marketable_order` in paper → refuses with a distinct status.

### P0-3 — Transient boot market-data failure wipes the entire local position ledger

- **Severity:** P0 — a single transient data-API error at startup deletes every persisted position, destroying the basis for protective exits, SELL mirroring, and exposure caps; the resulting "unhealthy" report does not stop the bot. Confirmed by B4-02.
- **Location:** `orchestrators/runtime_bootstrap.py:218-225` (`fetch_live_positions` swallows all errors → `return []`), `orchestrators/startup_health.py:210-239` (prune deletes persisted rows whose keys aren't in `live`), `app.py:1275-1304` (unhealthy report only logged, startup proceeds).
- **Evidence:**
  ```python
  async def fetch_live_positions(wallet):
      try: return await fetch_data(...)
      except Exception: return []            # API outage == "no positions"
  # reconcile_persisted_positions_to_live: for record not in live_keys: delete_position(...)
  ```
  `fetch_data` raises after 4 retries (`utils/fetchData.py:54-69`); a 429/5xx/timeout/non-list body all collapse to `[]`, and the prune has no empty-live guard. Post-wipe, `startup_drift_check` sees no local positions → `drift_ok=True`, neutralizing the downstream safety net.
- **Trigger:** data-API 429/outage during restart (the boot fetch burst is unguarded, unlike the snapshot path's `_guarded_leader_fetch`) → all rows for the user deleted → bot trades with an empty book while the wallet still holds tokens.
- **Recommended fix:** Distinguish failure (`None`) from genuinely-empty (`[]`) in `fetch_live_positions`; skip the prune (and mark the check `error`, fail-closed in live) when `live` is empty while `persisted` is non-empty.
- **Effort:** Low. **Confidence:** High.
- **Regression test:** persisted positions present + `fetch_live_positions` raises → assert no `delete_position` call and startup health = error (live) / warning (paper).

### P0-4 — Startup-reconcile failure does not gate trading (`reconciler_ok=False` is recorded, never read)

- **Severity:** P0 — the bot trades on an un-reconciled post-restart snapshot for users whose startup reconciliation failed (the exact double-fill exposure the code's own comment claims to gate). Confirmed by B4-01, DD-C (DDC-04).
- **Location:** `app.py:1107-1119` (sets `reconciler_ok=False`), `app.py:1572-1585` (monitors spawned unconditionally); only readers of `reconciler_ok` are `services/healthServer.py:14-23` (HTTP readiness display). The implemented gate `_wallets_blocked_by_reconcile_failures` (app.py:1416-1431) lives only on the dynamic-discovery path, which is dead (`on_new_leaders=None`, app.py:1484 — B4-11).
- **Evidence:**
  ```python
  if reconcile_failures:
      application.readiness.update(reconciler_ok=False)   # comment: "monitors will see this and stay closed"
  # ...startup continues; run_leader_monitor / risk loop never read reconciler_ok.
  ```
- **Trigger:** venue order-status endpoint flaky at boot → per-user reconcile appends to `failures_out` → bot proceeds; an unconfirmed BUY from the prior session that actually filled is invisible; next leader signal re-buys; protective state stale.
- **Recommended fix:** filter `monitored_leaders` and risk contexts through `_wallets_blocked_by_reconcile_failures` before spawn (the helper already exists), or `return 1` when `reconcile_failures` is non-empty.
- **Effort:** Low. **Confidence:** High.
- **Regression test:** force `startup_reconcile_unconfirmed` to record a failure → assert no `run_leader_monitor` task is created for the affected leader.

### P0-5 — Startup normalization deletes venue-accepted `pending` orders before reconciliation can see them

- **Severity:** P0 — an order accepted by the venue but not yet locally recorded (crash/ambiguity between POST and save) is deleted at boot with no venue lookup, and the fill lands as an untracked off-bot position. Confirmed by B4-06, DD-C (DDC-02).
- **Location:** ordering: `app.py:1022` (`normalize_startup_order_statuses`) runs and commits **before** `app.py:1098` (`startup_reconcile_unconfirmed`); purge SQL `orchestrators/startup_health.py:261-303` (UPDATE pending/pending_lock/delayed → `terminal_failed`, then DELETE, no venue probe); reconcile visibility filter `storage/postgres/orders_repo.py:827-828` (`order_id IS NOT NULL`).
- **Evidence:**
  ```sql
  UPDATE orders SET status='terminal_failed' WHERE status IN ('pending','pending_lock','delayed') ...;
  DELETE FROM orders WHERE ... status IN ('failed','submission_failed','created','retryable_failed','terminal_failed');
  ```
  The only POST site (`executionService.py:1071`) saves `submitted_unconfirmed` *after* the wire send; a kill between POST and save leaves the row at the pre-POST `pending`, which `normalize` deletes. `reconcile_submitted_unconfirmed` queries only `order_id IS NOT NULL AND status = ANY(_UNCONFIRMED_STATES)` — a deleted row is invisible.
- **Trigger:** crash after POST reaches venue but before local save, then cold start.
- **Recommended fix:** run `startup_reconcile_unconfirmed` *before* the purge, and restrict the DELETE to `order_id IS NULL` rows the venue confirms absent (probe by client-order-id / idempotency key). Cross-references the FAK-repost no-pending-row gap (B1-03).
- **Effort:** Medium. **Confidence:** High.
- **Regression test:** seed a `pending` row with no order_id but a venue that returns a live/filled order for its client-order-id; run the boot sequence; assert the row is reconciled (position created), not deleted.

---

## P1 Findings

> Selected from B1–B12 + deep dives; deduped by (mechanism, primary file). Full per-batch detail retained in the appendix log.

| ID | Title | Location | Why it matters |
|---|---|---|---|
| **P1-a** | 5xx-after-accept → duplicate order (wrapper classifies venue errors by string, not status_code) | `utils/createClobClient.py:630-647`; `services/executionService.py:1069-1199` | **Verified**: `post_signed_order` catches the SDK exception and returns a dict whose `is_ambiguous` set lacks 5xx tokens → `success=False, ambiguous=False, non_retryable=False` → retry loop re-signs (fresh ms timestamp) and re-POSTs a duplicate funded order. Borderline P0; needs the venue to both accept and return 5xx. Fix: classify by `status_code in {500,502,503,504}` or `None` → route to `submitted_unconfirmed` break. (B1-01/B7-01/DDD A3) |
| **P1-b** | Manual-sync absolute SET races reconciler `apply_fill` (disjoint locks) | `services/manual_sync_service.py:547-575,654-675`; `storage/postgres/positions_repo.py:207-235` (no lock) vs `:272-340` (advisory lock + FOR UPDATE) | New-row double-count is guarded by a 45s seed holdoff, but the **existing-row** UPDATE path has no holdoff and no shared lock: a reconciler partial-fill committing inside manual-sync's read→write gap is clobbered (lost update). Fix: route the absolute SET through the same `pg_advisory_xact_lock("position:…")` + FOR UPDATE. (B10-01/DDA-02) |
| **P1-c** | BUY copy decision, sizing, and drift/edge/near-resolution gates run on the **stale leader signal price**, not the fresh quote | `orchestrators/trade_pipeline.py:1744,1941`; `services/tradeScoringService.py:92-110` | `live_price = float(signal.price)` makes `price_drift_pct`/`edge_decay_pct` identically 0 and sizes BUYs off the old price; the protective drift gate is inert on every BUY. The fresh bundle is used for the order price but never for the decision. Fix: derive `live_price` from `bundle.quote.best_ask`. (B5-01) |
| **P1-d** | WSS liveness anchored on **outbound** PING masks a dead feed | `clients/market_ws.py:250-253`; `cache/market_state_store.py:1039-1058`; `services/market_data_service.py:720-737` | `wss_connection_age = now − max(last_heartbeat, last_pong)`; self-sent PING refreshes `last_heartbeat` every 5s, so a silent/half-open feed is served as "fresh" until the in-loop pong detector closes it (~10–15s), defeating the 2s book-age budget. Fix: anchor liveness on **inbound** activity only. (B6-01) |
| **P1-e** | Trailing stop / post-profit-lock remainder cannot exit below entry | `services/positionManager.py:602` (`cp <= trail_trigger and cp > pos.entry_price`) | After profit-lock arms the trailing stop, a reversal straight through entry never triggers (the `cp > entry` clause suppresses it); only the opt-in, default-off absolute stop-loss can catch it. Classic "gave back all gains and then some" hole. Fix: drop the `cp > entry` clause. (B8-03) |
| **P1-f** | `positionActionCoordinator._waiter_counts` leaks on cancellation → scope lock never pruned | `services/positionActionCoordinator.py:114-123`; trigger `services/risk/risk_monitor.py:1872` | Counter incremented before `await lock`, decremented only after acquisition; the risk loop's `wait_for` timeout cancels while blocked → counter stranded → `release_scope`/`prune` refuse to drop the scope forever (unbounded registry growth + permanent "busy"). Fix: decrement in a `finally` around the wait. (B8-02/DDA-03) |
| **P1-g** | Submission lock is a persisted `pending_lock` row with no runtime auto-release | `storage/postgres/orders_repo.py:165-195`; cleanup only at startup/CLI | Unlike the reservation/position advisory locks (`pg_advisory_xact_lock`, auto-release on crash), a task that dies holding the row-lock blocks that idempotency key — including a re-attempted protective exit — until the next full boot. Fix: periodic `resolve_stale_submission_locks(apply=True)` or a stale-TTL takeover in the `ON CONFLICT`. (B3-04) |
| **P1-h** | Migration creates a role-keyed unique index **before** the column it references | `storage/postgres/migrations.py:502-504` (index) vs `:510-511` (ALTER ADD COLUMN), one transaction | On a DB created before `wallet_role` existed, the index statement raises undefined-column → the single migration transaction aborts → bot cannot start. The code's own comment says the ALTER "MUST run before" the index. Fix: reorder. (B3-05) |
| **P1-i** | Empty / disjoint active-leader set leaves open copied positions **unmonitored** for protective exit | `orchestrators/runtime_bootstrap.py:1029-1050`; `services/risk/risk_monitor.py:1757-1761` | Risk contexts are built only from `active_leaders`; if rotation/discovery yields an empty set (or a multi-user follow no longer intersects active leaders) while positions are open, the risk monitor returns early and stop-losses never run. Fix: build risk-only contexts from users holding open positions, independent of active-leader follows. (B9-01/B9-02) |
| **P1-j** | Geoblock is fail-open by omission (boot-only, opt-in, never re-checked, not on the trade path) | `services/compliance.py` (module fail-closed); callers `app.py:825-829`, `bot_cli/.../doctor.py:466` | `check_geoblock` itself is correctly fail-closed, but it runs once at startup only when `enforce_geoblock` is set, and its 15-minute `expires_at` is consumed by nothing. Three duplicate compliance modules invite drift. Fix: durable, periodically-revalidated gate consulted on the trade path; consider default-on for live. (B10-09) |
| **P1-k** | `PositionKey` lowercases `user_id` while repos store it checksum-cased → orphan-row hazard | `domain/position_key.py:25-28,45`; `domain/order_lifecycle.py:107`; `storage/postgres/positions_repo.py` (no normalization) | A `PositionKey` built from a checksum address queries/locks with the lowercased form while rows are written checksum-cased → no match → orphan / false drift / duplicate. Matches the prior memory finding. Fix: pick one canonical casing (do not lowercase `user_id`). (B11-07) |
| **P1-l** | Rich-console markup/log injection via leader-controlled market title/outcome | `utils/logger.py:45,1019,1089,1175` | Untrusted `title`/`market`/`outcome`/`reason` strings interpolate into Rich markup f-strings → an attacker-named market injects styling and clickable phishing `[link=…]` into the operator terminal (file JSONL sink is safe). Fix: `rich.markup.escape()` untrusted fields. (B11-04) |
| **P1-m** | Circuit breaker never trips on `VenueUnavailableError`/unknown (the dominant outage mode) | `services/executionService.py:1146-1196` (break, no `record_failure`) vs `:1200` (for-else) | A venue outage breaks each `execute()` as ambiguous without recording a failure, so the shared `_CLOB_CIRCUIT` (threshold 5) stays closed and the bot keeps POSTing into a down venue, piling up `submitted_unconfirmed` rows. Fix: `record_failure()` in the unavailable/unknown handlers. (B1-08/B7-05) |
| **P1-n** | Release gate's only concurrent-duplicate test is **skipped**; crash-recovery test is **AST-only** | `tests/release_gate/test_live_execution_invariants.py:42-54` (`@pytest.mark.skip`); `tests/release_gate/test_startup_recovery.py:17-108` (inspect/AST) | The two CLAUDE.md non-negotiables that authorize live trading — duplicate-order suppression under real concurrency, and crash-recovery no-double-fill — are not actually exercised by the gate. The skip reason even documents the P0-1-adjacent TOCTOU. Fix: make the pre-submit sequence transactional, un-skip; promote a behavioral restart no-double-fill test. (B12-01/B12-05) |
| **P1-o** | 9 most safety-critical release-gate tests skip silently in CI with no guard | `.github/workflows/ci.yml:60`; testcontainers-gated files | Reservation/protective-exit/e2e/migration tests `pytest.skip` on any Docker hiccup; the job runs `-q` with no `-rs`/no-skip guard, so the gate can report green with the invariants unrun. Fix: fail CI if any release-gate test skips. (B12-02) |
| **P1-p** | In-loop FAK fill accounting reads `size_matched`, which the venue POST never returns | `services/executionService.py:160-200,1290-1312` | **DD-D confirmed via Context7**: POST /order returns `makingAmount`/`takingAmount`+`status`, never `size_matched`; the latter is GET-only. So `_response_matched_size` is always 0 live → a non-deferred partial fill reposts the **full** amount (over-buy). Rescued for the common matched/delayed path by the GET reconciler. Fix: parse `makingAmount`/`takingAmount`, or always defer POST to the reconciler. (B1-06/DDD-01) |
| **P1-q** | `unmatched` POST status is unclassified end-to-end | `services/executionService.py:240-252`; `services/execution/reconciliation_service.py:150-189` | **DD-D**: a marketable order can rest as `status:"unmatched"`; it is in none of the deferred/non-terminal/terminal sets → mislabeled `submitted_unconfirmed`, rescued only because GET later reports the resting order. Fix: map `unmatched` → live/non-terminal. (DDD-02) |
| **P1-r** | User-stream dedup keys on `tx_hash`, a field the trade event does not contain | `clients/user_ws.py:434` | **DD-D**: the WSS trade event carries `id` (trade UUID), not `tx_hash`; the dedup read is always `None` (matches the stored "manual-sync dedup gap" memory). Fix: dedup on `id`. (DDD-03/B6-03) |

---

## P2 Findings (condensed)

| ID | Title | Location |
|---|---|---|
| P2-a | `mypy` runs `\|\| true`; 155 type errors ship incl. gateway arg-type at the composition root, `Decimal\|None`→`Decimal` in trade_pipeline, BaseException/int confusion in the risk loop | `.github/workflows/ci.yml:119`; `orchestrators/runtime_bootstrap.py:275`; `risk_monitor.py:1857`; `trade_pipeline.py:2426` |
| P2-b | Pool `acquire()` has no timeout (queues forever under exhaustion); no server-side `statement_timeout` | `storage/postgres/connection.py:43-90` |
| P2-c | `_validate_semantics` does not reject `NaN`/`Inf`; `config/validation.py` (which has the guards) is unused by the load path → `SLIPPAGE_TOLERANCE=nan` / `MAX_BOOK_SPREAD_BPS=inf` accepted | `config/app_config.py:725-946`; `config/validation.py` (0 importers) |
| P2-d | Config default drift between legacy `env.py` and typed `app_config.py` (`MAX_BOOK_SPREAD_BPS` 500 vs 2500; `CIRCUIT_FAILURE_THRESHOLD` 5 vs 15; `ENABLE_NEAR_MAX_EXIT` false vs true) | `config/env.py` vs `config/app_config.py:1140,1439,1143` |
| P2-e | `feeService.get_fee_bps` is fetched then discarded — fees never enter notional/sizing/idempotency | `services/feeService.py:35`; `services/executionService.py:956` |
| P2-f | `pnlTracker` realized/unrealized PnL is `Decimal` internally but float at every caller boundary, and feeds `leader_score` → copy decision | `services/pnlTracker.py`; `trade_pipeline.py:2728-2760`; `risk_monitor.py:1694` |
| P2-g | `/health/ready` checks boolean flags but never the tracked `last_trade_poll_ts`/`last_risk_check_ts` → green with dead loops; supervisor detects crashes only on `wait(ALL_COMPLETED)` timeout (up to `discovery_interval`=3600s) and the late detection resets the restart counter so fail-closed is unreachable | `services/healthServer.py:83-104`; `app.py:1603,700-709` |
| P2-h | One unguarded `set_cursor` await crashes the whole leader monitor on a DB blip; crashed leader monitors are never restarted and never decrement `monitored_leader_count` | `services/tradeMonitor.py:600-605`; `app.py:1624-1632` |
| P2-i | SDK GET calls lack an `asyncio.timeout` wrapper (rely solely on the process-global httpx Timeout); a hung GET stalls a reconcile loop (SDK never retries GET) | `integrations/polymarket/clob_v2_gateway.py:549,627` |
| P2-j | `neg_risk` silently defaults `False` when `get_neg_risk` transport-fails and the book also omits it → a negative-risk order signed for the wrong exchange | `utils/createClobClient.py:363-376` |
| P2-k | Error classification by message substring instead of status_code (the `:171` TODO) → retryable/terminal misclassification | `integrations/polymarket/clob_v2_gateway.py:167-250` |
| P2-l | `cancel_order` has no 404 branch → cancel of an already-gone order misclassified | `integrations/polymarket/clob_v2_gateway.py:658-682` |
| P2-m | SELL clamp-to-live-holdings silently skipped when the live fetch returns `None` (degrades to venue rejection + drift) | `orchestrators/trade_pipeline.py:2218-2245` |
| P2-n | `below_minimum` upsizes a dust copy to `buy_min_amount` with no operator gate | `services/copyStrategy.py:250-253` |
| P2-o | Legacy CLI `bot reconcile`/`bot orders` can `cancel_orders` a live order the runtime believes in-flight (if `RECONCILE_AUTO_CANCEL=true`, default off) and writes a divergent `matched_total` cursor the runtime never reads | `services/reconciliation.py`; `bot_cli/commands/{orders,reconcile}.py` |
| P2-p | Realized-PnL and exit-trigger PnL% do a `Decimal→float→Decimal` round-trip (DD-B CLASS-A/C) feeding scoring/exit thresholds | `reconciliation_service.py:1382,1410`; `services/risk/exit_policy.py:57,65` |
| P2-q | `bot run` CLI discards `app.main()` exit code and installs no SIGTERM handler → fail-closed exits report success; docs reference a nonexistent `--live` flag | `bot_cli/commands/run.py:33-36` |
| P2-r | `Decimal(fill_size)` without `str()` in a (test-only) atomic ledger writer — latent if ever wired to a float caller | `storage/postgres/orders_repo.py:1016-1017` |

## P3 Findings (condensed)

22 `print()` in `services/discovery/rotation_service.py` (CLI tool, acceptable); blocking `input()` in the same (ASYNC250, CLI-only); dead code (`services/position_action_coordinator.py`, `utils/idempotency.py` IO helpers, `test.py`, `services/accountingService.py`, `services/marketStructureService.py`, `services/backtest.py`, stale `near_one_exit` `.pyc`); 40 `# type: ignore` (11 in the gateway); `_raise_classified` typed `-> None` not `NoReturn`; `_safe_audit` swallows audit failures with bare `pass`; `bootstrap.py` excluded from hard ruff (currently clean); no `filterwarnings` config; unclosed aiohttp session at suite teardown (root cause: `utils/fetchData.py` process-global `_session` with no autouse close fixture).

---

## Performance Review

The bot is I/O-bound (venue REST/WSS + Postgres), not CPU-bound; there are no hot algorithmic loops. `pytest --durations` shows the slowest tests at ~4.3s each, all fixture/config setup, not product code. Concrete performance/scalability concerns:

- **DB pool exhaustion has no backpressure (P2-b).** `connection.py` creates a 2–20 pool with a 30s per-statement `command_timeout` but **no `acquire()` timeout**. Under the ~8 runtime loops × per-user runtimes, plus a long `rebuild_user_positions_from_fills` holding one connection for an entire transaction, callers can queue on `acquire()` indefinitely with no error or log. At input scale this is a livelock, not a crash. Add an acquire timeout + server-side `statement_timeout`/`idle_in_transaction_session_timeout`, and chunk the rebuild.
- **Boot-time DML runs on every startup (B3-07).** `migrations.py:612-618` runs an O(n·m) self-join `DELETE` to de-dup `discovery_episode_events` on every process start, lengthening the single boot transaction and holding locks as the discovery dataset grows. Gate it behind an existence check on the unique index.
- **Falcon client opens a fresh `aiohttp.ClientSession` per request (B9-06)** under pagination — inefficient but not a correctness risk.
- **Hung GET stalls a loop (P2-i).** Because the SDK never retries GET and the gateway wraps SDK calls only in the process-global httpx timeout, a network black-hole on `get_order`/`get_fills` blocks the reconcile loop for the full httpx timeout with no independent ceiling. Wrap each outbound SDK call in `asyncio.timeout`.
- **No unbounded queues / no N+1 / no expensive serialization** were found. The monitor→pipeline handoff is an inline `await` (natural backpressure), and `recent_activities` is capped at 200.

Profiling approach when tuning: run under `PYTHONASYNCIODEBUG=1` to surface slow callbacks and un-awaited coroutines; add `asyncpg` `log_min_duration_statement` to find slow queries; the reconcile/risk loops are the latency-sensitive paths to instrument first.

---

## Architecture Review

**Strengths.** Clean gateway isolation (`interfaces/clob_gateway.py` + `integrations/polymarket/clob_v2_gateway.py`) keeps SDK types out of execution; canonical `PositionKey` identity; `Decimal`-clean money path; a real composition root with no import-time I/O; per-scope coordinator + DB advisory locks + idempotency layering; reconciler-owned settlement as a deliberate single-writer design.

**Weaknesses that drive the findings.**

1. **Settlement atomicity is the central architectural gap (P0-1).** The reconciler is correctly the sole settlement writer, but "apply fill" and "advance the de-dup watermark" are two separate transactions on two connections, with the watermark best-effort. The single-writer invariant is necessary but not sufficient without atomicity; the system needs a connection-aware repo method that writes the fill, the position delta, and the cumulative-matched watermark in one transaction, keyed on a venue-stable identifier rather than wall-clock time.
2. **Two parallel config systems (`config/env.py` legacy vs `config/app_config.py` typed) have drifted (P2-d).** Same env var, different defaults, depending on which path a service reads. The typed config should be the single source; `env.py` defaults should be imported from one shared constants module (as `config/addresses.py` already does).
3. **The live-gate is diffuse (P0-2).** Paper/live enforcement lives only at copy-tier resolution in the pipeline; the gateway, ExecutionService, and risk-exit dispatch have no independent guard. A single bypass-proof choke point belongs inside the gateway submit methods.
4. **Startup sequencing couples destructive normalization with recovery (P0-5).** Purging order rows before reconciliation, and pruning positions on an empty live snapshot (P0-3), make boot-time data loss reachable from transient errors. Recovery steps must precede destructive normalization and must fail-closed on fetch failure.
5. **Dead/duplicate surface invites confusion (B10).** Two reconcilers, three position-coordinator modules (one dead), two idempotency modules, an unused accounting service, and a stale `test.py` at the repo root. None is individually dangerous, but the legacy CLI reconciler writing a separate cursor (P2-o) and `manual_sync` bypassing the reconciler's lock (P1-b) are concrete hazards born of the duplication. Recommend a dedicated cleanup pass (issue #77) *after* the P0 fixes, with the legacy CLI reconciler migrated or retired and a runtime-up guard added.
6. **`IntentRegistry` implies an enforced lifecycle that is not wired (B2-04).** The IDLE→…→DONE state machine, duplicate-intent blocking, and reserved-total accounting are written only by startup recovery and never consulted live; durable protection rests entirely on DB reservations + idempotency. Either wire it through the submit path or delete it so it doesn't imply a guarantee it doesn't provide.

---

## Security Review

No remote-code-execution, SQL-injection, or hardcoded-secret issues were confirmed.

- **SQL (S608):** all 9 flagged f-string sites in `discovery_repo.py`/`orders_repo.py` are false positives — only `$N` placeholder indices and static column names are interpolated; every value is an `asyncpg` bind parameter. CI's blanket `B608` skip should be replaced with per-line `# nosec B608` so a future real injection still fails (B12-07).
- **Secrets:** `AppConfig.raw` strips `PRIVATE_KEY*`/`CLOB_*` (`_strip_sensitive`), the JSON log formatter routes errors through a redactor, DSN logging is host-only, and `create_clob_client` never interpolates the key into logs/exceptions. The hardened `PRIVATE_KEY_FILE` reader confines paths to `$HOME`, resolves symlinks, and enforces `0600`. **Residuals:** the legacy `config/env._load_secret_from_file` reader is weaker (no symlink resolution / no path confinement) — B11-08; and two log sinks (`print_clob_error`, `log_api_response`) surface raw SDK error text outside the redaction envelope, safe only because the SDK never embeds the key today (B7-09). Route both through a hex-string redactor.
- **Log/markup injection:** leader-controlled market titles reach Rich `console.print` unescaped → operator-terminal phishing/spoofing (P1-l). The JSONL file sink is `json.dumps`-safe.
- **`secrets.env` override scope is correctly allowlisted** to credentials only (not trading policy) — verified, matches CLAUDE.md.
- **Container:** non-root `botuser`, fail-closed `PAPER_TRADE_ONLY=true` baked — but `--env-file` can override it back to live at the OS level (B12-04), which compounds P0-2.
- **Randomness:** `random.uniform` is retry jitter only (S311), never cryptographic — fine.
- **Dependency CVEs:** not scanned (no network `pip-audit` run). web3 7.14.1 / aiohttp 3.13.5 / asyncpg 0.31.0 / websockets 15.0.1 are recent; run `pip-audit` before any live cutover.

---

## Reliability and Failure Modes

The crash-recovery walkthrough (DD-C) produced a per-state verdict:

| Kill point | Cold-start verdict |
|---|---|
| Reservation active, no POST | SAFE (worktree) / degraded registry accounting (HEAD — DDC-01) |
| `pending`/`pending_lock`, POST not sent | SAFE (correctly purged) |
| POST hit wire, response lost, crash before save | **LOST-POSITION** (P0-5) |
| `submitted_unconfirmed` + order_id, fill not applied | SAFE (reconciler re-polls) |
| fills+positions committed, watermark not written | **DOUBLE-APPLY** (P0-1) |
| `pending_lock` held, holder killed | STRANDED-LOCK until boot (P1-g) |
| Protective SELL submitted, position not closed | block self-heals; rides the P0-1 SELL double-apply |
| manual_sync mid absolute-SET | SAFE (idempotent), but races reconciler (P1-b) |

**Timeouts/retries:** outbound aiohttp sessions carry timeouts; the gateway relies on the global httpx timeout (P2-i gap). Submission retry is bounded (3 attempts, jittered backoff) and FAK repost re-checks freshness/size — but the circuit breaker doesn't trip on the dominant outage mode (P1-m) and the 5xx-after-accept path retries into a duplicate (P1-a).

**Partial failures:** the `except Exception` count (465) is mostly benign telemetry, but the cardinal ones are real: the reconciler watermark swallow (P0-1), the boot position-fetch swallow (P0-3), `runtime_bootstrap` converting fatal misconfig to degraded-running (B4-10 fail-open drift wiring), and risk-trigger status writes swallowed to DEBUG (B8-08).

**Supervision:** the supervisor detects a crashed loop only on the `wait(ALL_COMPLETED)` timeout (up to 1h) and the late detection resets the restart budget, so fail-closed is structurally unreachable at default config; leader-monitor death is permanent and invisible to `/health/ready` (P2-g/P2-h, B4-03/B4-04).

**Data-loss risks:** P0-1 (double-apply), P0-3 (position wipe), P0-5 (deleted accepted order). All three are reachable from a single transient error or a restart.

---

## Test Coverage Review

2,288 unit tests pass; the suite is broad and well-organized by issue/phase. The gap is **what the release gate actually verifies**:

**Release-gate coverage map (CLAUDE.md non-negotiables):**

| Non-negotiable | Covered? |
|---|---|
| Duplicate-order under real concurrency | **GAP** — the one test is `@pytest.mark.skip` (P1-n) |
| Crash-recovery no-double-fill | **GAP** — AST/signature pinning only, behavioral coverage is unit-tier (P1-n) |
| Reservation TTL/lifecycle | Covered but Docker-gated, skips silently (P1-o) |
| Settlement idempotency | Partial (signature pin; behavioral idempotent-transitions unit-tier only) |
| Stale-data fail-closed | Cache-layer covered; execution-path stale-book refusal not in a dedicated gate |
| Paper-mode blocks real order | Dockerfile-default asserted (pure); no behavioral "blocks submission" gate; `--env-file` override unguarded |
| PositionKey uniqueness | Covered (thorough) |
| Coordinator serialization | Covered |
| Protective-exit re-entry block | Behavioral tests Docker-gated; signature tests pure |
| WSS heartbeat/reconnect/resync | Covered |
| Metadata-driven sizing / effective idempotency key / typed-config fail-closed / gateway isolation | Covered (pure) |

**CI blind spots:** mypy `|| true` (155 errors ship), ruff excludes `bootstrap.py`, bandit skips `B608`/`B108`, F401/F841 ignored repo-wide, and — most dangerous — **no skip-detection on the release-gate job**, so the Docker-gated invariant tests can all skip and the gate still reports green (B12-02).

**Missing tests for every P0/P1** are specified in each finding above. The highest-value additions: a behavioral restart no-double-fill test (P0-1) promoted into `release_gate/`, a concurrent-duplicate test un-skipped after the pre-submit sequence is made transactional (P1-n), a paper-default-blocks-submission test (P0-2), a boot-fetch-failure-doesn't-prune test (P0-3), and a reconcile-before-purge test (P0-5). Mock realism: DD-D confirmed the conftest mocks return `size_matched` matching the parser, but the **venue** returns `makingAmount`/`takingAmount` — so the mocks validate the parser, not the contract; add a venue-shaped POST-response fixture (P1-p).

---

## Remediation Plan (prioritized)

**Phase 0 — commit the pending fix (done first, smallest).**
- Commit the working-tree `intent_recovery.py` `await reservations_repo.list_active()` fix + its test. At HEAD, DB-backed restart recovery is a silent no-op (DDC-01). Goal: stop shipping broken recovery. Risk: none (already written + tested). Validate: `pytest tests/test_issue70_reservations_intent_lifecycle.py`.

**Phase 1 — critical correctness/safety (P0).** Each is one finding = one branch = test-first.
1. **P0-1 reconciler atomicity** — add a connection-aware `apply_fill + advance_watermark` in one transaction; key fill de-dup on `(order_id, cumulative_matched)` not wall-clock; remove the `except: pass`. *Files:* `reconciliation_service.py`, `fills_repo.py`, `orders_repo.py`, a `migrations.py` index change. **Highest risk; do not rush; dedicated regression suite required (CLAUDE.md).**
2. **P0-2 fail-closed live gate** — default `paper_trade_only=True`, derive `dry_run`, add `LIVE_TRADING_ENABLED`, tier fallback `watchlist`, gateway-level paper guard. *Files:* `app_config.py`, `coordinator.py`, `trade_pipeline.py`, `clob_v2_gateway.py`. Risk: test fallout (many tests assume default-live).
3. **P0-3 boot fetch fail-closed** — `fetch_live_positions` returns `None` on failure; prune skipped on `None`/empty-with-persisted. *Files:* `runtime_bootstrap.py`, `startup_health.py`. Low risk.
4. **P0-4 gate trading on reconcile** — filter monitors via `_wallets_blocked_by_reconcile_failures` (already exists). *File:* `app.py`. Low risk.
5. **P0-5 reconcile before purge** — reorder; restrict DELETE to venue-confirmed-absent `order_id IS NULL`. *Files:* `app.py`, `startup_health.py`. Medium risk.

**Phase 2 — reliability / failure-handling (P1).** P1-a (5xx classification — small), P1-b (manual-sync advisory lock), P1-c (fresh-quote decision price), P1-d (inbound-anchored WSS liveness), P1-e (trailing-stop below entry), P1-f (waiter-count leak), P1-g (submission-lock auto-release), P1-h (migration reorder — small), P1-i (orphan-position risk contexts), P1-m (circuit-breaker record_failure), P1-p/q/r (POST-response parsing, `unmatched` status, user-stream `id` dedup).

**Phase 3 — performance.** P2-b (pool acquire timeout + statement_timeout), P2-i (per-call asyncio.timeout), B3-07 (gate boot DML).

**Phase 4 — architecture refactors.** Single config source (P2-d), gateway-level paper choke point, wire-or-delete `IntentRegistry`, legacy reconciler migration/retirement with a runtime-up guard (P2-o).

**Phase 5 — test coverage.** Promote behavioral restart/concurrency tests into `release_gate/`; add CI skip-detection; venue-shaped mock fixtures; un-skip P1-n after P0-1.

**Phase 6 — cleanup (issue #77), last.** Delete dead modules (B10 inventory), purge stale `.pyc`, fix mypy hard on the money path, escape Rich markup (P1-l), redact the two raw log sinks.

---

## Suggested Patch Sequence (smallest safe order)

1. **Patch 0:** commit `intent_recovery.py` + test (recovery-critical, already written).
2. **P0-3** (boot fetch fail-closed) — localized, clearly fail-closed, low blast radius.
3. **P0-4** (gate trading on reconcile) — localized to app.py spawn, reuses existing helper.
4. **P1-a** (5xx → `submitted_unconfirmed` instead of retry) — localized to the wrapper classifier, strengthens duplicate protection.
5. **P1-h** (migration reorder) — one-line, prevents a startup abort.
6. **P0-2** (fail-closed live gate) — high value, but flips test assumptions; do after the low-risk set so its test fallout is isolated.
7. **P0-5** (reconcile before purge) — startup reorder, needs a venue-probe path.
8. **P0-1** (reconciler atomicity) — **last and on its own**, with a dedicated regression suite; it changes settlement timing that earlier patches depend on, and CLAUDE.md forbids weakening reconciliation protections.
9. **P1-b** (manual-sync advisory lock), then the remaining P1s.

> **Must NOT be patched without a dedicated regression test:** `reconciliation_service.py`, `reservation_service.py`, `intentRegistry.py`, `executionService.py` submit/retry/FAK, `orders_repo.py` locking, fills/positions application, `position_key.py`, quantize directions, TTL/grace constants.
> **Must NOT change at all during remediation:** the no-real-orders / no-live-trading posture; duplicate-order, stale-lock, and reconciliation protections may only be **strengthened**, never weakened.

---

## Open Questions (only those that block correct implementation)

1. **P0-2 intent:** is default-live the deliberate production posture (operator always sets `PAPER_TRADE_ONLY`), or should the audit flip the default to paper? CLAUDE.md's fail-closed mandate says flip it; confirm before changing the default, since it cascades through tests and operator runbooks.
2. **P0-1 de-dup key:** is `(order_id, cumulative_matched)` an acceptable stable fill-identity, or does the venue ever report the same cumulative-matched for two distinct settlement events (e.g. cancel-replace at the same matched total)? Resolving this determines the migration shape.
3. **Legacy CLI reconciler (P2-o):** keep it (with a runtime-up advisory guard) or retire it and migrate `bot_cli/commands/{orders,reconcile}.py` onto the runtime reconciler's cursor?

---

## Final Risk Rating: 🔴 RED — unsafe for production

**Justification.** Five P0 issues are confirmed, all on the money/settlement/recovery axis and reachable from a single transient error or a restart: the reconciler double-applies fills (corrupting the position ledger that protective exits and sizing depend on), the live gate fails open with no master switch, a boot-time API hiccup deletes the entire position book, reconcile failure doesn't stop trading, and startup normalization deletes venue-accepted orders. Independently, the **release gate that is supposed to authorize live trading does not actually exercise its two most important invariants** (duplicate suppression under concurrency is skipped; crash-recovery no-double-fill is AST-only), so "CI green + release-gate pass" currently does **not** establish the CLAUDE.md non-negotiables.

The fundamentals are good enough that this is a **fixable RED, not a structural one** — the money math, identity model, and gateway isolation are sound, and every P0 has a concrete, bounded fix with a specified regression test. Once P0-1 through P0-5 are fixed **with the behavioral release-gate tests promoted and passing**, and the manual-sync race (P1-b) is closed, the rating moves to ORANGE pending the remaining P1s, then YELLOW. Live trading must remain blocked until then, consistent with CLAUDE.md.

---

## Post-Audit Fixes Applied (this branch)

The audit-remediation run fixed the verified findings test-first, one commit per finding, on `fix/develop-runtime-hardening`. Prior-session P0/P1 fixes plus this run:

| Commit | Finding | Change |
|---|---|---|
| `82e1065` | Patch 0 / DDC-01 | `await reservations_repo.list_active()` — HEAD recovery was a silent no-op |
| `f906083` | **P0-3** | boot live-position fetch failure no longer wipes the ledger (fail closed) |
| `d12eb7e` | **P0-4** | reconcile-failed users' monitors blocked before spawn |
| `742424a` | **P1-a** | 5xx/transport POST classified ambiguous → reconcile, not retry→duplicate |
| `2b0ae85` | **P1-h** | `wallet_role` column added before the role-keyed index (no startup abort) |
| `05dd415` | **P1-f** | coordinator waiter count dropped on cancel-while-waiting (no stranded scope) |
| `4ee7175` | **P1-e** | trailing stop fires on retrace below entry |
| `8ced608` | **P1-r** | WSS trade `id` surfaced; REST `transaction_hash` captured |
| `2539636` | **P1-d** | WSS liveness anchored on inbound activity only |
| `4cdd0c2` | **P1-l** | Rich markup escaped in leader-controlled log fields |
| `3b27129` | **P1-c** | copy decision scored against the fresh quote, not the stale signal |
| `d135aed` | **P1-k** | `PositionKey` preserves checksum `user_id` casing (no orphan rows) |
| `8404f7f` | **P1-b** | manual-sync absolute SET under the position advisory lock |
| `b1b499b` | **P1-m** | circuit breaker trips on venue-unavailable submissions |
| `0c86c93` | **P1-g** | stale submission locks self-heal on acquire (TTL takeover) |
| `4beaf22` | **P1-i** | open positions with no active-leader follow are still risk-monitored |
| `371f1fa` | **P1-p** | a bare cancelled FAK with an order id is deferred, not reposted (no over-buy) |
| `30d747d` | **P1-q** | `unmatched` order status classified non-terminal (Context7-confirmed) |
| `800c011` | **P1-j** | geoblock consulted on the BUY trade path with expiry revalidation |
| `b9ba21e` | **P0-5** | startup purge preserves venue-accepted (order_id-bearing) orders |
| `d9f09e2` | **P0-1** | atomic reconciler settlement + stable `(order_id, cumulative_matched)` fill dedup |

Each carries a regression test (real-Postgres behavioural where the guard lives in SQL: P0-1, P0-5, P1-g).

**Remaining (out of this run's fixed set):**

- **P0-2 (live-gate fail-open):** intentionally **skipped at the owner's direction** ("no paper trade, just fix bugs, no need stupid gate"). The gate code is left as-is — this run neither enables live trading nor weakens any protection. Live-gate fail-open is therefore an **owner-accepted residual**, not an open bug. Deployed Docker/compose already default `PAPER_TRADE_ONLY=true`.
- **P0-5 residual:** a lost-POST order with **no** `order_id` (crash between the wire send and the local save) still cannot be recovered without a venue-probe-by-maker path + a new gateway method (deferred; CLAUDE.md forbids rushing reconciliation).
- **P1-n / P2-\* / P3-\*:** not in this run (P0+P1 scope). The release-gate duplicate-order and crash-recovery invariants remain skipped/AST-only; a dead duplicate `services/compliance.py` (shadowed by the `services/compliance/` package) is a P3 cleanup.

**Rating after these fixes:** the settlement / recovery axis is now fixed with regression tests (P0-1 atomic settlement + stable dedup, P0-3, P0-4, P0-5 order preservation) and 15 P1s are closed. Strictly by the rubric a confirmed-but-skipped P0 (P0-2, owner-accepted) keeps the headline 🔴 **RED**; on the money/ledger axis the codebase is materially safer (🟠 ORANGE-equivalent). **Live trading must remain blocked** until the live-gate posture is decided and the behavioural release-gate invariants (P1-n) actually run.

---

## Post-Audit Fixes Applied — P0-2 reversal + P2 / P3 / P2-a (later passes)

After the P0/P1 run, the owner reversed the P0-2 decision and the P2/P3 tail was implemented (subagent-orchestrated, orchestrator-verified, one commit per finding) on `fix/develop-runtime-hardening`.

| Commit | Finding | Change |
|---|---|---|
| `299c5a2` | **P0-2** (reversed) | `LIVE_TRADING=enabled` master switch; default + unknown ⇒ paper; live ⇒ doctor-or-die **fail-stop** (any doctor failure prints the exact error and exits — no silent degrade to paper); tier resolver fails closed to `watchlist`; Docker/compose drop `PAPER_TRADE_ONLY` so the switch is the single gate |
| `692fc85` | **P2-c / P2-d** | reject `NaN`/`Inf` floats (`math.isfinite`); wire `CIRCUIT_FAILURE_THRESHOLD` from typed config into the breaker; align drifted `env.py` defaults |
| `12f6665` | **P2-b / P2-r** | `_BoundedAcquirePool` adds an `acquire()` timeout + server-side `statement_timeout` / `idle_in_transaction_session_timeout`; `Decimal(str(...))` in the atomic ledger writer. (Also carried the P1-g `orders.`-qualified `ON CONFLICT` SQL fix.) |
| `ea33c86` | **P2-i / P2-j / P2-k / P2-l** | `asyncio.timeout` around each SDK GET; `_classify_error` status-code-first; `cancel_order` 404 ⇒ terminal `already_gone`; neg-risk fail-closed when neither V2 nor book is determinable |
| `4b32c5a` | P0-1 / P1-g regression | dedup-aware `append_fill_and_apply` added to 3 reconciler test fakes (real-PG surfaced the gaps) |
| `8bd06bb` | **P2-f** (+ P2-p exit part) | exit-trigger stop/take-profit/trailing comparisons kept in `Decimal` (no float-ULP boundary drift) |
| `2db2d5c` | **P2-m / P2-e** | SELL fails closed when live holdings are unknown (`None`), releasing the atomic claim via `retryable_failed`; fee-aware BUY affordability gate (`notional × (1 + fee_bps/1e4)` vs free cash) before the reservation — fees gate only, sizing + idempotency-key inputs untouched |
| `9af2d57` | **P2-g / P2-h** | `/health/ready` fails on a ticked-then-stale loop heartbeat (flap-safe: ts≤0 not reported); guarded `set_cursor` persist (no monitor crash on a transient PG blip); leader-monitor crash decrements `monitored_leader_count` |
| `5e15cd5` | **P2-q** | `bot run` propagates `app.main()`'s exit code (fail-closed exits no longer report success) |
| `9097396` | **P3** | delete dead `services/backtest.py`, the shadowed `services/compliance.py` flat-file + its dead facade, and the stray root `test.py` |
| `39a160b` | **P2-a** | money-path type-correctness (`_raise_classified -> NoReturn`; gateway `condition_id` fail-closed; `http_client` `raise None` guard; `gather` result guards widened to `BaseException`) + a **blocking** `--follow-imports=silent` mypy gate over the order-signing + HTTP spine; global mypy stays soft-fail |
| `53e5160` | **P1-n** | promoted the skipped duplicate-order release-gate invariant to a **behavioural** test — which exposed TWO real duplicate-order races (up to 7 venue POSTs per N=25 burst, ~10% of runs) and fixed both: (1) the `execute()` loser path no longer falls through to submit when `is_submission_locked()` momentarily reads False — a non-acquirer now fails closed unconditionally; (2) the post-expiry retry no longer destructively re-marks the lock row (which clobbered a peer's takeover) — `try_acquire_submission_lock` is the sole single-winner authority, taking over the `pending` row at the pending-TTL. New concurrent + concurrent-post-expiry stress tests (run 10–20× each). Crash-recovery invariant confirmed already behavioural (`test_restart_rehydration_risk`, `test_reconciler_idempotent_transitions`) |

Each carries a regression/behavioural test; the whole touched set is green (real-PG where the guard lives in SQL). Whole-repo `compileall` + CI-parity hard ruff pass; scoped money-path mypy went 129 → 122 errors with the two spine files now blocking-clean.

**Deferred / scoped out (documented, not silently dropped):**

- **P2-o** — the legacy `services/reconciliation.py` was **kept**, not retired. It is a bulk venue open-order **drift sweeper** (`reconcile() -> stale_orders_cancelled`) that the live `bot orders` / `bot reconcile` CLI commands consume; the new per-order `ReconciliationService` exposes no equivalent bulk-sweep entrypoint, so there is no clean redirect. Retiring it needs a dedicated task (build a bulk-sweep method on the new reconciler first). Retiring it blindly would have removed an operator pre-live tool — refused per the "do not weaken reconciliation protections" constraint.
- **P2-p (realized-PnL part)** — the `reconciliation_service.py:1382,1410` `Decimal→float→Decimal` round-trip feeding scoring is **not** fixed here (only the `exit_policy.py` exit-trigger comparisons were). Deferred.
- **P2-h (restart part)** — a crashed leader monitor's count is now corrected, but auto-restart still relies on dynamic discovery; true restart-on-crash lives in `orchestrators/runtime_monitor.py` (out of the supervision file set). Deferred.
- **P2-n** — excluded: the `below_minimum` gate flag already exists; only an optional operator-suppress config remained (not a safety gap).
- **P3 kept (NOT dead, contrary to the condensed list):** `utils/idempotency.py` (re-exported by `utils/__init__` + `config/app_config`), `services/position_action_coordinator.py` (snake_case shim with a tested identity contract), `services/marketStructureService.py` (safety-relevant reconciliation logic with a live backing repo — confirm intent before deleting).

**Rating after these fixes:** with P0-2 now a fail-stop master switch (no paper fallback on doctor failure), the P2/P3 tail closed, and **P1-n now behavioural** — the duplicate-order invariant runs for real and the two latent races it exposed are fixed (the crash-recovery invariant was already behaviourally covered) — every must-fix audit axis (money/ledger, settlement, duplicate-order, gateway-isolation, config-fail-closed, supervision) is hardened with executing regression tests. Rating: 🟡 **YELLOW**. Live trading remains blocked pending the operator-owned items in the CLAUDE.md live-gate checklist (mock-CLOB integration pass, paper-trading soak, and manual verification of collateral / negative-risk / gateway readiness / config / Docker-runtime / DB backup / rollback). Those are operator/process gates, not code gates.

**P1-n note (why behavioural testing mattered):** the duplicate-order invariant had been `@pytest.mark.skip`'d with a plausible-sounding "deferred follow-up" reason. Running it surfaced a real, intermittent path to **7× the intended position on a single copy** — exactly the failure that loses real money fastest, and exactly what an AST-only / skipped gate cannot catch. No skipped or AST-only invariant should be treated as covered before live.

---

## Appendix — Verification Verdicts and Refuted Suspects

**Adversarially verified:** P0-1 (CONFIRMED — no enclosing transaction, bare-except watermark, wall-clock dedup, no missed guard); P1-a 5xx-duplicate (CONFIRMED — wrapper catches the exception and returns a string-classified dict; gateway `_classify_error` is never reached for the POST path; SDK raises `PolyApiException(status_code=5xx)` proven in installed source); P1-b manual-sync race (CONFIRMED, downgraded to P1 — new-row double-count guarded by a 45s holdoff, residual lost-update on existing rows); P0-2 paper gate (CONFIRMED — both flags default False, no master switch, Docker/compose mitigate container deploys only).

**Refuted by contract/code (kept to prevent re-discovery):** WSS-wake reconcile vs risk-loop is **not** an unguarded race (both route through the per-scope coordinator lock); pipeline vs risk same-scope is serialized in-process and DB-backed cross-process; reservation force-release during grace cannot corrupt fill accounting (order-keyed, independent of the reservation row); the money path is **Decimal-clean end-to-end** on every value-bearing surface (DD-B — only two avoidable PnL/exit round-trips, both P2); `assets_ids` is the correct subscribe field and `price_change` size "0" correctly deletes a level (DD-D CONFIRMED); the S608 SQL sites are parameterized false positives; durable re-entry block correctly fires only on confirmed full close; `near_one_exit` has zero live references.

**HEAD-vs-worktree:** at HEAD, `intent_recovery.py:43` calls `list_active()` without `await` → iterating a coroutine raises `TypeError`, swallowed by the caller → DB-backed restart recovery is a complete no-op. The working-tree fix (the uncommitted `await`) is correct and must be committed (Patch 0).
