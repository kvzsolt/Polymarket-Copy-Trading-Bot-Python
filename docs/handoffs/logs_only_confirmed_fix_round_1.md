# Fix round 1 — logs-only `on_chain_confirmed` source (review findings on `e2e1163..4bec47d`)

Reviewed by the controlling agent: **Approved after this fix round.** Full suite 3126 passed; design matches the brief.
Start in the repo root on `feature/hybrid-ccxt-implementation` at HEAD `4bec47d` (clean tree). Read CLAUDE.md, AGENTS.md,
`.superpowers/sdd/logs-only-confirmed/brief.md` and `report.md` first. TDD (RED first for 1–3), deterministic tests,
`ruff format`/`ruff check` + `pyright` on touched files, focused tests, then the full suite once
(`python -m pytest -q -x -p no:cacheprovider`); conventional commits, no attribution trailer; never log URLs/keys; never
touch `secrets.env`.

1. **IMPORTANT — empty-roster fallback re-opens the unfiltered logs feed.**
   `services/onchain_detection/order_filled_detector.py::log_subscription_params_for_watched` falls back to
   `log_subscription_params()` (address-only, ~40k CU/min) when the watched set is empty — exactly the cost `logs_only`
   exists to avoid (e.g. discovery rotates the roster to empty). Change: with an empty watched set the leader-filtered
   variant returns `()` (no subscriptions). Make `OnchainDetectionRuntime` tolerate zero mined subscriptions: start with
   none, `connected` → `None` when there are no sockets, INFO `onchain_mined_subscriptions_idle | watched=0`, and
   resubscribe when `update_watched_wallets` delivers a non-empty set. Legacy path (no params factory) stays byte-identical
   (still one address-only sub). Tests: detector returns `()` for empty; runtime opens 0 sockets on an empty roster then 2
   after an update; decide and document (docs/known_limitations.md) what `onchain_ws_ok` shows with zero sockets — it must
   not read as a false "WS down" (e.g. reason `no_watched_leaders`, or leave the check not-ready-until-first-connect and say so).
2. **MINOR — resubscribe gap.** `orchestrators/onchain_detection_runtime.py::_resubscribe_mined_once` stops the old sockets
   before opening the new ones → a fill gap on every roster change. Open the new sockets first, then stop the old ones;
   update the resubscribe test to assert that ordering.
3. **MINOR — unguarded background loop.** `_resubscribe_mined_loop` has no exception guard; a raise kills the task silently.
   Wrap the body: WARN `onchain_mined_resubscribe_failed | error=<redact_error_detail(exc)>`, keep the
   `_resubscribe_requested` coalescing. Test: a params/websocket factory that raises once → warning logged, the next request
   is still processed.
4. **MINOR — formatting.** `ruff format config/app_config.py` (around line 1377) — format only, no logic change.

Append a "Fix round 1" section to `.superpowers/sdd/logs-only-confirmed/report.md` (RED evidence, commands, results).
Report back: status, commit shas, one-line test summary, concerns — the controlling agent will re-review the fix diff.
