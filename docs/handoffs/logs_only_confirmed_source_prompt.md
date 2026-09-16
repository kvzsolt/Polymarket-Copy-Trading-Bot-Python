# Implementation prompt — logs-only `on_chain_confirmed` detection source (provider-agnostic, Alchemy-compatible)

Paste everything below this line into the implementing agent (Claude Code / Codex / any coding agent) started in the
repository root `/home/anon/Work/bots/Polymarket-Copy-Trading-Bot-Python` on branch `feature/hybrid-ccxt-implementation`
(HEAD `e2e1163`, clean tree, 3097 tests passing).

---

## Goal (one sentence)

Add `detection.onchain.confirmed_source: mempool_reserved | logs_only` so `detection.mode: on_chain_confirmed` can run on a
plain `eth_subscribe` **`logs`** WebSocket subscription — topic-filtered on Polymarket's `OrderFilled` event **and** the followed
leader wallets — at near-zero provider cost on **any** WSS endpoint (Alchemy, Infura, QuickNode, self-hosted), while the
existing Alchemy gen-2 mempool-reserved path (`mempool_reserved`, the default) and `on_chain_pending` stay **byte-identical**.

## Why

- Today `on_chain_confirmed` (gen-2 `PolygonSourceTransactionRuntime`) opens `alchemy_pendingTransactions` (full body,
  `toAddress` = both exchanges) + an address-only `logs` sub + `newHeads` ≈ 85k CU/min ≈ $54/day on Alchemy pay-as-you-go
  ($0.45 / 1M CU). `on_chain_pending` ≈ $27/day. Polling ≈ $0 but slower.
- A `logs` sub filtered by `OrderFilled` topic0 + leader as indexed maker/taker yields a handful of events per hour →
  inside Alchemy's free 30M CU/month tier and Infura's free 3M credits/day. Latency = block inclusion (~2 s Polygon blocks;
  ~0.7 s behind the mempool), still ahead of REST polling. No orphan-position risk (mined `OrderFilled` is authoritative).
- Alchemy-specific methods must remain supported (the operator may scale to mempool speed later): keep
  `alchemy_pendingTransactions`, gen-2, `on_chain_pending` untouched.

## Read first (in this order)

1. `CLAUDE.md`, `AGENTS.md` (repo rules — binding).
2. `config/app_config.py` → `OnchainDetectionConfig`, `_validate_detection_mode`, `DetectionModeResolver`.
3. `orchestrators/detection_source_factory.py` (`DetectionSourceRequest`, `DetectionSourceBuilder` Protocol,
   `ProvisionalPendingSourceBuilder` = gen-1 pending, `ConfirmedSourceBuilder` = gen-2, `DetectionSourceFactory`).
4. `orchestrators/onchain_detection_runtime.py` (gen-1 `OnchainDetectionRuntime(enable_pending, enable_mined, websocket_factory, …)`,
   `update_watched_wallets`, stream names `pendingtx` / `orderfilled`).
5. `clients/onchain_ws.py` (`OnchainWebSocket`, subscribe/ack, reconnect/backoff, `OnchainWsStateListener`).
6. `services/onchain_detection/order_filled_detector.py` — verified `OrderFilled` ABI:
   `OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, uint8 side, uint256 tokenId,
   uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee, bytes32 builder, bytes32 metadata)`;
   topic0 `0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee`; **topic1 = orderHash, topic2 = maker,
   topic3 = taker**. `log_subscription_params()` currently builds `["logs", {"address": [both exchanges], "topics": [topic0]}]`
   with client-side maker/taker matching (a single filter cannot OR topic2/topic3 — hence two subscriptions below).
7. `services/onchain_detection/ws_health_reporter.py` (`OnchainWsHealthReporter`, streams `pendingtx|orderfilled`,
   readiness check `onchain_ws_ok`), `services/metrics.py` (bounded schemas `onchain_ws_connected{stream}`, `onchain_ws_reconnect_total{stream}`).
8. `app.py`: `_build_detection_source_factory`, `_build_onchain_websocket_factory`, `_build_onchain_ws_health_reporter(readiness, metrics_registry, *, mode)`
   (currently returns `None` unless mode is `on_chain_pending`), `_build_pending_copy_tracker`, `_validate_detection_confidence_floor`,
   the on-chain composition block (`detection_mode = DetectionStartupReporter(...).report(...)` → factory build → runtime start).
9. `orchestrators/onchain_signal_dispatch.py` — `CrossSourceTradeClaimPolicy`: confirmed signals carry `log_index`/`order_hash`,
   so a tx-level cross-source claim protects against double copy with REST polling (which keeps running in every mode).
10. `config/detection.yml`, `config/environments/live/detection.yml` (currently `mode: on_chain_pending`, `pending_tx_max_signal_age_ms: 700`,
    `pending_copy_*` keys), `config/addresses.py`, `docs/02-configuration.md`, `docs/known_limitations.md`,
    `docs/audit/pending_detection_notes.md`, `secrets.env.example`.
11. Tests to extend: `tests/test_config.py` (live-selects test at the bottom; base invariants: pending flag `false`, `mode` unpinned),
    `tests/test_live_environment_overrides.py` (exact live file key sets; `detection.yml == {mode, onchain}`; no `token` substring),
    `tests/test_detection_source_factory.py`, `tests/test_onchain_ws_subscribe_params.py`, `tests/test_order_filled_detector.py`
    (or wherever `log_subscription_params` is tested), `tests/test_onchain_detection_runtime.py`, `tests/test_onchain_ws_health.py`,
    `tests/test_app_onchain_wiring.py`, `tests/test_onchain_signal_dispatch.py`.

## Binding constraints

- Classes + constructor injection, SOLID (a NEW `DetectionSourceBuilder`; no if/else chains in `app.py` — extend the factory
  key), frozen dataclasses, PEP 8, Google-style docstrings on every module/class/public method, structured logging.
- **Never log WS URLs, API keys, bearer credentials.** Never touch `secrets.env`. Never run the bot in live mode (paper only).
- **Zero added latency on the detection→submit hot path**; roster resubscribe is off-path/bounded.
- No new dependencies. Decimal for money (unchanged here). Do not touch CCXT adapter code or money-path semantics.
- TDD: RED test first, deterministic (fake sockets/clock, no network, no wall-clock).
- Never `git stash` / `reset --hard` / `clean` / `checkout -- <path>`; revert by editing. Conventional commits, no attribution
  trailer, commit per coherent slice.
- Existing behaviour for `confirmed_source: mempool_reserved` and for `on_chain_pending` MUST be byte-identical.

## Design (binding)

1. **Config** — `OnchainDetectionConfig.confirmed_source: Literal["mempool_reserved", "logs_only"] = "mempool_reserved"`
   (+ YAML key with a comment in `config/detection.yml`; leaf name must be unique across `config/*.yml`).
   Also allow `pending_tx_provider: "generic"` (keep the field name; `alchemy` remains default).
   Validation in `_validate_detection_mode`:
   - `on_chain_confirmed` + `mempool_reserved` → unchanged (Alchemy provider + `wss://` URL required).
   - `on_chain_confirmed` + `logs_only` → requires a `wss://` URL (`onchain_ws_url or pending_tx_ws_url`); provider may be
     `alchemy` or `generic`.
   - `on_chain_pending` + `logs_only` → `EnvValidationError` (pending is Alchemy mempool only).
   - `pending_tx_provider: generic` with `on_chain_pending` or `mempool_reserved` → `EnvValidationError`.
2. **Subscriptions** — extend `order_filled_detector.log_subscription_params(exchange_addresses, watched_wallets=None)`
   (or add `log_subscription_params_for_watched(...)` returning a tuple of two params lists) so that, when a watched set is
   given, the runtime opens **two** `logs` subscriptions:
   - maker role: `["logs", {"address": [ctf, negrisk], "topics": [[topic0], null, [<leaders as 32-byte topics>]]}]`
   - taker role: `["logs", {"address": [ctf, negrisk], "topics": [[topic0], null, null, [<leaders as 32-byte topics>]]}]`
   Leader address → topic: lowercase, left-padded to 32 bytes (`0x` + 24 zero hex chars + 40 hex chars). Cap the watched set
   per filter (e.g. 100) with a WARN naming only the count. Keep the existing client-side maker/taker match as a second guard.
   Do NOT add `newHeads` unless gen-1 actually needs it (it does not today — confirm by reading the runtime; the goal is minimal CU).
   Read the installed `websockets` client and the current `OnchainWebSocket` subscribe/ack code before changing anything.
3. **Roster changes** — on `update_watched_wallets`, resubscribe the two filters (unsubscribe + subscribe on the live socket,
   or trigger a reconnect) — bounded, off the hot path, logged at INFO with counts only.
4. **Factory** — new `LogsOnlyConfirmedSourceBuilder` in `orchestrators/detection_source_factory.py`
   (mode `ON_CHAIN_CONFIRMED`, `confirmed_source == "logs_only"`), building
   `OnchainDetectionRuntime(enable_pending=False, enable_mined=True, websocket_factory=..., ...)` with the topic-filtered params.
   `ConfirmedSourceBuilder` (gen-2) stays for `mempool_reserved`. Extend `DetectionSourceRequest` with `confirmed_source` and make
   `DetectionSourceFactory` select by `(mode, confirmed_source)`; duplicate key → `ValueError`.
5. **Health** — `_build_onchain_ws_health_reporter(..., mode, confirmed_source)` returns a reporter for `on_chain_pending` AND
   for `on_chain_confirmed` + `logs_only` (stream label `orderfilled`), never for gen-2 `mempool_reserved`.
   The pending-copy tracker is NOT built in confirmed modes (unchanged).
6. **Provenance/dedup** — signals from this source are `provisional=False`, `confidence=MINED_AUTHORITATIVE`, carry
   `log_index`/`order_hash`; verify with a dispatch test that `CrossSourceTradeClaimPolicy` tx-level claim applies and
   `detection_signals_total{source="on_chain_confirmed"}` increments.
7. **Live env** — `config/environments/live/detection.yml` → `mode: on_chain_confirmed`, `onchain.confirmed_source: logs_only`,
   keep `pending_tx_provider: alchemy` (operator's WSS is Alchemy) and the existing keys; top-level key set stays exactly
   `{mode, onchain}`; no `token` substring anywhere in live YAML. Update the live-selects test in `tests/test_config.py` and
   the mode assertion in `tests/test_live_environment_overrides.py`; base invariants stay green.
8. **Docs** — `docs/02-configuration.md` (knob table + a cost table: logs_only ≈ free tier on Alchemy/Infura; mempool_reserved
   ≈ $54/day; on_chain_pending ≈ $27/day at $0.45/M CU; polling ≈ $0), `docs/known_limitations.md` (logs_only detects at block
   inclusion — no mempool lead; no orphan-position risk; roster changes resubscribe; provider-agnostic), `docs/audit/pending_detection_notes.md`
   (rollout: live default is now logs_only confirmed; one env var `PMCOPY__DETECTION__MODE=on_chain_pending` flips back to mempool),
   `secrets.env.example` (any `wss://` provider works for logs_only; Alchemy required for pending/mempool_reserved).
   Only touch `docs/frontend-control-plane.md` / `docs/ui/*` if a capability or view changes (it should not; run
   `python scripts/export_api_contract.py --check`).

## Tests (RED first)

- `tests/test_config.py`: knob default `mempool_reserved`; validation matrix (4 cases above); YAML default loads; live selects
  `on_chain_confirmed` + `logs_only`.
- Subscription params: maker/taker filter shapes exactly as in §2; address→topic padding lowercase; cap + WARN (caplog);
  no-watched-set falls back to today's params.
- `tests/test_onchain_detection_runtime.py`: mined-only runtime opens the two subscriptions (fake `websocket_factory` records
  params); `update_watched_wallets` triggers resubscribe; nothing pending-related is opened.
- `tests/test_detection_source_factory.py`: `(ON_CHAIN_CONFIRMED, logs_only)` → gen-1 mined-only runtime with topic-filtered
  params; `(ON_CHAIN_CONFIRMED, mempool_reserved)` → gen-2 unchanged; `(ON_CHAIN_PENDING, logs_only)` rejected at config.
- `tests/test_app_onchain_wiring.py`: health reporter built for logs_only, not for mempool_reserved; tracker not built in confirmed modes.
- `tests/test_onchain_signal_dispatch.py`: provenance + tx-level claim for a mined signal from this source.
- `tests/test_live_environment_overrides.py`: mode/knob assertions, key set unchanged.

## Verification

```bash
ruff format <touched> && ruff check <touched> && pyright <touched modules>
python -m pytest -q tests/test_config.py tests/test_live_environment_overrides.py tests/test_detection_source_factory.py \
  tests/test_onchain_ws_subscribe_params.py tests/test_onchain_detection_runtime.py tests/test_onchain_ws_health.py \
  tests/test_app_onchain_wiring.py tests/test_onchain_signal_dispatch.py tests/test_order_filled_detector.py
python -m pytest -q -x -p no:cacheprovider          # full suite must stay green (3097+)
python scripts/export_api_contract.py --check       # must still pass
```
Paper dry-run (optional, operator-run): `PMCOPY_ENVIRONMENT=paper PMCOPY__DETECTION__MODE=on_chain_confirmed
PMCOPY__DETECTION__ONCHAIN__CONFIRMED_SOURCE=logs_only python bot.py run` → log markers `detection_mode_selected | mode=on_chain_confirmed`,
`OnchainWS[orderfilled]: subscribed` (×2), `/metrics` `onchain_ws_connected{stream="orderfilled"} 1`; then Alchemy usage
should show only `logs` (+ `eth_subscribe`) CU, no `alchemy_pendingTransactions`.

## Report contract

Write a full report to `.superpowers/sdd/logs-only-confirmed/report.md` (RED evidence per slice, files, tests, commands + results,
the exact subscription JSON the runtime sends, whether `newHeads` was needed, concerns/deviations). Return only: status,
commit shas, one-line test summary, concerns. Ask before guessing if blocked (e.g. topic layout doubt, resubscribe semantics of
the WS client).
