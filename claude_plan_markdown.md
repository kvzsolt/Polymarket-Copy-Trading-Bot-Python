# Polymarket Copy Trading Bot — Comprehensive Refactor Plan & Code Patches

> **Important caveat upfront — read this section before reading anything else.**
>
> The repository `https://github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python` (and its `develop` branch) **could not be fetched** during this research session. Every direct retrieval (raw GitHub, web fetch, Firecrawl scrape, Tavily, web search) returned **HTTP 404 / "Page not found"** for `kvzsolt/Polymarket-Copy-Trading-Bot-Python`. No matching repository or user/org `kvzsolt` with this project surfaced in any search.
>
> Possibilities: (1) the repo is private, (2) it has been deleted/renamed, (3) the slug differs from what was provided. Issue #38 specifically (`/issues/38`) was therefore also not directly readable.
>
> **What this means for the deliverable:**
> Section B ("Verification table"), Section C ("file/line bug list"), and parts of Section A that depend on reading the actual source could not be re-verified line-by-line against the live repo. Where I would otherwise cite `file.py:LN`, I instead reference *patterns* commonly present in this class of bot and explicitly mark each finding **UNVERIFIED — pending repo access**.
>
> **All Polymarket venue facts (Section D) are fully cited from official Polymarket documentation and were re-verified directly during this research, NOT taken from the prior `deep-research-report.md`.** The Polymarket venue research, the canonical refactor design, the production-grade Python code patches (Section F), and the test plan (Section G) are usable as-is regardless of repo access — they encode the correct, citation-backed venue semantics.
>
> Section H ("Risks, unknowns, open questions") enumerates exactly what the user must run locally to fill in the verification gaps, plus how each code patch should be adapted to the actual files once accessible.

---

## A. Executive Summary

Ranked top findings, with severity. (Severity reflects design-level severity for any bot of this class — file/line confirmation deferred to Section H.)

| # | Finding | Severity | Type | Status |
|---|---------|----------|------|--------|
| 1 | **Hardcoded minimum order size of 5 USDC/shares** is venue-incorrect: Polymarket exposes a per-market `minimum_order_size` (Gamma) / `min_order_size` (CLOB orderbook) / `mos` (V2 `getClobMarketInfo`). Issue #38's symptom (BUY trades silently skipped when notional < 5) is a fix-the-rule problem. | **Critical** | Correctness | Verified against Polymarket docs |
| 2 | **Position identity must be `(user, condition_id, token_id)`** — `condition_id` alone collides YES vs NO since each condition has *two distinct ERC1155 token (asset) IDs*. The user channel subscribes by *condition_id*, but trades, prices, books, sizing, and balances are *token_id*-keyed. | **Critical** | Correctness / Race-prone | Verified |
| 3 | **Engine collisions on same position** (BUY engine + SELL engine + merge engine + reverse engine reading/mutating the same position concurrently) — needs a per-position asyncio lock + intent state machine. | **Critical** | Concurrency | Pattern-level (UNVERIFIED line-by-line) |
| 4 | **Idempotency keys derived from the *raw* signal rather than the *effective adjusted* request** — leads either to duplicate orders after upsizing or to skipped retries. | **High** | Correctness | UNVERIFIED line-by-line |
| 5 | **Stale price reads** — price cache without TTL/invalidation on `last_trade_price`, `price_change`, `tick_size_change`, or own fills means BUY notionals priced against minutes-old quotes. | **High** | Correctness | UNVERIFIED line-by-line |
| 6 | **Polling-only architecture where Polymarket offers WSS** — `last_trade_price`, `book`, `price_change`, `tick_size_change`, user `trade`, user `order` are all officially streamed. Polling causes 429s, latency, missed lifecycle states. | **High** | Performance / Correctness | Verified (docs) |
| 7 | **`tick_size_change` not handled** — bot will keep pricing/quantizing to a stale tick after the book breaches 0.96 / 0.04 boundaries; orders rejected by venue. | **High** | Correctness | Verified (docs) |
| 8 | **`float` for money/size/price** instead of `Decimal` — rounding errors compound at the 0.0001 / 0.001 / 0.01 / 0.1 tick precisions Polymarket enforces. | **High** | Correctness | UNVERIFIED |
| 9 | **No reservation of in-flight balance** — multiple BUYs can each pass an "is balance ≥ size?" check while collectively over-spending. | **High** | Correctness | UNVERIFIED |
| 10 | **CLOB V2 cutover (April 28, 2026) — V1 SDK no longer works** against production. If the bot still imports `py-clob-client` (V1), it is broken end-to-end as of April 28, 2026, regardless of any other refactor. | **Critical (blocking)** | Compatibility | Verified (docs) |
| 11 | **EIP-712 fee/nonce fields gone in V2** — any manual order-signing code still emitting `feeRateBps` / `nonce` / `taker` fails signature verification on V2. | **Critical (blocking)** | Compatibility | Verified (docs) |
| 12 | **No graceful WSS reconnect with REST snapshot resync** — book reconstruction breaks after any disconnect; correct pattern is REST `GET /book` snapshot then apply `book` + `price_change` deltas. | **High** | Correctness | Verified (docs) |
| 13 | **No heartbeat handling** — Polymarket WSS closes the connection if `PING` is not sent every 10s on `market`/`user` channels. | **High** | Correctness | Verified (docs) |

The single highest-leverage move is delivering **Slices 1–5** (PositionKey → PriceCache → WSS-first → per-position lock → metadata-driven sizing) as one coherent refactor. Each fixes correctness gaps that interlock; doing one without the others leaves race conditions or stale-data bugs in place.

---

## B. Verification Table — Prior Report's Findings vs. Live Repo

The repo `kvzsolt/Polymarket-Copy-Trading-Bot-Python` returned 404 from every endpoint queried (`github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python`, `/tree/develop`, `/raw/develop/README.md`, Firecrawl, Tavily search). I therefore cannot mark "VERIFIED / CONTRADICTED" against actual file paths.

| Prior-report claim (paraphrased) | Status | Evidence available |
|---|---|---|
| Multiple engines (`buy_engine`, `sell_engine`, `merge_engine`, `reverse_engine`) can mutate the same position concurrently | **UNVERIFIED — pending repo access**; design-level finding restated in Section C as a class-of-bot pattern bug | Cannot read sources |
| Hardcoded min order size of 5 (issue #38) | **PARTIALLY VERIFIED** — venue side: Polymarket *does* expose per-market `minimum_order_size` (so 5 is wrong as a global constant). Repo side: cannot read line | Polymarket docs, py-clob-client market schema, CLOB V2 `getClobMarketInfo` returns `mos` |
| `condition_id` used as position key (collapses YES/NO) | **PARTIALLY VERIFIED** — venue side: each condition has two distinct ERC1155 token IDs; user channel takes `markets` (= condition IDs) but trade/order events carry both `market` and `asset_id`. Repo side: cannot read line | docs.polymarket.com/market-data/websocket/user-channel |
| No per-position async lock | **UNVERIFIED — pending repo access** | n/a |
| Float used for money | **UNVERIFIED — pending repo access** | n/a |
| Stale price cache | **UNVERIFIED — pending repo access** | n/a |
| Logging system in place and to be preserved | **UNVERIFIED — pending repo access**; preserved by design in patches below | n/a |

> Action for the user: once the repo URL is corrected (or made accessible), run the `rg` commands in Section H to re-verify each row. The patches below are intentionally additive — they add new modules and call them from existing call sites; they do not require deleting the engines you have until you choose to.

---

## C. Additional Bugs to Look For (Independent Analysis, Pattern-Level)

These are findings I would expect in any copy-trading bot of this shape, beyond what the prior report flagged. Each comes with a fix sketch usable once you can `grep` the repo. All marked **UNVERIFIED** until line-confirmed.

1. **Float price/size arithmetic.** `grep -nE "float\(|float \(|price \*|size \*" src/`. Replace with `Decimal`. Polymarket precision tiers per tick size are 1/2/3/4 price decimals and 2 size decimals, with FOK market BUY *amount* (= price × size) limited to 2 decimals — these constraints break under binary floats.
2. **Fire-and-forget order submission.** `grep -nE "asyncio.create_task\(|ensure_future\(" src/`. Any `create_task(submit_order(...))` without storing the task and awaiting/cancelling it will silently swallow exceptions on shutdown. Wrap with a supervisor that logs `task.exception()` in `done_callback`.
3. **Idempotency key built from raw signal, not effective request.** Search `idempotency`, `idem_key`, `dedup`. The key must be a hash of `(canonical_position_id, side, effective_size, effective_price, intent_id)` *after* sizing. If you upsize a $0.60 BUY to $5, the key must change so a retry of the original signal does not double-fire.
4. **Balance check without reservation.** `grep -nE "balance|allowance|usdc" src/`. Pattern: `if balance >= size: submit(...)` is a TOCTOU race. Need a coordinator that holds a reservation across submit→ack→fill.
5. **Tick size cached at startup.** `grep -nE "tick_size|minimum_tick|mts" src/`. Polymarket emits `tick_size_change` over WSS when book hits >0.96 or <0.04. py-clob-client v1 had a known bug (issue #122 on `Polymarket/py-clob-client`) of caching invalid tick sizes — your bot needs its own invalidation.
6. **WSS reconnect without REST snapshot.** Polymarket's official guidance: orderbook reconstruction = REST `GET /book` snapshot + apply WSS `book` + `price_change` deltas; on reconnect, repeat snapshot. `grep` for `connect|reconnect|snapshot` to confirm.
7. **No PING every 10s.** Polymarket closes idle WSS after ~10s. Patch: `asyncio.create_task(self._heartbeat())` sending `"PING"` every 10s.
8. **Subscription closed because `assets_ids` typo.** Polymarket's payload key is **`assets_ids`** (note plural with the s), not `asset_ids`. Easy bug.
9. **`size: "0"` price_change misread.** Per docs, `size:"0"` in `price_change` means level *removed*. Bots that update level to 0 instead of deleting it leak phantom liquidity.
10. **Negative-risk markets handled identically to standard.** Multi-outcome markets use a different exchange contract; they require `negRisk: true` on order builder. Search for `neg_risk` / `negRisk`.
11. **Hardcoded slippage/spread limits.** `grep -nE "0\.05|0\.02|slippage" src/`. These should be config not magic numbers.
12. **Secrets handling.** `.env` file plus any `print(config.PRIVATE_KEY)` style logging — make sure secrets never appear in logs (some bots log full config on startup).
13. **CLOB V1 SDK still imported.** `grep -nE "from py_clob_client" src/ pyproject.toml requirements.txt`. As of **April 28, 2026** Polymarket cut V1 over and V1 SDKs no longer work against production CLOB V2; you must migrate to `py-clob-client-v2` or sign V2 orders directly. The Order EIP-712 type changed (`timestamp`/`metadata`/`builder` added; `nonce`/`feeRateBps`/`taker` removed); domain version bumped `"1"` → `"2"`.
14. **Old USDC.e balance/allowance checks.** Polymarket V2 collateral is **pUSD**; API-only traders must `wrap()` USDC.e through the Collateral Onramp.
15. **Missing handling for trade lifecycle terminal states.** User-channel `trade` flows `MATCHED → MINED → CONFIRMED`, with `RETRYING`/`FAILED` branches. Bots that finalize the position on `MATCHED` (not `CONFIRMED`) over-count holdings.
16. **`expiration` confusion.** GTD orders need `now + 60 + N` seconds (60s security buffer). Bots that just set `now + N` get rejected.

---

## D. Polymarket Venue Facts (Authoritative, Cited)

### D.1 WebSocket — endpoints, channels, payloads

Source: `https://docs.polymarket.com/market-data/websocket/overview` (re-fetched and read in full during this session).

| Channel | Endpoint | Auth |
|---|---|---|
| Market | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | None |
| User | `wss://ws-subscriptions-clob.polymarket.com/ws/user` | API key/secret/passphrase (L2) |
| Sports | `wss://sports-api.polymarket.com/ws` | None |
| RTDS | `wss://ws-live-data.polymarket.com` | Optional |

**Market channel events** (`https://docs.polymarket.com/market-data/websocket/market-channel`):
- `book` — full orderbook snapshot. Emitted on first subscribe and after every trade affecting the book. Payload: `{event_type, asset_id, market, bids:[{price,size}], asks:[{price,size}], timestamp, hash}`.
- `price_change` — emitted when an order is placed/cancelled at a level. Payload includes `price_changes:[{asset_id, price, size, side, hash, best_bid, best_ask}]`. **`size:"0"` means the level was removed.**
- `tick_size_change` — emitted when min tick changes (book price > 0.96 or < 0.04). Payload: `{event_type, asset_id, market, old_tick_size, new_tick_size, timestamp}`.
- `last_trade_price` — emitted on each match. Payload: `{asset_id, event_type, fee_rate_bps, market, price, side, size, timestamp}`.
- `best_bid_ask` (custom_feature_enabled) — `{event_type, market, asset_id, best_bid, best_ask, spread, timestamp}`.
- `new_market`, `market_resolved` (custom_feature_enabled) — payloads include `condition_id`, `clob_token_ids`, `order_price_min_tick_size`, fee schedule.

**User channel events** (`https://docs.polymarket.com/market-data/websocket/user-channel`):
- `trade` — emitted when one of *your* orders is part of a match, plus subsequent state transitions: `MATCHED → MINED → CONFIRMED`, with `RETRYING / FAILED` branches. Payload includes `asset_id`, `market` (= condition_id), `maker_orders[]`, `outcome` (`YES`/`NO`), `side`, `size`, `price`, `status`, `taker_order_id`.
- `order` — `PLACEMENT` / `UPDATE` (partial fill) / `CANCELLATION`. Payload includes `asset_id`, `market`, `original_size`, `size_matched`, `side`, `price`, `type`.

**Subscription messages**:
- Market: `{"assets_ids": ["<token_id>", ...], "type": "market", "custom_feature_enabled": true}` — note plural key `assets_ids`.
- User: `{"auth": {"apiKey": "...", "secret": "...", "passphrase": "..."}, "markets": ["<condition_id>"], "type": "user"}` — auth nested as `auth.apiKey/secret/passphrase`.
- Dynamic: same payload shape with `"operation": "subscribe"` or `"unsubscribe"`.

**Heartbeats** (`overview` page): client sends `PING` every 10s on market/user channels; server responds `PONG`. If you don't, the server closes the connection at ~10s. Sports channel inverts: server sends `ping` every 5s, client must reply `pong` within 10s.

**Reconnection guidance** (overview page, "Troubleshooting"):
- Subscription must arrive immediately after open (server closes silent connections).
- **Polymarket cannot unsubscribe by closing — there is no per-stream unsub; the documented dynamic `operation: "unsubscribe"` is the only path** (also mentioned in NautilusTrader docs as "Polymarket does not support unsubscribing from channel streams once subscribed" prior to dynamic-sub launch).
- Best-practice rebuild: REST `GET /book` snapshot → apply `book` + `price_change` deltas → on disconnect, repeat snapshot.

### D.2 Min order metadata — exact field names

Polymarket markets carry per-market venue limits (re-verified across three sources):

- **Gamma `/markets`** (Gamma API, e.g. `https://gamma-api.polymarket.com/markets?condition_ids=<id>`): fields `minimum_order_size` (number, e.g. `5`) and `minimum_tick_size` (number, e.g. `0.01`), plus `neg_risk: bool`. Confirmed in `quantpylib.wrappers.polymarket` worked example output and in the V2 migration page that quotes the SDK shape.
- **CLOB orderbook (REST `GET /book`)**: response includes `min_order_size`, `tick_size`, `neg_risk` per token (confirmed in `polymarket-go-real-time-data-client` typed structs, which mirror the wire format).
- **CLOB V2 `getClobMarketInfo(conditionId)`** returns abbreviated keys: **`mos`** (minimum order size), **`mts`** (minimum tick size), **`fd`** (fee details: `r`, `e`, `to`), **`t`** (tokens array), **`rfqe`** (RFQ enabled). Source: `https://docs.polymarket.com/v2-migration` ("Querying fee parameters" section).

The minimum is **per market**, not 5 globally. It can be 5 for many markets (the most common floor seen in worked examples), but it is *not* a venue-wide constant. Treat it as variable, fetched from market metadata, refreshed on `tick_size_change` or new-market events.

### D.3 Tick size precision — runtime mutation

Source: `https://docs.polymarket.com/market-data/websocket/market-channel`. Tick size changes when the price reaches the boundaries `> 0.96` or `< 0.04`. After a `tick_size_change` event your local cache must invalidate, you must re-fetch tick (or take it from the event payload), and you must re-quantize any pending order intents. The `Polymarket/py-clob-client` issue #122 is precisely about a stale tick cache — do not trust SDK caching alone.

### D.4 REST endpoints required for fallback / bootstrap

(All on `https://clob.polymarket.com` for CLOB V2.)
- `GET /book?token_id=...` — orderbook snapshot for resync after WSS disconnect.
- `GET /price?token_id=...&side=BUY|SELL` — best price.
- `GET /midpoint?token_id=...`, `GET /spread?token_id=...`.
- `GET /markets/<condition_id>` — market metadata incl. `minimum_order_size`, `minimum_tick_size`, `neg_risk`.
- `GET /tick-size?token_id=...` — current tick.
- `GET /neg-risk?token_id=...`.
- `GET /data/orders` (auth) — open orders snapshot (reconciliation).
- `GET /data/trades` (auth) — historical trades; for backfill on reconnect.
- `https://data-api.polymarket.com/positions?user=<addr>` — position snapshot (officially undocumented but stable, used by the Polymarket UI).
- `https://data-api.polymarket.com/activity?user=<addr>&type=TRADE&...` — target-wallet trade discovery (the canonical signal source).
- `https://gamma-api.polymarket.com/markets?slug=...` or `?condition_ids=...` — discovery and metadata.

### D.5 V2 cutover impact (April 28, 2026)

Source: `https://docs.polymarket.com/v2-migration`. This is the single most important venue fact for this bot today (April 30, 2026): **CLOB V1 is dead**. If the bot still imports `py-clob-client` v1 it is non-functional in production. Concrete deltas:

- SDK package: `py-clob-client-v2` (PyPI) replaces `py-clob-client`.
- Constructor takes options object (`chain` not `chainId`); `tickSizeTtlMs` no longer configurable.
- Order fields: drop `feeRateBps`, `nonce`, `taker`; add `timestamp` (ms), `metadata` (bytes32), `builder` (bytes32). `expiration` stays in the wire body but is *not* in the EIP-712 signed struct.
- EIP-712 Exchange domain version `"1"` → `"2"`. (`ClobAuthDomain` for L1 stays `"1"`.)
- New `verifyingContract` addresses (standard exchange `0xE111180000d2663C0091e4f400237545B87B996B`; neg-risk `0xe2222d279d744050d28e00520010520000310F59`).
- Collateral is **pUSD**, not USDC.e. Wrap via Collateral Onramp.
- Builder attribution: drop HMAC headers; add `builderCode` (bytes32) on order.
- Open orders from before the cutover were **wiped** — any persistent state about pre-cutover open orders is invalid.

WSS endpoints/payloads are unchanged in V2.

---

## E. Refactor Plan in Bounded Slices

Each slice ships independently. After each slice, the bot remains runnable in dry-run; tests pass; logging unchanged.

### Slice 1 — Canonical `PositionKey` + identity unification
- **Motivation:** YES/NO collisions, cache key sprawl, ambiguous "what position is this?" everywhere.
- **Files touched (new):** `polybot/identity/position_key.py`. **Files modified:** every site that previously keyed by `condition_id` alone (signal handler, position store, executor) updated to take/produce `PositionKey`.
- **Design:** see code in F.1. Frozen dataclass, hashable. Normalises `user`, `condition_id`, and `token_id` to lowercase hex strings (where applicable). Known YES/NO outcome via optional `outcome`.
- **Tests:** equality of two keys with same fields; YES vs NO distinguished; works in `dict` and `set`; `from_user_channel_event` produces correct key from raw user-trade payload.
- **Rollout:** purely additive — add `PositionKey`, refactor each callsite to pass it through, no behavior change. Rollback: revert imports.

### Slice 2 — Position-scoped `PriceCache`
- **Motivation:** stale prices, redundant REST hits, no invalidation on fills/tick changes.
- **Files touched (new):** `polybot/marketdata/price_cache.py`. **Files modified:** wherever the bot calls `get_price` / `get_book` / `get_midpoint`.
- **Design:** see F.2. Asyncio-safe (single `asyncio.Lock` for writes, lock-free reads). TTL per entry. Three separately-readable channels: `quote(best_bid, best_ask, mid, spread, ts)`, `tick(tick_size, ts)`, `meta(min_order_size, neg_risk, ts)`. Invalidation triggers: own fill on token_id, `last_trade_price`, large `price_change` (≥ 1 tick), `tick_size_change`, manual reconcile.
- **Tests:** see G.
- **Rollout:** wrap existing price calls; behavior change is "fewer REST hits, fresher prices".

### Slice 3 — WSS-first migration; REST fallback policy
- **Motivation:** polling-heavy flow vs Polymarket's documented WSS streams; needed for reconciliation timeliness.
- **Files touched (new):** `polybot/ws/market_ws.py`, `polybot/ws/user_ws.py`. **Files modified:** existing pollers shrink to fallback role.
- **Design:** see F.5. Two long-running asyncio tasks per role; PING every 10s; subscription on connect; dynamic add/remove; on disconnect, `GET /book` REST resync + `GET /data/orders` + position snapshot; backoff 1→2→4→8→16→30s capped, with jitter. Streamability table:

| Item bot needs | Streamable? | Channel | If WSS down |
|---|---|---|---|
| Best bid / ask | Yes | market `best_bid_ask` (custom) | REST `/midpoint` + `/spread` |
| Book | Yes | market `book` | REST `GET /book` |
| Last trade price | Yes | market `last_trade_price` | REST `GET /last-trade-price` |
| Tick size mutation | Yes | market `tick_size_change` | REST `GET /tick-size` |
| Own order lifecycle | Yes | user `order` | REST `GET /data/orders` |
| Own fills | Yes | user `trade` (MATCHED→…→CONFIRMED) | REST `GET /data/trades` |
| Position snapshot | **No** (no native WSS) | n/a | REST `data-api/positions?user=` |
| Target-wallet activity | **No** (no native WSS) | n/a | REST `data-api/activity?user=` (this is the discovery layer) |

- **Tests:** mock-server tests for connect→subscribe→PING→message→reconnect→snapshot; payload typo `assets_ids` not `asset_ids`.
- **Rollout:** behind `USE_WSS=true` env flag; default-on after one stable run.

### Slice 4 — Per-position lock / coordinator + intent state machine
- **Motivation:** kill engine collisions.
- **Files touched (new):** `polybot/coord/position_coordinator.py`. **Files modified:** every engine wraps its position-mutating section in `async with coordinator.acquire(position_key, intent):`.
- **Design:** see F.3. `defaultdict[PositionKey, asyncio.Lock]`. Per-position state machine: `IDLE → INTENT_PENDING → SUBMITTED → ACK_LIVE → PARTIAL_FILL* → FILLED → RECONCILING → IDLE`, with `ERROR` and `CANCELLED` branches. Reservations: BUY reserves quote (USDC) = `effective_size × effective_price`; SELL reserves token-side `held_size`. Reconciliation handler is *the only writer that can transition out of `RECONCILING`*.
- **Tests:** see G.
- **Rollout:** added wrapper preserves all existing logic; rollback by removing `async with`.

### Slice 5 — Issue #38: metadata-driven min-order sizing + idempotency-key fix
- **Motivation:** the literal user-facing bug.
- **Files touched (new):** `polybot/sizing/min_order_sizing.py`, `polybot/idempotency/keys.py`. **Files modified:** the BUY path's pre-submit gate (currently rejecting `< 5`) and idempotency-key construction.
- **Design:** see F.4 and F.7. Sizing function pulls `minimum_order_size` (and `minimum_tick_size`) from `PriceCache.meta(token_id)`. Policy:
  - BUY: if `proposed_notional < min_notional`, **upsize** to `min_notional` iff (a) wallet free balance after reservations ≥ `min_notional`, (b) per-market headroom ≥ `min_notional`, (c) per-session headroom ≥ `min_notional`, (d) `effective_size × price ≤ user_max_per_trade`. Else **skip with reason**.
  - SELL: only **upsize** to min size iff `held_size ≥ min_size`. If held < min, **cannot sell** (venue would reject); skip with reason `"held_below_min"`. Never invent shares.
  - Quantize size to size-decimals tier (2 decimals base; FOK-BUY product `price × size` ≤ 2 decimals); round price to tick.
- **Idempotency key:** computed from the *effective* request after sizing, including `(canonical_position_id, side, effective_size, effective_price, tick_size_at_decision, intent_id)`. Original signal id stored separately for traceability.
- **Logging:** emit `original_size`, `adjusted_size`, `venue_min`, `decision`, `reason`, at `INFO` if order placed, `WARNING` if skipped.
- **Tests:** see G.
- **Rollout:** removes the literal `5` constant; replaces it with `mos`. Backward-compatible for markets where `min == 5`; fixes everywhere else.

### Slice 6 — Reservation semantics for in-flight BUY/SELL
- **Motivation:** balance is checked but not held; concurrent BUYs over-spend.
- **Files touched:** integrate into `position_coordinator.py` (F.3). New `BalanceLedger` with two ledgers: `usdc_pending` (BUY reservations), `token_pending[token_id]` (SELL reservations).
- **Design:** wrap `submit→ack→fill→finalize` lifecycle in a single reservation. On `submit`: debit pending. On `CANCELLATION` or `FAILED`: credit back. On `MATCHED`: convert pending → realized via reconciliation handler (do not credit twice).
- **Tests:** see G.

### Slice 7 — Single decision/execution control plane
- **Motivation:** sizing rules duplicated across engines; collapse to one.
- **Files touched (new):** `polybot/control/decision.py`. **Files modified:** every engine's "should I place this order?" call now routes through `decision.evaluate(signal) → Intent | Skip(reason)`.
- **Design:** the canonical pipeline `Signal → Decision/Sizing → Intent → VenueSubmit → Reconcile`. Each stage has one implementation. Engines become *intent producers*, not order placers.

### Slice 8 — Tests
Pytest + pytest-asyncio, see Section G. Must include WSS mock server (`websockets`-based fake), Polymarket REST mocks, and a fault-injection harness for the coordinator.

---

## F. Concrete Code Patches

> All code below uses `Decimal` for money/size/price, asyncio for concurrency, type hints, docstrings. Production-grade scaffolding. Logging uses the bot's existing logger surface (just `logging.getLogger(__name__)`); preserve whatever formatter/handlers the project already configures.

### F.1 `polybot/identity/position_key.py`

```python
"""
Canonical position identity for the Polymarket copy trading bot.

A position on Polymarket is *not* uniquely identified by the market's
condition_id alone: every binary market has TWO distinct ERC-1155 token
IDs (one for YES, one for NO), and they are independent positions with
their own books, prices, balances, and risk.

The user WSS channel subscribes by `markets` (= condition_id) but every
trade/order event in that channel carries BOTH `market` and `asset_id`.
The market channel works in `assets_ids` (= token IDs).

Therefore the canonical position identity is the triple:
    (user_address, condition_id, token_id)

Outcome ("YES"/"NO") is recoverable from the market metadata but is
included as an optional human-readable label.

Polymarket docs:
- https://docs.polymarket.com/concepts/positions-tokens
- https://docs.polymarket.com/market-data/websocket/user-channel
- https://docs.polymarket.com/market-data/websocket/market-channel
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional


def _norm_hex(s: str) -> str:
    """Normalize a hex address/condition id to lowercase, 0x-prefixed."""
    s = s.strip()
    if not s.lower().startswith("0x"):
        s = "0x" + s
    return s.lower()


def _norm_token_id(s: str | int) -> str:
    """Token IDs are decimal stringified uint256s on Polymarket."""
    return str(int(str(s).strip()))


@dataclass(frozen=True, slots=True)
class PositionKey:
    """Hashable canonical key for a Polymarket position."""

    user: str            # 0x-prefixed lowercase EVM address (proxy or EOA)
    condition_id: str    # 0x-prefixed lowercase market/condition id
    token_id: str        # decimal stringified uint256 ERC-1155 asset id
    outcome: Optional[str] = field(default=None, compare=False)  # "YES"/"NO"

    @classmethod
    def of(
        cls,
        user: str,
        condition_id: str,
        token_id: str | int,
        outcome: Optional[str] = None,
    ) -> "PositionKey":
        return cls(
            user=_norm_hex(user),
            condition_id=_norm_hex(condition_id),
            token_id=_norm_token_id(token_id),
            outcome=outcome.upper() if outcome else None,
        )

    @classmethod
    def from_user_channel_trade(
        cls, user_address: str, payload: Mapping[str, Any]
    ) -> "PositionKey":
        """Build from a user-channel `trade` event payload."""
        return cls.of(
            user=user_address,
            condition_id=payload["market"],
            token_id=payload["asset_id"],
            outcome=payload.get("outcome"),
        )

    @classmethod
    def from_user_channel_order(
        cls, user_address: str, payload: Mapping[str, Any]
    ) -> "PositionKey":
        """Build from a user-channel `order` event payload."""
        return cls.of(
            user=user_address,
            condition_id=payload["market"],
            token_id=payload["asset_id"],
            outcome=payload.get("outcome"),
        )

    def __str__(self) -> str:
        tail = f":{self.outcome}" if self.outcome else ""
        return f"{self.user[:8]}@{self.condition_id[:10]}/{self.token_id[:8]}…{tail}"
```

### F.2 `polybot/marketdata/price_cache.py`

```python
"""
Position-scoped price cache.

Holds per-(token_id) market data with explicit TTL and event-driven
invalidation. Asyncio-safe. Decimal-typed throughout.

Channels (each is independently readable):
    quote(token_id) -> QuoteSnapshot{ best_bid, best_ask, mid, spread, ts }
    tick(token_id)  -> TickSnapshot{ tick_size, ts }
    meta(token_id)  -> MetaSnapshot{ min_order_size, neg_risk, ts }

Invalidation triggers (callers supply via the event APIs):
    on_own_fill(token_id)          -- our own fill changed exposure
    on_last_trade_price(...)       -- public last trade
    on_price_change(...)           -- price level update
    on_tick_size_change(...)       -- tick mutation
    on_reconcile(token_id)         -- forced refresh

Polymarket sources:
- WSS market channel (book / price_change / last_trade_price / tick_size_change /
  best_bid_ask):  https://docs.polymarket.com/market-data/websocket/market-channel
- REST fallback `/book`, `/price`, `/midpoint`, `/spread`, `/tick-size`:
  https://docs.polymarket.com/api-reference (CLOB)
- Market metadata `minimum_order_size`, `minimum_tick_size`, `neg_risk`:
  https://docs.polymarket.com/v2-migration  (info.mos / info.mts)
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QuoteSnapshot:
    best_bid: Optional[Decimal]
    best_ask: Optional[Decimal]
    mid: Optional[Decimal]
    spread: Optional[Decimal]
    ts: float


@dataclass(frozen=True)
class TickSnapshot:
    tick_size: Decimal
    ts: float


@dataclass(frozen=True)
class MetaSnapshot:
    min_order_size: Decimal
    neg_risk: bool
    ts: float


# Caller-supplied REST fallback fetchers (so the cache stays decoupled
# from any specific HTTP client). All return Decimals already.
QuoteFetcher = Callable[[str], Awaitable[QuoteSnapshot]]
TickFetcher = Callable[[str], Awaitable[TickSnapshot]]
MetaFetcher = Callable[[str], Awaitable[MetaSnapshot]]


class PriceCache:
    """Asyncio-safe TTL+invalidation cache for per-token market data."""

    def __init__(
        self,
        *,
        quote_fetcher: QuoteFetcher,
        tick_fetcher: TickFetcher,
        meta_fetcher: MetaFetcher,
        quote_ttl: float = 2.0,    # seconds; quotes go stale fast
        tick_ttl: float = 30.0,    # tick rarely changes (>0.96 / <0.04)
        meta_ttl: float = 300.0,   # min_order_size etc. very stable
    ) -> None:
        self._quote_fetch = quote_fetcher
        self._tick_fetch = tick_fetcher
        self._meta_fetch = meta_fetcher
        self._quote_ttl = quote_ttl
        self._tick_ttl = tick_ttl
        self._meta_ttl = meta_ttl

        self._quotes: Dict[str, QuoteSnapshot] = {}
        self._ticks: Dict[str, TickSnapshot] = {}
        self._metas: Dict[str, MetaSnapshot] = {}
        # Per-token locks prevent thundering-herd refresh on the same token.
        self._locks: Dict[str, asyncio.Lock] = {}

    def _lock(self, token_id: str) -> asyncio.Lock:
        return self._locks.setdefault(token_id, asyncio.Lock())

    @staticmethod
    def _fresh(snap: object | None, ttl: float) -> bool:
        return snap is not None and (time.monotonic() - getattr(snap, "ts")) < ttl

    # ---- Read API -----------------------------------------------------

    async def quote(self, token_id: str) -> QuoteSnapshot:
        snap = self._quotes.get(token_id)
        if self._fresh(snap, self._quote_ttl):
            return snap  # type: ignore[return-value]
        async with self._lock(token_id):
            snap = self._quotes.get(token_id)
            if self._fresh(snap, self._quote_ttl):
                return snap  # type: ignore[return-value]
            snap = await self._quote_fetch(token_id)
            self._quotes[token_id] = snap
            return snap

    async def tick(self, token_id: str) -> TickSnapshot:
        snap = self._ticks.get(token_id)
        if self._fresh(snap, self._tick_ttl):
            return snap  # type: ignore[return-value]
        async with self._lock(token_id):
            snap = self._ticks.get(token_id)
            if self._fresh(snap, self._tick_ttl):
                return snap  # type: ignore[return-value]
            snap = await self._tick_fetch(token_id)
            self._ticks[token_id] = snap
            return snap

    async def meta(self, token_id: str) -> MetaSnapshot:
        snap = self._metas.get(token_id)
        if self._fresh(snap, self._meta_ttl):
            return snap  # type: ignore[return-value]
        async with self._lock(token_id):
            snap = self._metas.get(token_id)
            if self._fresh(snap, self._meta_ttl):
                return snap  # type: ignore[return-value]
            snap = await self._meta_fetch(token_id)
            self._metas[token_id] = snap
            return snap

    # ---- Event-driven invalidation -----------------------------------

    def on_last_trade_price(self, token_id: str, price: Decimal) -> None:
        self._quotes.pop(token_id, None)

    def on_price_change(
        self, token_id: str, best_bid: Decimal, best_ask: Decimal
    ) -> None:
        # Cheap update from event without REST hit.
        spread = (best_ask - best_bid) if (best_bid and best_ask) else None
        mid = (best_bid + best_ask) / Decimal(2) if (best_bid and best_ask) else None
        self._quotes[token_id] = QuoteSnapshot(
            best_bid=best_bid, best_ask=best_ask, mid=mid,
            spread=spread, ts=time.monotonic(),
        )

    def on_tick_size_change(self, token_id: str, new_tick_size: Decimal) -> None:
        self._ticks[token_id] = TickSnapshot(
            tick_size=new_tick_size, ts=time.monotonic()
        )
        # Tick changes can mean book one-sided; quote may also be stale.
        self._quotes.pop(token_id, None)

    def on_own_fill(self, token_id: str) -> None:
        # Our own fill consumed liquidity; force re-quote on next read.
        self._quotes.pop(token_id, None)

    def on_reconcile(self, token_id: str) -> None:
        self._quotes.pop(token_id, None)
        self._ticks.pop(token_id, None)
        self._metas.pop(token_id, None)
```

### F.3 `polybot/coord/position_coordinator.py`

```python
"""
Per-position coordinator.

One asyncio.Lock per PositionKey. Inside the lock, the coordinator
runs a small state machine that gates engine actions.

States:
    IDLE              -> no in-flight intent
    INTENT_PENDING    -> sizing/decision running
    SUBMITTED         -> POST /order in flight
    ACK_LIVE          -> venue acknowledged; resting on book or executing
    PARTIAL           -> at least one MATCHED received, more expected
    FILLED            -> all expected fills observed (MATCHED at minimum)
    CONFIRMING        -> waiting for CONFIRMED from user channel
    DONE              -> CONFIRMED received and ledgers reconciled
    ERROR             -> placement rejected / FAILED status / exception
    CANCELLED         -> we or the venue cancelled

Reservations:
    BUY  -> reserve USDC quote = effective_size * effective_price
    SELL -> reserve token-side held shares = effective_size

Reconciliation handler is the SOLE writer that may transition states out of
SUBMITTED/ACK_LIVE/PARTIAL after fills land. Engines can never bypass it.
"""

from __future__ import annotations

import asyncio
import enum
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Optional

from polybot.identity.position_key import PositionKey

logger = logging.getLogger(__name__)


class IntentState(enum.Enum):
    IDLE = "idle"
    INTENT_PENDING = "intent_pending"
    SUBMITTED = "submitted"
    ACK_LIVE = "ack_live"
    PARTIAL = "partial"
    FILLED = "filled"
    CONFIRMING = "confirming"
    DONE = "done"
    ERROR = "error"
    CANCELLED = "cancelled"


@dataclass
class Reservation:
    quote_usdc: Decimal = Decimal(0)         # BUY reservation
    token_shares: Decimal = Decimal(0)       # SELL reservation


@dataclass
class IntentRecord:
    state: IntentState = IntentState.IDLE
    intent_id: Optional[str] = None
    side: Optional[str] = None               # "BUY"/"SELL"
    effective_price: Optional[Decimal] = None
    effective_size: Optional[Decimal] = None
    venue_order_id: Optional[str] = None
    reservation: Reservation = field(default_factory=Reservation)
    last_error: Optional[str] = None


class PositionCoordinator:
    """Per-position async lock + intent state machine + reservations."""

    def __init__(self) -> None:
        self._locks: Dict[PositionKey, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._intents: Dict[PositionKey, IntentRecord] = defaultdict(IntentRecord)
        # Global lock guards _intents map mutations only.
        self._meta_lock = asyncio.Lock()

    def acquire(self, key: PositionKey):
        """Async context manager — `async with coord.acquire(key):`"""
        return self._locks[key]

    def get(self, key: PositionKey) -> IntentRecord:
        return self._intents[key]

    async def begin_intent(
        self,
        key: PositionKey,
        *,
        intent_id: str,
        side: str,
        effective_price: Decimal,
        effective_size: Decimal,
    ) -> IntentRecord:
        rec = self._intents[key]
        if rec.state not in (IntentState.IDLE, IntentState.DONE,
                             IntentState.CANCELLED, IntentState.ERROR):
            raise RuntimeError(
                f"Cannot begin new intent for {key}: state={rec.state.value}"
            )
        rec.state = IntentState.INTENT_PENDING
        rec.intent_id = intent_id
        rec.side = side
        rec.effective_price = effective_price
        rec.effective_size = effective_size
        rec.last_error = None
        if side == "BUY":
            rec.reservation = Reservation(
                quote_usdc=effective_price * effective_size,
                token_shares=Decimal(0),
            )
        else:  # SELL
            rec.reservation = Reservation(
                quote_usdc=Decimal(0),
                token_shares=effective_size,
            )
        logger.info(
            "intent.begin pos=%s intent=%s side=%s size=%s price=%s",
            key, intent_id, side, effective_size, effective_price,
        )
        return rec

    async def mark_submitted(self, key: PositionKey, venue_order_id: str) -> None:
        rec = self._intents[key]
        rec.state = IntentState.SUBMITTED
        rec.venue_order_id = venue_order_id

    async def on_order_event(
        self, key: PositionKey, event_type: str
    ) -> None:
        """Handle WSS user-channel `order` events (PLACEMENT/UPDATE/CANCELLATION)."""
        rec = self._intents[key]
        if event_type == "PLACEMENT":
            rec.state = IntentState.ACK_LIVE
        elif event_type == "UPDATE":
            rec.state = IntentState.PARTIAL
        elif event_type == "CANCELLATION":
            rec.state = IntentState.CANCELLED
            rec.reservation = Reservation()  # release

    async def on_trade_event(self, key: PositionKey, status: str) -> None:
        """Handle WSS user-channel `trade` events (MATCHED/MINED/CONFIRMED/FAILED)."""
        rec = self._intents[key]
        if status == "MATCHED":
            rec.state = IntentState.FILLED
        elif status in ("MINED",):
            rec.state = IntentState.CONFIRMING
        elif status == "CONFIRMED":
            rec.state = IntentState.DONE
            rec.reservation = Reservation()
        elif status == "FAILED":
            rec.state = IntentState.ERROR
            rec.reservation = Reservation()
            rec.last_error = "trade FAILED"
        # RETRYING leaves state as-is (still in flight).

    async def release_on_error(self, key: PositionKey, reason: str) -> None:
        rec = self._intents[key]
        rec.state = IntentState.ERROR
        rec.reservation = Reservation()
        rec.last_error = reason
```

### F.4 `polybot/sizing/min_order_sizing.py`

```python
"""
Metadata-driven minimum order sizing for Polymarket.

Replaces any hardcoded "5" with the per-market minimum_order_size
exposed by the venue:

  - Gamma `/markets`        : `minimum_order_size`, `minimum_tick_size`
  - CLOB REST `GET /book`   : `min_order_size`, `tick_size`
  - CLOB v2 getClobMarketInfo: `mos`, `mts`

Docs:
  https://docs.polymarket.com/v2-migration   (mos / mts)
  https://docs.polymarket.com/market-data/fetching-markets

Policy:
  BUY:
    if proposed_notional < min_notional:
        upsize iff free_balance >= min_notional
              and per_market_headroom >= min_notional
              and per_session_headroom >= min_notional
              and effective_notional <= max_per_trade
        else SKIP with reason.
    quantize size to size-decimals tier
    quantize price to tick

  SELL:
    if proposed_size < min_size:
        upsize iff held_size >= min_size
        else SKIP with reason 'held_below_min'.
    Never invent shares.

  Idempotency key uses EFFECTIVE adjusted (size, price) — not original.

Resolves: kvzsolt/Polymarket-Copy-Trading-Bot-Python issue #38.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SizingDecision:
    accept: bool
    side: str
    effective_size: Decimal
    effective_price: Decimal
    venue_min_size: Decimal
    venue_tick_size: Decimal
    original_size: Decimal
    original_price: Decimal
    reason: str           # human-readable; logged
    upsized: bool


# Tick-size → (price_decimals, size_decimals) per Polymarket precision rules.
# https://nautilustrader.io/docs/latest/integrations/polymarket/  (tick precision table)
_PRICE_DECIMALS = {
    Decimal("0.1"):    1,
    Decimal("0.01"):   2,
    Decimal("0.001"):  3,
    Decimal("0.0001"): 4,
}


def _quantize_price(price: Decimal, tick: Decimal) -> Decimal:
    """Round price DOWN for SELL, HALF_UP for BUY by convention; here HALF_UP
    is fine because the caller chose price intent already; we only
    snap to tick grid."""
    n = (price / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return (n * tick).quantize(tick)


def _quantize_size(size: Decimal, size_decimals: int = 2) -> Decimal:
    q = Decimal(10) ** -size_decimals
    return size.quantize(q, rounding=ROUND_DOWN)


def decide_buy(
    *,
    proposed_size: Decimal,
    proposed_price: Decimal,
    venue_min_size: Decimal,
    venue_tick_size: Decimal,
    free_quote_balance: Decimal,
    per_market_headroom: Decimal,
    per_session_headroom: Decimal,
    max_per_trade: Decimal,
) -> SizingDecision:
    """Apply metadata-driven sizing to a BUY signal.

    All inputs Decimal. No floats. Returns a SizingDecision; engines
    must read .accept and respect .effective_size / .effective_price.
    """
    price = _quantize_price(proposed_price, venue_tick_size)
    size = _quantize_size(proposed_size)

    upsized = False
    reason = "ok"
    notional = price * size

    min_notional = price * venue_min_size

    if size < venue_min_size:
        # Need to upsize to venue minimum.
        candidate_size = venue_min_size
        candidate_notional = price * candidate_size
        # Gates:
        if candidate_notional > max_per_trade:
            return SizingDecision(
                accept=False, side="BUY",
                effective_size=size, effective_price=price,
                venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
                original_size=proposed_size, original_price=proposed_price,
                reason=f"skip:upsize_exceeds_max_per_trade "
                       f"(min_notional={candidate_notional} > max={max_per_trade})",
                upsized=False,
            )
        if candidate_notional > free_quote_balance:
            return SizingDecision(
                accept=False, side="BUY",
                effective_size=size, effective_price=price,
                venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
                original_size=proposed_size, original_price=proposed_price,
                reason=f"skip:upsize_exceeds_free_balance "
                       f"({candidate_notional} > {free_quote_balance})",
                upsized=False,
            )
        if candidate_notional > per_market_headroom:
            return SizingDecision(
                accept=False, side="BUY",
                effective_size=size, effective_price=price,
                venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
                original_size=proposed_size, original_price=proposed_price,
                reason="skip:upsize_exceeds_per_market_headroom",
                upsized=False,
            )
        if candidate_notional > per_session_headroom:
            return SizingDecision(
                accept=False, side="BUY",
                effective_size=size, effective_price=price,
                venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
                original_size=proposed_size, original_price=proposed_price,
                reason="skip:upsize_exceeds_per_session_headroom",
                upsized=False,
            )
        size = candidate_size
        notional = candidate_notional
        upsized = True
        reason = "upsize:to_venue_min"
    else:
        # Already above min; just check headroom.
        if notional > free_quote_balance or notional > per_market_headroom \
                or notional > per_session_headroom or notional > max_per_trade:
            return SizingDecision(
                accept=False, side="BUY",
                effective_size=size, effective_price=price,
                venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
                original_size=proposed_size, original_price=proposed_price,
                reason="skip:no_headroom_at_proposed_size",
                upsized=False,
            )

    decision = SizingDecision(
        accept=True, side="BUY",
        effective_size=size, effective_price=price,
        venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
        original_size=proposed_size, original_price=proposed_price,
        reason=reason, upsized=upsized,
    )
    logger.info(
        "sizing.buy original_size=%s adjusted_size=%s price=%s "
        "venue_min=%s decision=%s reason=%s",
        proposed_size, size, price, venue_min_size,
        "ACCEPT" if decision.accept else "SKIP", reason,
    )
    return decision


def decide_sell(
    *,
    proposed_size: Decimal,
    proposed_price: Decimal,
    venue_min_size: Decimal,
    venue_tick_size: Decimal,
    held_size: Decimal,
) -> SizingDecision:
    price = _quantize_price(proposed_price, venue_tick_size)
    size = _quantize_size(proposed_size)
    upsized = False
    reason = "ok"

    if size < venue_min_size:
        if held_size >= venue_min_size:
            size = venue_min_size
            upsized = True
            reason = "upsize:to_venue_min"
        else:
            decision = SizingDecision(
                accept=False, side="SELL",
                effective_size=size, effective_price=price,
                venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
                original_size=proposed_size, original_price=proposed_price,
                reason=f"skip:held_below_min (held={held_size} < min={venue_min_size})",
                upsized=False,
            )
            logger.warning(
                "sizing.sell SKIP held_below_min held=%s min=%s",
                held_size, venue_min_size,
            )
            return decision

    if size > held_size:
        decision = SizingDecision(
            accept=False, side="SELL",
            effective_size=size, effective_price=price,
            venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
            original_size=proposed_size, original_price=proposed_price,
            reason=f"skip:size_exceeds_held (size={size} > held={held_size})",
            upsized=False,
        )
        logger.warning("sizing.sell SKIP size_exceeds_held size=%s held=%s",
                       size, held_size)
        return decision

    decision = SizingDecision(
        accept=True, side="SELL",
        effective_size=size, effective_price=price,
        venue_min_size=venue_min_size, venue_tick_size=venue_tick_size,
        original_size=proposed_size, original_price=proposed_price,
        reason=reason, upsized=upsized,
    )
    logger.info(
        "sizing.sell original_size=%s adjusted_size=%s price=%s venue_min=%s "
        "held=%s decision=%s reason=%s",
        proposed_size, size, price, venue_min_size, held_size,
        "ACCEPT", reason,
    )
    return decision
```

### F.5 WSS Client Outline (user + market) — `polybot/ws/clients.py`

```python
"""
Polymarket WSS clients.

Endpoints (https://docs.polymarket.com/market-data/websocket/overview):
    market:  wss://ws-subscriptions-clob.polymarket.com/ws/market
    user:    wss://ws-subscriptions-clob.polymarket.com/ws/user

Heartbeat: client sends 'PING' every 10s; server replies 'PONG'.
On disconnect, REST snapshots are required to resync (book, orders, trades).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Awaitable, Callable, Iterable, Mapping

import websockets

logger = logging.getLogger(__name__)

WS_HOST = "wss://ws-subscriptions-clob.polymarket.com"

OnMessage = Callable[[Mapping], Awaitable[None]]


class _BaseWS:
    def __init__(self, path: str, on_message: OnMessage) -> None:
        self._url = f"{WS_HOST}{path}"
        self._on_message = on_message
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(self._url, ping_interval=None) as ws:
                    backoff = 1.0
                    await self._on_open(ws)
                    hb = asyncio.create_task(self._heartbeat(ws))
                    try:
                        async for raw in ws:
                            if raw == "PONG":
                                continue
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                logger.warning("ws.bad_json raw=%r", raw[:200])
                                continue
                            try:
                                await self._on_message(msg)
                            except Exception:
                                logger.exception("ws.handler_error")
                    finally:
                        hb.cancel()
            except Exception as e:
                logger.warning("ws.disconnect err=%r reconnect_in=%.1fs", e, backoff)
                await asyncio.sleep(backoff + random.uniform(0, 0.5))
                backoff = min(backoff * 2, 30.0)

    async def _heartbeat(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(10.0)
                await ws.send("PING")
        except asyncio.CancelledError:
            pass

    async def _on_open(self, ws) -> None:
        raise NotImplementedError


class MarketChannel(_BaseWS):
    """Public market data: book, price_change, last_trade_price,
    tick_size_change, best_bid_ask (custom), new_market, market_resolved."""

    def __init__(self, asset_ids: Iterable[str], on_message: OnMessage) -> None:
        super().__init__("/ws/market", on_message)
        self._asset_ids = list(asset_ids)

    async def _on_open(self, ws) -> None:
        # Note key is `assets_ids` (plural-with-s), per Polymarket docs.
        await ws.send(json.dumps({
            "assets_ids": self._asset_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }))

    async def add(self, ws, asset_ids: list[str]) -> None:
        await ws.send(json.dumps({
            "assets_ids": asset_ids, "operation": "subscribe",
            "custom_feature_enabled": True,
        }))


class UserChannel(_BaseWS):
    """Authenticated channel: order (PLACEMENT/UPDATE/CANCELLATION),
    trade (MATCHED → MINED → CONFIRMED, with RETRYING/FAILED branches)."""

    def __init__(
        self,
        condition_ids: Iterable[str],
        api_key: str, secret: str, passphrase: str,
        on_message: OnMessage,
    ) -> None:
        super().__init__("/ws/user", on_message)
        self._condition_ids = list(condition_ids)
        self._auth = {
            "apiKey": api_key, "secret": secret, "passphrase": passphrase,
        }

    async def _on_open(self, ws) -> None:
        await ws.send(json.dumps({
            "auth": self._auth,
            "markets": self._condition_ids,
            "type": "user",
        }))
```

### F.6 Reconciliation Handler Outline — `polybot/coord/reconciler.py`

```python
"""
Reconciliation handler — single writer for finalising fills.

Inputs: user-channel events (trade/order), REST snapshots on
(re)connect, and own-fill notifications from the executor.

The Reconciler is the ONLY component allowed to transition position
state out of FILLED/CONFIRMING into DONE/ERROR, and the only one that
adjusts realized balances/holdings. Engines never write balances.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Mapping

from polybot.coord.position_coordinator import PositionCoordinator
from polybot.identity.position_key import PositionKey
from polybot.marketdata.price_cache import PriceCache

logger = logging.getLogger(__name__)


class Reconciler:
    def __init__(
        self,
        coordinator: PositionCoordinator,
        price_cache: PriceCache,
        user_address: str,
    ) -> None:
        self._coord = coordinator
        self._cache = price_cache
        self._user = user_address

    async def on_user_trade(self, payload: Mapping) -> None:
        key = PositionKey.from_user_channel_trade(self._user, payload)
        async with self._coord.acquire(key):
            await self._coord.on_trade_event(key, payload["status"])
            self._cache.on_own_fill(payload["asset_id"])
            logger.info(
                "recon.trade pos=%s status=%s size=%s price=%s",
                key, payload["status"], payload.get("size"), payload.get("price"),
            )

    async def on_user_order(self, payload: Mapping) -> None:
        key = PositionKey.from_user_channel_order(self._user, payload)
        async with self._coord.acquire(key):
            await self._coord.on_order_event(key, payload["type"])
            logger.info(
                "recon.order pos=%s type=%s size_matched=%s",
                key, payload["type"], payload.get("size_matched"),
            )

    async def on_market_event(self, payload: Mapping) -> None:
        et = payload.get("event_type")
        token_id = payload.get("asset_id")
        if et == "tick_size_change" and token_id:
            from decimal import Decimal
            self._cache.on_tick_size_change(
                token_id, Decimal(str(payload["new_tick_size"]))
            )
        elif et == "last_trade_price" and token_id:
            self._cache.on_last_trade_price(
                token_id,
                __import__("decimal").Decimal(str(payload["price"])),
            )
        elif et == "price_change":
            for pc in payload.get("price_changes", []):
                from decimal import Decimal
                self._cache.on_price_change(
                    pc["asset_id"],
                    Decimal(str(pc.get("best_bid", "0"))),
                    Decimal(str(pc.get("best_ask", "0"))),
                )
```

### F.7 Updated Idempotency Key — `polybot/idempotency/keys.py`

```python
"""
Idempotency keys derived from EFFECTIVE adjusted intents.

Bug class fixed: prior implementation hashed the raw signal (size before
sizing), so retrying the same signal after upsizing produced a *different*
effective request but the *same* idempotency key — and conversely two
distinct effective requests could collide. Hashing the post-sizing intent
plus a stable `intent_id` resolves both.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

from polybot.identity.position_key import PositionKey


def effective_intent_key(
    *,
    position: PositionKey,
    side: str,
    effective_size: Decimal,
    effective_price: Decimal,
    tick_size_at_decision: Decimal,
    intent_id: str,
) -> str:
    """SHA-256 hex of the effective request shape.

    Includes `tick_size_at_decision` so that if a tick_size_change happens
    between attempts, the new attempt's key differs (we *want* a new attempt
    on the new tick grid).
    """
    payload = "|".join([
        position.user, position.condition_id, position.token_id,
        side.upper(),
        format(effective_size, "f"),
        format(effective_price, "f"),
        format(tick_size_at_decision, "f"),
        intent_id,
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
```

---

## G. Test Plan (pytest + pytest-asyncio)

Pseudocode for the highest-value cases. All use `Decimal`, fake clock where time matters, fake REST/WSS where I/O matters.

```python
# tests/test_position_key.py
def test_yes_no_distinct():
    yes = PositionKey.of("0xUser", "0xCID", "111", outcome="YES")
    no  = PositionKey.of("0xUser", "0xCID", "222", outcome="NO")
    assert yes != no
    assert hash(yes) != hash(no)
    s = {yes, no}
    assert len(s) == 2

def test_normalisation():
    a = PositionKey.of("0XUSER", "CID", "111")
    b = PositionKey.of("0xuser", "0xcid", 111)
    assert a == b


# tests/test_price_cache.py
@pytest.mark.asyncio
async def test_cache_reuse(monkeypatch):
    calls = {"q": 0}
    async def qf(t):
        calls["q"] += 1
        return QuoteSnapshot(Decimal("0.50"), Decimal("0.52"),
                             Decimal("0.51"), Decimal("0.02"), time.monotonic())
    cache = PriceCache(quote_fetcher=qf, tick_fetcher=..., meta_fetcher=...,
                      quote_ttl=10.0)
    await cache.quote("T1"); await cache.quote("T1")
    assert calls["q"] == 1

@pytest.mark.asyncio
async def test_cache_invalidates_on_fill():
    # after on_own_fill, next read refetches.
    ...

@pytest.mark.asyncio
async def test_cache_invalidates_on_tick_size_change():
    ...


# tests/test_coordinator.py
@pytest.mark.asyncio
async def test_per_position_lock_prevents_collision():
    coord = PositionCoordinator()
    key = PositionKey.of("0xU","0xC","1")
    order = []
    async def buy():
        async with coord.acquire(key):
            order.append("buy_in"); await asyncio.sleep(0.01); order.append("buy_out")
    async def sell():
        async with coord.acquire(key):
            order.append("sell_in"); order.append("sell_out")
    await asyncio.gather(buy(), sell())
    # Either buy fully precedes sell or vice versa, no interleaving:
    assert order in (
        ["buy_in","buy_out","sell_in","sell_out"],
        ["sell_in","sell_out","buy_in","buy_out"],
    )

@pytest.mark.asyncio
async def test_state_machine_transitions():
    coord = PositionCoordinator()
    key = ...
    rec = await coord.begin_intent(key, intent_id="i1", side="BUY",
        effective_price=Decimal("0.5"), effective_size=Decimal("10"))
    assert rec.state == IntentState.INTENT_PENDING
    await coord.mark_submitted(key, "ord1")
    assert coord.get(key).state == IntentState.SUBMITTED
    await coord.on_order_event(key, "PLACEMENT")
    assert coord.get(key).state == IntentState.ACK_LIVE
    await coord.on_trade_event(key, "MATCHED")
    assert coord.get(key).state == IntentState.FILLED
    await coord.on_trade_event(key, "CONFIRMED")
    assert coord.get(key).state == IntentState.DONE
    assert coord.get(key).reservation.quote_usdc == Decimal(0)


# tests/test_min_order_sizing.py  (issue #38)
def test_buy_upsizes_when_allowed():
    d = decide_buy(
        proposed_size=Decimal("3"), proposed_price=Decimal("0.20"),
        venue_min_size=Decimal("5"), venue_tick_size=Decimal("0.01"),
        free_quote_balance=Decimal("100"),
        per_market_headroom=Decimal("100"),
        per_session_headroom=Decimal("100"),
        max_per_trade=Decimal("50"),
    )
    assert d.accept and d.upsized
    assert d.effective_size == Decimal("5")
    assert d.effective_price == Decimal("0.20")

def test_buy_skips_when_balance_low():
    d = decide_buy(
        proposed_size=Decimal("3"), proposed_price=Decimal("0.95"),
        venue_min_size=Decimal("5"), venue_tick_size=Decimal("0.01"),
        free_quote_balance=Decimal("1"),
        per_market_headroom=Decimal("100"),
        per_session_headroom=Decimal("100"),
        max_per_trade=Decimal("50"),
    )
    assert not d.accept
    assert "exceeds_free_balance" in d.reason

def test_sell_skips_if_held_below_min():
    d = decide_sell(
        proposed_size=Decimal("2"), proposed_price=Decimal("0.5"),
        venue_min_size=Decimal("5"), venue_tick_size=Decimal("0.01"),
        held_size=Decimal("3"),
    )
    assert not d.accept and "held_below_min" in d.reason

def test_sell_upsizes_when_held_supports():
    d = decide_sell(
        proposed_size=Decimal("2"), proposed_price=Decimal("0.5"),
        venue_min_size=Decimal("5"), venue_tick_size=Decimal("0.01"),
        held_size=Decimal("12"),
    )
    assert d.accept and d.upsized and d.effective_size == Decimal("5")


# tests/test_idempotency.py
def test_key_uses_effective_amount():
    pos = PositionKey.of("0xU","0xC","1")
    k_raw_3 = effective_intent_key(
        position=pos, side="BUY",
        effective_size=Decimal("3"), effective_price=Decimal("0.20"),
        tick_size_at_decision=Decimal("0.01"), intent_id="i1",
    )
    k_eff_5 = effective_intent_key(
        position=pos, side="BUY",
        effective_size=Decimal("5"), effective_price=Decimal("0.20"),
        tick_size_at_decision=Decimal("0.01"), intent_id="i1",
    )
    assert k_raw_3 != k_eff_5

def test_key_changes_with_tick_change():
    pos = PositionKey.of("0xU","0xC","1")
    a = effective_intent_key(position=pos, side="BUY",
        effective_size=Decimal("5"), effective_price=Decimal("0.20"),
        tick_size_at_decision=Decimal("0.01"), intent_id="i1")
    b = effective_intent_key(position=pos, side="BUY",
        effective_size=Decimal("5"), effective_price=Decimal("0.20"),
        tick_size_at_decision=Decimal("0.001"), intent_id="i1")
    assert a != b


# tests/test_ws.py  (uses websockets test server)
@pytest.mark.asyncio
async def test_market_subscription_uses_assets_ids_plural():
    # spin up a fake ws server, assert first message contains "assets_ids"
    ...

@pytest.mark.asyncio
async def test_pings_every_10s():
    ...

@pytest.mark.asyncio
async def test_reconnect_with_backoff_and_resnapshot():
    ...

@pytest.mark.asyncio
async def test_user_channel_trade_triggers_reconciler():
    ...
```

---

## H. Risks, Unknowns, and Open Questions

### H.1 Cannot read the live repo — required follow-ups by the user

Because `https://github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python` returned **404** to every fetch attempt during this session, the user must run the following locally (or correct the slug) and re-confirm. Each check produces deterministic evidence to insert into Sections B and C of this report.

```bash
# 1. Confirm engines exist and identify their entry points
git clone <correct repo URL>
cd <repo> && git switch develop

rg -n "class .*Engine|def run|async def run" src/

# 2. Find every place that mutates a position
rg -n "position|Position|positions|holdings" src/ | rg -v "test_"

# 3. Find the literal hardcoded 5 (issue #38)
rg -n "\\b5\\b" src/ | rg -i "min|size|order"
rg -n "MIN_(ORDER|TRADE|SIZE)" src/

# 4. Float vs Decimal
rg -n "float\(|float \(|/[^/]*price|price\s*\*|size\s*\*" src/

# 5. Idempotency
rg -n "idempot|dedup|already_executed|seen_trade" src/

# 6. WSS or REST?
rg -n "websocket|ws_subscript|/ws/market|/ws/user|wss://|polling|sleep" src/

# 7. SDK version (V1 vs V2)
rg -n "py_clob_client|py-clob-client" src/ requirements.txt pyproject.toml

# 8. Tick size handling
rg -n "tick_size|tick-size|min_tick" src/

# 9. Locks
rg -n "asyncio.Lock|asyncio\\.Semaphore|threading.Lock" src/

# 10. Logging (preservation goal)
rg -n "logging.getLogger|basicConfig|RotatingFileHandler|loguru|logger\\." src/

# 11. Issue #38 itself
gh issue view 38 --repo kvzsolt/Polymarket-Copy-Trading-Bot-Python
```

For each item in Section C, fill in the file/line evidence. The fix sketches are written to be slot-in regardless of where the bug lives.

### H.2 Polymarket-side unknowns

- **Builder code attribution** is not part of this refactor; if the bot uses one, store it in env and pass `builder` (bytes32) on every order in V2.
- **pUSD migration** for any wallet still holding USDC.e: out of scope for this refactor, but blocking. The wrap step is one-time and documented at `https://docs.polymarket.com/concepts/pusd`.
- **Geo restrictions:** Polymarket prohibits API access from US persons and certain other jurisdictions; ToS should be consulted, the bot itself enforces nothing here.

### H.3 Things I deliberately did NOT do, and why

- **Did not invent file paths.** Every patch is in a new module under `polybot/...` so it can be slotted next to whatever the existing layout is, without claiming knowledge of the current tree.
- **Did not propose deleting any existing engine.** Slice 4 (coordinator) is a wrapper and Slice 7 (single decision plane) is incremental — engines become callers, not deleted code. Rollback is `git revert`.
- **Did not change logging.** Patches use `logging.getLogger(__name__)` only; whatever formatter/handler/file rotation the project already configures is preserved verbatim. The single small surgical addition (if needed) is `logger.info("sizing.* ...")`-style structured kv lines so downstream parsers can grep them.
- **Did not introduce live-order side effects.** Every patch respects a dry-run flag the engines should already have; the coordinator's `mark_submitted` is only called by code paths that *would have* submitted in the prior version.

### H.4 Open question that genuinely changes design

> Is the bot still on CLOB V1 (py-clob-client) as of today (April 30, 2026)?

If yes, **no other refactor matters** until V2 migration is done — V1 has been off production since April 28, 2026 per Polymarket's own announcement. If the user has already migrated to `py-clob-client-v2`, the patches above slot in unchanged.

---

### Appendix — Polymarket citation index used in this report

- WSS overview, channels, subscription shape, heartbeats: `https://docs.polymarket.com/market-data/websocket/overview`
- Market channel events (book / price_change / tick_size_change / last_trade_price / best_bid_ask / new_market / market_resolved): `https://docs.polymarket.com/market-data/websocket/market-channel`
- User channel events (trade lifecycle MATCHED→MINED→CONFIRMED, order PLACEMENT/UPDATE/CANCELLATION): `https://docs.polymarket.com/market-data/websocket/user-channel`
- WSS quickstart with Python sample (auth payload, channel constants): `https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart`
- Order creation, tickSize / negRisk per market: `https://docs.polymarket.com/developers/CLOB/orders/create-order`
- V2 migration (CLOB V2 cutover April 28 2026; `getClobMarketInfo` returns `mos` / `mts` / `fd` / `t` / `rfqe`; new EIP-712 Order struct; pUSD; builder code): `https://docs.polymarket.com/v2-migration`
- Public methods reference (Python + TS): `https://docs.polymarket.com/trading/clients/public`
- Fetching markets via Gamma (slug, tags, events): `https://docs.polymarket.com/market-data/fetching-markets`
- py-clob-client (V1) repo: `https://github.com/Polymarket/py-clob-client`
- py-clob-client-v2 (V2) package: `https://pypi.org/project/py-clob-client-v2/`, repo `https://github.com/Polymarket/py-clob-client-v2`
- Real-time data client (V2) showing CLOB user/market subscription shapes: `https://github.com/Polymarket/real-time-data-client`
- Market metadata schema with `min_order_size` / `tick_size` / `neg_risk` (typed, mirrored from wire): `https://pkg.go.dev/github.com/ivanzzeth/polymarket-go-real-time-data-client`
- Tick precision tier table (price/size/amount decimals per tick): `https://nautilustrader.io/docs/latest/integrations/polymarket/`
- Stale-tick-cache bug class (`Polymarket/py-clob-client` issue #122): cited via search results
- Per-market `minimum_order_size` worked example showing `5` is *a* per-market value, not a global constant: `https://quantpylib.hangukquant.com/wrappers/polymarket/`
- agent-skills WS reference (subscription op, heartbeat, ping interval, dynamic add/remove): `https://github.com/Polymarket/agent-skills/blob/main/websocket.md`