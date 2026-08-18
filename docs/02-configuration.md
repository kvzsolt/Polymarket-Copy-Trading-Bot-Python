# Configuration

`AppConfig` is immutable and loaded once. There is no flat environment parser
and no hot reload.

Precedence, highest first:

1. Process variables named `PMCOPY__SECTION__FIELD`
2. Mode-`0600` `secrets.env`
3. Programmatic overrides (tests/composition roots)
4. Per-leader overrides `config/leaders/<wallet>.yml`
5. Environment overrides `config/environments/<PMCOPY_ENVIRONMENT>/<service>.yml`
   (the environment is selected explicitly; default `paper`, never inferred
   from credential presence)
6. Deprecated legacy shim: an operator's untracked `config/runtime.local.yml`
   (legacy section names translated at the loader boundary; planned removal)
7. Canonical checked-in `config/<service>.yml` (one file per owning service)
8. Typed model defaults

Legacy `PMCOPY__NETWORK__*` / `PMCOPY__LEADER__*` / `PMCOPY__POLLING__*` /
`PMCOPY__ONCHAIN_DETECTION__*` names keep working through the same
translation table. `secrets.env` is resolved from the deployment root
(`PMCOPY_DEPLOYMENT_ROOT` or marker walk-up). See
`config/environments/example/execution.yml` for an environment template.
Public wallet addresses, manual leaders, limits, and the paper/live selector
belong in environment files. Credentials and private RPC URLs belong in
`secrets.env`:

```dotenv
POSTGRES_PASSWORD=change-me
PMCOPY__DATABASE__DSN=postgresql://bot:change-me@localhost/polymarket_copytrading_python
PMCOPY__WALLET__PRIVATE_KEY=0x...
# Optional private provider override:
# PMCOPY__NETWORK__RPC_URL=https://...
# Optional Falcon integration:
# PMCOPY__DISCOVERY__API__TOKEN=...
```

The plain `POSTGRES_PASSWORD` name is retained only because the official
PostgreSQL image consumes it. The application itself accepts nested
`PMCOPY__...` keys only.

`database.dsn` and the Polygon RPC URL are required even in paper mode because
idempotency, reconciliation, discovery history, and the roster are
PostgreSQL-backed.

`wallet.funder_address` is required in paper mode: paper never authenticates,
so the CCXT transport resolves no wallet and this public address is the
identity the balance and live-position health probes use. In live mode it is
the verified Deposit Wallet funder address CCXT signs against — it must be
set and must match the wallet derived from `PMCOPY__WALLET__PRIVATE_KEY` or
startup fails closed.

Unknown YAML, secret, or nested process keys fail validation instead of being
silently ignored.

## Leader detection source (`detection.mode`)

`detection.mode` (env `PMCOPY__DETECTION__MODE`) selects the single on-chain
leader-detection source. REST polling of the Polymarket data API keeps running
as a parallel fallback in every mode; the copy layer dedups the same fill by
trade key, so a signal is copied once regardless of which source saw it first.

| `detection.mode` | Source | Requirements |
|---|---|---|
| `polling` (default, checked-in base) | data-api REST polling only | — |
| `on_chain_confirmed` + `confirmed_source: mempool_reserved` (default) | Alchemy mempool sighting + receipt confirmation (`PolygonSourceTransactionRuntime`); executes only after the fill is mined | `wss://` URL in `detection.onchain.onchain_ws_url` or `pending_tx_ws_url`, `pending_tx_provider: alchemy` |
| `on_chain_confirmed` + `confirmed_source: logs_only` (**checked-in live default**, `config/environments/live/detection.yml`) | Mined `OrderFilled` logs only (`OnchainDetectionRuntime`): two `eth_subscribe` `logs` subscriptions topic-filtered on the OrderFilled topic0 and the followed leaders (maker role topic2, taker role topic3) on any plain WSS provider. Detection lands at block inclusion; a mined fill is authoritative, so there is no provisional copy and no pending-copy confirmation to run | `wss://` URL as above; `pending_tx_provider: alchemy` or `generic` |
| `on_chain_pending` | Provisional copy on the mempool sighting (`OnchainDetectionRuntime`, `alchemy_pendingTransactions` with calldata), guarded only by `pending_tx_max_signal_age_ms` and `pending_tx_detection_confidence_required`. **No mined correction path**: the mined subscription is off (dedup), so a provisional copy of a sighting that never mines is reported by the pending-copy confirmation tracker below but never unwound automatically — paper soak + explicit unwind procedure required (`docs/audit/pending_detection_notes.md`) | as `mempool_reserved`, plus `pending_tx_hashes_only: false` and `pending_tx_max_signal_age_ms > 0` |

`on_chain_pending` with `confirmed_source: logs_only` is rejected at startup
(the pending mode is the Alchemy mempool subscription and has no logs-only
variant), and `pending_tx_provider: generic` is accepted only for
`on_chain_confirmed` + `logs_only` (the mempool subscriptions of the other
on-chain modes are Alchemy-specific).

In `on_chain_pending`, `pending_tx_detection_confidence_required` must be
`market_unattributed` or `pending_probable`: a mempool sighting is only ever
labelled `pending_probable`, so `mined_authoritative`/`confirmed_final` is a
floor that drops every pending signal. The looser floors are accepted; the
higher two fail startup closed. Other modes are unaffected.

Indicative provider cost of each source at $0.45 / 1M compute units
(Alchemy pay-as-you-go; the filtered `logs` volume is a handful of events per
hour, so it sits inside the Alchemy free 30M CU/month tier and Infura's free
3M credits/day):

| Source | Approximate cost |
|---|---|
| `polling` | ≈ $0 (REST polling of the data API) |
| `on_chain_confirmed` + `logs_only` | ≈ free tier (two topic-filtered `logs` subscriptions + `eth_subscribe`) |
| `on_chain_pending` | ≈ $27/day (`alchemy_pendingTransactions` full body + `logs` + `newHeads`) |
| `on_chain_confirmed` + `mempool_reserved` | ≈ $54/day (adds the gen-2 receipt confirmation machinery) |

`config/environments/live/detection.yml` selects `on_chain_confirmed` +
`logs_only` and keeps the mempool keys pinned for the fallback:

| Live key | Value | Why |
|---|---|---|
| `mode` | `on_chain_confirmed` | Copy on the mined fill — authoritative, near-zero provider cost, provider-agnostic. |
| `onchain.confirmed_source` | `logs_only` | Topic-filtered `logs` subscriptions instead of the mempool-reserved gen-2 runtime. |
| `onchain.pending_tx_provider` | `alchemy` | The operator's WSS is Alchemy; `logs_only` also accepts `generic`. |
| `onchain.pending_tx_max_signal_age_ms` | `700` | Inert in this mode; kept for the `on_chain_pending` fallback. |
| `onchain.pending_tx_detection_confidence_required` | `pending_probable` | Inert in this mode; kept for the `on_chain_pending` fallback. |
| `onchain.pending_copy_confirmation_enabled` | `true` | Inert in this mode; kept for the `on_chain_pending` fallback. |
| `onchain.pending_copy_confirmation_deadline_seconds` | `60` | Inert in this mode; kept for the `on_chain_pending` fallback. |
| `onchain.pending_copy_confirmation_max_tracked` | `2048` | Inert in this mode; kept for the `on_chain_pending` fallback. |

One environment variable flips live back to the mempool path:
`PMCOPY__DETECTION__MODE=on_chain_pending` (the pinned pending keys above
take effect again). Switching to the gen-2 mempool-reserved confirmed
runtime additionally needs `PMCOPY__DETECTION__ONCHAIN__CONFIRMED_SOURCE=mempool_reserved`.

The `wss://` endpoint and `polymarket.rpc_url` (which the tracker needs) still
come from `secrets.env` — no live YAML file carries a credential. Base
`config/detection.yml` keeps `mode` unpinned and
`enable_onchain_pending_detection: false`; selecting `PMCOPY_ENVIRONMENT=paper`
therefore still runs pure polling.

### Pending-copy confirmation (`detection.onchain.pending_copy_*`)

Keys consumed by the pending-copy confirmation tracker
(`services/onchain_detection/pending_copy_confirmation_tracker.py`), which
follows up every copy taken off a mempool sighting by polling the leader
transaction's receipt. Only ever consulted in `on_chain_pending`, and only
when `polymarket.rpc_url` is set — without an HTTP RPC endpoint the tracker is
not built and startup logs `pending_copy_tracker_disabled |
reason=no_rpc_endpoint`.

The receipt poll offsets are fixed at 5/15/30/60 s after the copy and are not
configurable; `pending_copy_confirmation_deadline_seconds` is only the age at
which the final verdict is taken (it always forces one last poll).

The tracker **reports**, it does not unwind: a copy whose leader transaction
reverted, never mined, or could not be checked logs WARN
`pending_copy_unconfirmed | tx=… leader=… age_s=… reason=…`, increments
`pending_copy_unconfirmed_total{reason}`, and emits a bounded DETECTION
telemetry failure. Closing the resulting position stays an operator action —
see `docs/known_limitations.md` for the residual risks.

| Key | Default | Meaning |
|---|---|---|
| `pending_copy_confirmation_enabled` | `true` | Track each provisional copy and log/count whether the leader tx confirmed. |
| `pending_copy_confirmation_deadline_seconds` | `60.0` | Age after which an unmined tracked copy is reported unconfirmed. Must be `> 0`. |
| `pending_copy_confirmation_max_tracked` | `2048` | Ceiling on concurrently tracked copies; the oldest entry is evicted when full. Must be `> 0`. |
| `pending_copy_auto_unwind_enabled` | `false` | Opt-in forced protective exit of an unconfirmed copy. Off by default: unwinding is a money-path action and stays operator-driven. |

An invalid mode, a non-polling mode without a `wss://` endpoint, a
non-Alchemy provider outside `on_chain_confirmed` + `logs_only`,
`on_chain_pending` with `confirmed_source: logs_only` or
`pending_tx_hashes_only: true`, a
`pending_copy_confirmation_deadline_seconds`/`pending_copy_confirmation_max_tracked`
of zero or less in `on_chain_pending`, or any
non-polling mode together with `copy_trading.copy_tasks.enabled: true` (the v1
copy-task route has no on-chain-vs-polling dedup yet) fails startup closed. Select an on-chain mode per environment
(`config/environments/<env>/detection.yml`) or via the environment variable;
the base `config/detection.yml` must stay `polling`. Changing the mode
requires a restart — the control plane does not expose `detection.mode`.

Startup logs `detection_mode_selected | mode=<mode> onchain_ws=set|unset
polling_fallback=true provider=alchemy` (never the URL) and warns
`detection_ws_url_unused | mode=polling` when a WS URL is configured but
polling is selected.

The booleans `detection.onchain.enable_onchain_pending_detection` and
`enable_onchain_mined_detection` are **deprecated**. They are still accepted:
when no layer sets `detection.mode` (the checked-in base file deliberately
does not pin it; the typed default is `polling`), either boolean derives
`on_chain_confirmed` with a `detection_legacy_flags_deprecated` warning
(provisional copy is never derived); when some layer sets `detection.mode`, the
booleans are ignored with a `detection_legacy_flags_ignored` warning. Each
distinct warning is logged once per process. Replace them with `detection.mode`.

## Bankroll profile — 350 pUSD moderate

The checked-in `sizing` / `risk` / `execution` / `protective_exits` values are
tuned for a **350 pUSD** bankroll at **moderate** risk: ~5 % per copy, a ~10 %
daily-loss stop, ~60 % maximum deployed exposure. They live in the **base**
files, not in an environment override, because paper and live must resolve
identical sizing/risk/protective-exit configuration
(`tests/test_live_environment_overrides.py::test_live_environment_does_not_change_other_services`).
`tests/test_live_operator_tuning.py` pins every value below and the behaviour
it produces.

Derivation from a bankroll `B = 350`:

| Quantity | Formula | Value |
|---|---|---|
| Per-copy cap | `B x 5 %` | **17.5** |
| Daily-loss BUY stop | `B x 10 %` | **35** |
| Maximum deployed exposure | `B x 60 %` | **210** |
| Cash reserve | `max(35, B x 0.40)` -> tradable `B - reserve` | **140** held, **210** tradable at start |
| Concurrent open positions | `floor(210 / 17.5)` | **12** |
| Per-leader cap | 8 copies of one leader (`8 x 17.5`) | **140** |
| Per-market cap | 2 copies in one condition (`2 x 17.5`) | **35** |

| File | Key | Value | Why |
|---|---|---|---|
| `sizing.yml` | `buy_max_amount` | `17.5` | The per-copy cap. `buy_min_amount` stays `5.0`, so every copy is clamped into `[5, 17.5]`. |
| `sizing.yml` | `percentage`, `max_position_pct` | `5.0`, `0.1` | Unchanged. `percentage` only bites for leader trades between $100 and $350; `max_position_pct` is a 24 h free-cash snapshot (see limitations). |
| `risk.yml` | `enable_balance_tiered_policy` | `true` | Master switch: `headroom_for_buy` is the only consumer of every cap below. Off, all of them are dead config. |
| `risk.yml` | `balance_policy_reserve_pct` / `_reserve_floor` | `0.40` / `35.0` | `reserve = max(floor, equity x pct)`; tradable cash is `free - reserve`. |
| `risk.yml` | `balance_policy_hard_cap_pct` / `_hard_cap_ceiling` | `0.05` / `17.5` | `hard_cap = min(tradable, equity x pct, ceiling)` — the per-copy cap again, from the risk side. |
| `risk.yml` | `max_open_positions` / `balance_policy_max_open_positions` | `12` / `12` | Same number on both branches, so the cap does not depend on which one the policy reads. |
| `risk.yml` | `balance_policy_allow_partial_exits` | `true` | Without it the risk monitor upgrades the near-max 0.6 trim to a full close. |
| `risk.yml` | `max_leader_exposure_usdc` / `max_market_exposure_usdc` / `max_category_exposure_usdc` | `140` / `35` / `210` | 40 % per leader (2-leader roster), 2 copies per condition, 60 % overall. The category cap is a no-op today (see limitations). |
| `risk.yml` | `max_daily_drawdown_pct` | `10.0` | `max_daily_loss = max(2.5, day_start_equity x 10 %)` = 35; once the day's realized loss reaches it, BUY headroom is zero. |
| `execution.yml` | `max_position_size_usdc` / `min_balance_headroom_usdc` | `20.0` / `140.0` | In-memory `BalanceRegime` belt behind the risk policy: 17.5 plus rounding slack, and the reserve as a free-cash floor. |
| `execution.yml` | `max_daily_spend_usdc` | `0.0` (disabled) | It measures *gross submitted* spend, so any non-zero value would starve re-entries after a normal day of round trips. |
| `execution.yml` | `sub_min_sell_policy` | unset (`round_up`) | `skip` marks a sub-minimum leader SELL `skipped_sell_size_zero` and never retries it, stranding the position. |
| `protective_exits.yml` | `enable_stop_loss` / `stop_loss_threshold` / `stop_loss_grace_period_seconds` | `true` / `0.08` / `300.0` | Salvage exit. `0.08` is an absolute price, deliberately below `execution.min_share_price` `0.10` (see limitations). |

Both caps must clear for a BUY: the account-cap policy (`headroom_for_buy`,
equity/exposure-aware) and the `BalanceRegime` belt (absolute, in-memory). The
smaller one wins. Neither adds IO to the detection-to-submit path — both reuse
the balance snapshot the pipeline already fetched.

To run a different bankroll, rescale the seven derived numbers together;
changing `buy_max_amount` alone leaves the risk caps contradicting it. The
caveats attached to this profile are listed in `docs/known_limitations.md`.

## Operator API (`control_plane`)

`config/control_plane.yml` owns the transport policy — bind host and port,
request and WebSocket bounds, rate limits, CORS allowlist, portfolio cache
intervals. The base file keeps `enabled: false`; the live environment turns it
on with `config/environments/live/control_plane.yml`, which contains
`enabled: true` and nothing else, so the bind stays `127.0.0.1:9103`.

Credentials never appear in a YAML file. They come from the secret layer only:

| Key | Variable | Role |
|---|---|---|
| `control_plane.auth.token` | `PMCOPY__CONTROL_PLANE__AUTH__TOKEN` | `operator` — every permission |
| `control_plane.auth.viewer_token` | `PMCOPY__CONTROL_PLANE__AUTH__VIEWER_TOKEN` | `viewer` — every `*.read` plus `telemetry.stream`, no writes |

An enabled, authenticated control plane without an operator credential fails
startup closed. The viewer credential is optional and is composed into a second
bearer principal only when set; when set it must differ from the operator
credential and, like it, is capped at 1024 characters. Both are `SecretStr`, so
`pmcopy config print` and `dump_config_payload` redact them, and the whole
`control_plane.auth` object is excluded from the settings API — the control
plane cannot read or rotate its own credentials. An unauthenticated
non-loopback bind is rejected. See `docs/frontend-control-plane.md`.

Discovery thresholds, component weights, normalization ranges, decay,
shrinkage, scheduler cadence, backfill limits, hysteresis, and rank-driven
sizing multipliers all live in `config/discovery.yml`.

Useful checks:

```bash
pmcopy config validate
pmcopy config print
python scripts/run_discovery_only.py --list-only
```

`pmcopy config print` always redacts secrets; it has no unsafe unredacted mode.
