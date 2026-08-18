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
| `on_chain_confirmed` | Alchemy mempool sighting + receipt confirmation (`PolygonSourceTransactionRuntime`); executes only after the fill is mined | `wss://` URL in `detection.onchain.onchain_ws_url` or `pending_tx_ws_url`, `pending_tx_provider: alchemy` |
| `on_chain_pending` | Provisional copy on the mempool sighting (`OnchainDetectionRuntime`, `alchemy_pendingTransactions` with calldata), guarded only by `pending_tx_max_signal_age_ms` and `pending_tx_detection_confidence_required`. **No mined correction path**: the mined subscription is off (dedup), so a provisional copy of a sighting that never mines is reported by the pending-copy confirmation tracker below but never unwound automatically — paper soak + explicit unwind procedure required (`docs/audit/pending_detection_notes.md`) | as above, plus `pending_tx_hashes_only: false` and `pending_tx_max_signal_age_ms > 0` |

In `on_chain_pending`, `pending_tx_detection_confidence_required` must be
`market_unattributed` or `pending_probable`: a mempool sighting is only ever
labelled `pending_probable`, so `mined_authoritative`/`confirmed_final` is a
floor that drops every pending signal. The looser floors are accepted; the
higher two fail startup closed. Other modes are unaffected.

### Pending-copy confirmation (`detection.onchain.pending_copy_*`)

Keys consumed by the pending-copy confirmation tracker
(`services/onchain_detection/pending_copy_confirmation_tracker.py`), which
follows up every copy taken off a mempool sighting by polling the leader
transaction's receipt. Only ever consulted in `on_chain_pending`, and only
when `polymarket.rpc_url` is set — without an HTTP RPC endpoint the tracker is
not built and startup logs `pending_copy_tracker_disabled |
reason=no_rpc_endpoint`.

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

An invalid mode, a non-polling mode without a `wss://` endpoint, a non-Alchemy
provider, `on_chain_pending` with `pending_tx_hashes_only: true`, a
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
