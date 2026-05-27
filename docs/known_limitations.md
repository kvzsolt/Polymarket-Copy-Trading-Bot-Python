# Known Limitations — RC1

When to use: triaging unexpected behaviour, deciding whether to file a bug,
or comparing the RC1 surface against the roadmap.

## Trade detection still HTTP polling

Leader trade detection runs over HTTP polling at the `fetch_interval`
configured in `polling.fetch_interval`. Real-time WSS-based detection is
out of scope for RC1 — the market WSS runtime only fuels market data, not
trade discovery. Expect a 1–5 second floor on copy latency depending on poll
interval and venue HTTP latency.

## V2 `execute_intent` is the future path

`services/execution/coordinator.py:ExecutionCoordinator.execute_intent` is
an intent-shaped V2 architecture wired only in tests. The production trade
pipeline / risk monitor / near-one exit / protective exit all go through the
PR6 production methods (`submit_copy_trade`, `submit_risk_exit`,
`submit_near_one_exit`, `submit_protective_exit`). Migrating the production
path to the V2 OrderSubmitter is a future PR; RC1 keeps both alive without
mixing them.

## External / manual positions pause by default

When `PositionDriftService` observes a venue position the bot did not open,
it pauses the scope (`size_mismatch_positive` or
`venue_missing_local_position` → `PAUSE_SCOPE`). To allow tracking the
external position read-only without pausing, set
`POSITION_DRIFT_ALLOW_EXTERNAL_POSITION_TRACKING=true`. RC1 ships with this
flag **disabled** so manual venue activity always surfaces.

## Live liquidity / venue delays can still cause skips

Even with the PR5 confirmation delay policy, the venue can:

- Return `delayed` for several seconds before flipping to `live` or `matched`.
- Drop into `submitted_unconfirmed` for the full `ORDER_CONFIRMATION_MAX_WAIT_SECONDS`
  window.
- Reject a SELL with `insufficient_balance` when manual sells beat the bot.
- Reject `min_order_size` orders below current venue minimum.

The coordinator never retries blindly. The reconciler is the single source
of terminal settlement, and the strict probe path is the only auto-release
permitted in RC1. Operators should expect some signals to result in
`skip_code="market_data_stale"`, `book_no_liquidity`, `book_spread_wide`,
`stale_market_data`, or `position_drift_unresolved`.

## Forbidden legacy symbols (informational)

These names were removed in earlier PRs and must not return. Operators
running their own forks should grep their patches against:

- `services.price_cache`
- `PositionPriceCache`
- `MarketMetaService`

The doctor's `code_gate_*` checks (PR8) and `scripts/rc1_check.sh` (PR9)
scan Python source for these. Documentation may mention them; production
code may not.

## Direct ExecutionService fallback only behind a live guard

`orchestrators/trade_pipeline.py`, `services/riskMonitor.py`, and
`services/near_one_exit.py` retain a `getattr(execution_service, "execute")`
alias that fires when `context.execution_coordinator is None` AND the bot
is NOT in live mode. Live mode + missing coordinator returns
`execution_coordinator_missing`. Operators should never see that skip code
in production; it only appears in test fixtures.
