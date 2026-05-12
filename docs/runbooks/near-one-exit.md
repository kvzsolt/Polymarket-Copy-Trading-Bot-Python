# Near-one exit runbook

## What it does

`NearOneExitService` (`services/near_one_exit.py`) watches every open copied
position. When a token's quote stays at or above
`NEAR_ONE_EXIT_THRESHOLD` (default `0.99`) for at least
`NEAR_ONE_EXIT_DWELL_SECONDS` (default `60`), the service submits a SELL for
the entire held size through `ExecutionService.execute()`. The bot exits to
USDC at near-par via the normal CLOB SELL path — no on-chain
`CTF.redeemPositions` transaction is ever issued.

The dwell timer arms on the first quote at or above threshold and clears
the moment a quote drops below. Stale or missing quotes do **not** advance
the timer (NearOne is fail-closed on no-data — see Freshness below).

## Env flags

| key | default | meaning |
| --- | --- | --- |
| `ENABLE_NEAR_ONE_EXIT` | `false` | master enable. Off by default. |
| `NEAR_ONE_EXIT_THRESHOLD` | `0.99` | price at which dwell begins. Decimal. |
| `NEAR_ONE_EXIT_DWELL_SECONDS` | `60` | seconds the quote must remain >= threshold. |
| `NEAR_ONE_EXIT_CHECK_INTERVAL` | `5` | seconds between loop ticks. |

## Operator signals

`bot config print` includes a `near_one_exit` section.

Structured log lines:

- `near_one_exit_threshold_arm token=… price=… threshold=…`
- `near_one_exit_threshold_clear token=… price=…`
- `near_one_exit_fire token=… condition=… size=… price=… dwell=…s`
- `near_one_exit_sized_skip token=… reason=…`
- `near_one_exit_rejected token=… reason=… error=…`
- `near_one_exit_execute_error token=… error=…`

SELL `ExecutionRequest` carries `signal_id="near_one_exit:<conditionId>:<unix_ts>"`,
`reason="near_one_exit"`, and `recovery_metadata` with `trigger_price`,
`dwell_seconds`, `threshold`, `trigger_size_tokens`.

## Freshness

NearOne reads quotes via `MarketDataService.get_quote_or_none()` which:

1. tries the WSS-fed in-memory cache,
2. falls back to a REST orderbook snapshot when the cache is stale,
3. returns `None` if both paths fail.

A `None` return means the dwell timer is **not** advanced for that tick.
NearOne therefore inherits the same fail-closed-on-no-data behaviour as
the rest of the trading path. It will never fire from a single
stale-and-then-disappeared quote.

## How to disable

```
ENABLE_NEAR_ONE_EXIT=false
```

then restart the process. The doctor check confirms NearOne is in
`skipped` / `near_one_exit_disabled` state:

```
bot doctor --paper | jq .checks.near_one_exit
```

## How to recover from a stuck SELL

If a NearOne SELL fires but reconciliation does not close the position
(e.g. venue partial fill + reject):

1. Inspect the live order: `bot orders list --status submitted_unconfirmed`.
2. Cancel via `bot orders cancel <order_id>` if needed.
3. The `_firing` set in `NearOneExitService` clears the next time the
   position size goes to zero in `PositionManager.open_positions`. If the
   position is shrunk manually, the dwell timer re-arms naturally from
   the next WSS quote.

## Recovery from manual / off-bot sells

If you manually sell on Polymarket UI while NearOne is armed:

1. The next tick will see the smaller (or zero) position via
   `PositionManager.open_positions` (rehydrated from `PositionsRepo`).
2. If size went to zero, NearOne drops the dwell/firing state for that
   token automatically.
3. If size shrank but is still above venue-min, the next dwell-elapsed
   tick will SELL the remaining smaller size — same flow, just less
   size. `size_sell_order` clamps to held shares.

## When it should not fire

- token quote oscillates around threshold (timer clears each dip);
- venue minimum size is greater than held shares (skip with
  `sell_held_below_venue_min`);
- MarketDataService cannot produce a fresh quote (timer not advanced);
- position is already firing this session (idempotency guard);
- `ENABLE_NEAR_ONE_EXIT=false` (loop exits immediately).

## Why not on-chain redemption?

Selling at the threshold yields effectively the same payout as
`CTF.redeemPositions`, but:

- no Polygon gas burn,
- no extra signing surface,
- works before final venue resolution (`redeemable=true` requires
  resolution + `curPrice >= 0.99` on the data API),
- shares the same reservation, idempotency, and reconciliation surfaces
  the rest of the bot already covers in tests.

The legacy redemption pipeline (`services/redemption*`, `services/autoRedeem.py`,
`storage/sqlite/redemptions_repo.py`, the `redemptions` SQLite table) was
removed in issue-78 and replaced by this service.
