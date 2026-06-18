# Execution Lifecycle

> The complete order execution lifecycle: states, transitions, reservations, reconciliation, and the execution coordinator.

---

## Overview

The execution lifecycle is the path a trading signal takes from detection to completion. It is implemented as a **strict state machine** with well-defined transitions, backed by SQLite persistence and an idempotency layer that guarantees no signal is ever executed twice.

The key components are:
- **Order Lifecycle Status** — The state machine that tracks order progress
- **Reservation Service** — Prevents duplicate execution by reserving capital/position
- **Execution Coordinator** — Orchestrates the flow from intent to fill
- **Reconciliation Service** — Resolves orders left in uncertain states

---

## Order States

Every order transitions through a defined set of states:

```
created → reserved → submitting → submitted_unconfirmed → filled
                                              ├→ cancelled
                                              ├→ expired
                                              └→ rejected
```

### State Definitions

| State | Description |
|---|---|
| `created` | Initial state — intent has been detected but not yet reserved |
| `reserved` | Capital/position has been reserved for this intent |
| `submitting` | Order is being sent to the CLOB |
| `submitted_unconfirmed` | Order submitted to CLOB but final status unknown |
| `partially_filled` | Order has received a partial fill |
| `filled` | Order is fully filled — terminal state |
| `cancelled` | Order was cancelled — terminal state |
| `expired` | Order expired — terminal state |
| `rejected` | Order was rejected by the CLOB — terminal state |
| `retryable_failed` | Submission failed but can be retried |
| `terminal_failed` | Permanent failure — terminal state |
| `reconciliation_required` | Order needs manual/automated reconciliation |
| `unknown_unconfirmed` | CLOB returned an unrecognized status |

### Terminal States

Once an order reaches a terminal state, **no further transitions are possible**. The state machine enforces this by raising a `ValueError` on any attempted transition from a terminal state.

Terminal states: `filled`, `cancelled`, `expired`, `rejected`, `terminal_failed`

---

## State Machine Transitions

The allowed transitions are strictly defined in `ALLOWED_TRANSITIONS`:

```
created            → {reserved, terminal_failed}
reserved           → {submitting, terminal_failed}
submitting         → {submitted_unconfirmed, retryable_failed, terminal_failed,
                      reconciliation_required, filled, partially_filled,
                      rejected, cancelled}
submitted_unconfirmed → {partially_filled, filled, cancelled, expired,
                         rejected, reconciliation_required}
unknown_unconfirmed → {reconciliation_required, filled, cancelled,
                       expired, terminal_failed}
retryable_failed   → {reserved, submitting, terminal_failed}
reconciliation_required → {filled, cancelled, expired, terminal_failed,
                           submitted_unconfirmed}
partially_filled   → {filled, cancelled, expired, terminal_failed,
                      reconciliation_required}
```

Any transition not in this set is **invalid** and will raise `ValueError`. This prevents bugs from creating impossible order states.

---

## Reservation System

The reservation system is the bot's primary defense against duplicate execution. Before any order is submitted, the `ReservationService` reserves the required capital (for BUYs) or position size (for SELLs).

### How Reservations Work

#### BUY Reservations

1. The `OrderIntent` specifies the desired size and price
2. `ReservationService.reserve_for_intent()` calculates `required_usdc = size × price`
3. It checks the funder's available USDC balance via the CLOB API
4. If `balance < required_usdc`, the reservation fails with "insufficient USDC"
5. If sufficient, a reservation record is created in the `reservations` table
6. The reservation includes an expiration timestamp (`ttl_seconds` default: 120)

#### SELL Reservations

1. The `OrderIntent` specifies the desired sell size
2. `ReservationService` checks the user's open positions for the matching token
3. If `available_size < required_size`, the reservation fails with "insufficient position"
4. If sufficient, a reservation record is created

### Duplicate Prevention

Before creating a reservation, the service checks for existing active reservations with the same `intent_key` (from `OrderIntentKey.stable_key()`):

```python
stable_key = intent.key.stable_key()
existing = reservations_repo.get_active_by_intent_key(stable_key)
```

If an active (non-expired) reservation exists, a `ReservationConflict` is raised. This is the core duplicate-prevention mechanism — the same signal can never be reserved twice.

### Reservation Expiry

Reservations expire after `PENDING_SUBMISSION_TTL_SECONDS` (default: 120 seconds). When a reservation expires:
- The reserved capital is released back to the available pool
- The reservation is marked as `released` with reason `expired`
- A new reservation for the same intent key can be created

### Reservation Release

Reservations are released when the order reaches a terminal state:
- **Fill** — Released with reason `filled` or `venue_fill_confirmed`
- **Cancel/Reject/Expire** — Released with the terminal status as reason
- **Terminal failure** — Released with reason `terminal_failed`

---

## Status Classification

When the CLOB returns a status for an order, the bot classifies it into one of the lifecycle statuses:

### Venue Status Mapping

| Venue Status | Classified As |
|---|---|
| `matched`, `filled` | `FILLED` |
| `cancelled`, `expired`, `rejected`, `failed` | `TERMINAL_FAILED` |
| `live`, `open`, `pending`, `delayed`, `submitted` | `SUBMITTED_UNCONFIRMED` |
| Any other value | `UNKNOWN_UNCONFIRMED` |

### Missing Status Handling

If the CLOB returns no status or the status is `None`, the order is classified as `UNKNOWN_UNCONFIRMED`. **The bot never creates a fill from missing status data.** This is a critical safety guarantee — ambiguous states are always treated conservatively.

---

## How Missing Status Is Handled

When an order is in `SUBMITTED_UNCONFIRMED` or `UNKNOWN_UNCONFIRMED` state and the next status check returns no useful information:

1. The order remains in its current unconfirmed state
2. The reconciliation service is notified (the order is enqueued for reconciliation)
3. The reservation remains active — capital is still locked
4. The reconciliation service will continue polling until a definitive status is obtained

This "wait and reconcile" approach ensures:
- No phantom fills are created from missing data
- Capital is not double-allocated (the reservation stays active)
- The order will eventually be resolved through reconciliation

---

## Reconciliation for Unconfirmed Orders

The `ReconciliationService` resolves orders that are stuck in uncertain states. It runs on a configurable interval (`RECONCILE_INTERVAL_SECONDS`, default: 300 seconds).

### Reconciliation Flow

1. **Fetch order record** — Get the order from the database with its current status and `venue_order_id`
2. **Check for venue order ID** — If missing, mark as `reconciliation_required` and return unresolved
3. **Query the CLOB** — Fetch the order status and fills from the venue
4. **If venue is unreachable** — Keep the reservation active, return unresolved
5. **If fills exist** — Persist the fills, update positions, release the reservation, mark as resolved
6. **If order is live/open/pending** — Mark as `submitted_unconfirmed`, return pending
7. **If order is cancelled/expired/rejected** — Mark as terminal, release reservation, return resolved
8. **If status is unrecognized** — Mark as `reconciliation_required`, return unresolved

### Reconciliation Results

| Result | Meaning |
|---|---|
| `resolved(filled)` | Order was filled, fills persisted |
| `resolved(terminal)` | Order was cancelled/expired/rejected |
| `pending` | Order is still live on the CLOB |
| `unresolved` | Could not determine status, needs retry |

### Stale Order Handling

| Parameter | Default | Description |
|---|---|---|
| `RECONCILE_STALE_SECONDS` | `3600` | Seconds before an order is considered stale |
| `RECONCILE_AUTO_CANCEL` | `false` | Automatically cancel stale orders |

When `RECONCILE_AUTO_CANCEL=true`, orders that have been unconfirmed for longer than `RECONCILE_STALE_SECONDS` are automatically cancelled. Use this cautiously — it assumes the order never filled.

---

## Execution Coordinator Flow

The `ExecutionCoordinator` is the central orchestrator for order execution. Its `execute_intent()` method handles the full lifecycle:

### Step-by-Step Flow

```
1. RESERVE
   └─ reservation_service.reserve_for_intent(intent)
   └─ Raises ReservationConflict if duplicate
   └─ Raises ValueError if insufficient balance/position

2. CREATE ORDER
   └─ orders_repo.create_from_intent(intent, reservation_id, status=RESERVED)

3. TRANSITION: RESERVED → SUBMITTING
   └─ State machine validates the transition

4. SUBMIT TO VENUE
   └─ clob_gateway.submit_order(intent)
   └─ On RetryableVenueError → transition to RETRYABLE_FAILED, return
   └─ On unknown exception → transition to TERMINAL_FAILED, release reservation, re-raise

5. CLASSIFY VENUE RESULT
   └─ classify_venue_status(venue_result.status)

6. IF FILLED
   └─ Persist fill via fills_repo
   └─ Update position via positions_repo
   └─ Release reservation with reason "filled"
   └─ Transition to FILLED
   └─ Audit log: execution_filled

7. IF UNCONFIRMED
   └─ Transition to SUBMITTED_UNCONFIRMED
   └─ Enqueue for reconciliation: reconciler.enqueue(order_id)
   └─ Audit log: execution_submitted_unconfirmed

8. IF TERMINAL (cancelled/rejected/expired)
   └─ Release reservation
   └─ Transition to terminal state
   └─ Audit log: execution_{status}
```

### Error Handling

- **Retryable errors** (network timeouts, rate limits): Order transitions to `RETRYABLE_FAILED`. On the next tick, the reservation is re-acquired and submission is retried.
- **Terminal errors** (invalid parameters, auth failure): Order transitions to `TERMINAL_FAILED`. Reservation is released. No retry.
- **Unknown errors**: Treated as terminal. Reservation is released. The exception is re-raised for logging.

### Audit Trail

Every state transition is logged via the `AuditLogger` protocol:

```python
{
    "user_id": "user123",
    "leader_wallet": "0xabc...",
    "condition_id": "0x123...",
    "token_id": "0x456...",
    "side": "BUY",
    "intent_key": "user123|0xabc...|0x123...|0x456...|BUY|copy_buy",
    "order_id": "ord_abc123",
    "reservation_id": "res_xyz789",
    "venue_order_id": "venue_qrs456",
    "source_signal_id": "sig_001",
    "error": ""
}
```

---

## Intent Key and Idempotency

The `OrderIntentKey` is the idempotency key for the execution system:

```python
stable_key = "|".join([
    user_id.lower(),
    leader_wallet.lower(),
    condition_id,
    token_id,
    side,        # BUY | SELL
    reason,      # copy_buy | copy_sell | risk_exit
])
```

This key uniquely identifies a trading intent. The reservation system uses it to prevent duplicate execution, and the audit system uses it for correlation.

---

## Next Steps

- See [05-risk-controls.md](05-risk-controls.md) for how risk decisions gate execution.
- See [09-troubleshooting.md](09-troubleshooting.md) for debugging stuck orders.
