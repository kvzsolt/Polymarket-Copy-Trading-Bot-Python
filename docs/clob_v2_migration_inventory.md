# CLOB V2 Migration Inventory

**Status:** v2-migration-slice3 (Phase 2c — Package-Adaptive Seam)
**Last Updated:** 2026-04-30

## Overview

This document tracks the CLOB V1 (py-clob-client v0.34.6) surface area in active production use. It serves as a reference for the V2 migration which will replace imports with v2-compatible equivalents.

---

## Current Dependency

- **Package:** `py-clob-client==0.34.6`
- **Location:** `pyproject.toml`
- **Migration Target:** `py-clob-client-v2` (TBD version)

---

## V1 Type/Constant Imports by Module

### Core Client (ACTIVE)

**File:** `utils/createClobClient.py`
- Imports:
  - `from py_clob_client.client import ClobClient`
  - `from py_clob_client.clob_types import (MarketOrderArgs, OrderArgs, OrderType, PartialCreateOrderOptions)`
- Usage:
  - `ClobClient()` — instantiates the CLOB client
  - `MarketOrderArgs()` — builds market order parameters
  - `OrderArgs()` — builds limit order parameters
  - `OrderType` enum (e.g., `OrderType.FOK`) — order fill type
  - `PartialCreateOrderOptions()` — order creation options (tick_size, neg_risk)
- Wrapped by: `ClobClientWrapper` class
- Caller: Bootstrapped in app initialization and wrapped as singleton

### Execution Service (ACTIVE)

**File:** `services/executionService.py`
- Imports:
  - `from py_clob_client.clob_types import OrderType`
  - `from py_clob_client.order_builder.constants import BUY, SELL`
- Usage:
  - `OrderType` enum for order type classification
  - `BUY` / `SELL` string constants for side determination
- Called by: Order lifecycle and risk management flows

### Order Gateway (ACTIVE)

**File:** `clients/order_gateway.py`
- Imports:
  - `from py_clob_client.clob_types import OrderArgs, OrderType`
- Usage:
  - `OrderArgs()` — constructs order request
  - `OrderType[order_type]` — enum lookup from string
- Role: Domain port adapter; bridges execution service to CLOB client

### Legacy/Backward Compat (DEPRECATED)

**File:** `utils/postOrder.py`
- Status: DEPRECATED — kept only for old test compatibility
- Imports:
  - `from py_clob_client.clob_types import OrderArgs`
  - `from py_clob_client.order_builder.constants import BUY, SELL`
- Note: Marked with deprecation docstring; not used in active runtime

---

## V1 Surface Area Summary

### Types
| Type | Module | Status | Used In |
|------|--------|--------|---------|
| `ClobClient` | `py_clob_client.client` | ACTIVE | createClobClient.py |
| `MarketOrderArgs` | `py_clob_client.clob_types` | ACTIVE | createClobClient.py |
| `OrderArgs` | `py_clob_client.clob_types` | ACTIVE | createClobClient.py, order_gateway.py, postOrder.py (legacy) |
| `OrderType` | `py_clob_client.clob_types` | ACTIVE | createClobClient.py, executionService.py, order_gateway.py |
| `PartialCreateOrderOptions` | `py_clob_client.clob_types` | ACTIVE | createClobClient.py |

### Constants
| Constant | Module | Status | Used In |
|----------|--------|--------|---------|
| `BUY` | `py_clob_client.order_builder.constants` | ACTIVE | executionService.py, postOrder.py (legacy) |
| `SELL` | `py_clob_client.order_builder.constants` | ACTIVE | executionService.py, postOrder.py (legacy) |

### Methods on ClobClient
| Method | Signature | Active | Notes |
|--------|-----------|--------|-------|
| `__init__` | `(host, key, chain_id, signature_type, funder)` | ✓ | Called in createClobClient |
| `get_order_book` | `(asset)` | ✓ | Wrapped in ClobClientWrapper |
| `create_market_order` | `(order_args, options)` | ✓ | Wrapped async in ClobClientWrapper |
| `post_order` | `(signed_order, orderType)` | ✓ | Wrapped in ClobClientWrapper.post_signed_order |
| `create_and_post_order` | `(order_args)` | ✓ | Wrapped in ClobClientWrapper.post_order |
| `create_or_derive_api_creds` | `()` | ✓ | Called on client init |
| `set_api_creds` | `(creds)` | ✓ | Called on client init |

---

## Migration Phases

### Phase 1: Compatibility Seam (DONE)
✓ Create inventory doc
✓ Create `utils/clob_v1_compat.py` — centralize all V1 imports
✓ Update call sites to import through seam
✓ Add seam validation tests
✓ **No behavior change**, no V2 code yet

### Phase 2a: API Delta Inventory (DONE)
✓ Document confirmed V1 → V2 method signature deltas
✓ Identify three critical breakpoints

### Phase 2b: Method Adapter Seams (DONE)
✓ Add `derive_api_creds(client)` adapter — wraps `create_or_derive_api_creds()` (V2: `create_or_derive_api_key()`)
✓ Add `submit_signed_order(client, signed_order, order_type)` adapter — wraps `post_order(..., orderType=)` (V2: `order_type=`)
✓ Add `cancel_order_by_id(client, order_id)` adapter — wraps `cancel(order_id=)` (V2: `cancel_order(OrderPayload(...))`)
✓ Route `createClobClient.py` through `derive_api_creds` and `submit_signed_order`
✓ Route `order_gateway.py` through `cancel_order_by_id`
✓ Add adapter delegation tests (mock-based)
✓ Update `__all__` and existing seam tests
✓ **No behavior change**, no V2 code yet

### Phase 2c: Package-Adaptive Seam (CURRENT SLICE)
✓ `_is_v2_client(client)` — duck-type detection via `hasattr(client, "create_or_derive_api_key")`
✓ `_OrderPayload` probe at import time — fails silently on V1 SDK, stays None
✓ `derive_api_creds` dispatches to `create_or_derive_api_key()` on V2 client
✓ `submit_signed_order` dispatches with `order_type=` kwarg on V2 client
✓ `cancel_order_by_id` dispatches to `cancel_order(OrderPayload(...))` on V2 client; raises RuntimeError if _OrderPayload unavailable
✓ Tests cover V1 and V2 paths for all three adapters, plus _is_v2_client detection
✓ **Runtime stays V1**; V2 paths activate automatically when V2-shaped client detected

### Phase 3: V2 SDK Integration (FUTURE)
- Add py-clob-client-v2 to pyproject.toml
- Map V1 → V2 types/methods in compat module
- Swap adapter internals to V2 equivalents
- Run integration tests

### Phase 4: Execution Service Auth/Order Migration (FUTURE)
- Migrate order signing logic to V2 patterns
- Update order creation and submission

### Phase 5: Edge Case Cleanup (FUTURE)
- Remove legacy postOrder.py if no longer used in tests
- Clean up deprecation warnings
- Final validation

---

## Constraints for Migration

1. **No behavior changes in Phase 1** — compatibility seam is transparent
2. **No dependency upgrade yet** — py-clob-client v0.34.6 stays in pyproject.toml
3. **Keep order semantics unchanged** — all order sizing, signing, auth logic stays identical
4. **Keep runtime defaults unchanged** — no default overrides or feature flags
5. **Keep test coverage** — add seam validation; don't remove existing tests

---

## Files Changed in Phase 1

- `docs/clob_v2_migration_inventory.md` — this doc
- `utils/clob_v1_compat.py` — NEW compatibility seam
- `utils/createClobClient.py` — updated imports
- `services/executionService.py` — updated imports
- `clients/order_gateway.py` — updated imports
- `tests/test_clob_v1_compat_seam.py` — NEW seam validation

## Files Changed in Phase 2b

- `utils/clob_v1_compat.py` — added `derive_api_creds`, `submit_signed_order`, `cancel_order_by_id` adapters
- `utils/createClobClient.py` — routed through `derive_api_creds` and `submit_signed_order`
- `clients/order_gateway.py` — routed through `cancel_order_by_id`
- `tests/test_clob_v1_compat_seam.py` — updated `__all__` expectations
- `tests/test_clob_v1_method_adapters.py` — NEW adapter delegation tests
- `docs/clob_v2_migration_inventory.md` — updated phases

---

## Files Changed in Phase 2c

- `utils/clob_v1_compat.py` — added `_is_v2_client()`, `_OrderPayload` probe, V2 dispatch branches in all three adapters
- `tests/test_clob_v1_method_adapters.py` — replaced unspec'd MagicMock with `spec=` fakes; added V2-path tests and `_is_v2_client` tests

---

## Fee Handling in V2

V2 removed `feeRateBps` from the signed order payload — the exchange no longer
requires the taker to embed a fee rate in the order struct.  Fee information is
still relevant for:

- **Analytics / accounting** — use `get_fee_rate_bps(token_id)` to read the
  current taker fee rate for a token and incorporate it into P&L calculations.
- **Pre-trade cost modelling** — `services/feeService.py` and the
  `TRANSACTION_FEE_BPS` env var are the appropriate place to account for fees
  before order sizing; do not re-introduce `feeRateBps` into the signed order.

---

## Next Steps (Post-Merge)

All migration phases complete. Post-merge checklist:

1. Operator runs `scripts/smoke_v2.py` and verifies all checks pass
2. Operator sets `CLOB_V2_READY=true` in environment
3. Operator verifies pUSD balance/allowance on funded wallet (checked automatically at startup)
4. Paper-trade validation (`PAPER_TRADE_ONLY=true`) before live mode
5. Remove `py-clob-client` (V1) from dependency history if desired
