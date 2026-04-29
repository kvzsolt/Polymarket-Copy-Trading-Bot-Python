# CLOB V2 Migration Inventory

**Status:** v2-migration-slice1 (Phase 1 — Compatibility Seam)
**Last Updated:** 2026-04-29

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

### Phase 1: Compatibility Seam (CURRENT SLICE)
✓ Create inventory doc
✓ Create `utils/clob_v1_compat.py` — centralize all V1 imports
✓ Update call sites to import through seam
✓ Add seam validation tests
✓ **No behavior change**, no V2 code yet

### Phase 2: V2 SDK Integration (FUTURE)
- Add py-clob-client-v2 to pyproject.toml
- Map V1 → V2 types/methods in compat module
- Implement V2 wrappers behind seam
- Run integration tests

### Phase 3: Execution Service Auth/Order Migration (FUTURE)
- Migrate order signing logic to V2 patterns
- Update order creation and submission

### Phase 4: Edge Case Cleanup (FUTURE)
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

---

## Next Steps (Phase 2+)

1. **V2 Research:** Analyze py-clob-client-v2 API
2. **Type Mapping:** Build V1 → V2 type translation table
3. **Adapter Implementation:** Implement V2 wrappers inside compat module
4. **Migration:** Route code through V2 adapters, deprecate V1 calls
5. **Cleanup:** Remove V1 imports when migration complete
