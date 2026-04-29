# CLOB V1 → V2 API Delta (Source-Verified)

**Status:** Phase 2a — API delta inventory (research slice)
**Last Updated:** 2026-04-29
**Source:** `py-clob-client==0.34.6` vs `py-clob-client-v2==1.0.0` (downloaded and inspected)

---

## Package-Level Changes

| Aspect | V1 (`py-clob-client`) | V2 (`py-clob-client-v2`) |
|--------|----------------------|--------------------------|
| Package name | `py-clob-client` | `py-clob-client-v2` |
| Import root | `py_clob_client` | `py_clob_client_v2` |
| Version | 0.34.6 | 1.0.0 |
| Signing | External `py-builder-signing-sdk` | Built-in `Signer` class |
| Dependencies | `eth-account`, `eth-utils`, `httpx`, `poly_eip712_structs`, `py-builder-signing-sdk`, `py-order-utils`, `python-dotenv` | `eth-account>=0.13.0`, `eth-utils>=4.1.1`, `poly_eip712_structs>=0.0.1`, `py-order-utils>=0.3.2`, `httpx[http2]>=0.27.0` |
| Dropped deps | — | `py-builder-signing-sdk`, `python-dotenv` |

---

## ClobClient Constructor

```python
# V1
ClobClient(host, chain_id=None, key=None, creds=None,
           signature_type=None, funder=None,
           builder_config=None,            # py_builder_signing_sdk.config.BuilderConfig
           tick_size_ttl=300.0)

# V2
ClobClient(host, chain_id, key=None, creds=None,
           signature_type=None, funder=None,
           builder_config=None,            # py_clob_client_v2.clob_types.BuilderConfig
           use_server_time=False,
           retry_on_error=False)
```

| Change | Detail | Breaking? |
|--------|--------|-----------|
| `chain_id` position | V1: keyword with default `None`. V2: required positional (2nd arg) | **YES** — but our code already passes it as keyword, so compatible |
| `tick_size_ttl` removed | V2 has built-in market info caching, no user-controlled TTL | No — we never passed this param |
| `use_server_time` added | V2 can use server time for order timestamps | No — defaults to `False` |
| `retry_on_error` added | V2 can auto-retry on version mismatch | No — defaults to `False` |
| `builder_config` type | V1: `py_builder_signing_sdk.config.BuilderConfig`. V2: `py_clob_client_v2.clob_types.BuilderConfig` | No — we don't pass builder_config |

**Our bot's constructor call** (`utils/createClobClient.py:492-498`):
```python
client = ClobClient(
    host,
    key=runtime_cfg.private_key,
    chain_id=runtime_cfg.chain_id,
    signature_type=int(runtime_cfg.signature_type),
    funder=...,
)
```
→ V2 compatible as-is (all args passed as keywords).

---

## Auth Method Rename

| V1 | V2 | Breaking? |
|----|-----|-----------|
| `create_or_derive_api_creds()` | `create_or_derive_api_key()` | **YES** |
| `set_api_creds(creds)` | `set_api_creds(creds)` | No |

**Our bot's call** (`utils/createClobClient.py:499`):
```python
user_api_creds = client.create_or_derive_api_creds()
```
→ **MUST change** to `client.create_or_derive_api_key()`.

Both return an `ApiCreds` dataclass with identical fields: `api_key`, `api_secret`, `api_passphrase`.

---

## Type Changes

### OrderArgs

```python
# V1 OrderArgs (py_clob_client.clob_types)
@dataclass
class OrderArgs:
    token_id: str
    price: float
    size: float
    side: str
    expiration: int = 0
    fee_rate_bps: int = 0
    nonce: int = 0
    taker: str = ZERO_ADDRESS

# V2 OrderArgsV2 (py_clob_client_v2.clob_types), aliased as OrderArgs
@dataclass
class OrderArgsV2:
    token_id: str
    price: float
    size: float
    side: str
    expiration: int = 0
    fee_rate_bps: int = 0       # REMOVED (handled server-side in V2)
    nonce: int = 0              # REMOVED
    taker: str = ZERO_ADDRESS   # REMOVED
    builder_code: str = BYTES32_ZERO  # NEW
    metadata: str = BYTES32_ZERO      # NEW
```

| Field | V1 | V2 | Impact |
|-------|-----|-----|--------|
| `fee_rate_bps` | Present | **Removed** | Our bot never sets this — safe |
| `nonce` | Present | **Removed** | Our bot never sets this — safe |
| `taker` | Present | **Removed** | Our bot never sets this — safe |
| `builder_code` | Absent | **New** (default: zero) | No action needed unless we want builder fees |
| `metadata` | Absent | **New** (default: zero) | No action needed |

**Our bot's usage** (`clients/order_gateway.py:33-38`):
```python
args = OrderArgs(token_id=..., side=..., size=..., price=...)
```
→ V2 compatible (only uses core fields).

### MarketOrderArgs

```python
# V1 MarketOrderArgs (py_clob_client.clob_types)
@dataclass
class MarketOrderArgs:
    token_id: str
    amount: float
    side: str
    price: float = 0
    order_type: OrderType = OrderType.FOK
    fee_rate_bps: int = 0
    nonce: int = 0
    taker: str = ZERO_ADDRESS

# V2 MarketOrderArgsV2, aliased as MarketOrderArgs
@dataclass
class MarketOrderArgsV2:
    token_id: str
    amount: float
    side: str
    price: float = 0
    order_type: OrderType = OrderType.FOK
    fee_rate_bps: int = 0         # REMOVED
    nonce: int = 0                # REMOVED
    taker: str = ZERO_ADDRESS     # REMOVED
    user_usdc_balance: float = 0  # NEW — used for fee-adjusted buy amounts
    builder_code: str = BYTES32_ZERO  # NEW
    metadata: str = BYTES32_ZERO      # NEW
```

| Field | V1 | V2 | Impact |
|-------|-----|-----|--------|
| `fee_rate_bps` | Present | **Removed** | We never set it — safe |
| `nonce` | Present | **Removed** | We never set it — safe |
| `taker` | Present | **Removed** | We never set it — safe |
| `user_usdc_balance` | Absent | **New** (default: 0) | Optional fee optimization for BUY market orders |
| `builder_code` | Absent | **New** (default: zero) | Optional |
| `metadata` | Absent | **New** (default: zero) | Optional |

**Our bot's usage** (`utils/createClobClient.py:287-293`):
```python
MarketOrderArgs(token_id=..., amount=..., side=..., price=..., order_type=...)
```
→ V2 compatible (only uses core fields).

### OrderType — No Change

Both V1 and V2: `GTC`, `FOK`, `GTD`, `FAK` (same string constants).

### PartialCreateOrderOptions — No Change

Both V1 and V2: `tick_size: Optional[TickSize]`, `neg_risk: Optional[bool]`.

### BUY / SELL Constants — No Change

Both V1 and V2: `BUY = "BUY"`, `SELL = "SELL"` in `order_builder.constants`.

---

## Method Signature Changes

### `post_order` (Signed Order Submission)

```python
# V1
client.post_order(order, orderType='GTC', post_only=False)

# V2
client.post_order(order, order_type='GTC', post_only=False, defer_exec=False)
```

| Change | Detail | Breaking? |
|--------|--------|-----------|
| `orderType` → `order_type` | Parameter **renamed** | **YES** |
| `defer_exec` added | New optional param | No — defaults to `False` |

**Our bot's call** (`utils/createClobClient.py:310`):
```python
self.client.post_order(signed_order, orderType=order_type)
```
→ **MUST change** to `order_type=order_type`.

### `cancel` → `cancel_order`

```python
# V1
client.cancel(order_id)        # Takes bare string

# V2
client.cancel_order(OrderPayload(orderID=order_id))  # Takes OrderPayload dataclass
```

| Change | Detail | Breaking? |
|--------|--------|-----------|
| Method renamed | `cancel` → `cancel_order` | **YES** |
| Argument type | Bare `str` → `OrderPayload` dataclass | **YES** |

**Our bot's call** (`clients/order_gateway.py:55`):
```python
client.cancel(order_id=order_id)
```
→ **MUST change** to `client.cancel_order(OrderPayload(orderID=order_id))`.

### `create_market_order` — Compatible

```python
# V1
client.create_market_order(order_args, options=None)

# V2
client.create_market_order(order_args, options=None)
```
Same signature. V2 internally adds fee caching and version negotiation but the call interface is identical.

### `create_and_post_order` — Extended

```python
# V1
client.create_and_post_order(order_args, options=None)

# V2
client.create_and_post_order(order_args, options=None, order_type='GTC', post_only=False, defer_exec=False)
```
New optional params only — backward compatible.

### `get_order_book` — Compatible

Both: `client.get_order_book(token_id)`. V1 returns `OrderBookSummary`; V2 returns dict or `OrderBookSummary`. Our code already handles both (uses `getattr` + dict fallback).

### `get_order` — Compatible

Both: `client.get_order(order_id)`. Same interface.

---

## Signature Type Enum

| Value | V1 | V2 | Notes |
|-------|-----|-----|-------|
| 0 | EOA | EOA | Same |
| 1 | POLY_PROXY | POLY_PROXY | Same |
| 2 | POLY_GNOSIS_SAFE | POLY_GNOSIS_SAFE | Same |
| 3 | — | **POLY_1271** (NEW) | Smart contract wallets; V1 doesn't support this |

**Our bot's enum** (`config/env.py:40-43`):
```python
class SignatureType(IntEnum):
    EOA = 0
    POLY_PROXY = 1
    GNOSIS_SAFE = 2
```
→ Our naming uses `GNOSIS_SAFE` (V2 uses `POLY_GNOSIS_SAFE`). Since we pass `int(signature_type)` to the SDK, integer values match — **no change needed**.

---

## V2-Only Features (Not Used by Our Bot Yet)

| Feature | Description | Migration Priority |
|---------|-------------|-------------------|
| Order version auto-negotiation | V2 client auto-detects V1/V2 exchange contracts | Automatic (handled by SDK) |
| `get_pre_migration_orders()` | Fetch open V1 orders during transition | Low — informational |
| Builder fees | `BuilderConfig`, `builder_code` on orders | Low — optional revenue feature |
| RFQ (Request for Quote) | `RfqClient` for block trades | Low — not needed for copy trading |
| `user_usdc_balance` on market buy | Fee-adjusted buy amount calculation | Medium — improves fill accuracy |
| `use_server_time` | Server-synced timestamps | Low — fallback if local clock is off |
| Market info caching | Built-in tick_size, neg_risk, fee caching | Already handled by our `ClobClientWrapper` |

---

## Breaking Changes Summary (Bot-Specific)

Only **3 call sites** require code changes when switching from V1 to V2:

| # | Location | Current V1 Call | Required V2 Call | Risk |
|---|----------|----------------|-----------------|------|
| 1 | `utils/createClobClient.py:499` | `client.create_or_derive_api_creds()` | `client.create_or_derive_api_key()` | Low — rename only |
| 2 | `utils/createClobClient.py:310` | `client.post_order(signed_order, orderType=order_type)` | `client.post_order(signed_order, order_type=order_type)` | Low — kwarg rename |
| 3 | `clients/order_gateway.py:55` | `client.cancel(order_id=order_id)` | `client.cancel_order(OrderPayload(orderID=order_id))` | Low — method + arg type change |

All other call sites (constructor, `create_market_order`, `create_and_post_order`, `get_order_book`, `get_order`, type constructors, constants) are **V2-compatible as-is**.

---

## Dependency Conflict Risk

V1 and V2 share common dependencies (`eth-account`, `eth-utils`, `httpx`, `poly_eip712_structs`, `py-order-utils`). V2 requires higher minimums:
- `eth-account>=0.13.0` (V1 has no floor)
- `eth-utils>=4.1.1` (V1 has no floor)
- `httpx[http2]>=0.27.0` (V1 uses plain `httpx`)

**Risk:** If V1 pins older versions, upgrading to V2 deps may break V1. The safe path is to switch fully from V1 to V2 in one step (swap `py-clob-client` → `py-clob-client-v2` in `pyproject.toml`), not run both side-by-side.

V2 drops the `py-builder-signing-sdk` dependency entirely (signing is now built-in). This simplifies the dependency tree.
