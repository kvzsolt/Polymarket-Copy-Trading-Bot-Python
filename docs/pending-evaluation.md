# Bounded read-only pending evaluation

`services/pending_evaluation.py:BoundedPendingEvaluator` consumes the existing
provider owner's grouped Alchemy hash notifications. It has no socket, trading
callback, signing key, order submission, provider failover or mined-source control.
The integration owner supplies one filtered `alchemy_pendingTransactions`
subscription with `hashesOnly: true` and the two approved exchange destinations.

The evaluator's dependencies are constructor-injected:

- `gateway.call_observed("alchemy", "eth_getTransactionByHash", [hash], category="pending")`
  performs one admission before one body read and returns `ObservedRpcResult`.
- `stream_admission.charge_stream("alchemy", byte_count, category="pending")`
  charges all delivered bytes before UTF-8/JSON decoding or filtering. ACKs,
  unrelated hashes and malformed frames all count. The caller must not charge
  those same bytes again.
- `receipt_clock.capture()` returns the native `TransportReceipt` sample. Missing
  native nanoseconds, mixed process clocks or clock regression invalidate local
  evidence; milliseconds are never multiplied to pretend greater precision.
- `evidence_sink.append_event(event)` persists exact canonical evidence locally;
  `invalidate(reason)` exposes local write failure. Neither operation waits for
  Hungary delivery.
- Optional `decoder`, `verifier` and `deadline` provide deterministic test seams.

Call `observe_frame(raw, receipt, expected_subscription=...)` for every received
text or binary frame. Production uses `expected_subscription=""` until its ACK is
accepted, then the exact owned subscription ID. Foreign subscription frames never
trigger body reads. The optional `None` value is intended for standalone adapters
that already enforce ownership. The returned string describes an observed stage,
not permission to trade.

The socket owner must bound its task count and queue, close the pending-only stream
when `stop_requested` becomes true, call `expire()` on a bounded heartbeat, and call
`await aclose()` on retirement or shutdown. `inflight`, `stop_reason` and immutable
`observations()` summaries expose the finite work state. A roster change retires
the optional pending session; it does not mutate live mined-owner policies.

## Bounds and unknown outcomes

`PendingEvaluationLimits` defaults to 1,000 distinct retained hashes, 100 body
lookups, four in-flight reads, a 60-second session, a three-second body/attribution
deadline, five-second error backoff, and 16,384 bytes per frame. Configuration has
hard maxima. There is no unbounded lookup queue or automatic retry of a missing
body. Delivered-byte exhaustion and body admission failure stop the optional
session. Transport errors cause bounded backoff. Duplicate hashes are still
charged but never cause a second lookup in the same session.

Incomplete observations are durable `pending_observation` v2 events:
`hash_observed`, `body_unavailable`, `body_invalid`, `deadline_unknown`,
`budget_stopped` and `capacity_stopped`. A complete observation can emit
`attributed`. No event assigns a profitability score or asserts that a pending
transaction will execute. Competing sender/nonce bodies remain separate; their
replacement and finality remain unknown without independently mined proof.

`pending_body_observed` v2 records the original body receipt. Its `received_ns`
must equal `body_received_ns`; this body alone cannot create an attributed
candidate. A later `pending_observation(stage="attributed")` references its exact
source ID and supplies the separate `attributable_received_ns`. The receiver joins
these facts rather than treating body availability as owner attribution time.
Late bodies and already-mined bodies remain retained; a known block number is
preserved and cannot yield a pending candidate. Legacy v1 canonical bytes are
unchanged.

## Signed V2 orders

`integrations/pmrecon/pending_decoder.py` accepts direct native `matchOrders` calls
to the two approved V2 exchanges. It decodes seven flat ABI arguments, checks
bounded maker arrays before dynamic decoding, and verifies canonical round-trip
encoding. It retains every native owner, role, index and original signature.
Wrappers and unknown selectors require separate reviewed adapters.

`integrations/pmrecon/order_verification.py:V2OrderVerifier` decodes orders from the
exact canonical source event. It uses installed `eth-account` 0.13.7 EIP-712 message
encoding and recovery, with the explicit chain 137/name/version 2/exchange domain.
There is no signing in production. Deterministic message signatures exist only in
tests using a public fixture scalar.

The official contract source is pinned to
[`ccc0596074f4dfd62c944fbca4de252893b82b4b`](https://github.com/Polymarket/ctf-exchange-v2/tree/ccc0596074f4dfd62c944fbca4de252893b82b4b).
The implementation follows the public wire semantics in
[Hashing.sol](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/mixins/Hashing.sol),
[Structs.sol](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/libraries/Structs.sol), and
[Signatures.sol](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/mixins/Signatures.sol).
The pinned Solady dependency accepts 64-byte EIP-2098 and 65-byte signatures,
requires the recovery convention used by `ecrecover`, and accepts high-s
signatures; tests preserve those semantics rather than introducing a different
library's policy. Independent ABI hashing tests follow the pinned
[BaseExchangeTest.sol](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/test/BaseExchangeTest.sol)
formula and retain fixed vectors for both V2 exchange domains.

The `order_verification` event is explicitly `producer_verified`. It binds the
source ID, canonical source SHA-256, exact calldata SHA-256, signature SHA-256,
full order hash, domain, owner, signer, signature type, native role and index.
`input_hashes.order_fields` is SHA-256 of sorted compact ASCII JSON with these
keys: `salt`, `maker`, `signer`, `tokenId`, `makerAmount`, `takerAmount`, `side`,
`signatureType`, `timestamp`, `metadata`, `builder`. Integers remain integers,
addresses are lowercase, and bytes32 fields are lowercase `0x` hex.

Hash computation, cryptographic signature validity, wallet authority and
preapproval are independent statuses. EOA authority requires owner equal to the
verified signer. Proxy/Safe signatures can recover a signer, but wallet authority
stays unknown without pinned factory/contract evidence. ERC-1271 validity stays
unknown without its contract call. An empty signature may rely on preapproval;
no missing signature, state read or fee is replaced with a success or zero.

## Executed checks and limits

Run from the bot checkout, without loading dotenv:

```sh
PYTHON_DOTENV_DISABLED=1 .venv/bin/python -m pytest -q tests/test_aethertrade_pending_evaluation.py tests/test_aethertrade_order_verification.py
```

Tests use injected time/network and a real temporary SQLite outbox. They check
exact source clocks, missing/null/invalid bodies, admission failures, byte charges,
subscription ownership, cancellation, capacity, deadline overshoot, competing
nonces, per-order attribution and actual message recovery.
The fixed hash vectors are derived from the official ABI formula; no EVM/Forge
execution, real-provider subscription, funded trade or deployment is claimed.
These tests do not validate the whole legacy MILP/Nautilus pipeline.


## Finnish runtime ownership

`orchestrators/pending_evaluation_runtime.py` owns the optional socket and is
composed by `NativeDetectionRuntime` only when `detection.native.pending_enabled`
is explicitly true and at least one enabled Alchemy trader requests pending.
The default remains false. Provider admission and verified free-account limits
are also required; enabling a trader policy does not override those gates.

The runtime admits subscription cost before opening one connection, bounds its
subscription write to eight seconds, checks the exact subscription ACK, samples
native receipt time before decode, and allows at most four concurrent body tasks.
If those slots are occupied, the next received frame is still charged, then the
sampler stops with a durable coverage gap; it does not create an unbounded queue.
Idle receive deadlines apply the finite evaluator lifetime. There is no automatic
pending restart or fallback provider.

Pending observation leaves effective entry detection mined. A terminal pending
budget stop applies each trader's configured `mined` or `pause` policy; the pause
reason cannot be erased by a later concurrent lookup or socket cleanup failure.
Roster changes retire the sampler. Local evidence-write failure invalidates the
local evidence path. Remote Hungarian delivery remains a separate bounded worker.
The mined observer, execution reconciliation and risk owners remain independent.
