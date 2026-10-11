# Local evidence publication

`EvidenceRuntime` records source observations to a Finland-local SQLite outbox
before returning to its caller. Its independently scheduled publisher sends the
retained events to one configured pmrecon origin. A stalled Hungary connection
does not participate in the capture call. Local durable writes still require
filesystem I/O and fsync; their latency needs measurement on the target VPS.

The producer does not submit orders, infer missing fees or leader attribution,
calculate scores, enable pending execution, or promote a trading mode. The tests
below validate this producer boundary, not the old MILP/Nautilus pipeline.

## Composition

The application config loader owns `config.evidence.EvidenceConfig` and the
private `SecretStr` token. It must call
`EvidenceSecretBoundary.reject_yaml(payload)` for each YAML layer. No component
in this implementation loads dotenv or reads a credential file itself.

Configuration fields are `enabled`, `outbox_path`, `max_events`, `max_bytes`,
`max_history`, `origin`, `token`, `timeout_seconds`, `batch_size`, and
`retry_delay_seconds`. Defaults are a 10,000-event/64 MiB pending queue,
1,000,000 retained identities, five-second total request timeout, batches of up
to 100, and five seconds between idle/failed attempts. `enabled` defaults to
true. Config construction permits an empty secret for inspection; runtime
construction rejects a missing secret or disabled durable sink. The default
loopback origin is an explicit configurable endpoint, not an automatic fallback.

```python
runtime = EvidenceRuntime.from_config(app_config)
await runtime.start()  # schedules delivery; does not await a connection
runtime.record(provider, kind, payload, original_receipt, recovered=False)
runtime.append_event(canonical_event_dict)
health = runtime.health()
delivery = runtime.delivery_status
await runtime.aclose()
```

`record` implements `interfaces.grouped_detection.RawObservationSink`. It maps
`mined` to `chain_log` and requires the receipt's actual `wall_timestamp_ns` and
`boot_id`. Millisecond-only legacy receipts fail visibly; multiplying them into
fake nanosecond precision is forbidden. `block_header` emits validated pinned
coordinates plus the complete bounded original `raw_header`. Unsupported extra
header fields are rejected, not discarded. Root-owned market capture can append
`market_frame` with the original text, stream identity and positive sequence.

`append_event` accepts a wire object or immutable `EvidenceEvent`. Existing
BookTap output remains supported without importing pmrecon into the bot. Root
market and follower integration are separate ownership slices; this component
alone does not wire app startup, the market connection, or the fill ledger.

## Identity and acknowledgement

`PmreconEvidenceCodec` preserves the receiver's canonical sorted JSON bytes and
defaults. Money remains exact strings. Nanoseconds are decimal integer strings.
Unknown optional fee and attribution values stay absent/unknown. Implicit IDs
are SHA-256 of canonical bytes excluding `event_id`; explicit IDs preserve the
receiver's same-ID/same-content idempotency rule. Recovered observations and
removed logs retain their flags and source clocks.

The five original v1 kinds keep their original canonical representation.
Additive `block_header`, `market_frame`, and `follower_fill_observed` records carry
`schema_version: 2`; these do not rewrite v1 defaults or identities.

The POST body is exactly `{"events":[...]}`. Producer batches are bounded by
100 events and 1,000,000 serialized bytes, below pmrecon's default 1 MiB request
limit. A single canonical event is limited to 900,000 bytes. The ACK must have
protocol `aethertrade.evidence.v1` and exactly one ordered result per submitted
item, with matching event ID, positive unique remote sequence and boolean
duplicate flag. Remote global sequence numbers need not be adjacent. Only the
exact local FIFO prefix is acknowledged. Payloads are cleared transactionally;
identity/digest tombstones remain for conflict detection after restart.

An ambiguous request, malformed success, HTTP 429 or transient server failure
retains the original event bytes for idempotent retry. This retry policy applies
to evidence ingestion only. It is not permission to retry an ambiguous order.
Atomic receiver HTTP 400/401/403/404/409/413/422 responses quarantine the submitted
prefix and block further delivery. The receiver does not identify an offending
item in an atomic failure, so no item is silently removed to advance the queue.
Remote rejection leaves local continuity valid and permits bounded local capture,
including after restart. It does not create a synthetic loss gap. Delivery stays
blocked until a separate reviewed repair; no automatic authentication retry occurs.

## Local continuity and bounds

`health().valid` describes local continuity. `delivery_status` separately reports
`not_started`, `publishing`, `retry`, `acknowledged`, `idle`, `blocked`, `failed`,
or `closed`. A remote outage alone does not claim local evidence was lost. It
eventually reaches a configured queue limit if the receiver remains unavailable.

Capacity, conflicting IDs, invalid observations, or local write failure latch
invalid continuity. Ordinary appends then stop; exact already-known duplicates
remain idempotent. One reserved `gap` event, using the actual discovery clock,
sits outside the ordinary pending quota. The gap has no invented block range.
Its v1 reason is `overflow`; the local health row preserves the more specific
safe reason. Analysis must consume that gap and the local health state.

SQLite uses FULL synchronization, bounded page count, zero busy wait and one
process owner. The journal mode prevents a stalled reader from growing a WAL
without bound. Confirmed identities consume the separate finite history budget;
exhausting it invalidates continuity instead of silently forgetting old IDs.
Pending queries use the status index and do not scan acknowledged history.

Before each append a preallocated, fsynced `.health` latch marks a possible
interrupted write. Successful commits clear it. A terminal delivery rejection
uses a distinct persistent stop marker, so even a failed quarantine transaction
cannot resume delivery on restart. A delivery-only stop marker preserves local
validity. An interrupted local write marker, including a write made while delivery
was blocked, invalidates continuity and attempts to persist the reserved gap.
Total failure of the storage medium
cannot be made durably writable by software: in-process health remains invalid
and the source must stop. Never delete the spool or latch to claim recovery.
Operator-approved repair/retention and restoration of analysis completeness are
separate procedures; there is no automatic reset or rotation here.

`invalidate(reason)` exposes the same durable loss path for root-owned bounded
capture queues. Safe reasons are normalized without retaining arbitrary errors.
The transport never logs tokens, URLs or raw errors. Context-local filters
suppress HTTPX/HTTPCore logs for private deliveries while leaving unrelated HTTP
logging intact. Redirects, proxy environment settings, URL credentials, query
parameters and unbounded response bodies are rejected. Plain HTTP requires an
explicit loopback/private IP origin; production routing/TLS verification remains
environment-specific.

## Follower transaction seam

`PostgresEvidenceOutbox.append(connection, event)` requires an already-active
caller transaction and returns a stable sequence. It neither connects, commits,
starts its own transaction, nor performs network I/O. A conflicting ID raises so
the fill transaction owner can roll back. The migration and actual ledger call
site are root-owned integration work; they must share the same fill transaction.

Required columns in `evidence_transaction_outbox` are `seq bigserial primary key`,
`event_id text unique not null`, `digest text not null`, and `payload jsonb not
null`. Root-owned draining/status/capacity columns may extend this shape. This
append seam alone is not proof of a deployed migration or an atomic live fill.

## Reproducible local checks

From the bot checkout, without loading secrets:

```bash
PYTHON_DOTENV_DISABLED=1 .venv/bin/python -m pytest -q \
  tests/test_aethertrade_evidence_outbox.py \
  tests/test_aethertrade_evidence_publisher.py \
  tests/test_aethertrade_evidence_adapters.py \
  tests/test_aethertrade_evidence_contract.py \
  tests/test_aethertrade_evidence_runtime.py

npx --no-install pyright domain/evidence.py interfaces/evidence.py \
  storage/evidence_outbox.py services/evidence_publisher.py \
  integrations/pmrecon config/evidence.py orchestrators/evidence_runtime.py
```

The retained v1 fixture cases in `tests/fixtures/aethertrade_evidence_v1.json`
must remain byte-identical; `tests/test_aethertrade_evidence_contract.py` pins
them. These checks make no provider calls and no trades.

This checkout does not vendor the external pmrecon receiver: the receiver
cross-check tests and the receiver-driven reference program have been removed.

Deployment, real provider quotas, VPS disk latency/failure soak, external TLS,
credential provisioning, pending execution and whole legacy strategy validation
are not established by these local checks. The parent integration runs the full
repository suite after shared app/config changes settle.
