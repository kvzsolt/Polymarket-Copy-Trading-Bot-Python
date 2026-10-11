# Durable follower fill evidence

Each new authoritative PostgreSQL fill can produce a `follower_fill_observed`
event (schema version 2) in the same transaction as the fill and position
accounting. The application injects `PostgresFillEvidenceProjection` into
`FillsRepo`; both `append_fill` and the atomic position/recovered SELL paths
participate. A duplicate fill returns before another event or receipt timestamp
is created. An evidence append failure rolls back that accounting transaction.

The event identity combines the follower account and the actual database fill
ID. Order IDs, transaction hashes, and cumulative settlement watermarks are not
fill identities. Decimal strings preserve the observed price, size, and fee.
An absent fee remains unknown, while an explicitly observed zero remains zero.
The event also retains `execution_mode` as `paper`, `live`, or `unknown`. Both
copy submission paths persist their original mode in order metadata. Recovery
uses that original mode, even after a configuration change; older recovered
orders without explicit mode stay unknown. A direct current fill whose order
claims a conflicting known mode fails the transaction instead of being relabeled.
`received_ns` records the local native nanosecond clock sampled during the fill
transaction; venue fill time is not substituted for that observation.

The native owner decoder supplies `source_action_id`. Both copy execution paths
preserve it in order recovery metadata, and immediate fills also carry it in
their accounting record. Reconciliation reads only explicit metadata scoped to
the same follower and order. Conflicting attribution fails the transaction.
Legacy fills and risk exits with no source action remain observed but
unattributed. No transaction hash, promotion ID, or cumulative fill amount is
used to guess a link. Existing historical rows are not automatically backfilled.

Migration `20260917_11_evidence_transaction_outbox.sql` adds the transaction
outbox and its pending index through the existing ordered migration catalog.
`PostgresEvidenceDrain.drain_once()` locks at most 100 undrained rows, verifies
their identities and digests, and appends the unchanged events to the local
durable evidence publisher. It marks rows drained only after the local append
returns a durable cursor. A crash between append and marking replays the same
event; the local publisher deduplicates it. Failed batches retain their pending
PostgreSQL rows.

Hungary delivery runs independently. Neither the fill transaction nor the local
drain performs remote delivery, so a Hungary outage alone does not block risk
exits. Local PostgreSQL or durable storage failure remains an accounting or
evidence health failure and is surfaced by its owning runtime.

The tests exercise source propagation, unknown fees and attribution, PostgreSQL
commit/rollback and duplicate behavior, recovered SELL accounting, bounded
drains, and a crash after SQLite append. PostgreSQL tests use disposable local
containers. These tests do not validate live provider delivery, an environment
deployment, or the older MILP/Nautilus pipeline.
