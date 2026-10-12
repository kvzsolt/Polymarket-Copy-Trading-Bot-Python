# Historical protocol evidence import

`pmcopy history-import` previews a reviewed finite plan by default. Preview reads
only the supplied plan file; it does not load runtime configuration, open a job
journal, admit RPC requests, instantiate an evidence publisher, or trade.

```sh
PYTHONPATH=src .venv/bin/python -m pmcopy.bot_cli.main history-import --plan /reviewed/history-plan.json
```

Explicit collection uses the existing `ProviderRuntime` admission ledger and its
configured Alchemy or Infura endpoint. It requires the normal verified free-plan
limits and usage baseline. It neither switches providers nor retries with an
unmetered transport. Every request uses `category="backfill"` and is subject to
both the shared provider limits and this job's durable request/CU limits.

```sh
PYTHONPATH=src .venv/bin/python -m pmcopy.bot_cli.main history-import \
  --plan /reviewed/history-plan.json \
  --journal /var/lib/pmcopy/history-reviewed-job.sqlite3 \
  --execute-read-only
```

This command writes only its local job journal. Configure the running bot's
`evidence.history_journal_path` with this exact journal path to drain committed
events through the bot's existing `EvidenceRuntime`. The default is `null`, so
historical delivery is disabled unless explicitly configured. There is no
directory scan and no second owner of the live evidence outbox. Hungary network
availability does not affect collection or this local handoff.

Plans are immutable and bound to a SHA-256 digest stored with the journal. Resume
using the same plan and journal. Changing a bound, scope, provider, or read plan
requires a new job journal. A shared-provider budget failure can be resumed when
admission becomes available; a consumed per-job hard limit is not reset on
restart. Completed jobs return their persisted result without requesting RPC.

The JSON plan includes these fields:

| Field | Meaning |
| --- | --- |
| `job_id` | 1–100 letters, digits, `_` or `-` |
| `provider` | `alchemy` or `infura`, already configured and admitted |
| `from_block`, `to_block` | Inclusive finite range, first block at least 1, at most 1,000 blocks |
| `wallets` | Explicit addresses, 1–256 distinct entries |
| `scope` | Reviewed assets, approvals, deployments and optional CTF position mappings |
| `state_reads` | Explicit approved ABI/code/storage reads at `boundaries` or `all_blocks` |
| `chunk_size` | 1–10 blocks, default 10 |
| `max_requests` | Durable attempt bound, 1–10,000, default 1,000 |
| `max_cu` | Durable provider-specific cost bound, 1–1,000,000, default 100,000 |
| `max_rows` | Retained response/event rows, 1–5,000, default 5,000 |
| `max_bytes` | Retained response/event payload bytes, 1,024–64,000,000, default 16,000,000 |

The collector always requests the complete approved CTF, pUSD and USDC.e emitter
set without wallet or event-topic filtering. `contracts`, if supplied, must be
exactly those three addresses. Each chunk retains every header, including the
opening block before the requested range, all returned logs and each distinct
transaction's full receipt. Approved-emitter receipt logs must exactly match the
range result. Fresh opening/closing anchors and parent-hash continuity must
agree before the cursor advances. Partial chunks never advance the cursor.

Original RPC payloads and native receipt nanoseconds are durable before chunk
publication. Resume reuses these exact responses and event IDs. Local delivery
marks a job event only after the existing publisher has durably appended it, so
a crash between those actions safely replays the same bytes. The normal job row
and byte limits reserve one additional bounded incomplete-coverage diagnostic
(at most 262,144 bytes); SQLite also has a finite page bound for metadata overhead.

State reads are block-hash pinned with `requireCanonical: true`. Supported methods
are `balanceOf`, `allowance`, `isApprovedForAll`, `payoutNumerators`,
`payoutDenominator`, `getOutcomeSlotCount`, `getCollectionId`, `getPositionId`,
`codehash` and `implementation`. The last two use `eth_getCode` and an explicitly
reviewed `eth_getStorageAt` slot. No proxy slot, implementation address, position
mapping, payout or code identity is inferred. A reviewed proxy implementation can
be read only for its explicitly approved code identity. Values remain exact
decimal strings, byte hashes or booleans.

For reconciliation, supply matching reviewed deployment code identities and
`codehash` reads at every block, including the opening block. Proxy deployments
also need per-block implementation-slot and implementation-code observations.
Supply exact opening/closing balance and approval reads for the declared scope.
CTF lifecycle mappings need explicit outcome-count, collection and token-ID
proof reads; payout-dependent reconciliation needs its actual payout reads.
Omitting these proofs preserves an unknown result even when collection completes.

`history_coverage.complete` is a finite collector claim, not proof of provider
completeness, chain finality, whole-account inventory, profitability, or live
trading readiness. pmrecon independently checks the referenced evidence and
conservation. These tests do not validate the older MILP/Nautilus pipeline.
No live-provider collection or environment-specific deployment verification has
been performed as part of this implementation.
