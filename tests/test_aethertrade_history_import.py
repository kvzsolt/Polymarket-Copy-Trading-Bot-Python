"""Historical evidence imports are explicit, bounded and resumable without trading."""

from copy import deepcopy

import pytest

from pmcopy.config.addresses import (
    POLYGON_USDCE_ADDRESS,
    POLYMARKET_CONDITIONAL_TOKENS_ADDRESS,
    POLYMARKET_PUSD_ADDRESS,
)
from pmcopy.domain.observed_rpc import ObservedRpcResult
from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.domain.source_time import TransportReceipt

CTF, PUSD, USDCE = (
    value.lower()
    for value in (
        POLYMARKET_CONDITIONAL_TOKENS_ADDRESS,
        POLYMARKET_PUSD_ADDRESS,
        POLYGON_USDCE_ADDRESS,
    )
)
OWNER = "0x" + "11" * 20
CONTRACTS = sorted((CTF, PUSD, USDCE))


def plan_dict(**changes):
    """A small reviewed scope never claims an implicit deployment or asset map."""
    return {
        "job_id": "history-test",
        "provider": "alchemy",
        "from_block": 16,
        "to_block": 17,
        "wallets": [OWNER],
        "contracts": CONTRACTS,
        "chunk_size": 1,
        "max_requests": 50,
        "max_cu": 5000,
        "max_rows": 100,
        "max_bytes": 2_000_000,
        "scope": {
            "assets": [{"contract": PUSD, "token_id": None}],
            "approvals": [],
            "deployments": [
                {
                    "contract": PUSD,
                    "kind": "erc20",
                    "code_hash": "0x" + "ab" * 32,
                    "proxy_implementation": None,
                    "implementation_code_hash": None,
                    "review_ref": "fixture-review",
                }
            ],
            "positions": [],
        },
        "state_reads": [
            {"contract": PUSD, "method": "balanceOf", "arguments": [OWNER], "at": "boundaries"}
        ],
        **changes,
    }


def plan(**changes):
    from pmcopy.domain.history_import import HistoryImportPlan

    return HistoryImportPlan.from_dict(plan_dict(**changes))


class HistoryClock:
    def capture(self):
        return TransportReceipt(1800000000123, 55, "history-clock", 1800000000123456789)


class HistoryGateway:
    """Only admitted observed read calls exist; no trade or REST API is exposed."""

    def __init__(self, *, fail_second_chunk=False, bad_receipt=False):
        self.calls = []
        self.fail_second_chunk = fail_second_chunk
        self.bad_receipt = bad_receipt

    @staticmethod
    def block(number):
        return {
            "number": hex(number),
            "hash": "0x" + f"{number:064x}",
            "parentHash": "0x" + f"{number - 1:064x}",
            "timestamp": hex(1000 + number),
            "transactions": ["0x" + "bb" * 32] if number == 16 else [],
        }

    @staticmethod
    def log():
        return {
            "address": PUSD,
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x" + "00" * 32,
                "0x" + OWNER[2:].rjust(64, "0"),
            ],
            "data": "0x" + f"{1:064x}",
            "blockNumber": "0x10",
            "blockHash": HistoryGateway.block(16)["hash"],
            "transactionHash": "0x" + "bb" * 32,
            "transactionIndex": "0x0",
            "logIndex": "0x0",
            "removed": False,
        }

    async def call_observed(self, provider, method, params, *, category):
        assert category == "backfill" and provider == "alchemy"
        self.calls.append((provider, method, deepcopy(params), category))
        if method == "eth_getBlockByNumber":
            value = self.block(int(params[0], 16))
        elif method == "eth_getLogs":
            assert sorted(params[0]["address"]) == CONTRACTS
            assert "topics" not in params[0]
            if self.fail_second_chunk and params[0]["fromBlock"] == "0x11":
                raise BudgetExhausted("fixture budget exhausted")
            value = (
                [self.log()]
                if int(params[0]["fromBlock"], 16) <= 16 <= int(params[0]["toBlock"], 16)
                else []
            )
        elif method == "eth_getTransactionReceipt":
            value = {
                "transactionHash": "0x" + "bb" * 32,
                "transactionIndex": "0x0",
                "blockNumber": "0x10",
                "blockHash": self.block(16)["hash"],
                "status": "0x1",
                "logs": [self.log()],
            }
            if self.bad_receipt:
                value["logs"] = []
        elif method == "eth_call":
            assert params[1].get("requireCanonical") is True
            value = "0x" + f"{42:064x}"
        else:
            raise AssertionError(f"unapproved fixture method: {method}")
        return ObservedRpcResult(value, HistoryClock().capture())


def test_preview_is_pure_and_plan_identity_is_stable():
    original = plan_dict()
    from pmcopy.domain.history_import import HistoryImportPlan

    parsed = HistoryImportPlan.from_dict(original)
    preview = parsed.preview()
    assert preview["read_only"] is True and preview["execute"] is False
    assert preview["from_block"] == 16 and preview["opening_block"] == 15
    assert parsed.digest == HistoryImportPlan.from_dict(deepcopy(original)).digest
    original["scope"]["assets"].clear()
    assert parsed.scope.wire()["assets"]


@pytest.mark.parametrize(
    "changes",
    [
        {"from_block": 0},
        {"to_block": 1016},
        {"provider": "other"},
        {"contracts": [PUSD]},
        {"max_requests": 0},
        {"max_cu": True},
        {
            "state_reads": [
                {
                    "contract": PUSD,
                    "method": "transfer",
                    "arguments": [OWNER, "1"],
                    "at": "boundaries",
                }
            ]
        },
    ],
)
def test_plan_rejects_unbounded_or_unreviewed_reads(changes):
    with pytest.raises(ValueError):
        plan(**changes)


def test_job_reservations_and_cached_native_receipts_survive_restart(tmp_path):
    from pmcopy.storage.history_import import HistoryImportJournal

    selected = plan(max_requests=1)
    path = tmp_path / "history.sqlite3"
    journal = HistoryImportJournal(path, selected)
    journal.reserve_request(20)
    observation = ObservedRpcResult(HistoryGateway.block(15), HistoryClock().capture())
    journal.record_response("opening", observation)
    journal.close()
    reopened = HistoryImportJournal(path, selected)
    assert reopened.response("opening") == observation
    with pytest.raises(ValueError, match="request_limit"):
        reopened.reserve_request(20)
    assert reopened.cursor == 15
    reopened.close()


@pytest.mark.asyncio
async def test_collector_captures_full_emitters_headers_receipts_and_pinned_boundaries(tmp_path):
    from pmcopy.services.history_import import HistoryImportCollector
    from pmcopy.storage.history_import import HistoryImportJournal

    selected, gateway = plan(), HistoryGateway()
    journal = HistoryImportJournal(tmp_path / "history", selected)
    collector = HistoryImportCollector(
        selected, journal, gateway, rpc_cost=lambda p, m: 20, clock=HistoryClock()
    )
    outcome = await collector.collect()
    assert outcome.complete and journal.cursor == 17
    values = [event.wire() for event in journal.pending()]
    assert {event["kind"] for event in values} >= {
        "block_header",
        "protocol_log",
        "receipt",
        "contract_state",
        "history_coverage",
    }
    coverage = next(event for event in reversed(values) if event["kind"] == "history_coverage")
    assert coverage["complete"] is True and coverage["plan_digest"] == selected.digest
    assert len(coverage["header_event_ids"]) == 3
    assert len(coverage["receipt_event_ids"]) == 1
    assert all(event["recovery"] for event in values)
    assert {event["block_number"] for event in values if event["kind"] == "contract_state"} == {
        15,
        17,
    }
    journal.close()


@pytest.mark.asyncio
async def test_budget_failure_keeps_partial_chunk_uncommitted_and_restart_reuses_evidence(tmp_path):
    from pmcopy.services.history_import import HistoryImportCollector
    from pmcopy.storage.history_import import HistoryImportJournal

    selected, path = plan(), tmp_path / "history"
    journal = HistoryImportJournal(path, selected)
    first = await HistoryImportCollector(
        selected,
        journal,
        HistoryGateway(fail_second_chunk=True),
        rpc_cost=lambda p, m: 20,
        clock=HistoryClock(),
    ).collect()
    assert not first.complete and first.reason == "budget_exhausted"
    assert journal.cursor == 16
    retained = {event.event_id: event.canonical_bytes for event in journal.pending()}
    journal.close()
    journal = HistoryImportJournal(path, selected)
    second_gateway = HistoryGateway()
    result = await HistoryImportCollector(
        selected, journal, second_gateway, rpc_cost=lambda p, m: 20, clock=HistoryClock()
    ).collect()
    assert result.complete and journal.cursor == 17
    assert not any(
        method == "eth_getTransactionReceipt" for _, method, _, _ in second_gateway.calls
    )
    current = {event.event_id: event.canonical_bytes for event in journal.pending()}
    assert all(current[identity] == payload for identity, payload in retained.items())
    journal.close()


@pytest.mark.asyncio
async def test_receipt_omitting_a_returned_log_cannot_advance_cursor(tmp_path):
    from pmcopy.services.history_import import HistoryImportCollector
    from pmcopy.storage.history_import import HistoryImportJournal

    selected = plan()
    journal = HistoryImportJournal(tmp_path / "history", selected)
    result = await HistoryImportCollector(
        selected,
        journal,
        HistoryGateway(bad_receipt=True),
        rpc_cost=lambda p, m: 20,
        clock=HistoryClock(),
    ).collect()
    assert not result.complete and result.reason == "invalid_evidence"
    assert journal.cursor == 15
    journal.close()


def test_shared_provider_price_query_does_not_admit_or_mutate_a_ledger():
    from pmcopy.storage.provider_budget import SqliteProviderBudget

    assert SqliteProviderBudget.rpc_cost("alchemy", "eth_getLogs") == 60
    with pytest.raises(ValueError):
        SqliteProviderBudget.rpc_cost("infura", "eth_getLogs")
    with pytest.raises(ValueError):
        SqliteProviderBudget.rpc_cost("alchemy", "eth_sendRawTransaction")


def coverage_value():
    selected = plan()
    return {
        "kind": "history_coverage",
        "schema_version": 2,
        "provider": selected.provider,
        "chain_id": 137,
        "received_ns": "1800000000123456789",
        "clock_id": "history-clock",
        "recovery": True,
        "job_id": selected.job_id,
        "plan_digest": selected.digest,
        "from_block": selected.from_block,
        "to_block": selected.to_block,
        "wallets": list(selected.wallets),
        "contracts": list(selected.contracts),
        "header_event_ids": [],
        "receipt_event_ids": [],
        "state_event_ids": [],
        "log_event_ids": [],
        "complete": False,
        "reason": "budget_exhausted",
        "scope": selected.scope.wire(),
    }


def test_coverage_codec_preserves_bounded_explicit_unknown_scope():
    from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec

    event = PmreconEvidenceCodec.encode(coverage_value())
    wire = event.wire()
    assert wire["complete"] is False and wire["reason"] == "budget_exhausted"
    assert wire["scope"]["assets"] == [{"contract": PUSD}]
    assert wire["received_ns"] == "1800000000123456789"
    with pytest.raises(ValueError):
        PmreconEvidenceCodec.encode(coverage_value() | {"complete": True})


def test_state_calls_are_hash_pinned_exact_abi_reads():
    from pmcopy.domain.history_import import HistoryStateRead
    from pmcopy.services.history_state_reads import HistoryStateReadAdapter

    read = HistoryStateRead(PUSD, "balanceOf", (OWNER,))
    block_hash = HistoryGateway.block(15)["hash"]
    method, params = HistoryStateReadAdapter.request(read, block_hash)
    assert method == "eth_call"
    assert params == [
        {"to": PUSD, "data": "0x70a08231" + OWNER[2:].rjust(64, "0")},
        {"blockHash": block_hash, "requireCanonical": True},
    ]
    assert HistoryStateReadAdapter.value(read, "0x" + "00" * 31 + "2a") == "42"
    with pytest.raises(ValueError):
        HistoryStateReadAdapter.value(read, "0x2a")


@pytest.mark.parametrize(
    "method,args,payload,expected",
    [
        (
            "getCollectionId",
            ("0x" + "00" * 32, "0x" + "11" * 32, "1"),
            "0x" + "22" * 32,
            "0x" + "22" * 32,
        ),
        ("getOutcomeSlotCount", ("0x" + "11" * 32,), "0x" + "00" * 31 + "02", "2"),
        ("isApprovedForAll", (OWNER, "0x" + "22" * 20), "0x" + "00" * 31 + "01", True),
    ],
)
def test_state_result_types_do_not_guess_position_or_approval_values(
    method, args, payload, expected
):
    from pmcopy.domain.history_import import HistoryStateRead
    from pmcopy.services.history_state_reads import HistoryStateReadAdapter

    assert HistoryStateReadAdapter.value(HistoryStateRead(CTF, method, args), payload) == expected


def test_direct_scope_construction_cannot_bypass_review_validation():
    import json
    from dataclasses import replace
    from pmcopy.domain.history_import import ReviewedHistoryScope

    selected = plan()
    scope = selected.scope.wire()
    scope["assets"][0]["contract"] = OWNER
    with pytest.raises(ValueError):
        replace(selected, scope=ReviewedHistoryScope(json.dumps(scope)))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"max_requests": 1}, "request_limit"),
        ({"max_cu": 10}, "budget_exhausted"),
        ({"max_rows": 1}, "row_limit"),
        ({"max_bytes": 1024}, "byte_limit"),
    ],
)
async def test_hard_job_caps_keep_a_durable_incomplete_claim(tmp_path, changes, reason):
    from pmcopy.services.history_import import HistoryImportCollector
    from pmcopy.storage.history_import import HistoryImportJournal

    selected = plan(**changes)
    journal = HistoryImportJournal(tmp_path / "job", selected)
    gateway = HistoryGateway()
    result = await HistoryImportCollector(
        selected, journal, gateway, rpc_cost=lambda p, m: 20, clock=HistoryClock()
    ).collect()
    assert not result.complete and result.reason == reason and journal.cursor == 15
    assert journal.pending()[-1].wire()["complete"] is False
    journal.close()


@pytest.mark.asyncio
async def test_changed_closing_anchor_cannot_commit_a_chunk(tmp_path):
    from pmcopy.services.history_import import HistoryImportCollector
    from pmcopy.storage.history_import import HistoryImportJournal

    class ChangedGateway(HistoryGateway):
        async def call_observed(self, provider, method, params, *, category):
            observed = await super().call_observed(provider, method, params, category=category)
            if (
                method == "eth_getBlockByNumber"
                and params[0] == "0x10"
                and sum(call[1] == method and call[2] == params for call in self.calls) > 1
            ):
                return ObservedRpcResult(
                    observed.payload | {"hash": "0x" + "cc" * 32}, observed.receipt
                )
            return observed

    selected = plan()
    journal = HistoryImportJournal(tmp_path / "job", selected)
    result = await HistoryImportCollector(
        selected, journal, ChangedGateway(), rpc_cost=lambda p, m: 20, clock=HistoryClock()
    ).collect()
    assert result.reason == "chain_changed" and journal.cursor == 15
    assert not journal.references()["log_event_ids"]
    journal.close()


@pytest.mark.asyncio
async def test_local_drain_replays_same_bytes_after_append_before_mark_crash(tmp_path):
    from pmcopy.services.history_import import HistoryImportCollector
    from pmcopy.storage.history_import import HistoryImportJournal
    from pmcopy.orchestrators.history_import_runtime import HistoryImportRuntime

    selected = plan()
    journal = HistoryImportJournal(tmp_path / "job", selected)
    await HistoryImportCollector(
        selected, journal, HistoryGateway(), rpc_cost=lambda p, m: 20, clock=HistoryClock()
    ).collect()
    first = journal.pending()[0]

    class Sink:
        def __init__(self):
            self.events = {}
            self.fail = True

        def append_event(self, event):
            assert (
                self.events.setdefault(event.event_id, event.canonical_bytes)
                == event.canonical_bytes
            )
            if self.fail:
                self.fail = False
                raise RuntimeError("fixture crash after durable append")
            return len(self.events)

    sink = Sink()
    runtime = HistoryImportRuntime(journal, sink, batch_size=2)
    with pytest.raises(RuntimeError):
        runtime.publish_local_once()
    assert journal.pending()[0] == first
    assert runtime.publish_local_once() == 2
    assert len(sink.events) == 2
    assert all(event.event_id != first.event_id for event in journal.pending())
    journal.close()



@pytest.mark.asyncio
async def test_repeated_hard_limit_reuses_original_diagnostic_with_real_clock_advancement(tmp_path):
    from pmcopy.services.history_import import HistoryImportCollector
    from pmcopy.storage.history_import import HistoryImportJournal

    class AdvancingClock:
        def __init__(self):
            self.tick = 1800000000123456789

        def capture(self):
            self.tick += 1
            return TransportReceipt(self.tick // 1000000, self.tick, "clock", self.tick)

    selected = plan(max_rows=1)
    journal = HistoryImportJournal(tmp_path / "job", selected)
    collector = HistoryImportCollector(
        selected, journal, HistoryGateway(), rpc_cost=lambda p, m: 20, clock=AdvancingClock()
    )
    first = await collector.collect()
    second = await collector.collect()
    assert first.reason == second.reason == "row_limit"
    assert first.coverage_event_id == second.coverage_event_id
    journal.close()


@pytest.mark.parametrize("changed", [{"recovery": 1}, {"schema_version": 2.0}, {"chain_id": 137.0}])
def test_history_producer_rejects_coerced_literal_scalars(changed):
    from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec

    with pytest.raises(ValueError):
        PmreconEvidenceCodec.encode(coverage_value() | changed)


@pytest.mark.asyncio
async def test_exhausted_retention_cap_never_spends_provider_budget_on_resume(tmp_path):
    from pmcopy.services.history_import import HistoryImportCollector
    from pmcopy.storage.history_import import HistoryImportJournal

    selected = plan(max_rows=1)
    journal = HistoryImportJournal(tmp_path / "job", selected)
    gateway = HistoryGateway()
    collector = HistoryImportCollector(
        selected, journal, gateway, rpc_cost=lambda p, m: 20, clock=HistoryClock()
    )
    await collector.collect()
    before = len(gateway.calls)
    await collector.collect()
    assert len(gateway.calls) == before
    journal.close()
