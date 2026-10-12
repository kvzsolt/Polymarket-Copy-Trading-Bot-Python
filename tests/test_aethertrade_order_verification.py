"""Pinned V2 ABI/EIP-712 checks with public deterministic message-signing fixtures."""

from hashlib import sha256

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak
import pytest

from pmcopy.domain.source_time import TransportReceipt
from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec

EXCHANGE = "0xe111180000d2663c0091e4f400237545b87b996b"
NEG_RISK = "0xe2222d279d744050d28e00520010520000310f59"
# Public fixture scalar. No wallet is loaded and no transaction is ever signed/sent.
FIXTURE_KEY = (1).to_bytes(32, "big")
OWNER = Account.from_key(FIXTURE_KEY).address.lower()
OTHER = "0x" + "22" * 20
TUPLE = (
    "(uint256,address,address,uint256,uint256,uint256,uint8,uint8,uint256,bytes32,bytes32,bytes)"
)
ARGUMENTS = ["bytes32", TUPLE, TUPLE + "[]", "uint256", "uint256[]", "uint256", "uint256[]"]
TYPEHASH = "bb86318a2138f5fa8ae32fbe8e659f8fcf13cc6ae4014a707893055433818589"
FIELDS = [
    ("salt", "uint256"),
    ("maker", "address"),
    ("signer", "address"),
    ("tokenId", "uint256"),
    ("makerAmount", "uint256"),
    ("takerAmount", "uint256"),
    ("side", "uint8"),
    ("signatureType", "uint8"),
    ("timestamp", "uint256"),
    ("metadata", "bytes32"),
    ("builder", "bytes32"),
]


def order(*, owner=OWNER, signer=OWNER, signature_type=0, salt=1, signature=b""):
    """Keep raw field order independent of the producer's domain/encoder."""
    return (
        salt,
        owner,
        signer,
        123,
        50_000_000,
        100_000_000,
        0,
        signature_type,
        1_800_000_000_000,
        bytes(32),
        bytes.fromhex("12" * 32),
        signature,
    )


def signed(value, exchange=EXCHANGE):
    """Produce a real test EIP-712 signature using independently declared public fields."""
    message = encode_typed_data(
        domain_data={
            "name": "Polymarket CTF Exchange",
            "version": "2",
            "chainId": 137,
            "verifyingContract": exchange,
        },
        message_types={"Order": [{"name": name, "type": typ} for name, typ in FIELDS]},
        message_data={name: value[index] for index, (name, _) in enumerate(FIELDS)},
    )
    signature = bytes(Account.sign_message(message, FIXTURE_KEY).signature)
    return value[:-1] + (signature,)


def calldata(taker=None, makers=None):
    """Encode the seven flat external function arguments, never a wrapping tuple."""
    taker = taker or signed(order())
    makers = (
        makers if makers is not None else [signed(order(owner=OTHER, signature_type=1, salt=2))]
    )
    body = encode(
        ARGUMENTS,
        [
            bytes.fromhex("ab" * 32),
            taker,
            makers,
            50_000_000,
            [50_000_000] * len(makers),
            0,
            [0] * len(makers),
        ],
    )
    return "0x3c2b4399" + body.hex()


def source(raw=None, exchange=EXCHANGE):
    """Canonical pending-body evidence retains all separate native receipt samples."""
    return PmreconEvidenceCodec.encode(
        {
            "kind": "pending_body",
            "provider": "alchemy",
            "received_ns": "1800000000000000300",
            "hash_received_ns": "1800000000000000100",
            "body_received_ns": "1800000000000000200",
            "clock_id": "finland-test",
            "transaction_hash": "0x" + "aa" * 32,
            "target": exchange,
            "sender": OTHER,
            "nonce": 1,
            "calldata": raw or calldata(),
        }
    )


def receipt():
    return TransportReceipt(1_800_000_000_000, 400, "finland-test", 1_800_000_000_000_000_400)


def reference_hash(value, exchange):
    """Independent ABI formula from pinned BaseExchangeTest._generateOrderHash."""
    domain = keccak(
        encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [
                keccak(
                    text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
                ),
                keccak(text="Polymarket CTF Exchange"),
                keccak(text="2"),
                137,
                exchange,
            ],
        )
    )
    struct = keccak(
        encode(
            ["bytes32"] + [typ for _, typ in FIELDS], [bytes.fromhex(TYPEHASH)] + list(value[:-1])
        )
    )
    return "0x" + keccak(b"\x19\x01" + domain + struct).hex()


def test_decoder_preserves_all_native_owner_orders_roles_and_signatures():
    from pmcopy.integrations.pmrecon.pending_decoder import V2PendingCalldataDecoder

    values = [signed(order(owner=OTHER, signature_type=1, salt=2)), signed(order(salt=3))]
    decoded = V2PendingCalldataDecoder().decode(EXCHANGE, calldata(makers=values))
    assert [(item.owner, item.role, item.index) for item in decoded] == [
        (OWNER, "taker", 0),
        (OTHER, "maker", 0),
        (OWNER, "maker", 1),
    ]
    assert decoded[1].signature == values[0][-1]


@pytest.mark.parametrize(
    "fault",
    [
        "wrapper",
        "truncated",
        "trailing",
        "too_many",
        "wrong_target",
        "invalid_side",
        "wrong_selector",
    ],
)
def test_decoder_rejects_unqualified_or_unbounded_calls(fault):
    from pmcopy.integrations.pmrecon.pending_decoder import V2PendingCalldataDecoder

    raw, target = calldata(), EXCHANGE
    if fault == "wrapper":
        raw = (
            "0x3c2b4399"
            + encode(
                ["(" + ",".join(ARGUMENTS) + ")"], [(bytes(32), order(), [order()], 1, [1], 0, [0])]
            ).hex()
        )
    elif fault == "truncated":
        raw = raw[:-2]
    elif fault == "trailing":
        raw += "00" * 32
    elif fault == "too_many":
        raw = calldata(makers=[order()] * 65)
    elif fault == "wrong_target":
        target = OTHER
    elif fault == "wrong_selector":
        raw = "0xdeadbeef" + raw[10:]
    else:
        value = list(order())
        value[6] = 2
        raw = calldata(taker=tuple(value))
    with pytest.raises(ValueError):
        V2PendingCalldataDecoder().decode(target, raw)


@pytest.mark.parametrize("exchange", [EXCHANGE, NEG_RISK])
def test_real_eip712_hash_and_recovery_match_official_v2_formula(exchange):
    from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier

    value = signed(order(), exchange)
    event = source(calldata(taker=value), exchange)
    verified = V2OrderVerifier().verify(event, receipt(), owners=frozenset({OWNER}))
    wire = verified[0].wire()
    assert wire["order_hash"] == reference_hash(value, exchange)
    assert wire["hash_status"] == wire["signature_status"] == wire["authority_status"] == "verified"
    assert wire["preapproval_status"] == "unknown"
    assert wire["source_event_id"] == event.event_id and wire["source_digest"] == event.digest
    assert (
        wire["input_hashes"]["calldata"]
        == sha256(bytes.fromhex(event.wire()["calldata"][2:])).hexdigest()
    )
    assert wire["domain"] == {
        "name": "Polymarket CTF Exchange",
        "version": "2",
        "chain_id": 137,
        "verifying_contract": exchange,
    }


@pytest.mark.parametrize("signature_type", [1, 2, 3])
def test_signature_recovery_never_fabricates_contract_wallet_authority(signature_type):
    from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier

    value = signed(order(owner=OTHER, signature_type=signature_type))
    wire = (
        V2OrderVerifier()
        .verify(source(calldata(taker=value)), receipt(), owners=frozenset({OTHER}))[0]
        .wire()
    )
    assert wire["authority_status"] == "unknown"
    assert wire["signature_status"] == ("unknown" if signature_type == 3 else "verified")
    assert wire["preapproval_status"] == "unknown"


@pytest.mark.parametrize("signature", [b"", bytes(65), b"invalid-length"])
def test_missing_signature_preserves_preapproval_unknown_and_invalid_signature_is_not_valid(
    signature,
):
    from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier

    wire = (
        V2OrderVerifier()
        .verify(
            source(calldata(taker=order(signature=signature))), receipt(), owners=frozenset({OWNER})
        )[0]
        .wire()
    )
    assert wire["signature_status"] == ("unknown" if not signature else "invalid")
    assert wire["preapproval_status"] == "unknown"
    assert wire["authority_status"] != "verified"


def test_compact_signature_matches_pinned_solady_and_v_zero_does_not():
    from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier

    value = signed(order())
    signature = value[-1]
    compact = signature[:32] + (
        int.from_bytes(signature[32:64], "big") | ((signature[64] - 27) << 255)
    ).to_bytes(32, "big")
    verifier = V2OrderVerifier()
    wire = verifier.verify(
        source(calldata(taker=value[:-1] + (compact,))), receipt(), owners=frozenset({OWNER})
    )[0].wire()
    assert wire["signature_status"] == "verified"
    wire = verifier.verify(
        source(calldata(taker=value[:-1] + (signature[:-1] + bytes([signature[-1] - 27]),))),
        receipt(),
        owners=frozenset({OWNER}),
    )[0].wire()
    assert wire["signature_status"] == "invalid"


def test_wrong_domain_or_wrong_signer_fails_and_mixed_batch_stays_per_order():
    from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier

    wrong_domain = signed(order(), NEG_RISK)
    event = source(
        calldata(taker=wrong_domain, makers=[signed(order(salt=2)), signed(order(salt=3))])
    )
    wires = [
        item.wire()
        for item in V2OrderVerifier().verify(event, receipt(), owners=frozenset({OWNER}))
    ]
    assert [item["signature_status"] for item in wires] == ["invalid", "verified", "verified"]
    assert len({item["order_hash"] for item in wires}) == 3
    assert [(item["order_role"], item["order_index"]) for item in wires] == [
        ("taker", 0),
        ("maker", 0),
        ("maker", 1),
    ]


def test_verifier_refuses_forged_source_kind_or_regressing_clock():
    from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier

    verifier = V2OrderVerifier()
    wrong = PmreconEvidenceCodec.encode(
        {
            "kind": "gap",
            "provider": "bot",
            "reason": "disconnected",
            "received_ns": "1",
            "clock_id": "finland-test",
        }
    )
    with pytest.raises(ValueError):
        verifier.verify(wrong, receipt())
    with pytest.raises(ValueError):
        verifier.verify(source(), TransportReceipt(1, 1, "other", 1_000_000))


@pytest.mark.parametrize(
    "exchange,expected",
    [
        (EXCHANGE, "0x1bf102cae05ddf529c1e6e77ed7502ca58d19018e76c51e947a7dbc05cb31750"),
        (NEG_RISK, "0x4e868aeac5dc2b1d8d329dcef60e68e67287216473a498aadda9060c68f8dd81"),
    ],
)
def test_fixed_vectors_from_pinned_official_hash_formula(exchange, expected):
    from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier

    assert reference_hash(order(), exchange) == expected
    result = V2OrderVerifier().verify(source(calldata(taker=order()), exchange), receipt())[0]
    assert result.wire()["order_hash"] == expected


def test_pinned_solady_high_s_signature_and_eoa_owner_mismatch():
    from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier

    value = signed(order(owner=OTHER))
    signature = value[-1]
    curve_order = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
    high_s = (
        signature[:32]
        + (curve_order - int.from_bytes(signature[32:64], "big")).to_bytes(32, "big")
        + bytes([55 - signature[-1]])
    )
    result = (
        V2OrderVerifier()
        .verify(source(calldata(taker=value[:-1] + (high_s,))), receipt())[0]
        .wire()
    )
    assert result["signature_status"] == "verified"  # Pinned Solady accepts high-s.
    assert result["authority_status"] == "invalid"  # EOA maker must equal recovered signer.
    assert result["preapproval_status"] == "unknown"
