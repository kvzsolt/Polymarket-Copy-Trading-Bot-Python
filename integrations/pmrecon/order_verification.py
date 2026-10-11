"""Source-bound V2 EIP-712 verification using installed eth-account, never signing.

Protocol source: Polymarket/ctf-exchange-v2 at
ccc0596074f4dfd62c944fbca4de252893b82b4b, Hashing.sol/Structs.sol/Signatures.sol.
Only tests sign synthetic messages. Contract-wallet authorization needs separate
pinned evidence and is deliberately unknown here.
"""

from hashlib import sha256
import json

from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_keys.exceptions import BadSignature
from eth_utils.crypto import keccak

from domain.evidence import EvidenceEvent
from domain.pending_evaluation import PendingEvaluationError, PendingOrder
from domain.source_time import TransportReceipt
from integrations.pmrecon.codec import PmreconEvidenceCodec
from integrations.pmrecon.pending_decoder import V2PendingCalldataDecoder


class V2OrderVerifier:
    """Compute complete hashes and ECDSA results without inferring execution validity."""

    _FIELDS = (
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
    )

    def __init__(self, decoder: V2PendingCalldataDecoder | None = None) -> None:
        """Inject the bounded source decoder, with no provider or signing-key dependency."""
        self._decoder = decoder or V2PendingCalldataDecoder()

    def verify(
        self,
        source: EvidenceEvent,
        receipt: TransportReceipt,
        *,
        owners: frozenset[str] | None = None,
    ) -> tuple[EvidenceEvent, ...]:
        """Verify orders decoded from these exact source bytes, never caller-asserted orders."""
        try:
            if PmreconEvidenceCodec.encode(source.wire()) != source:
                raise PendingEvaluationError("verification_noncanonical_source")
            raw = source.wire()
            ns = receipt.wall_timestamp_ns
            if (
                raw["kind"] not in {"pending_body", "pending_body_observed"}
                or ns is None
                or receipt.boot_id != raw["clock_id"]
                or ns < int(raw["received_ns"])
            ):
                raise PendingEvaluationError("verification_invalid_source_clock")
            orders = self._decoder.decode(raw["target"], raw["calldata"])
            return tuple(
                self._verify_order(order, source, receipt)
                for order in orders
                if owners is None or order.owner in owners
            )
        except (KeyError, ValueError, TypeError):
            raise PendingEvaluationError("verification_invalid_source") from None

    def _verify_order(
        self, order: PendingOrder, source: EvidenceEvent, receipt: TransportReceipt
    ) -> EvidenceEvent:
        """Keep signature recovery, wallet authority and preapproval as separate facts."""
        source_wire = source.wire()
        domain = {
            "name": "Polymarket CTF Exchange",
            "version": "2",
            "chainId": 137,
            "verifyingContract": source_wire["target"].lower(),
        }
        message = dict(
            zip(
                (name for name, _ in self._FIELDS),
                (
                    order.salt,
                    order.owner,
                    order.signer,
                    order.token_id,
                    order.maker_amount,
                    order.taker_amount,
                    order.side,
                    order.signature_type,
                    order.timestamp,
                    "0x" + order.metadata.hex(),
                    "0x" + order.builder.hex(),
                ),
                strict=True,
            )
        )
        signable = encode_typed_data(
            domain_data=domain,
            message_types={"Order": [{"name": name, "type": typ} for name, typ in self._FIELDS]},
            message_data=message,
        )
        order_hash = (
            "0x" + keccak(b"\x19" + signable.version + signable.header + signable.body).hex()
        )
        signature_status = "unknown"
        authority_status = "unknown"
        if order.signature and order.signature_type != 3:
            signature_status = "invalid"
            try:
                signature = order.signature
                if len(signature) == 64:
                    vs = int.from_bytes(signature[32:], "big")
                    signature = (
                        signature[:32]
                        + (vs & ((1 << 255) - 1)).to_bytes(32, "big")
                        + bytes([27 + (vs >> 255)])
                    )
                if len(signature) == 65 and signature[64] in (27, 28):
                    recovered = Account.recover_message(signable, signature=signature).lower()
                    signature_status = "verified" if recovered == order.signer else "invalid"
            except (ValueError, TypeError, OverflowError, BadSignature):
                signature_status = "invalid"
            if order.signature_type == 0:
                authority_status = (
                    "verified"
                    if signature_status == "verified" and order.owner == order.signer
                    else "invalid"
                )
        fields = json.dumps(
            message, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
        return PmreconEvidenceCodec.encode(
            {
                "kind": "order_verification",
                "received_ns": str(receipt.wall_timestamp_ns),
                "clock_id": receipt.boot_id,
                "source_event_id": source.event_id,
                "source_digest": source.digest,
                "order_hash": order_hash,
                "order_role": order.role,
                "order_index": order.index,
                "owner": order.owner,
                "signer": order.signer,
                "signature_type": order.signature_type,
                "domain": {
                    "name": domain["name"],
                    "version": domain["version"],
                    "chain_id": 137,
                    "verifying_contract": domain["verifyingContract"],
                },
                "hash_status": "verified",
                "signature_status": signature_status,
                "authority_status": authority_status,
                "preapproval_status": "unknown",
                "verifier_version": "ctf-v2-eip712-ccc0596074-v1",
                "input_hashes": {
                    "calldata": sha256(bytes.fromhex(source_wire["calldata"][2:])).hexdigest(),
                    "order_fields": sha256(fields).hexdigest(),
                    "signature": sha256(order.signature).hexdigest(),
                },
            }
        )
