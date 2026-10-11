"""Encode a small approved read ABI and decode exact block-pinned return values."""

import re

from eth_utils.crypto import keccak

from domain.history_import import CTF, HistoryBoundary, HistoryImportError, HistoryStateRead


class HistoryStateReadAdapter:
    """No signer, transaction sender, arbitrary calldata, or provider fallback exists."""

    @staticmethod
    def request(read: HistoryStateRead, block_hash: str) -> tuple[str, list[object]]:
        """Pin every supported read to the exact canonical block hash (EIP-1898)."""
        pin = {"blockHash": HistoryBoundary.digest(block_hash), "requireCanonical": True}
        if read.method == "codehash":
            return "eth_getCode", [read.contract, pin]
        if read.method == "implementation":
            return "eth_getStorageAt", [read.contract, read.storage_slot, pin]
        signatures = {
            "balanceOf": "balanceOf(address,uint256)"
            if read.contract == CTF
            else "balanceOf(address)",
            "allowance": "allowance(address,address)",
            "isApprovedForAll": "isApprovedForAll(address,address)",
            "payoutNumerators": "payoutNumerators(bytes32,uint256)",
            "payoutDenominator": "payoutDenominator(bytes32)",
            "getOutcomeSlotCount": "getOutcomeSlotCount(bytes32)",
            "getCollectionId": "getCollectionId(bytes32,bytes32,uint256)",
            "getPositionId": "getPositionId(address,bytes32)",
        }
        selector = keccak(text=signatures[read.method])[:4].hex()
        words = [
            arg[2:].lower().rjust(64, "0")
            if arg.startswith("0x")
            else f"{HistoryBoundary.uint(arg):064x}"
            for arg in read.arguments
        ]
        return "eth_call", [{"to": read.contract, "data": "0x" + selector + "".join(words)}, pin]

    @staticmethod
    def value(read: HistoryStateRead, result: object) -> str | bool:
        """Keep uint256 and boolean types exact; never infer a missing call result."""
        if read.method == "codehash":
            if not isinstance(result, str) or not re.fullmatch(
                r"0x(?:[0-9a-fA-F]{2}){0,131072}", result
            ):
                raise HistoryImportError("invalid_evidence")
            return "0x" + keccak(bytes.fromhex(result[2:])).hex()
        if not isinstance(result, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", result):
            raise HistoryImportError("invalid_evidence")
        if read.method == "implementation":
            if result[2:26] != "0" * 24:
                raise HistoryImportError("invalid_evidence")
            return "0x" + result[26:].lower()
        if read.method == "getCollectionId":
            return result.lower()
        number = int(result, 16)
        if read.method == "isApprovedForAll":
            if number not in (0, 1):
                raise HistoryImportError("invalid_evidence")
            return bool(number)
        return str(number)
