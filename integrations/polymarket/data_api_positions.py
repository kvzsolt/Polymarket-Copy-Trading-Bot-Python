"""Read dust-inclusive, bounded Polymarket account inventory pages."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

from clients.network.http_client import AsyncHttpClient
from clients.network.decimal_json_reader import DecimalJsonReader
from utils.logger import get_logger

logger = get_logger("DataApiPositions")


class DataApiPositionSizeReader:
    """Validate complete public holdings before reporting shares or absence.

    The injected client provider retains the application's shared HTTP lifecycle.
    Reads are bounded to ten seconds and the documented maximum offset. Public
    offset pagination is not an atomic account or chain snapshot.
    """

    _PAGE_SIZE = 500
    _MAX_OFFSET = 10000
    _READ_TIMEOUT_SECONDS = 10.0
    _URL = "https://data-api.polymarket.com/positions"

    def __init__(self, client_provider: Callable[[], AsyncHttpClient]) -> None:
        self._client_provider = client_provider

    async def shares(self, wallet: str, token_id: str) -> Decimal | None:
        """Return exact outcome shares, or None for incomplete/invalid evidence.

        Args:
            wallet: Profile/funder address, normalized at this boundary.
            token_id: Raw decimal outcome token ID.

        Returns:
            Non-negative shares, including zero only after complete pagination.
            Invalid identity, payload, timeout, or transport failure returns None.
        """
        if re.fullmatch(r"[0-9]+", token_id) is None or int(token_id) <= 0:
            return None
        rows = await self.positions(wallet)
        if rows is None:
            return None
        for row in rows:
            if row["asset"] == token_id:
                _, size = self._parse_position(row, wallet.strip().lower())
                return size
        return Decimal("0")

    async def positions(self, wallet: str) -> list[dict[str, object]] | None:
        """Return validated complete rows for existing snapshot consumers.

        Monetary JSON values remain Decimal. Zero-value and dust holdings are
        retained; market value alone cannot establish disposal or redemption.
        None means incomplete/unavailable evidence, never confirmed absence.
        """
        normalized_wallet = wallet.strip().lower()
        if re.fullmatch(r"0x[0-9a-f]{40}", normalized_wallet) is None:
            return None
        try:
            async with asyncio.timeout(self._READ_TIMEOUT_SECONDS):
                return await self._read_positions(normalized_wallet)
        except (TimeoutError, ValueError, InvalidOperation):
            logger.warning("account_holdings_unavailable reason=invalid_or_incomplete_evidence")
            return None
        except Exception as exc:
            logger.warning("account_holdings_unavailable error_type=%s", type(exc).__name__)
            return None

    async def _read_positions(self, wallet: str) -> list[dict[str, object]]:
        client = self._client_provider()
        seen: set[str] = set()
        rows: list[dict[str, object]] = []
        for offset in range(0, self._MAX_OFFSET + 1, self._PAGE_SIZE):
            page = await client.get_json(
                self._URL,
                params={
                    "user": wallet,
                    "sizeThreshold": 0,
                    "limit": self._PAGE_SIZE,
                    "offset": offset,
                },
                silent=True,
                json_decoder=DecimalJsonReader.decode,
            )
            if not isinstance(page, list) or len(page) > self._PAGE_SIZE:
                raise ValueError("Invalid holdings page")
            for row in page:
                asset, size = self._parse_position(row, wallet)
                if asset in seen:
                    raise ValueError("Repeated holdings identity")
                seen.add(asset)
                parsed: dict[str, object] = dict(row)
                parsed.update(asset=asset, size=size)
                rows.append(parsed)
            if len(page) < self._PAGE_SIZE:
                return rows
        raise ValueError("Holdings pagination is incomplete")

    @staticmethod
    def _parse_position(row: object, wallet: str) -> tuple[str, Decimal]:
        if not isinstance(row, dict):
            raise ValueError("Invalid holdings row")
        identities = [row[key] for key in ("asset", "tokenID", "token_id") if key in row]
        if not identities or any(value != identities[0] for value in identities):
            raise ValueError("Missing or conflicting outcome identity")
        asset = identities[0]
        if not isinstance(asset, str) or re.fullmatch(r"[0-9]+", asset) is None or int(asset) <= 0:
            raise ValueError("Invalid outcome identity")
        account = row.get("proxyWallet")
        if account is not None and (not isinstance(account, str) or account.lower() != wallet):
            raise ValueError("Holdings account mismatch")
        raw_size = row.get("size")
        if isinstance(raw_size, bool) or not isinstance(raw_size, (str, int, Decimal)):
            raise ValueError("Invalid holdings size")
        size = Decimal(raw_size)
        if not size.is_finite() or size < 0:
            raise ValueError("Invalid holdings size")
        return asset, size
