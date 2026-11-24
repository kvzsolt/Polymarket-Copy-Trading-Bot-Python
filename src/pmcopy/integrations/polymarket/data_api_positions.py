"""Read dust-inclusive, bounded Polymarket account inventory pages."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

from pmcopy.clients.network.http_client import AsyncHttpClient
from pmcopy.clients.network.decimal_json_reader import DecimalJsonReader
from pmcopy.domain.position_lifecycle import PositionLifecyclePolicy
from pmcopy.utils.logger import get_logger

logger = get_logger("DataApiPositions")


class DataApiPositionSizeReader:
    """Validate complete public holdings before reporting shares or absence.

    The injected client provider retains the application's shared HTTP lifecycle.
    Reads are bounded to ten seconds and the documented maximum offset. Public
    offset pagination is not an atomic account or chain snapshot.
    """

    _PAGE_SIZE = 500
    _MAX_OFFSET = 10000
    _MAX_PAGES = (_MAX_OFFSET // _PAGE_SIZE) + 1
    _READ_TIMEOUT_SECONDS = 10.0
    _URL = "https://data-api.polymarket.com/v2/positions"
    _V2_TO_V1_ALIASES: tuple[tuple[str, str], ...] = (
        ("condition_id", "conditionId"),
        ("proxy_wallet", "proxyWallet"),
        ("avg_price", "avgPrice"),
        ("current_price", "curPrice"),
        ("current_value", "currentValue"),
        ("total_size", "totalBought"),
        ("realized_pnl", "realizedPnl"),
        ("unrealized_pnl", "cashPnl"),
        ("percent_pnl", "percentPnl"),
        ("percent_realized_pnl", "percentRealizedPnl"),
        ("negative_risk", "negativeRisk"),
        ("outcome_index", "outcomeIndex"),
        ("opposite_token_id", "oppositeAsset"),
        ("opposite_outcome", "oppositeOutcome"),
        ("event_id", "eventId"),
        ("event_slug", "eventSlug"),
        ("end_date", "endDate"),
    )

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
        seen_cursors: set[str] = set()
        rows: list[dict[str, object]] = []
        cursor: str | None = None
        for _page_index in range(self._MAX_PAGES):
            params: dict[str, object] = {
                "user": wallet,
                "sizeThreshold": 0,
                "limit": self._PAGE_SIZE,
            }
            if cursor is not None:
                params["cursor"] = cursor
            payload = await client.get_json(
                self._URL,
                params=params,
                silent=True,
                json_decoder=DecimalJsonReader.decode,
            )
            page, next_cursor, has_more = self._extract_page(payload)
            if len(page) > self._PAGE_SIZE:
                raise ValueError("Invalid holdings page")
            for row in page:
                asset, size = self._parse_position(row, wallet)
                if asset in seen:
                    raise ValueError("Repeated holdings identity")
                seen.add(asset)
                parsed = self._normalize_row(row, asset=asset, size=size)
                classification = PositionLifecyclePolicy().classify(
                    redeemable=parsed.get("redeemable"),
                    settlement_price=parsed.get("curPrice"),
                )
                parsed.update(
                    lifecycle=classification.lifecycle.value,
                    settlement_price=classification.settlement_price,
                )
                rows.append(parsed)
            if not has_more:
                return rows
            if next_cursor is not None:
                if next_cursor in seen_cursors:
                    raise ValueError("Repeated holdings pagination cursor")
                seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise ValueError("Holdings pagination is incomplete")

    @classmethod
    def _extract_page(
        cls,
        payload: object,
    ) -> tuple[list[dict[str, object]], str | None, bool]:
        if isinstance(payload, list):
            return payload, None, len(payload) == cls._PAGE_SIZE
        if not isinstance(payload, dict) or "data" not in payload:
            raise ValueError("Invalid holdings page")
        raw_data = payload["data"]
        if raw_data is None:
            page: list[dict[str, object]] = []
        elif isinstance(raw_data, list):
            page = raw_data
        else:
            raise ValueError("Invalid holdings page")
        pagination = payload.get("pagination")
        if pagination is None:
            return page, None, len(page) == cls._PAGE_SIZE
        if not isinstance(pagination, dict):
            raise ValueError("Invalid holdings pagination")
        raw_has_more = pagination.get("has_more")
        if raw_has_more is not None and type(raw_has_more) is not bool:
            raise ValueError("Invalid holdings pagination has_more flag")
        raw_cursor = pagination.get("next_cursor")
        if raw_cursor is not None and not isinstance(raw_cursor, str):
            raise ValueError("Invalid holdings pagination cursor")
        next_cursor = raw_cursor.strip() if isinstance(raw_cursor, str) and raw_cursor.strip() else None
        has_more = bool(raw_has_more) if raw_has_more is not None else (next_cursor is not None)
        if has_more and next_cursor is None:
            raise ValueError("Holdings pagination cursor missing")
        return page, next_cursor, has_more

    @classmethod
    def _normalize_row(
        cls,
        row: dict[str, object],
        *,
        asset: str,
        size: Decimal,
    ) -> dict[str, object]:
        parsed: dict[str, object] = dict(row)
        for v2_key, v1_key in cls._V2_TO_V1_ALIASES:
            if v1_key not in parsed and v2_key in parsed:
                parsed[v1_key] = parsed[v2_key]
            elif v2_key not in parsed and v1_key in parsed:
                parsed[v2_key] = parsed[v1_key]
        parsed.update(
            asset=asset,
            token_id=asset,
            tokenId=asset,
            tokenID=asset,
            size=size,
            current_size=size,
        )
        return parsed

    @staticmethod
    def _parse_position(row: object, wallet: str) -> tuple[str, Decimal]:
        if not isinstance(row, dict):
            raise ValueError("Invalid holdings row")
        identities = [
            row[key]
            for key in ("asset", "tokenID", "token_id", "tokenId")
            if key in row
        ]
        if not identities or any(value != identities[0] for value in identities):
            raise ValueError("Missing or conflicting outcome identity")
        asset = identities[0]
        if not isinstance(asset, str) or re.fullmatch(r"[0-9]+", asset) is None or int(asset) <= 0:
            raise ValueError("Invalid outcome identity")
        for account_key in ("proxyWallet", "proxy_wallet"):
            account = row.get(account_key)
            if account is not None and (not isinstance(account, str) or account.lower() != wallet):
                raise ValueError("Holdings account mismatch")
        size_values = [row[key] for key in ("size", "current_size") if key in row]
        if not size_values:
            raise ValueError("Invalid holdings size")
        parsed_sizes: list[Decimal] = []
        for raw_size in size_values:
            if isinstance(raw_size, bool) or not isinstance(raw_size, (str, int, Decimal)):
                raise ValueError("Invalid holdings size")
            size_val = Decimal(raw_size)
            if not size_val.is_finite() or size_val < 0:
                raise ValueError("Invalid holdings size")
            parsed_sizes.append(size_val)
        if any(size_val != parsed_sizes[0] for size_val in parsed_sizes):
            raise ValueError("Conflicting holdings size")
        return asset, parsed_sizes[0]
