"""Gasless settlement via the official Relayer, never a second order transport."""

from collections.abc import Mapping
import json
import re

import aiohttp

from clients.network.http_client import AsyncHttpClient
from config.redemption import RedemptionConfig
from domain.redemption import (
    RedemptionAttempt,
    RedemptionAuthorization,
    RedemptionConfirmation,
    RedemptionMarket,
    RedemptionSnapshot,
    RedemptionState,
    SettlementAmbiguous,
    SettlementAuthError,
    SettlementMarketError,
    SettlementUnavailable,
)
from integrations.polymarket.data_api_positions import DataApiPositionSizeReader
from integrations.polymarket.settlement_chain import PolygonSettlementReader
from integrations.polymarket.settlement_wallet import DepositWalletCodec, SignedSettlementBatch
from interfaces.settlement import SettlementClock


class PolymarketSettlementGateway:
    """Translate public holdings, wallet batches, and receipts into settlement models."""

    _RELAYER = "https://relayer-v2.polymarket.com"

    def __init__(
        self,
        reader: AsyncHttpClient,
        writer: AsyncHttpClient,
        chain: PolygonSettlementReader,
        clock: SettlementClock,
        *,
        credentials: RedemptionConfig | None = None,
        signer: DepositWalletCodec | None = None,
    ) -> None:
        """Separate retriable public reads from single-attempt private submissions."""
        if writer.retry_policy.attempts != 1:
            raise ValueError("Relayer writes require a single-attempt HTTP transport")
        self._reader = reader
        self._writer = writer
        self._chain = chain
        self._clock = clock
        self._credentials = credentials
        self._signer = signer
        self._positions = DataApiPositionSizeReader(lambda: self._reader)
        self._prepared: SignedSettlementBatch | None = None

    async def inventory(self, wallet: str) -> tuple[RedemptionMarket, ...]:
        """Read complete dust-inclusive holdings and group complementary outcomes."""
        await self._chain.require_wallet(wallet)
        rows = await self._positions.positions(wallet)
        if rows is None:
            raise SettlementUnavailable("redemption_inventory_unavailable")
        markets: dict[str, RedemptionMarket] = {}
        try:
            for row in rows:
                if row["size"] == 0:
                    continue
                condition = row.get("conditionId")
                asset, opposite = row.get("asset"), row.get("oppositeAsset")
                index = row.get("outcomeIndex")
                if (
                    not isinstance(condition, str)
                    or not isinstance(asset, str)
                    or not isinstance(opposite, str)
                ):
                    raise ValueError
                if type(index) is not int or index not in (0, 1):
                    raise ValueError
                tokens = (asset, opposite) if index == 0 else (opposite, asset)
                title = row.get("title", "")
                if not isinstance(title, str):
                    raise ValueError
                title = "".join(char for char in title if char.isprintable())[:200]
                risk = row.get("negativeRisk")
                if type(risk) is not bool:
                    raise ValueError
                market = RedemptionMarket(condition.lower(), tokens, risk, title)
                old = markets.get(market.condition_id)
                if old is not None and (old.token_ids != tokens or old.negative_risk != risk):
                    raise ValueError
                markets[market.condition_id] = old or market
        except (ValueError, TypeError, KeyError):
            raise SettlementUnavailable("redemption_inventory_identity_invalid") from None
        return tuple(markets[key] for key in sorted(markets))

    async def inspect(self, wallet: str, market: RedemptionMarket) -> RedemptionSnapshot:
        """Cross-check closed or open Gamma metadata before reading chain evidence.

        Args:
            wallet: Selected follower's Deposit Wallet.
            market: Binary condition identity from the position inventory.

        Returns:
            A fresh on-chain snapshot after matching both outcome identities.

        Raises:
            SettlementMarketError: Metadata is unavailable, missing, invalid, or
                conflicting. Only an empty successful lookup permits fallback.
        """
        for closed in ("true", "false"):
            try:
                rows = await self._reader.get_json(
                    "https://gamma-api.polymarket.com/markets",
                    params={"condition_ids": market.condition_id, "closed": closed, "limit": 2},
                    silent=True,
                )
            except ValueError:
                raise SettlementMarketError("redemption_market_metadata_invalid") from None
            except Exception:
                raise SettlementMarketError("redemption_market_metadata_unavailable") from None
            if not isinstance(rows, list):
                raise SettlementMarketError("redemption_market_metadata_invalid")
            if not rows:
                continue
            try:
                if len(rows) != 1 or not isinstance(rows[0], dict):
                    raise ValueError
                metadata = rows[0]
                condition = metadata.get("conditionId")
                tokens = metadata.get("clobTokenIds")
                risk = metadata.get("negRisk")
                if isinstance(tokens, str):
                    tokens = json.loads(tokens)
                if (
                    not isinstance(condition, str)
                    or not isinstance(tokens, list)
                    or len(tokens) != 2
                    or not all(isinstance(token, str) for token in tokens)
                    or type(risk) is not bool
                ):
                    raise ValueError
                identity = RedemptionMarket(condition, (tokens[0], tokens[1]), risk)
            except (ValueError, TypeError):
                raise SettlementMarketError("redemption_market_metadata_invalid") from None
            if (
                identity.condition_id.lower() != market.condition_id.lower()
                or identity.token_ids != market.token_ids
                or identity.negative_risk != market.negative_risk
            ):
                raise SettlementMarketError("redemption_market_metadata_conflicting")
            return await self._chain.snapshot(wallet, market)
        raise SettlementMarketError("redemption_market_metadata_not_found")

    async def prepare(self, wallet: str, snapshot: RedemptionSnapshot) -> RedemptionAuthorization:
        """Check signer ownership, fetch a fresh nonce, and sign without submitting."""
        signer = self._require_signer()
        try:
            signer.require_owner(wallet)
        except ValueError:
            raise SettlementAuthError("deposit_wallet_owner_mismatch") from None
        await self._chain.require_wallet(wallet)
        params = self._mapping(
            await self._get_relayer(
                "/v1/account/transactions/params",
                params={"address": signer.signer_address, "type": "WALLET"},
            )
        )
        address = params.get("address")
        if not isinstance(address, str) or re.fullmatch(r"0x[0-9a-fA-F]{40}", address) is None:
            raise SettlementUnavailable("invalid_relayer_address")
        nonce = params.get("nonce")
        if (
            not isinstance(nonce, str)
            or re.fullmatch(r"[0-9]+", nonce) is None
            or int(nonce) >= 2**256
        ):
            raise SettlementUnavailable("invalid_relayer_nonce")
        deadline = max(self._clock.now(), await self._chain.latest_timestamp()) + 600
        self._prepared = signer.sign(
            wallet, signer.calls(snapshot), nonce=int(nonce), deadline=deadline
        )
        return self._prepared.authorization

    async def submit(self, authorization: RedemptionAuthorization) -> str:
        """Consume one prepared envelope and POST once; never retry ambiguity."""
        batch = self._prepared
        if batch is None or batch.authorization != authorization:
            raise SettlementAuthError("redemption_authorization_missing_or_consumed")
        self._prepared = None
        try:
            response = await self._writer.post_json(
                self._RELAYER + "/submit",
                json_body=batch.payload,
                headers=self._headers(),
                silent=True,
            )
        except aiohttp.ClientResponseError as error:
            if error.status in (401, 403):
                raise SettlementAuthError("relayer_authentication_failed") from None
            if 400 <= error.status < 500 and error.status not in (408, 425, 429):
                raise SettlementMarketError("relayer_submission_rejected") from None
            raise SettlementAmbiguous("relayer_submission_unconfirmed") from None
        except Exception:
            raise SettlementAmbiguous("relayer_submission_unconfirmed") from None
        if not isinstance(response, dict) or not self._valid_reference(
            response.get("transactionID")
        ):
            raise SettlementAmbiguous("relayer_submission_reference_missing")
        return str(response["transactionID"])

    async def confirm(self, wallet: str, attempt: RedemptionAttempt) -> RedemptionConfirmation:
        """Require Relayer confirmation plus canonical finalized receipt evidence."""
        if not self._valid_reference(attempt.transaction_id):
            raise SettlementUnavailable("invalid_relayer_transaction_reference")
        payload = self._mapping(
            await self._get_relayer("/v1/account/transactions/" + attempt.transaction_id)
        )
        if payload.get("transaction_id") != attempt.transaction_id:
            raise SettlementUnavailable("relayer_transaction_reference_mismatch")
        state = payload.get("state")
        if state in ("STATE_FAILED", "STATE_INVALID"):
            return RedemptionConfirmation(RedemptionState.FAILED)
        if state != "STATE_CONFIRMED":
            if state not in ("STATE_NEW", "STATE_EXECUTED", "STATE_MINED"):
                raise SettlementUnavailable("unknown_relayer_transaction_state")
            return RedemptionConfirmation(RedemptionState.PENDING)
        transaction_hash = payload.get("transaction_hash")
        if not isinstance(transaction_hash, str):
            raise SettlementUnavailable("relayer_transaction_hash_missing")
        try:
            payout = await self._chain.receipt_payout(wallet, attempt.snapshot, transaction_hash)
        except SettlementMarketError:
            return RedemptionConfirmation(RedemptionState.FAILED, transaction_hash)
        if payout is None:
            return RedemptionConfirmation(RedemptionState.PENDING, transaction_hash)
        return RedemptionConfirmation(RedemptionState.CONFIRMED, transaction_hash, payout)

    async def finalized_timestamp(self) -> int:
        """Expose authoritative expiry evidence to the recovery service."""
        return await self._chain.finalized_timestamp()

    def _require_signer(self) -> DepositWalletCodec:
        if self._signer is None or self._credentials is None:
            raise SettlementAuthError("redemption_execution_credentials_required")
        try:
            self._credentials.require_credentials()
        except ValueError:
            raise SettlementAuthError("redemption_execution_credentials_invalid") from None
        if self._credentials.relayer_api_key_address.lower() != self._signer.signer_address.lower():
            raise SettlementAuthError("relayer_api_key_address_does_not_match_signer")
        return self._signer

    def _headers(self) -> dict[str, str]:
        signer = self._require_signer()
        if self._credentials is None:
            raise SettlementAuthError("redemption_execution_credentials_required")
        return {
            "RELAYER_API_KEY": self._credentials.relayer_api_key.get_secret_value(),
            "RELAYER_API_KEY_ADDRESS": signer.signer_address,
        }

    async def _get_relayer(self, path: str, *, params: dict[str, str] | None = None) -> object:
        try:
            return await self._reader.get_json(
                self._RELAYER + path, params=params, headers=self._headers(), silent=True
            )
        except SettlementAuthError:
            raise
        except aiohttp.ClientResponseError as error:
            if error.status in (401, 403):
                raise SettlementAuthError("relayer_authentication_failed") from None
            raise SettlementUnavailable("relayer_read_unavailable") from None
        except Exception:
            raise SettlementUnavailable("relayer_read_unavailable") from None

    @staticmethod
    def _mapping(value: object) -> Mapping[str, object]:
        if not isinstance(value, dict):
            raise SettlementUnavailable("invalid_relayer_response")
        return value

    @staticmethod
    def _valid_reference(value: object) -> bool:
        return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value) is not None
