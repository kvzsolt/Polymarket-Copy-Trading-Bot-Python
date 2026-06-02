from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from eth_account import Account
from web3 import Web3

from utils.fetchData import fetch_data
from utils.logger import get_logger, log_redemption, redact_error_detail

if TYPE_CHECKING:
    from config.app_config import AppConfig
    from cache.market_state_store import MarketStateStore

logger = get_logger("RedemptionService")

_DATA_API_POSITIONS_URL = "https://data-api.polymarket.com/positions?user={wallet}"
_HASH_ZERO = b"\x00" * 32
_BINARY_INDEX_SETS = [1, 2]

_CTF_REDEEM_ABI: list[dict[str, Any]] = [
    {
        "constant": False,
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


@dataclass(frozen=True)
class RedemptionPosition:
    token_id: str
    condition_id: str
    size: Decimal
    current_value: Decimal
    redeemable: bool
    title: str = ""
    outcome: str = ""


@dataclass(frozen=True)
class RedemptionTxResult:
    success: bool
    condition_id: str
    tx_hash: str | None = None
    error: str | None = None


PositionFetcher = Callable[[str], Awaitable[list[dict[str, Any]]]]
ConditionRedeemer = Callable[[str], Awaitable[RedemptionTxResult]]


class RedemptionService:
    """Async auto-redemption loop for resolved Polymarket CTF positions.

    Eligibility is intentionally two-phase:
    - Data API must report ``redeemable=true`` for a non-zero position.
    - The local market cache must also show the token at the configured terminal
      price (default 0.01 or 0.99). API ``curPrice`` is not trusted for this gate.

    The Web3 transaction path is synchronous internally, so it is run via
    ``asyncio.to_thread`` to keep WSS, risk, and trade loops responsive.
    """

    def __init__(
        self,
        *,
        config: "AppConfig",
        wallet_address: str,
        market_store: "MarketStateStore",
        private_key: str | None = None,
        position_fetcher: PositionFetcher | None = None,
        condition_redeemer: ConditionRedeemer | None = None,
    ) -> None:
        self._config = config
        self._wallet = str(wallet_address or "").strip()
        self._market_store = market_store
        self._private_key = private_key or config.wallet.private_key
        self._position_fetcher = position_fetcher or self._fetch_positions
        self._condition_redeemer = condition_redeemer or self._redeem_condition
        self._inflight_conditions: set[str] = set()
        self._redeemed_conditions: set[str] = set()

    async def loop(self) -> None:
        cfg = self._config.redemption
        if not cfg.enable_auto_redeem:
            log_redemption(logger, status="disabled", funder=self._wallet)
            return
        if not self._validate_runtime_support():
            return

        log_redemption(
            logger,
            status="online",
            funder=self._wallet,
            reason=(
                f"interval={cfg.check_interval}s "
                f"terminal=[{cfg.resolved_low}, {cfg.resolved_high}]"
            ),
        )

        while True:
            try:
                await self.scan_once()
            except asyncio.CancelledError:
                logger.info("redemption_service_cancelled | user=%s", self._wallet)
                raise
            except Exception as exc:
                logger.warning(
                    "redemption_service_tick_error | user=%s error=%s",
                    self._wallet,
                    redact_error_detail(exc),
                )
            await asyncio.sleep(cfg.check_interval)

    async def scan_once(self) -> list[RedemptionTxResult]:
        cfg = self._config.redemption
        raw_positions = await self._position_fetcher(self._wallet)
        positions = [p for p in (_parse_position(row) for row in raw_positions) if p is not None]
        groups: dict[str, list[RedemptionPosition]] = {}

        for position in positions:
            if not position.redeemable:
                continue
            if position.size <= cfg.zero_threshold:
                continue
            cached_price = self._cached_resolved_price(position.token_id)
            if cached_price is None:
                logger.debug(
                    "redemption_skip_cache_price_not_terminal | cond=%s token=%s",
                    position.condition_id[:12],
                    position.token_id[:12],
                )
                continue
            groups.setdefault(position.condition_id, []).append(position)

        results: list[RedemptionTxResult] = []
        for condition_id, group in list(groups.items())[: cfg.max_conditions_per_tick]:
            if condition_id in self._redeemed_conditions or condition_id in self._inflight_conditions:
                continue
            self._inflight_conditions.add(condition_id)
            try:
                expected_value = sum((p.current_value for p in group), start=Decimal("0"))
                log_redemption(
                    logger,
                    status="submit",
                    funder=self._wallet,
                    condition_id=condition_id,
                    positions=len(group),
                    expected_value=str(expected_value),
                )
                result = await self._condition_redeemer(condition_id)
                results.append(result)
                if result.success:
                    self._redeemed_conditions.add(condition_id)
                    log_redemption(
                        logger,
                        status="success",
                        funder=self._wallet,
                        condition_id=condition_id,
                        positions=len(group),
                        expected_value=str(expected_value),
                        tx_hash=result.tx_hash,
                    )
                else:
                    log_redemption(
                        logger,
                        status="failed",
                        funder=self._wallet,
                        condition_id=condition_id,
                        tx_hash=result.tx_hash,
                        error=result.error,
                    )
            finally:
                self._inflight_conditions.discard(condition_id)

        return results

    async def _fetch_positions(self, wallet: str) -> list[dict[str, Any]]:
        data = await fetch_data(_DATA_API_POSITIONS_URL.format(wallet=wallet), silent=True)
        return data if isinstance(data, list) else []

    def _cached_resolved_price(self, token_id: str) -> Decimal | None:
        cfg = self._config.redemption
        state = self._market_store.get_state(token_id)
        if state is None:
            return None

        candidates: list[Decimal] = []
        for value in (
            getattr(state, "last_trade_price", None),
            getattr(state, "best_bid", None),
            getattr(state, "best_ask", None),
        ):
            parsed = _to_decimal(value)
            if parsed is not None:
                candidates.append(parsed)

        bid = _to_decimal(getattr(state, "best_bid", None))
        ask = _to_decimal(getattr(state, "best_ask", None))
        if bid is not None and ask is not None:
            candidates.append((bid + ask) / Decimal("2"))

        for price in candidates:
            if (
                abs(price - cfg.resolved_high) <= cfg.price_epsilon
                or abs(price - cfg.resolved_low) <= cfg.price_epsilon
            ):
                return price
        return None

    def _validate_runtime_support(self) -> bool:
        cfg = self._config.redemption
        try:
            signer = Account.from_key(self._private_key).address
        except Exception:
            log_redemption(
                logger,
                status="blocked",
                funder=self._wallet,
                reason="invalid_private_key",
            )
            return False
        if cfg.require_signer_match and signer.lower() != self._wallet.lower():
            log_redemption(
                logger,
                status="blocked",
                funder=self._wallet,
                reason="signer_mismatch_direct_ctf_redeem_requires_token_holder_signer",
                meta={"signer": signer},
            )
            return False
        return True

    async def _redeem_condition(self, condition_id: str) -> RedemptionTxResult:
        return await asyncio.to_thread(self._redeem_condition_sync, condition_id)

    def _redeem_condition_sync(self, condition_id: str) -> RedemptionTxResult:
        cfg = self._config.redemption
        try:
            # ``network.rpc_url`` defaults to "" when RPC_URL is unset; the
            # wallet config carries the public Polygon default. Fall back so a
            # redeem never fires against an empty provider URL.
            rpc_url = self._config.network.rpc_url or self._config.wallet.polygon_rpc_url
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            account = Account.from_key(self._private_key)
            ctf = w3.eth.contract(
                address=Web3.to_checksum_address(cfg.ctf_contract_address),
                abi=_CTF_REDEEM_ABI,
            )
            gas_price = w3.eth.gas_price
            if cfg.gas_price_buffer_pct:
                gas_price = gas_price * (100 + cfg.gas_price_buffer_pct) // 100
            tx = ctf.functions.redeemPositions(
                Web3.to_checksum_address(cfg.collateral_contract_address),
                _HASH_ZERO,
                _condition_id_bytes32(condition_id),
                _BINARY_INDEX_SETS,
            ).build_transaction(
                {
                    "from": account.address,
                    "chainId": self._config.network.chain_id,
                    "nonce": w3.eth.get_transaction_count(account.address),
                    "gas": cfg.gas_limit,
                    "gasPrice": gas_price,
                }
            )
            signed = account.sign_transaction(tx)
            raw_tx = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
            tx_hash_bytes = w3.eth.send_raw_transaction(raw_tx)
            tx_hash = w3.to_hex(tx_hash_bytes)
            receipt = w3.eth.wait_for_transaction_receipt(
                tx_hash_bytes,
                timeout=cfg.receipt_timeout_seconds,
            )
            status = int(getattr(receipt, "status", receipt.get("status", 0)))
            if status == 1:
                return RedemptionTxResult(success=True, condition_id=condition_id, tx_hash=tx_hash)
            return RedemptionTxResult(
                success=False,
                condition_id=condition_id,
                tx_hash=tx_hash,
                error="transaction_reverted",
            )
        except Exception as exc:
            return RedemptionTxResult(
                success=False,
                condition_id=condition_id,
                error=redact_error_detail(exc),
            )


def _parse_position(raw: dict[str, Any]) -> RedemptionPosition | None:
    if not isinstance(raw, dict):
        return None
    token_id = str(raw.get("asset") or raw.get("tokenID") or raw.get("token_id") or "").strip()
    condition_id = str(
        raw.get("conditionId") or raw.get("condition_id") or raw.get("condition") or ""
    ).strip()
    if not token_id or not condition_id:
        return None
    # Money fields must not silently coerce a *malformed* value to zero: a
    # non-null but unparseable ``size`` would otherwise collapse through the
    # ``size <= zero_threshold`` gate and the (potentially valuable) resolved
    # position would never be redeemed. An *absent* size key keeps the prior
    # behavior (0, later gated out); a present-but-unparseable value is skipped
    # with a visible warning rather than silently zeroed.
    raw_size = raw.get("size")
    if raw_size is None:
        size = Decimal("0")
    else:
        size = _to_decimal(raw_size)
        if size is None:
            logger.warning(
                "redemption_parse_skip | unparseable size — skipping position "
                "condition=%s token=%s raw_size=%r",
                condition_id, token_id[:12], raw_size,
            )
            return None
    current_value = _to_decimal(raw.get("currentValue")) or Decimal("0")
    return RedemptionPosition(
        token_id=token_id,
        condition_id=condition_id,
        size=size,
        current_value=current_value,
        redeemable=bool(raw.get("redeemable") is True),
        title=str(raw.get("title") or raw.get("slug") or ""),
        outcome=str(raw.get("outcome") or ""),
    )


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _condition_id_bytes32(condition_id: str) -> bytes:
    value = str(condition_id or "").strip()
    if not value:
        raise ValueError("condition_id is required")
    if value.startswith(("0x", "0X")):
        raw = bytes.fromhex(value[2:])
    else:
        raw = int(value, 10).to_bytes(32, "big")
    if len(raw) > 32:
        raise ValueError(f"condition_id exceeds bytes32: {condition_id}")
    return raw.rjust(32, b"\x00")
