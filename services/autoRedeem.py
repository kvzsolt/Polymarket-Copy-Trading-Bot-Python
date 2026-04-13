"""Auto-redeem — claims USDC from winning positions after market resolution."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

from web3 import Web3

from config.env import ENV, get_funder_address, get_private_key
from services.redemptionService import RedemptionService
from services.positionManager import get_position_manager, TrackedPosition
from services.pnlTracker import get_pnl_tracker
from utils.logger import get_logger

logger = get_logger("AutoRedeem")

# Minimal ABI for ConditionalTokens.redeemPositions
CTF_REDEEM_ABI = [
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
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]

# Polymarket CTF contract (same for standard and neg-risk on Polygon)
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
PARENT_COLLECTION_ID = b"\x00" * 32  # root collection


def _is_market_resolved(clob_client, condition_id: str) -> Optional[dict]:
    """Query the CLOB API for market resolution status.

    Returns market dict if resolved, None otherwise.
    """
    try:
        market = clob_client.get_market(condition_id)
        if not market:
            return None

        # Polymarket API returns various resolution indicators
        resolved = (
            market.get("resolved") is True
            or str(market.get("closed")).lower() == "true"
            or (
                str(market.get("end_date_iso", "")) < time.strftime("%Y-%m-%dT%H:%M:%SZ")
                and market.get("active") is False
            )
        )
        if resolved:
            return market
    except Exception as e:
        logger.debug(f"Market query failed for {condition_id[:12]}…: {e}")
    return None


def _get_winning_index_sets(market: dict) -> list[int]:
    """Determine which index sets to redeem based on market resolution.

    For binary markets, index_sets are [1] for outcome 0, [2] for outcome 1.
    We always pass both [1, 2] — CTF contract handles payouts correctly
    (winning outcome returns collateral, losing returns 0).
    """
    return [1, 2]


def _build_redeem_tx(
    w3: Web3,
    ctf_contract,
    condition_id: str,
    collateral_address: str,
    sender_address: str,
) -> Optional[dict]:
    """Build the redeemPositions transaction."""
    try:
        checksum_collateral = Web3.to_checksum_address(collateral_address)
        checksum_sender = Web3.to_checksum_address(sender_address)

        # Convert condition_id to bytes32
        if condition_id.startswith("0x"):
            cid_bytes = bytes.fromhex(condition_id[2:])
        else:
            cid_bytes = bytes.fromhex(condition_id)
        cid_bytes32 = cid_bytes.ljust(32, b"\x00")[:32]

        index_sets = _get_winning_index_sets({})

        nonce = w3.eth.get_transaction_count(checksum_sender)
        gas_price = w3.eth.gas_price

        tx = ctf_contract.functions.redeemPositions(
            checksum_collateral,
            PARENT_COLLECTION_ID,
            cid_bytes32,
            index_sets,
        ).build_transaction({
            "from": checksum_sender,
            "nonce": nonce,
            "gas": 300_000,
            "gasPrice": gas_price,
        })
        return tx
    except Exception as e:
        logger.error(f"Failed to build redeem tx for {condition_id[:12]}…: {e}")
        return None


def _send_redeem_tx(w3: Web3, tx: dict, private_key: str) -> Optional[str]:
    """Sign and send a redeem transaction. Returns tx hash or None."""
    try:
        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        hex_hash = tx_hash.hex()
        logger.info(f"Redeem tx sent: {hex_hash}")
        return hex_hash
    except Exception as e:
        logger.error(f"Failed to send redeem tx: {e}")
        return None


def _wait_for_receipt(w3: Web3, tx_hash: str, timeout: int = 120) -> bool:
    """Wait for transaction receipt. Returns True if successful."""
    try:
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        if receipt["status"] == 1:
            logger.info(f"Redeem confirmed: {tx_hash} (gas used: {receipt['gasUsed']})")
            return True
        else:
            logger.error(f"Redeem tx reverted: {tx_hash}")
            return False
    except Exception as e:
        logger.error(f"Redeem receipt wait failed: {e}")
        return False


async def _try_redeem_position(
    clob_client,
    w3: Web3,
    ctf_contract,
    pos: TrackedPosition,
    *,
    position_manager=None,
    pnl_tracker=None,
) -> bool:
    """Attempt to redeem a single resolved position. Returns True if successful."""
    pm = position_manager or get_position_manager()
    pnl = pnl_tracker or get_pnl_tracker()

    collateral_raw = ENV.get("USDC_CONTRACT_ADDRESS")
    collateral_address = collateral_raw if isinstance(collateral_raw, str) else ""
    proxy_raw = ENV.get("FUNDER_ADDRESS") or ENV.get("USER_ADDRESS", "")
    proxy_wallet = proxy_raw if isinstance(proxy_raw, str) else ""
    private_key = get_private_key()

    if not collateral_address:
        logger.error("Cannot redeem: USDC_CONTRACT_ADDRESS not configured")
        return False

    if not proxy_wallet:
        logger.error("Cannot redeem: FUNDER_ADDRESS/USER_ADDRESS not configured")
        return False

    if not private_key:
        logger.error("Cannot redeem: PRIVATE_KEY not configured")
        return False

    loop = asyncio.get_running_loop()

    # Build and send transaction in executor to avoid blocking
    tx = await loop.run_in_executor(
        None, _build_redeem_tx, w3, ctf_contract, pos.condition_id, collateral_address, proxy_wallet
    )
    if not tx:
        return False

    tx_hash = await loop.run_in_executor(None, _send_redeem_tx, w3, tx, private_key)
    if not tx_hash:
        return False

    success = await loop.run_in_executor(None, _wait_for_receipt, w3, tx_hash)
    if not success:
        return False

    # Record as a sell at price=1.0 (winning position pays full collateral)
    pnl_amount = (1.0 - pos.entry_price) * pos.size_tokens
    pm.track_sell(pos.condition_id, 1.0, pos.size_tokens, reason="auto_redeem")
    pnl.record_sell(
        condition_id=pos.condition_id,
        price=1.0,
        size_tokens=pos.size_tokens,
        usdc_value=pos.size_tokens,  # 1 token = $1 at resolution
        entry_price=pos.entry_price,
        realized_pnl=pnl_amount,
        exit_reason="auto_redeem",
        leader_wallet=pos.leader_wallet,
        event_slug=pos.event_slug,
        title=pos.title,
    )

    logger.info(
        f"✅ Redeemed {pos.condition_id[:12]}… "
        f"tokens={pos.size_tokens:.4f} pnl=${pnl_amount:.2f} tx={tx_hash}"
    )
    return True


async def auto_redeem_loop(clob_client, bot_params: dict):
    """Background task that periodically checks for resolved markets and redeems winnings.

    Runs as an asyncio task alongside the trade monitor and risk monitor.
    """
    enabled = ENV.get("ENABLE_AUTO_REDEEM", True)
    interval = ENV.get("REDEEM_CHECK_INTERVAL", 60)

    if not enabled:
        logger.info("Auto-redeem disabled — will not check for resolved markets")
        return

    rpc_url = ENV.get("RPC_URL")
    if not rpc_url:
        logger.error("Auto-redeem disabled: RPC_URL not configured")
        return

    logger.info(f"Auto-redeem started: check every {interval}s")
    redemption_service = RedemptionService(clob_client)
    user_id = get_funder_address()
    if not user_id:
        logger.error("Auto-redeem disabled: missing FUNDER_ADDRESS")
        return

    while True:
        try:
            redeemables = await redemption_service.scan_redeemables(user_id)
            for pos in redeemables:
                result = await redemption_service.redeem(pos)
                if result.submitted:
                    logger.info(
                        "Redeem submitted condition=%s size=%s status=%s tx=%s",
                        pos.condition_id[:12] + "…",
                        pos.size,
                        result.status,
                        result.tx_hash,
                    )
                elif result.error:
                    logger.warning(
                        "Redeem skipped/failed condition=%s reason=%s",
                        pos.condition_id[:12] + "…",
                        result.error,
                    )
        except Exception as e:
            logger.error(f"Auto-redeem loop error: {e}")

        await asyncio.sleep(interval)
