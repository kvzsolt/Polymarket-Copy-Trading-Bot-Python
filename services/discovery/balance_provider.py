from __future__ import annotations

from decimal import Decimal

from clients.balance_client import BalanceClient


class WalletBalanceProvider:
    def __init__(
        self,
        *,
        balance_client: BalanceClient,
        wallet_address: str,
        fallback_balance_usdc: Decimal | None = None,
    ):
        self.balance_client = balance_client
        self.wallet_address = wallet_address
        self.fallback_balance_usdc = fallback_balance_usdc
        self._cached_balance_usdc: Decimal | None = None

    async def get_available_balance_usdc(self) -> Decimal:
        if self._cached_balance_usdc is not None:
            return self._cached_balance_usdc
        balance = await self.balance_client.get_usdc_balance(self.wallet_address)
        if balance is not None:
            self._cached_balance_usdc = Decimal(str(balance))
            return self._cached_balance_usdc
        if self.fallback_balance_usdc is not None:
            self._cached_balance_usdc = Decimal(str(self.fallback_balance_usdc))
            return self._cached_balance_usdc
        raise RuntimeError(
            f"Unable to fetch USDC balance for wallet {self.wallet_address}"
        )


def build_wallet_balance_provider(config) -> WalletBalanceProvider:
    return WalletBalanceProvider(
        balance_client=BalanceClient(
            rpc_url=config.network.rpc_url or config.wallet.polygon_rpc_url,
            usdc_contract_address=config.network.usdc_contract_address,
            fallback_rpc_url=getattr(config.network, "fallback_rpc_url", ""),
        ),
        wallet_address=config.wallet.funder_address,
    )
