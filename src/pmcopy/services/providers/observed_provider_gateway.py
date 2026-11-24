"""Admitted RPC reads with original body receipts for read-only evaluation."""

from typing import Protocol

from pmcopy.domain.observed_rpc import ObservedRpcResult
from pmcopy.interfaces.provider_budget import ProviderAdmission
from pmcopy.services.providers.provider_gateway import ProviderGateway, ProviderUnavailable


class ObservedReadTransport(Protocol):
    """Keep observed and ordinary reads on one credential-owning transport."""

    async def request(self, provider: str, method: str, params: list[object]) -> object:
        """Return one original JSON-RPC result."""
        ...

    async def request_observed(
        self, provider: str, method: str, params: list[object]
    ) -> ObservedRpcResult:
        """Return the result with its native pre-decode body receipt."""
        ...


class ObservedProviderGateway(ProviderGateway):
    """Extend admitted reads without changing existing consumers or retry rules."""

    def __init__(self, admission: ProviderAdmission, transport: ObservedReadTransport) -> None:
        """Inject shared accounting and a transport with the observed read port."""
        super().__init__(admission, transport)
        self._observed_transport = transport

    async def call_observed(
        self, provider: str, method: str, params: list[object], *, category: str = "essential"
    ) -> ObservedRpcResult:
        """Admit once before I/O; preserve admission failures and redact network errors."""
        self._admission.admit_rpc(provider, method, category=category)
        try:
            return await self._observed_transport.request_observed(provider, method, params)
        except Exception:
            raise ProviderUnavailable("provider observed read unavailable") from None
