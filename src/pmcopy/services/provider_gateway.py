"""Single-attempt read gateway that charges all callers before transport dispatch."""

from __future__ import annotations

from typing import Protocol

from pmcopy.interfaces.provider_budget import ProviderAdmission


class ProviderUnavailable(RuntimeError):
    """Read evidence is unavailable; the message contains no endpoint or raw response."""


class ProviderReadTransport(Protocol):
    """Transport restricted to configured provider aliases and read-only methods."""

    async def request(self, provider: str, method: str, params: list[object]) -> object:
        """Dispatch one admitted read without transport-layer retry or failover."""
        ...


class ProviderGateway:
    """Keep budget admission inseparable from each RPC attempt.

    Callers own bounded retry/recovery policy. Each explicit retry passes through
    admission again, and cancellation never changes the account's spent estimate.
    """

    def __init__(self, admission: ProviderAdmission, transport: ProviderReadTransport) -> None:
        """Inject the shared ledger and a credential-owning transport adapter."""
        self._admission = admission
        self._transport = transport

    async def call(
        self,
        provider: str,
        method: str,
        params: list[object],
        *,
        category: str = "essential",
    ) -> object:
        """Admit one read, preserve quota failures, and redact transport failures."""
        self._admission.admit_rpc(provider, method, category=category)
        try:
            return await self._transport.request(provider, method, params)
        except Exception:
            raise ProviderUnavailable("provider read unavailable") from None
