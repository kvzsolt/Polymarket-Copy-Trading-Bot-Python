"""Normalized tracked leader user addresses (EOA + Proxy/Gnosis Safe).

Maintains pre-computed representations for zero-cost runtime matching:
1. 40-char lowercase hex without ``0x`` for fast ``matchOrders`` calldata scans.
2. 66-char 32-byte left-zero-padded ``0x000000000000000000000000<40-hex>``
   topics for server-side ``OrderFilled`` ``topics[2]`` / ``topics[3]`` filters.
3. Reverse alias lookup mapping any matched EOA or Proxy/Safe address back to
   the trader's canonical ``leader_wallet``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

_HEX_CHARS = frozenset("0123456789abcdef")
_TOPIC_ZERO_PAD = "0" * 24


def normalize_eth_address(raw: object) -> str:
    """Validate and normalize an Ethereum address to ``0x<40-lowercase-hex>``."""
    text = str(raw or "").strip().lower()
    if text.startswith("0x"):
        hex40 = text[2:]
    else:
        raise ValueError(f"Ethereum address must start with 0x: {raw!r}")
    if len(hex40) != 40 or any(ch not in _HEX_CHARS for ch in hex40):
        raise ValueError(f"Ethereum address must be 20 hex bytes: {raw!r}")
    return "0x" + hex40


def try_normalize_eth_address(raw: object) -> str | None:
    """Return ``0x<40-lowercase-hex>`` or ``None`` when ``raw`` is malformed."""
    text = str(raw or "").strip().lower()
    if text.startswith("0x"):
        hex40 = text[2:]
    elif len(text) == 40:
        hex40 = text
    else:
        return None
    if len(hex40) != 40 or any(ch not in _HEX_CHARS for ch in hex40):
        return None
    return "0x" + hex40


def pad_address_topic(address: str) -> str:
    """Return 32-byte left-zero-padded topic ``0x000000000000000000000000<40-hex>``."""
    canonical = normalize_eth_address(address)
    return "0x" + _TOPIC_ZERO_PAD + canonical[2:]


@dataclass(frozen=True, slots=True)
class TrackedTraderAddresses:
    """All on-chain identities (canonical wallet, EOA, Proxy/Safe) for one leader."""

    leader_wallet: str
    eoa_address: str = ""
    proxy_address: str = ""
    extra_addresses: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        canonical_leader = normalize_eth_address(self.leader_wallet)
        object.__setattr__(self, "leader_wallet", canonical_leader)
        eoa = normalize_eth_address(self.eoa_address) if str(self.eoa_address or "").strip() else ""
        proxy = (
            normalize_eth_address(self.proxy_address)
            if str(self.proxy_address or "").strip()
            else ""
        )
        extras = tuple(
            dict.fromkeys(
                normalize_eth_address(addr)
                for addr in self.extra_addresses
                if str(addr or "").strip()
            )
        )
        object.__setattr__(self, "eoa_address", eoa)
        object.__setattr__(self, "proxy_address", proxy)
        object.__setattr__(self, "extra_addresses", extras)

    @property
    def all_addresses(self) -> tuple[str, ...]:
        """Return deduplicated ``0x<40-hex>`` addresses owned by this trader."""
        candidates = (
            self.leader_wallet,
            self.proxy_address,
            self.eoa_address,
            *self.extra_addresses,
        )
        return tuple(dict.fromkeys(addr for addr in candidates if addr))


@dataclass(frozen=True, slots=True)
class TrackedUserDirectory:
    """Immutable normalized directory of all tracked leader addresses."""

    entries: tuple[TrackedTraderAddresses, ...] = ()
    leader_wallets: frozenset[str] = frozenset()
    addresses_with_prefix: tuple[str, ...] = ()
    hex40_without_prefix: frozenset[str] = frozenset()
    padded_topics: tuple[str, ...] = ()
    _alias_to_leader: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({}), repr=False
    )

    @property
    def is_empty(self) -> bool:
        """Return whether zero tracked user addresses are currently registered."""
        return not self.addresses_with_prefix

    def canonical_leader_for(self, address: object) -> str | None:
        """Resolve a matched EOA or Proxy/Safe address to its canonical ``leader_wallet``."""
        normalized = try_normalize_eth_address(address)
        if normalized is None:
            return None
        return self._alias_to_leader.get(normalized)

    @classmethod
    def empty(cls) -> "TrackedUserDirectory":
        """Return an empty directory."""
        return cls()

    @classmethod
    def from_entries(cls, entries: Iterable[TrackedTraderAddresses]) -> "TrackedUserDirectory":
        """Build a normalized directory from trader address records."""
        normalized_entries = tuple(entries)
        alias_map: dict[str, str] = {}
        leaders: set[str] = set()
        # First pass: canonical leader wallets always map to themselves.
        for entry in normalized_entries:
            leaders.add(entry.leader_wallet)
            alias_map[entry.leader_wallet] = entry.leader_wallet
        # Second pass: attach EOA / Proxy / extra aliases without overwriting a canonical leader.
        for entry in normalized_entries:
            for addr in entry.all_addresses:
                alias_map.setdefault(addr, entry.leader_wallet)

        ordered_addresses = tuple(sorted(alias_map))
        hex40 = frozenset(addr[2:] for addr in ordered_addresses)
        topics = tuple("0x" + _TOPIC_ZERO_PAD + addr[2:] for addr in ordered_addresses)
        return cls(
            entries=normalized_entries,
            leader_wallets=frozenset(leaders),
            addresses_with_prefix=ordered_addresses,
            hex40_without_prefix=hex40,
            padded_topics=topics,
            _alias_to_leader=MappingProxyType(alias_map),
        )

    @classmethod
    def from_wallets(
        cls,
        wallets: Iterable[str],
        *,
        traders: Iterable[Any] = (),
        wallet_configs: Iterable[Any] = (),
    ) -> "TrackedUserDirectory":
        """Build a directory for ``wallets``, enriched with configured EOA/Proxy metadata."""
        metadata_by_wallet: dict[str, tuple[str, str]] = {}
        for item in (*wallet_configs, *traders):
            raw_wallet = try_normalize_eth_address(getattr(item, "wallet", ""))
            if raw_wallet is None:
                continue
            eoa = str(getattr(item, "eoa_address", "") or "").strip()
            proxy = str(getattr(item, "proxy_address", "") or "").strip()
            prev_eoa, prev_proxy = metadata_by_wallet.get(raw_wallet, ("", ""))
            merged = (eoa or prev_eoa, proxy or prev_proxy)
            metadata_by_wallet[raw_wallet] = merged
            for alias_raw in (eoa, proxy):
                norm_alias = try_normalize_eth_address(alias_raw)
                if norm_alias is not None:
                    metadata_by_wallet.setdefault(norm_alias, merged)

        entries: list[TrackedTraderAddresses] = []
        seen: set[str] = set()
        for raw_wallet in wallets:
            if not str(raw_wallet or "").strip():
                continue
            canonical = normalize_eth_address(raw_wallet)
            if canonical in seen:
                continue
            seen.add(canonical)
            eoa, proxy = metadata_by_wallet.get(canonical, ("", ""))
            entries.append(
                TrackedTraderAddresses(
                    leader_wallet=canonical,
                    eoa_address=eoa,
                    proxy_address=proxy,
                )
            )
        return cls.from_entries(entries)

    @classmethod
    def from_config(
        cls,
        config: Any,
        *,
        active_wallets: Iterable[str] | None = None,
    ) -> "TrackedUserDirectory":
        """Build a directory from ``AppConfig`` and an optional active wallet set."""
        copy_trading = getattr(config, "copy_trading", None)
        traders = getattr(copy_trading, "traders", ()) or ()
        wallet_configs = getattr(copy_trading, "wallets", ()) or ()
        if active_wallets is None:
            enabled_fn = getattr(copy_trading, "enabled_wallets", None)
            raw_enabled = enabled_fn() if callable(enabled_fn) else ()
            if isinstance(raw_enabled, Iterable) and not isinstance(raw_enabled, (str, bytes)):
                wallets: Iterable[str] = tuple(str(addr) for addr in raw_enabled)
            else:
                wallets = ()
        else:
            wallets = active_wallets
        return cls.from_wallets(wallets, traders=traders, wallet_configs=wallet_configs)


__all__ = [
    "TrackedTraderAddresses",
    "TrackedUserDirectory",
    "normalize_eth_address",
    "pad_address_topic",
    "try_normalize_eth_address",
]
