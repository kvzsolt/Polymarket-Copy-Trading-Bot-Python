"""Durable informational usage accounting shared by provider consumers."""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from types import TracebackType

from pmcopy.domain.provider_budget import (
    BudgetSettings,
    ProviderConfigurationError,
    ProviderUsage,
    ProviderUsageSnapshot,
)
from pmcopy.interfaces.provider_budget import ProviderClock


class SqliteProviderBudget:
    """Persist usage estimates without quota, reserve, or throughput enforcement."""

    _PRICES = {
        "alchemy": {
            "eth_chainId": 0,
            "eth_blockNumber": 10,
            "eth_call": 26,
            "eth_getCode": 20,
            "eth_getBalance": 20,
            "eth_getBlockByNumber": 20,
            "eth_getBlockByHash": 20,
            "eth_getTransactionByHash": 20,
            "eth_getTransactionReceipt": 20,
            "eth_getTransactionCount": 20,
            "eth_getLogs": 60,
            "eth_subscribe": 10,
            "eth_unsubscribe": 10,
            "eth_gasPrice": 20,
            "eth_estimateGas": 20,
            "eth_getStorageAt": 20,
        },
        "infura": {
            "eth_chainId": 5,
            "eth_blockNumber": 80,
            "eth_call": 80,
            "eth_getCode": 80,
            "eth_getBalance": 80,
            "eth_getBlockByNumber": 80,
            "eth_getBlockByHash": 80,
            "eth_getTransactionByHash": 80,
            "eth_getTransactionReceipt": 80,
            "eth_getTransactionCount": 80,
            "eth_getLogs": 255,
            "eth_subscribe": 5,
            "eth_unsubscribe": 10,
            "eth_gasPrice": 80,
            "eth_estimateGas": 300,
            "eth_getStorageAt": 80,
        },
    }
    _CATEGORIES = frozenset({"essential", "pending", "backfill", "research"})

    def __init__(self, path: Path, settings: BudgetSettings, clock: ProviderClock) -> None:
        """Open accounting history without requiring plan or baseline approval."""
        self._settings = settings
        self._clock = clock
        self._lock = threading.RLock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, isolation_level=None, timeout=2, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS provider_usage (
                provider TEXT NOT NULL, period TEXT NOT NULL, category TEXT NOT NULL,
                amount INTEGER NOT NULL, PRIMARY KEY(provider, period, category)
            );
            CREATE TABLE IF NOT EXISTS provider_budget_meta (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
        """)
        try:
            self._initialize()
        except BaseException:
            self._db.close()
            raise

    def _initialize(self) -> None:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            row = self._db.execute(
                "SELECT value FROM provider_budget_meta WHERE key='billing_day'"
            ).fetchone()
            if row is not None and row[0] != str(self._settings.alchemy_billing_day):
                raise ProviderConfigurationError(
                    "billing day requires an explicit history migration"
                )
            if row is None:
                self._db.execute(
                    "INSERT INTO provider_budget_meta VALUES ('billing_day', ?)",
                    (str(self._settings.alchemy_billing_day),),
                )
                for provider, used in (
                    ("alchemy", self._settings.initial_alchemy_used),
                    ("infura", self._settings.initial_infura_used),
                ):
                    self._increment(provider, self._period(provider), "external_baseline", used)
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    def _period(self, provider: str) -> str:
        instant = self._clock.now()
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ProviderConfigurationError("billing clock requires an aware instant")
        instant = instant.astimezone(UTC)
        if provider in {"infura", "pending"}:
            return instant.date().isoformat()
        year, month = instant.year, instant.month
        if instant.day < self._settings.alchemy_billing_day:
            month -= 1
            if month == 0:
                year, month = year - 1, 12
        return f"{year:04d}-{month:02d}-{self._settings.alchemy_billing_day:02d}"

    def _used(self, provider: str, period: str) -> int:
        row = self._db.execute(
            "SELECT COALESCE(SUM(amount),0) FROM provider_usage WHERE provider=? AND period=?",
            (provider, period),
        ).fetchone()
        return int(row[0])

    def _increment(self, provider: str, period: str, category: str, amount: int) -> None:
        self._db.execute(
            "INSERT INTO provider_usage VALUES (?,?,?,?) ON CONFLICT(provider,period,category) "
            "DO UPDATE SET amount=amount+excluded.amount",
            (provider, period, category, amount),
        )

    @classmethod
    def rpc_cost(cls, provider: str, method: str) -> int:
        """Return a reviewed read-method price; unknown providers/methods are disabled."""
        try:
            return cls._PRICES[provider][method]
        except KeyError:
            raise ProviderConfigurationError("unpriced provider RPC method is disabled") from None

    def stream_cost(self, byte_count: int) -> int:
        """Conservatively round delivered Alchemy notification bytes upward."""
        if type(byte_count) is not int or byte_count < 0:
            raise ValueError("byte count must be a nonnegative integer")
        return int(
            (
                Decimal(byte_count) * Decimal("0.04") * self._settings.stream_margin
            ).to_integral_value(rounding=ROUND_CEILING)
        )

    def charge(
        self,
        provider: str,
        amount: int,
        *,
        category: str = "essential",
        already_delivered: bool = False,
    ) -> None:
        """Record estimated usage; the legacy delivery flag never changes admission."""
        self._charge(provider, amount, category=category)

    def charge_stream(self, provider: str, byte_count: int, *, category: str = "essential") -> None:
        """Record delivered Alchemy bytes; never infer Infura billing from bytes.

        Infura remains available for admitted bounded RPC recovery. Its standing
        log subscriptions require independently verified per-block metering.
        """
        if provider != "alchemy":
            raise ProviderConfigurationError("Infura standing subscription billing is unverified")
        self.charge(
            provider, self.stream_cost(byte_count), category=category, already_delivered=True
        )

    def admit_rpc(self, provider: str, method: str, *, category: str = "essential") -> int:
        """Count one read attempt without a local quota or throughput restriction."""
        cost = self.rpc_cost(provider, method)
        self._charge(provider, cost, category=category)
        return cost

    def _charge(
        self,
        provider: str,
        amount: int,
        *,
        category: str,
    ) -> None:
        if provider not in self._PRICES or category not in self._CATEGORIES:
            raise ProviderConfigurationError("unknown provider or budget category")
        if type(amount) is not int or amount < 0:
            raise ValueError("charge must be a nonnegative integer")
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                period, day = self._period(provider), self._period("pending")
                self._increment(provider, period, category, amount)
                if category == "pending":
                    self._increment("pending", day, category, amount)
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def snapshot(self) -> ProviderUsageSnapshot:
        """Expose provider-separated estimates without endpoints or service credentials."""
        with self._lock:
            return {
                "estimate_only": True,
                "limits_enforced": False,
                "as_of": self._clock.now().isoformat(),
                "other_app_usage_included": "configured initial estimate only",
                "pending_daily_used": self._used("pending", self._period("pending")),
                "alchemy": self._provider_snapshot("alchemy", "CU"),
                "infura": self._provider_snapshot("infura", "credits"),
            }

    def _provider_snapshot(self, provider: str, unit: str) -> ProviderUsage:
        period = self._period(provider)
        return {
            "period": period,
            "unit": unit,
            "used": self._used(provider, period),
            "limit": None,
            "remaining": None,
        }

    def close(self) -> None:
        """Close this connection without deleting history or resetting usage."""
        self._db.close()

    def __enter__(self) -> SqliteProviderBudget:
        """Return the ledger for bounded ownership in tests and command tools."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the connection regardless of the operation's result."""
        self.close()
