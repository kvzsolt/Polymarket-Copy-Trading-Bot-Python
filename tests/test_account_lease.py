"""Task 21 — typed account identity + cross-process account lease (unit).

Pins the fail-closed lease semantics that keep exactly ONE process an active
writer per copier account, and the typed account identity that stays stable
across display/config changes:

* a second process FAILS CLOSED while another process holds a live lease;
* renewal keeps the lease live (a challenger is still rejected);
* graceful shutdown releases the lease (next acquire is clean, no takeover);
* a crashed holder (no release) keeps blocking until the TTL expires; after
  expiry a new process takes over (acquired, flagged, warned);
* losing the lease to a takeover during renewal is a hard error the
  supervisor fail-closes on;
* a transient renewal outage is NOT a loss — the next tick retries;
* leases are per-account: two accounts in one process never conflict;
* the typed identity derives from the persisted user_id only, so display or
  wallet re-binding changes never mint a new ledger identity.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from pmcopy.domain.account_identity import CopierAccountId
from pmcopy.services.account_lease import (
    AccountLeaseManager,
    AccountLeaseLost,
    account_lease_renewal_loop,
)


# ---------------------------------------------------------------------------
# Typed identity
# ---------------------------------------------------------------------------


def test_account_id_preserves_persisted_value_byte_identical() -> None:
    account = CopierAccountId("copier-alpha")
    assert account.value == "copier-alpha"
    assert str(account) == "copier-alpha"


def test_account_id_equality_and_hash_key_on_value_only() -> None:
    a = CopierAccountId("0x" + "C" * 40)
    b = CopierAccountId("0x" + "C" * 40)
    assert a == b
    assert hash(a) == hash(b)
    # Case is NOT folded: the persisted spelling is the identity (same rule
    # as leader ids in multiUserConfig).
    assert a != CopierAccountId("0x" + "c" * 40)


def test_account_id_rejects_empty_and_padded_values() -> None:
    with pytest.raises(ValueError):
        CopierAccountId("")
    with pytest.raises(ValueError):
        CopierAccountId("   ")
    # Surrounding whitespace would silently split ledgers; reject it rather
    # than normalize (normalization would rewrite persisted keys).
    with pytest.raises(ValueError):
        CopierAccountId(" padded ")


def test_account_id_from_runtime_user_reads_only_user_id() -> None:
    runtime_user = SimpleNamespace(
        user_id="copier-alpha",
        wallet=SimpleNamespace(funder_address="0x" + "b" * 40),
    )
    account = CopierAccountId.from_runtime_user(runtime_user)
    assert account == CopierAccountId("copier-alpha")


def test_account_identity_stable_across_display_and_wallet_changes() -> None:
    """Renaming display/config surfaces must not create a new ledger identity.

    ``RuntimeUser`` carries no display name; the persisted ``user_id`` string
    IS the identity. Re-binding the wallet (deposit-wallet resolution) or any
    other config change leaves a NAMED user's id untouched, so the typed
    identity — and every ledger keyed on it — is unchanged. Only the
    config-less sentinel ids ("" / "default") re-key onto the wallet, because
    the wallet is then the only identity that account ever had.
    """
    from pmcopy.config.env import SignatureType, WalletConfig
    from pmcopy.services.multiUserConfig import RuntimeUser, bind_resolved_deposit_wallet

    def _user(user_id: str, funder: str) -> RuntimeUser:
        return RuntimeUser(
            user_id=user_id,
            wallet=WalletConfig(
                private_key=None,
                chain_id=137,
                signature_type=SignatureType.EOA,
                funder_address=funder,
                polygon_rpc_url="https://polygon-rpc.example.com",
                fallback_rpc_url=None,
            ),
        )

    named = _user("copier-alpha", "0x" + "aa" * 20)
    before = CopierAccountId.from_runtime_user(named)

    # Deposit-wallet re-binding changes the wallet surface, not the id.
    bind_resolved_deposit_wallet(named, "0x" + "bb" * 20)
    assert named.user_id == "copier-alpha"
    assert CopierAccountId.from_runtime_user(named) == before

    # A display-style rename of OTHER config (rpc, chain) is invisible here by
    # construction: the typed identity has exactly one component.
    assert CopierAccountId(before.value) == before


# ---------------------------------------------------------------------------
# Fake lease store mirroring the repo contract
# ---------------------------------------------------------------------------


class FakeClock:
    def __init__(self) -> None:
        self._now = datetime(2026, 10, 11, 12, 0, 0, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)


class FakeMonotonicClock:
    """Deterministic monotonic seam (seconds since an arbitrary origin)."""

    def __init__(self) -> None:
        self._t = 1000.0

    def monotonic(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds


class FakeLeaseRepo:
    """In-memory store with the AccountLeaseRepo semantics and fault hooks."""

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self.rows: dict[str, dict] = {}
        self.renew_exc: Exception | None = None
    async def acquire(
        self,
        *,
        user_id: str,
        holder_host: str,
        holder_token: str,
        mode: str,
        ttl_seconds: float,
    ):
        from pmcopy.storage.postgres.account_lease_repo import LeaseAcquisition

        now = self._clock.now()
        existing = self.rows.get(user_id)
        if existing is not None:
            expired = existing["expires_at"] <= now
            free = existing["released_at"] is not None or expired
            same_holder = existing["holder_token"] == holder_token
            if not (free or same_holder):
                held = dict(existing)
                return LeaseAcquisition(
                    acquired=False, takeover=False, reacquired=False, held_by=type(
                        "_H", (), {k: v for k, v in held.items()}
                    ),
                )
            # Same semantics as the real SQL: takeover = won an EXPIRED
            # foreign lease never released; a released row is a clean
            # handoff; same token is an idempotent reacquire.
            takeover = bool(free and not same_holder and existing["released_at"] is None)
            row = {
                "user_id": user_id,
                "holder_host": holder_host,
                "holder_token": holder_token,
                "mode": mode,
                "acquired_at": now if not same_holder else existing["acquired_at"],
                "renewed_at": now,
                "expires_at": now + timedelta(seconds=ttl_seconds),
                "released_at": None,
            }
            self.rows[user_id] = row
            return LeaseAcquisition(
                acquired=True,
                takeover=takeover,
                reacquired=bool(same_holder),
                lease=type("_L", (), dict(row)),
            )
        row = {
            "user_id": user_id,
            "holder_host": holder_host,
            "holder_token": holder_token,
            "mode": mode,
            "acquired_at": now,
            "renewed_at": now,
            "expires_at": now + timedelta(seconds=ttl_seconds),
            "released_at": None,
        }
        self.rows[user_id] = row
        return LeaseAcquisition(
            acquired=True, takeover=False, reacquired=False, lease=type("_L", (), dict(row))
        )

    async def renew(self, *, user_id: str, holder_token: str, ttl_seconds: float):
        if self.renew_exc is not None:
            raise self.renew_exc
        now = self._clock.now()
        row = self.rows.get(user_id)
        if row is None or row["holder_token"] != holder_token or row["released_at"] is not None:
            return None
        row["renewed_at"] = now
        row["expires_at"] = now + timedelta(seconds=ttl_seconds)
        return row["expires_at"]

    async def release(self, *, user_id: str, holder_token: str) -> bool:
        row = self.rows.get(user_id)
        if row is None or row["holder_token"] != holder_token or row["released_at"] is not None:
            return False
        row["released_at"] = self._clock.now()
        return True


def _manager(repo: FakeLeaseRepo, **kwargs) -> AccountLeaseManager:
    return AccountLeaseManager(
        repo=repo,
        ttl_seconds=kwargs.pop("ttl_seconds", 60.0),
        renewal_interval_seconds=kwargs.pop("renewal_interval_seconds", 15.0),
        **kwargs,
    )


ACCOUNT = CopierAccountId("copier-alpha")
ACCOUNT_B = CopierAccountId("copier-beta")


# ---------------------------------------------------------------------------
# Lease semantics (unit, fake store)
# ---------------------------------------------------------------------------


async def test_second_process_fails_closed_while_lease_active() -> None:
    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    first = _manager(repo)
    acquired = await first.acquire(ACCOUNT, mode="live")
    assert acquired.acquired

    second = _manager(repo)
    rejected = await second.acquire(ACCOUNT, mode="live")
    assert rejected.acquired is False
    assert rejected.takeover is False
    # The rejected process learns WHO holds the lease for the operator log.
    assert rejected.held_by is not None
    assert rejected.held_by.holder_token == first.process_token
    assert second.held_accounts == ()


async def test_renewal_keeps_lease_live_against_challenger() -> None:
    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    holder = _manager(repo)
    assert (await holder.acquire(ACCOUNT, mode="paper")).acquired

    for _ in range(5):
        clock.advance(15)
        renewals = await holder.renew_held()
        assert renewals[ACCOUNT.value] > clock.now()

    challenger = _manager(repo)
    rejected = await challenger.acquire(ACCOUNT, mode="paper")
    assert rejected.acquired is False


async def test_graceful_shutdown_releases_lease() -> None:
    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    first = _manager(repo)
    assert (await first.acquire(ACCOUNT, mode="live")).acquired

    released = await first.release_all()
    assert released == 1
    assert first.held_accounts == ()

    successor = _manager(repo)
    outcome = await successor.acquire(ACCOUNT, mode="live")
    assert outcome.acquired is True
    # Released cleanly: this is a fresh acquire, not an expiry takeover.
    assert outcome.takeover is False


async def test_crashed_holder_blocks_until_ttl_then_takeover_with_warning() -> None:
    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    crashed = _manager(repo)
    assert (await crashed.acquire(ACCOUNT, mode="live")).acquired
    # Crash: no renewal, no release.

    clock.advance(30)  # inside the 60s TTL window
    early = _manager(repo)
    assert (await early.acquire(ACCOUNT, mode="live")).acquired is False

    clock.advance(31)  # past the TTL
    successor = _manager(repo)
    outcome = await successor.acquire(ACCOUNT, mode="live")
    assert outcome.acquired is True
    assert outcome.takeover is True


async def test_lost_lease_during_renewal_raises_for_fail_closed() -> None:
    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    holder = _manager(repo)
    assert (await holder.acquire(ACCOUNT, mode="live")).acquired

    # Someone else took the lease over (e.g. our TTL lapsed mid-outage and a
    # second process started).
    row = repo.rows[ACCOUNT.value]
    row["holder_token"] = "someone-else"

    with pytest.raises(AccountLeaseLost) as excinfo:
        await holder.renew_held()
    assert ACCOUNT.value in str(excinfo.value)


async def test_transient_renewal_outage_is_not_a_loss() -> None:
    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    holder = _manager(repo)
    assert (await holder.acquire(ACCOUNT, mode="live")).acquired

    repo.renew_exc = RuntimeError("db temporarily unavailable")
    # A transient outage must NOT raise (the next tick retries; TTL outlives
    # several renewal intervals).
    await holder.renew_held()

    repo.renew_exc = None
    clock.advance(15)
    renewals = await holder.renew_held()
    assert renewals[ACCOUNT.value] > clock.now()


async def test_transient_outage_spanning_ttl_fails_closed_without_db_contact() -> None:
    """An outage longer than the TTL must fail-close the process locally.

    Every renew attempt errors (database unreachable), so no SQL outcome —
    not even a zero-row "lost" — can ever be observed. The manager still
    MUST stop treating itself as the holder once its OWN monotonic deadline
    (last successful acquire/renew + TTL) lapses: the server-side lease has
    certainly expired, so a rival may lawfully take over while this process
    would otherwise keep trading un-leased forever.
    """
    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    mono = FakeMonotonicClock()
    holder = _manager(repo, monotonic_clock=mono)
    assert (await holder.acquire(ACCOUNT, mode="live")).acquired

    repo.renew_exc = RuntimeError("database partition")
    # Failures strictly inside the TTL window: warn-and-continue.
    for _ in range(3):
        mono.advance(15)
        await holder.renew_held()  # 45s elapsed, no raise
    # Cross the local TTL deadline (61s since the successful acquire) with
    # ZERO successful DB contact in between: the raise must come from the
    # LOCAL deadline, not from a zero-row renew outcome.
    mono.advance(16)
    with pytest.raises(AccountLeaseLost) as excinfo:
        await holder.renew_held()
    assert ACCOUNT.value in str(excinfo.value)


async def test_lease_is_per_account_two_accounts_never_conflict() -> None:
    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    first = _manager(repo)
    assert (await first.acquire(ACCOUNT, mode="live")).acquired
    assert (await first.acquire(ACCOUNT_B, mode="live")).acquired

    other = _manager(repo)
    # The other process may hold the OTHER account only.
    assert (await other.acquire(ACCOUNT, mode="live")).acquired is False
    assert (await other.acquire(ACCOUNT_B, mode="live")).acquired is False

    repo_b = FakeLeaseRepo(FakeClock())
    solo = _manager(repo_b)
    assert (await solo.acquire(ACCOUNT, mode="live")).acquired

    assert set(first.held_accounts) == {ACCOUNT, ACCOUNT_B}
    assert (await first.release_all()) == 2


async def test_renewal_loop_renews_then_sleeps_and_surfaces_loss() -> None:
    import asyncio

    clock = FakeClock()
    repo = FakeLeaseRepo(clock)
    holder = _manager(repo, renewal_interval_seconds=0.01)
    assert (await holder.acquire(ACCOUNT, mode="live")).acquired

    task = asyncio.create_task(account_lease_renewal_loop(holder))
    await asyncio.sleep(0.05)
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # After losing the row, the loop must die (never limp along un-leased).
    repo.rows[ACCOUNT.value]["holder_token"] = "someone-else"
    task = asyncio.create_task(account_lease_renewal_loop(holder))
    with pytest.raises(AccountLeaseLost):
        await asyncio.wait_for(task, timeout=1.0)


# ---------------------------------------------------------------------------
# Bootstrap + app wiring contracts
# ---------------------------------------------------------------------------


async def test_user_runtime_state_carries_typed_account_identity() -> None:
    from unittest.mock import MagicMock, patch

    import pmcopy.config.app_config as app_config_mod
    from pmcopy.orchestrators.runtime_bootstrap import build_user_state
    from tests.config_helpers import load_test_config

    env = {
        "PRIVATE_KEY": "0x" + "a" * 64,
        "FUNDER_ADDRESS": "0x" + "b" * 40,
        "USER_ADDRESS": "0x" + "c" * 40,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
    }
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        config = load_test_config(env)
    runtime_user = SimpleNamespace(
        user_id="copier-alpha",
        wallet=SimpleNamespace(funder_address="0x" + "b" * 40),
    )
    state = build_user_state(
        runtime_user,
        client=object(),
        pool=MagicMock(),
        config=config,
        clob_gateway=object(),
    )
    assert state.account == CopierAccountId("copier-alpha")
    # The convenience property serves states built before the field existed.
    assert state.account_id == CopierAccountId("copier-alpha")


def test_app_startup_acquires_leases_before_building_user_states() -> None:
    """The lease seam must fail startup BEFORE any execution enablement."""
    import pmcopy.app as app_module

    source = inspect.getsource(app_module._run_application)
    acquire_index = source.index("account_lease_manager.acquire(")
    assert acquire_index < source.index("user_states[runtime_user.user_id] = build_user_state(")
    # Fail-closed branch on a rejected acquire.
    assert "refusing to start a second writer" in source
    # Renewal is supervised with the same bounded-restart machinery.
    assert '"account-lease-renewal"' in source


def test_app_startup_starts_evidence_drain_only_after_leases_are_held() -> None:
    """Final-review fix (reviewer minor): the database-to-spool evidence
    drain reads the SHARED database outbox.  A doomed second process (lease
    refused below) must never start that worker, so the drain starts only
    after every account lease is acquired."""
    import pmcopy.app as app_module

    source = inspect.getsource(app_module._run_application)
    assert source.index("account_lease_manager.acquire(") < source.index(
        "await evidence_drain.start()"
    )


def test_app_shutdown_releases_leases_before_pool_close() -> None:
    import pmcopy.app as app_module

    source = inspect.getsource(app_module._shutdown_runtime)
    assert "account_lease_manager" in source
    assert source.index("account_lease_manager.close()") < source.index("shutdown_pool()")


def test_account_lease_migration_is_in_the_catalog() -> None:
    from pmcopy.storage.postgres.v1_migration_runner import V1MigrationCatalog

    ids = [m.migration_id for m in V1MigrationCatalog().migrations()]
    assert "20261011_13_account_execution_leases" in ids
