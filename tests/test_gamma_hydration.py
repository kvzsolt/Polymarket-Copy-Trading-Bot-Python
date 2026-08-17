"""Gamma market hydration contracts.

`hydrate_resolved_market_state` surveys Gamma at boot to flag resolved
markets. Two invariants:

1. The lookup must use the ``condition_ids`` filter — Gamma's
   ``/markets/{id}`` route accepts only the numeric Gamma market id and
   returns 422 for a 0x condition id.
2. Only authoritative terminal evidence may mark a token resolved. A
   transient outage or malformed response must leave the position's
   monitoring intact for the lifetime of the process.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import orchestrators.runtime_bootstrap as runtime_bootstrap
from orchestrators.runtime_bootstrap import hydrate_resolved_market_state

COND = "0x" + "a" * 64
COND_GONE = "0x" + "b" * 64
TOKEN = "123"


class _Store:
    def __init__(self) -> None:
        self.resolved: list[tuple[str, str]] = []

    def mark_resolved(self, token: str, cond: str) -> None:
        self.resolved.append((token, cond))


class _Repo:
    def __init__(self) -> None:
        self.positions = [SimpleNamespace(condition_id=COND, token_id=TOKEN)]

    async def list_open_positions(self, _user_id: str):
        return list(self.positions)


def _runtime(store: _Store, repo: _Repo):
    user = SimpleNamespace(
        user_id="u1",
        wallet=SimpleNamespace(funder_address="0x" + "c" * 40, private_key=""),
        follows={},
    )
    state = SimpleNamespace(market_store=store, positions_repo=repo)
    return [user], {"u1": state}


def _market_doc(**overrides) -> dict:
    doc = {"conditionId": COND, "closed": False, "active": True, "archived": False}
    doc.update(overrides)
    return doc


@pytest.mark.asyncio
async def test_gamma_lookup_uses_the_condition_ids_filter_route(monkeypatch) -> None:
    urls: list[str] = []

    async def fake_fetch(url: str, *, silent: bool = False):
        urls.append(url)
        return [_market_doc()]

    monkeypatch.setattr(runtime_bootstrap, "fetch_data", fake_fetch)
    store, repo = _Store(), _Repo()
    users, states = _runtime(store, repo)

    await hydrate_resolved_market_state(runtime_users=users, user_states=states)

    assert len(urls) == 1
    assert "condition_ids=" in urls[0]
    assert COND in urls[0]
    # The /markets/{id} path route 422s for condition ids; it must not be used.
    assert f"/markets/{COND}" not in urls[0]


@pytest.mark.asyncio
async def test_closed_market_is_marked_resolved(monkeypatch) -> None:
    async def fake_fetch(url: str, *, silent: bool = False):
        return [_market_doc(closed=True)]

    monkeypatch.setattr(runtime_bootstrap, "fetch_data", fake_fetch)
    store, repo = _Store(), _Repo()
    users, states = _runtime(store, repo)

    summary = await hydrate_resolved_market_state(runtime_users=users, user_states=states)

    assert store.resolved == [(TOKEN, COND)]
    assert summary == {"u1": 1}


@pytest.mark.asyncio
async def test_inactive_market_is_marked_resolved(monkeypatch) -> None:
    async def fake_fetch(url: str, *, silent: bool = False):
        return [_market_doc(active=False)]

    monkeypatch.setattr(runtime_bootstrap, "fetch_data", fake_fetch)
    store, repo = _Store(), _Repo()
    users, states = _runtime(store, repo)

    await hydrate_resolved_market_state(runtime_users=users, user_states=states)

    assert store.resolved == [(TOKEN, COND)]


@pytest.mark.asyncio
async def test_open_market_is_not_marked(monkeypatch) -> None:
    async def fake_fetch(url: str, *, silent: bool = False):
        return [_market_doc()]

    monkeypatch.setattr(runtime_bootstrap, "fetch_data", fake_fetch)
    store, repo = _Store(), _Repo()
    users, states = _runtime(store, repo)

    await hydrate_resolved_market_state(runtime_users=users, user_states=states)

    assert store.resolved == []


@pytest.mark.asyncio
async def test_unknown_condition_is_terminal(monkeypatch) -> None:
    """A condition Gamma no longer indexes is authoritative: mark resolved."""

    async def fake_fetch(url: str, *, silent: bool = False):
        assert COND_GONE in url or COND in url
        return []

    monkeypatch.setattr(runtime_bootstrap, "fetch_data", fake_fetch)
    store, repo = _Store(), _Repo()
    users, states = _runtime(store, repo)

    await hydrate_resolved_market_state(runtime_users=users, user_states=states)

    assert store.resolved == [(TOKEN, COND)]


@pytest.mark.asyncio
async def test_transport_failure_never_marks_resolved(monkeypatch) -> None:
    """A Gamma outage at boot must not disable risk monitoring."""

    async def fake_fetch(url: str, *, silent: bool = False):
        raise TimeoutError("gamma unreachable")

    monkeypatch.setattr(runtime_bootstrap, "fetch_data", fake_fetch)
    store, repo = _Store(), _Repo()
    users, states = _runtime(store, repo)

    summary = await hydrate_resolved_market_state(runtime_users=users, user_states=states)

    assert store.resolved == []
    assert summary == {"u1": 0}


@pytest.mark.asyncio
async def test_malformed_payload_never_marks_resolved(monkeypatch) -> None:
    async def fake_fetch(url: str, *, silent: bool = False):
        return {"unexpected": "shape"}

    monkeypatch.setattr(runtime_bootstrap, "fetch_data", fake_fetch)
    store, repo = _Store(), _Repo()
    users, states = _runtime(store, repo)

    await hydrate_resolved_market_state(runtime_users=users, user_states=states)

    assert store.resolved == []


@pytest.mark.asyncio
async def test_malformed_row_never_marks_resolved(monkeypatch) -> None:
    async def fake_fetch(url: str, *, silent: bool = False):
        return ["not-a-dict"]

    monkeypatch.setattr(runtime_bootstrap, "fetch_data", fake_fetch)
    store, repo = _Store(), _Repo()
    users, states = _runtime(store, repo)

    await hydrate_resolved_market_state(runtime_users=users, user_states=states)

    assert store.resolved == []
