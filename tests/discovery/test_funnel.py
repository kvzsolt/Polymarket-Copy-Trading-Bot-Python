from __future__ import annotations

import time
from typing import Any

import pytest

from config.app_config import DiscoveryConfig
from integrations.falcon.client import Agent, FalconPage
from services.discovery.funnel import DiscoveryFunnel
from services.discovery.models import Candidate
from services.discovery.scoring import CompositeScorer

GOOD = "0x" + "1" * 40
BOT = "0x" + "2" * 40
DRAWDOWN = "0x" + "3" * 40
STALE = "0x" + "4" * 40


class FakeFalcon:
    async def query(
        self,
        *,
        agent_id: int,
        params: dict[str, str],
        limit: int = 100,
        offset: int = 0,
    ) -> FalconPage:
        assert all(isinstance(value, str) for value in params.values())
        if agent_id == Agent.WALLET360:
            wallet = params["proxy_wallet"]
            row = {
                "win_rate": 0.60,
                "max_drawdown_pct": 10,
                "market_diversity": 5,
                "bet_size_cv": 1.0,
                "bot_flag": wallet == BOT,
            }
            if wallet == DRAWDOWN:
                row["max_drawdown_pct"] = 90
            return FalconPage((row,), 1)
        if agent_id == Agent.MARKET_INSIGHTS:
            return FalconPage(
                (
                    {
                        "volume_24h": 100_000,
                        "liquidity_percentile": 90,
                        "top1_wallet_pct": 10,
                        "unique_traders_7d": 100,
                    },
                ),
                1,
            )
        raise AssertionError(f"unexpected query agent {agent_id}")

    async def paginate(
        self,
        *,
        agent_id: int,
        params: dict[str, str],
        page_size: int,
        max_pages: int,
    ):
        assert all(isinstance(value, str) for value in params.values())
        if agent_id == Agent.HSCORE:
            for index, wallet in enumerate((GOOD, BOT, DRAWDOWN, STALE), 1):
                yield {
                    "proxy_wallet": wallet,
                    "h_score": 80,
                    "roi_15d": 30,
                    "win_rate_15d": 0.60,
                    "sharpe_15d": 2,
                    "total_volume_15d": 25_000,
                    "total_trades_15d": 100,
                    "leaderboard_rank": index,
                }
            return
        if agent_id == Agent.TRADES and params["side"] == "BUY":
            wallet = params["wallet_proxy"]
            timestamp = time.time() - (72 * 3600 if wallet == STALE else 60)
            for index in range(3):
                yield {
                    "trade_id": f"{wallet}-{index}",
                    "timestamp": timestamp - index,
                    "condition_id": f"condition-{index}",
                    "side": "BUY",
                }


class EmptyWallet360Falcon(FakeFalcon):
    async def query(
        self,
        *,
        agent_id: int,
        params: dict[str, str],
        limit: int = 100,
        offset: int = 0,
    ) -> FalconPage:
        if agent_id == Agent.WALLET360:
            return FalconPage((), 0)
        return await super().query(
            agent_id=agent_id,
            params=params,
            limit=limit,
            offset=offset,
        )


class MemoryRepo:
    """In-memory stand-in mirroring ``DiscoveryRepo``'s stored config_hash.

    ``snapshots_by_hash`` models what the real ``latest_candidates`` query
    reads: rows carry the hash of the config that scored them.
    """

    def __init__(self, snapshots_by_hash: dict[str, list[Candidate]] | None = None) -> None:
        self.persisted: list[Candidate] = []
        self.completed: dict[str, Any] = {}
        self.snapshots_by_hash = snapshots_by_hash or {}
        self.rescored: list[tuple[list[Candidate], str]] = []

    async def begin_cycle(self, config_hash: str) -> str:
        return "cycle"

    async def persist_cycle(self, cycle_id, candidates, config_hash) -> None:
        self.persisted = candidates

    async def complete_cycle(self, cycle_id, **values) -> None:
        self.completed = values

    async def score_history(self, wallet: str, *, since_ts: float):
        return []

    async def latest_candidates(self, *, config_hash: str | None = None):
        if config_hash is None:
            return [
                candidate
                for candidates in self.snapshots_by_hash.values()
                for candidate in candidates
            ]
        return list(self.snapshots_by_hash.get(config_hash, []))

    async def persist_rescore(self, candidates, config_hash):
        self.rescored.append((list(candidates), config_hash))


@pytest.mark.asyncio
async def test_funnel_keeps_good_wallet_and_audits_every_rejection() -> None:
    config = DiscoveryConfig.model_validate(
        {
            "shortlist": {"sort_modes": ["h_score"]},
            "longitudinal": {"enabled": True},
        }
    )
    repository = MemoryRepo()
    funnel = DiscoveryFunnel(
        client=FakeFalcon(),  # type: ignore[arg-type]
        config=config,
        scorer=CompositeScorer(),
        repository=repository,
    )
    kept = await funnel.run()
    assert [candidate.wallet for candidate in kept] == [GOOD]
    reasons = {
        candidate.wallet: candidate.rejected_reason
        for candidate in repository.persisted
    }
    assert reasons == {
        GOOD: None,
        BOT: "bot_flagged",
        DRAWDOWN: "drawdown",
        STALE: "stale_activity",
    }
    assert kept[0].base_score > 0
    assert kept[0].longitudinal_score is not None
    assert repository.completed["candidate_count"] == 4
    assert repository.completed["kept_count"] == 1


@pytest.mark.asyncio
async def test_upstream_evaluation_outage_fails_cycle_instead_of_mass_demoting() -> None:
    config = DiscoveryConfig.model_validate(
        {
            "shortlist": {"sort_modes": ["h_score"]},
            "scheduler": {"max_evaluation_error_ratio": 0.25},
        }
    )
    repository = MemoryRepo()
    funnel = DiscoveryFunnel(
        client=EmptyWallet360Falcon(),  # type: ignore[arg-type]
        config=config,
        scorer=CompositeScorer(),
        repository=repository,
    )

    with pytest.raises(RuntimeError, match="error ratio"):
        await funnel.run()

    assert repository.completed["status"] == "failed"
    assert len(repository.persisted) == 4
    assert all(
        candidate.rejected_reason == "evaluation_error:ValueError"
        for candidate in repository.persisted
    )


# ---------------------------------------------------------------------------
# Degraded-cycle signalling
#
# A partial H-Score sort-mode failure still yields usable candidates, but the
# shortlist is incomplete: a wallet's absence is then missing evidence, not
# evidence of decay. run() must report that on ``last_cycle_degraded`` so
# LeaderRegistry can suppress absence-based demotion for exactly that cycle.
# ---------------------------------------------------------------------------


class OneModeFailsFalcon(FakeFalcon):
    """H-Score fails for ``sort_by=roi`` only; ``h_score`` stays healthy."""

    async def paginate(self, *, agent_id: int, params: dict[str, str], **kwargs: Any):
        if agent_id == Agent.HSCORE and params["sort_by"] == "roi":
            raise RuntimeError("sort mode 5xx")
        async for row in super().paginate(agent_id=agent_id, params=params, **kwargs):
            yield row


def _degraded_funnel(
    client: Any, repository: MemoryRepo, **overrides
) -> DiscoveryFunnel:
    return DiscoveryFunnel(
        client=client,
        config=DiscoveryConfig.model_validate(
            {"shortlist": {"sort_modes": ["h_score", "roi"]}, **overrides}
        ),
        scorer=CompositeScorer(),
        repository=repository,
    )


@pytest.mark.asyncio
async def test_partial_sort_mode_failure_marks_the_cycle_degraded() -> None:
    """Survivors are still returned, but the cycle is flagged incomplete."""
    repository = MemoryRepo()
    funnel = _degraded_funnel(OneModeFailsFalcon(), repository)

    kept = await funnel.run()

    assert [candidate.wallet for candidate in kept] == [GOOD]
    assert funnel.last_cycle_degraded is True
    assert repository.completed["status"] == "completed"


@pytest.mark.asyncio
async def test_healthy_cycle_is_not_marked_degraded() -> None:
    """A complete cycle must stay undegraded, or nothing could ever demote."""
    funnel = _degraded_funnel(FakeFalcon(), MemoryRepo())

    await funnel.run()

    assert funnel.last_cycle_degraded is False


@pytest.mark.asyncio
async def test_degraded_flag_is_reset_by_a_later_healthy_run() -> None:
    """The flag describes *this* cycle; it must not latch on."""
    repository = MemoryRepo()
    funnel = _degraded_funnel(OneModeFailsFalcon(), repository)
    await funnel.run()
    assert funnel.last_cycle_degraded is True

    # Same funnel instance, now-healthy upstream.
    funnel.client = FakeFalcon()  # type: ignore[assignment]
    await funnel.run()

    assert funnel.last_cycle_degraded is False


@pytest.mark.asyncio
async def test_degraded_flag_does_not_survive_a_failing_run() -> None:
    """A raised cycle must not leave a stale True for the next one to inherit.

    Sticky degradation would suppress absence-based demotion indefinitely, so
    a leader that genuinely decayed could never be demoted again.
    """
    repository = MemoryRepo()
    funnel = _degraded_funnel(OneModeFailsFalcon(), repository)
    await funnel.run()
    assert funnel.last_cycle_degraded is True

    # The next cycle has a healthy shortlist but fails later, during
    # evaluation. run() resets before its first await, so the raise must not
    # preserve the previous cycle's True.
    funnel.client = EmptyWallet360Falcon()  # type: ignore[assignment]
    funnel.config = DiscoveryConfig.model_validate(
        {
            "shortlist": {"sort_modes": ["h_score", "roi"]},
            "scheduler": {"max_evaluation_error_ratio": 0.25},
        }
    )
    with pytest.raises(RuntimeError, match="error ratio"):
        await funnel.run()

    assert funnel.last_cycle_degraded is False


# ---------------------------------------------------------------------------
# rescore() config-hash scoping
#
# rescore() reuses each snapshot's stored base_score and re-stamps the result
# with the *current* config hash. A snapshot scored under an older config must
# therefore never be rescored, or a stale base score is published as if the
# current scoring config had produced it.
# ---------------------------------------------------------------------------


def _rescore_funnel(repository: MemoryRepo, **overrides) -> DiscoveryFunnel:
    config = DiscoveryConfig.model_validate(
        {
            "shortlist": {"sort_modes": ["h_score"]},
            "longitudinal": {"enabled": False},
            **overrides,
        }
    )
    return DiscoveryFunnel(
        client=FakeFalcon(),  # type: ignore[arg-type]
        config=config,
        scorer=CompositeScorer(),
        repository=repository,
    )


def _snapshot(wallet: str, *, base_score: float) -> Candidate:
    candidate = Candidate(wallet=wallet, hscore={})
    candidate.base_score = base_score
    candidate.score = base_score
    return candidate


@pytest.mark.asyncio
async def test_rescore_excludes_snapshots_scored_under_an_old_config_hash() -> None:
    """Stale-config snapshots must not be re-stamped with the current hash."""
    repository = MemoryRepo({"stale-hash": [_snapshot(GOOD, base_score=99.0)]})
    funnel = _rescore_funnel(repository)
    assert funnel.config_hash != "stale-hash"

    candidates = await funnel.rescore()

    assert candidates == []
    # Nothing stale may be published under the current hash.
    assert repository.rescored == [([], funnel.config_hash)]


@pytest.mark.asyncio
async def test_rescore_still_reranks_snapshots_scored_under_the_current_hash() -> None:
    """Preservation: same-hash rescoring keeps working and re-ranks."""
    repository = MemoryRepo()
    funnel = _rescore_funnel(repository)
    repository.snapshots_by_hash = {
        funnel.config_hash: [
            _snapshot(BOT, base_score=1.0),
            _snapshot(GOOD, base_score=5.0),
        ],
        "stale-hash": [_snapshot(DRAWDOWN, base_score=99.0)],
    }

    candidates = await funnel.rescore()

    # Highest score first, and the stale-hash wallet is absent despite
    # carrying the largest base score of all.
    assert [candidate.wallet for candidate in candidates] == [GOOD, BOT]
    persisted, config_hash = repository.rescored[0]
    assert config_hash == funnel.config_hash
    assert [candidate.wallet for candidate in persisted] == [GOOD, BOT]


@pytest.mark.asyncio
async def test_rescore_requests_the_current_config_hash_from_the_repository() -> None:
    """The scope must be pushed into the query, not filtered afterwards.

    Filtering in Python would still read every wallet's snapshot back; the
    repository contract is what keeps old-hash rows out of the roster.
    """
    seen: list[str | None] = []

    class _RecordingRepo(MemoryRepo):
        async def latest_candidates(self, *, config_hash: str | None = None):
            seen.append(config_hash)
            return await super().latest_candidates(config_hash=config_hash)

    repository = _RecordingRepo()
    funnel = _rescore_funnel(repository)

    await funnel.rescore()

    assert seen == [funnel.config_hash]
