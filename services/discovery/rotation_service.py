from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal

from services.discovery.balance_provider import build_wallet_balance_provider
from services.discovery.candidate_scoring import copyability_score, score_candidate
from services.discovery.copyability_replay import CopyabilityReplayService
from services.discovery.runtime_env_editor import RuntimeEnvEditor
from services.discovery.signal_grouper import SignalGrouper
from storage.postgres.discovery_repo import DiscoveryRepo


@dataclass(frozen=True)
class RotationRecommendation:
    keep: tuple[str, ...]
    replace: tuple[tuple[str, str], ...]
    candidates: tuple[object, ...]


class RotationService:
    def __init__(self, *, pool, config, balance_provider=None):
        self.pool = pool
        self.config = config
        self.balance_provider = balance_provider or build_wallet_balance_provider(config)
        self.discovery_repo = DiscoveryRepo(pool)
        self.grouper = SignalGrouper(
            window_seconds=config.discovery.discovery_signal_group_window_seconds,
            max_gap_seconds=config.discovery.discovery_signal_group_max_gap_seconds,
            min_group_notional_usdc=Decimal(
                str(config.discovery.discovery_min_group_notional_usdc)
            ),
        )
        self.replay_service = CopyabilityReplayService(
            config=config,
            balance_provider=self.balance_provider,
        )

    async def run_interactive(
        self,
        *,
        env_file: str,
        dry_run: bool = False,
        max_active: int | None = None,
    ) -> None:
        editor = RuntimeEnvEditor(env_file)
        current = editor.read_leaders()
        available_balance = await self._fetch_balance()
        recommendation = await self.recommend(current, max_active=max_active)
        self._print_recommendation(current, recommendation, available_balance)

        if dry_run:
            print("Dry run: no changes written.")
            return

        choice = input("Select action [1=accept, 2=manual, 3=keep, 4=exit]: ").strip()
        if choice == "1":
            new_leaders = self._apply_recommendation(current, recommendation)
        elif choice == "2":
            raw = input("Enter comma-separated LEADER_WALLETS: ").strip()
            new_leaders = [x.strip().lower() for x in raw.split(",") if x.strip()]
        elif choice in {"3", "4", ""}:
            print("No changes written.")
            return
        else:
            print("Unknown selection; no changes written.")
            return

        if new_leaders == current:
            print("Leader set unchanged; no changes written.")
            return

        confirm = input(
            "Write LEADER_WALLETS="
            + ",".join(new_leaders)
            + " to "
            + str(env_file)
            + "? [y/N]: "
        ).strip().lower()
        if confirm not in {"y", "yes"}:
            print("No changes written.")
            return

        backup = editor.write_leaders(new_leaders)
        print(f"Updated {env_file}; backup written to {backup}")

    async def recommend(
        self,
        current: list[str],
        *,
        max_active: int | None = None,
    ) -> RotationRecommendation:
        candidates = await self.discovery_repo.list_evaluations(
            statuses=["live_eligible", "shadow"],
            limit=25,
        )
        current_set = {w.lower() for w in current}
        replacements = [
            c for c in candidates if c.wallet.lower() not in current_set
        ]
        replacements = await self._refresh_candidate_replays(replacements)
        replacements.sort(key=lambda c: (-c.final_score, c.wallet.lower()))
        max_active = max_active or len(current) or self.config.leader.auto_discover_top_n
        keep = list(current[:max_active])

        replace_pairs: list[tuple[str, str]] = []
        if replacements and current:
            weak_current = await self._weak_current_leaders(current)
            for idx, old in enumerate(weak_current):
                if idx >= len(replacements):
                    break
                replace_pairs.append((old, replacements[idx].wallet))

        return RotationRecommendation(
            keep=tuple(keep),
            replace=tuple(replace_pairs),
            candidates=tuple(replacements[:10]),
        )

    async def _fetch_balance(self) -> Decimal | None:
        if self.balance_provider is None:
            return None
        return Decimal(str(await self.balance_provider.get_available_balance_usdc()))

    async def _refresh_candidate_replays(self, candidates: list) -> list:
        refreshed = []
        for row in candidates:
            refreshed.append(await self._refresh_candidate_replay(row))
        return refreshed

    async def _refresh_candidate_replay(self, row):
        trades = await self.discovery_repo.list_trades(
            row.wallet,
            limit=int(self.config.discovery.discovery_history_trade_limit),
        )
        signals = self.grouper.group(trades)
        replay = await self.replay_service.replay_wallet(row.wallet, signals)
        copy_score = copyability_score(replay)
        score_version = str(
            getattr(
                self.config.discovery,
                "leader_score_version",
                getattr(row, "score_version", "leader_score_v2"),
            )
            or "leader_score_v2"
        )
        scored = score_candidate(
            candidate=row,
            falcon_quality=float(row.falcon_quality_score),
            copyability=copy_score,
            recent_consistency=float(row.recent_state_score),
            market_fit=float(row.market_fit_score),
            config=self.config,
            score_version=score_version,
        )
        return replace(
            row,
            final_score=float(scored["final_score"]),
            copyability_score=copy_score,
            score_version=score_version,
            score_breakdown_json=dict(scored["breakdown"]),
            total_grouped_signals=int(len(signals)),
            meaningful_signals=int(replay.meaningful_signals),
            copyable_signals=int(replay.copyable_signals),
            copyable_signal_ratio=float(replay.copyable_signal_ratio),
            copyable_notional_ratio=float(replay.copyable_notional_ratio),
        )

    async def _weak_current_leaders(self, current: list[str]) -> list[str]:
        weak: list[str] = []
        for wallet in current:
            row = await self.discovery_repo.get_evaluation(wallet)
            if row is None:
                weak.append(wallet)
                continue
            if (
                self.config.discovery.rotate_keep_live_if_7d_pnl_positive
                and row.pnl_7d > Decimal("0")
            ):
                continue
            if row.pnl_7d < Decimal(
                str(self.config.discovery.rotate_reject_live_if_7d_pnl_below)
            ):
                weak.append(wallet)
        return weak

    def _apply_recommendation(
        self,
        current: list[str],
        recommendation: RotationRecommendation,
    ) -> list[str]:
        replace_map = {old.lower(): new.lower() for old, new in recommendation.replace}
        out = [replace_map.get(wallet.lower(), wallet.lower()) for wallet in current]
        if not out and recommendation.candidates:
            out = [recommendation.candidates[0].wallet.lower()]
        return out

    def _print_recommendation(
        self,
        current: list[str],
        recommendation: RotationRecommendation,
        available_balance: Decimal | None,
    ) -> None:
        if available_balance is not None:
            print(f"Available USDC balance: {available_balance}")
            print()

        print("Current live leaders")
        print("-" * 60)
        if not current:
            print("(none)")
        for idx, wallet in enumerate(current, start=1):
            print(f"[{idx}] {wallet}")

        print()
        print("Best replacement candidates")
        print("-" * 60)
        if not recommendation.candidates:
            print("(none)")
        for idx, row in enumerate(recommendation.candidates, start=1):
            print(
                f"[{idx}] {row.wallet} final={row.final_score:.3f} "
                f"falcon={row.falcon_quality_score:.3f} "
                f"copy={row.copyability_score:.3f} "
                f"signals={row.copyable_signal_ratio:.0%} "
                f"notional={row.copyable_notional_ratio:.0%} "
                f"status={row.status}"
            )

        print()
        print("Recommendation")
        print("-" * 60)
        if not recommendation.replace:
            print("Keep current leaders.")
        for old, new in recommendation.replace:
            print(f"Replace {old} with {new}")
