"""Risk-controlled paper copies with injected storage, market, and execution ports."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Protocol

from pmcopy.config.app_config import AppConfig
from pmcopy.domain.auto_trader import AutoTraderScope
from pmcopy.domain.paper_evaluation import PaperBook, PaperFill, PaperLedger
from pmcopy.services.trading.order_sizing import size_buy_order
from pmcopy.services.risk.risk_engine import (
    PortfolioSnapshot,
    ProposedTrade,
    RiskEngine,
    RiskPolicy,
    headroom_for_buy,
)


class PaperLedgerRepository(Protocol):
    """Atomic paper inventory and economic-event deduplication."""

    async def load(self, scope: AutoTraderScope, wallet: str) -> PaperLedger | None:
        """Read a virtual portfolio."""
        ...

    async def claim_source(
        self, scope: AutoTraderScope, wallet: str, event: str, *, source: str, now: datetime
    ) -> bool:
        """Select one feed per transaction while allowing its distinct fill events."""
        ...

    async def seen(self, scope: AutoTraderScope, wallet: str, event: str) -> bool:
        """Read an economic-event receipt."""
        ...

    async def save(
        self, scope: AutoTraderScope, wallet: str, ledger: PaperLedger, *, event: str, now: datetime
    ) -> bool:
        """Compare-and-swap inventory with a dedup receipt."""
        ...


class PaperMarketReader(Protocol):
    """Fresh read-only market snapshots."""

    async def book(self, token_id: str, condition_id: str) -> PaperBook:
        """Read validated depth and fee metadata."""
        ...


class PaperExecutionPort(Protocol):
    """Paper writes, structurally isolated from private exchange methods."""

    async def fill(
        self,
        book: PaperBook,
        *,
        token_id: str,
        side: str,
        size: Decimal,
        limit: Decimal,
        event: str,
        now: datetime,
    ) -> PaperFill:
        """Simulate an explicitly limited FAK order."""
        ...


@dataclass(frozen=True, slots=True)
class PaperCopyOrder:
    """A normalized copy signal with explicit sizing and isolated seed funds."""

    scope: AutoTraderScope
    wallet: str
    token: str
    condition: str
    side: str
    desired_notional: Decimal
    leader_price: Decimal
    sell_fraction: Decimal
    event: str
    allocation: Decimal
    drift_pct: Decimal = Decimal("2")
    trader_score: Decimal = Decimal("1")
    event_ts: int | None = None
    leader_notional: Decimal = Decimal("0")
    transaction_event: str = ""
    observation_source: str = "polling"


class PaperCopyEvaluator:
    """Apply existing sizing and account risk rules to durable virtual portfolios."""

    def __init__(
        self,
        *,
        repository: PaperLedgerRepository,
        market: PaperMarketReader,
        execution: PaperExecutionPort,
        risk: RiskEngine,
        policy: RiskPolicy,
        clock: Callable[[], datetime],
        config: AppConfig | None = None,
    ) -> None:
        """Inject all I/O and the same risk policy used by the follower."""
        self.repository = repository
        self._market = market
        self._execution = execution
        self._risk = risk
        self._policy = policy
        self._clock = clock
        self._entry_policy = (
            PaperEntryPolicy(config=config, clock=clock) if config is not None else None
        )

    async def evaluate(self, order: PaperCopyOrder) -> PaperLedger:
        """Deduplicate before public reads and commit a risk-bounded simulated fill."""
        now = self._clock()
        ledger = await self.repository.load(order.scope, order.wallet)
        if ledger is None:
            ledger = PaperLedger.start(order.allocation, now)
            await self.repository.save(order.scope, order.wallet, ledger, event="seed", now=now)
            ledger = await self.repository.load(order.scope, order.wallet)
            if ledger is None:
                raise RuntimeError("paper portfolio initialization failed")
        if await self.repository.seen(order.scope, order.wallet, order.event):
            return ledger
        if order.transaction_event and not await self.repository.claim_source(
            order.scope,
            order.wallet,
            order.transaction_event,
            source=order.observation_source,
            now=now,
        ):
            return ledger
        book = await self._market.book(order.token, order.condition)
        ledger = await self.mark(ledger)
        if order.side == "BUY":
            fill = await self._buy(order, ledger, book, now)
        elif order.side == "SELL":
            position = next((p for p in ledger.positions if p.token == order.token), None)
            if position is None:
                fill = PaperFill()
            else:
                if not order.sell_fraction.is_finite() or not 0 < order.sell_fraction <= 1:
                    raise ValueError("paper sell requires an authoritative position fraction")
                floor = order.leader_price * (1 - order.drift_pct / 100)
                floor = max(
                    book.tick,
                    (floor / book.tick).to_integral_value(rounding=ROUND_CEILING) * book.tick,
                )
                fill = await self._execution.fill(
                    book,
                    token_id=order.token,
                    side="SELL",
                    size=position.shares * order.sell_fraction,
                    limit=floor,
                    event=order.event,
                    now=now,
                )
        else:
            raise ValueError("unsupported paper side")
        updated = ledger.apply(
            token=order.token,
            condition=order.condition,
            side=order.side,
            fill=fill,
            event=order.event,
        )
        updated = await self.mark(updated)
        if not await self.repository.save(
            order.scope, order.wallet, updated, event=order.event, now=now
        ):
            raise RuntimeError("paper portfolio changed concurrently")
        return updated

    async def mark(self, ledger: PaperLedger) -> PaperLedger:
        """Value complete inventory at depth-adjusted net liquidation proceeds."""
        prices: dict[str, Decimal] = {}
        for position in ledger.positions:
            book = await self._market.book(position.token, position.condition)
            remaining = position.shares
            value = Decimal(0)
            for price, size in sorted(book.bids, reverse=True):
                taken = min(size, remaining)
                value += taken * price * (1 - book.fee_rate * (1 - price))
                remaining -= taken
                if remaining == 0:
                    prices[position.token] = value / position.shares
                    break
        return ledger.mark(prices)

    async def _buy(
        self, order: PaperCopyOrder, ledger: PaperLedger, book: PaperBook, now: datetime
    ) -> PaperFill:
        if not ledger.valuation_complete or not ledger.evidence_complete or not book.asks:
            return PaperFill()
        cap = min(
            ledger.cash,
            self._policy.max_trade_usdc,
            ledger.equity * self._policy.max_portfolio_pct_per_trade,
        )
        desired = order.desired_notional
        if self._entry_policy is not None:
            admission = self._entry_policy.admit(order, ledger, book)
            if admission is None:
                return PaperFill()
            desired, configured_cap = admission
            cap = min(cap, configured_cap)
        sized = size_buy_order(
            live_price=min(p for p, _ in book.asks),
            base_notional_usdc=desired,
            max_notional_usdc=cap,
            market_min_size=book.minimum,
            tick_size=book.tick,
            slippage_ratio=order.drift_pct / 100,
        )
        if not sized.accept:
            return PaperFill()
        limit = min(sized.effective_price, order.leader_price * (1 + order.drift_pct / 100))
        limit = (limit / book.tick).to_integral_value(rounding=ROUND_FLOOR) * book.tick
        if not 0 < limit < 1:
            return PaperFill()
        amount = min(cap, sized.worst_notional)
        exposures: dict[str, Decimal] = {}
        for position in ledger.positions:
            exposures[position.condition] = (
                exposures.get(position.condition, Decimal(0)) + position.gross_cost
            )
        total = sum(exposures.values(), Decimal(0))
        portfolio = PortfolioSnapshot(
            ledger.equity,
            ledger.max_drawdown * 100,
            len(ledger.positions),
            exposures,
            {"unknown": total},
            {order.wallet: total},
        )
        decision = self._risk.evaluate(
            portfolio,
            self._policy,
            ProposedTrade(
                order.scope.follower_account,
                order.wallet,
                order.condition,
                "unknown",
                order.token,
                "BUY",
                amount,
            ),
        )
        if not decision.allow:
            return PaperFill()
        return await self._execution.fill(
            book,
            token_id=order.token,
            side="BUY",
            size=amount,
            limit=limit,
            event=order.event,
            now=now,
        )


class PaperEntryPolicy:
    """Reuse the follower's exact cash limits and optional trade-quality policy."""

    def __init__(self, *, config: AppConfig, clock: Callable[[], datetime]) -> None:
        """Inject immutable policy and a deterministic evaluation clock."""
        from pmcopy.services.trading.leader_scoring_service import LeaderScoringService
        from pmcopy.services.trading.trade_scoring_service import TradeScoringService

        self._config = config
        self._scorer = TradeScoringService(config=config, clock=lambda: clock().timestamp())
        self._leader = LeaderScoringService(config=config)

    def admit(
        self, order: PaperCopyOrder, ledger: PaperLedger, book: PaperBook
    ) -> tuple[Decimal, Decimal] | None:
        """Return exact target and cash cap, or reject the same entry-quality failures."""
        total = sum((position.gross_cost for position in ledger.positions), Decimal(0))
        market = sum(
            (
                position.gross_cost
                for position in ledger.positions
                if position.condition == order.condition
            ),
            Decimal(0),
        )
        headroom = headroom_for_buy(
            equity_usdc=ledger.equity,
            free_cash_usdc=ledger.cash,
            open_positions_count=len(ledger.positions),
            market_exposure_usdc=market,
            category_exposure_usdc=total,
            leader_exposure_usdc=total,
            daily_loss_usdc=max(Decimal(0), ledger.peak_equity - ledger.equity),
            day_start_equity_usdc=ledger.peak_equity,
            config=self._config,
        )
        if not headroom.allow:
            return None
        amount = order.desired_notional
        if self._config.execution.enable_copy_decision_gate:
            score = self._scorer.score_buy_trade(
                {
                    "price": order.leader_price,
                    "timestamp": order.event_ts,
                    "usdcSize": order.leader_notional,
                },
                live_price=min(price for price, _ in book.asks),
                leader_score=float(order.trader_score),
            )
            if (
                score.hard_fail_reason
                or score.score < self._config.copy_trading.copy_decision_min_score
            ):
                return None
            amount *= Decimal(str(self._leader.trust_multiplier(float(order.trader_score))))
        return amount, headroom.cap_notional_usdc
