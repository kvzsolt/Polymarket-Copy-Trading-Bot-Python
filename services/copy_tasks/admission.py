"""Pure copy-task admission policy.

The engine executes before market-data fetches, sizing, risk mutation,
reservations, and venue submission whenever its decision can be made from the
source event and persisted task snapshot. It performs no I/O.
"""

from __future__ import annotations

from services.copy_tasks.domain import (
    CopyTask,
    CopyTaskStatus,
    TaskAdmissionContext,
    TaskAdmissionDecision,
)


class CopyTaskAdmissionEngine:
    """Evaluate Kreo-compatible task gates without side effects."""

    def evaluate(
        self,
        task: CopyTask,
        context: TaskAdmissionContext,
        *,
        include_spend: bool = True,
    ) -> TaskAdmissionDecision:
        """Return an explicit allow/skip decision for one task and event.

        Args:
            task: Immutable task policy.
            context: Current source-event and persisted task snapshot.
            include_spend: Whether to evaluate amount-dependent spend gates.
                BUY orchestration performs a first pass with this disabled,
                then sizes the trade and performs a second pass with the exact
                follower notional. Direct callers retain the legacy single-pass
                behavior by default.

        Returns:
            Explicit allow or skip decision.
        """

        if task.status is CopyTaskStatus.DELETED:
            return TaskAdmissionDecision(False, "task_deleted")
        if task.status is CopyTaskStatus.PAUSED:
            return TaskAdmissionDecision(False, "task_paused")
        if not task.enabled:
            return TaskAdmissionDecision(False, "task_disabled")

        is_buy = context.action.upper() == "BUY"
        if is_buy and not task.schedule.admits_entry(context.now):
            return TaskAdmissionDecision(False, "outside_schedule")

        if task.trigger.enabled:
            if context.leader_trade_notional < task.trigger.min_notional:
                return TaskAdmissionDecision(False, "below_min_trigger")
            if (
                task.trigger.max_notional > 0
                and context.leader_trade_notional > task.trigger.max_notional
            ):
                return TaskAdmissionDecision(False, "above_max_trigger")

        if task.price.enabled and is_buy:
            if not task.price.min_price <= context.leader_price <= task.price.max_price:
                return TaskAdmissionDecision(False, "price_outside_band")

        market_filter = task.market_filter
        if market_filter.enabled and is_buy:
            title = context.market_title.casefold()
            category = context.category.casefold()
            if market_filter.include_keywords and not any(
                word in title for word in market_filter.include_keywords
            ):
                return TaskAdmissionDecision(False, "required_keyword_missing")
            if any(word in title for word in market_filter.exclude_keywords):
                return TaskAdmissionDecision(False, "excluded_keyword")
            if market_filter.categories and category not in market_filter.categories:
                return TaskAdmissionDecision(False, "category_filtered")
            if market_filter.tokens and context.token_symbol not in market_filter.tokens:
                return TaskAdmissionDecision(False, "token_filtered")
            if (
                market_filter.timeframes
                and context.market_timeframe not in market_filter.timeframes
            ):
                return TaskAdmissionDecision(False, "timeframe_filtered")
            if (
                market_filter.min_liquidity_usdc > 0
                and context.market_liquidity_usdc < market_filter.min_liquidity_usdc
            ):
                return TaskAdmissionDecision(False, "market_liquidity_below_minimum")
            if (
                market_filter.min_volume_24h_usdc > 0
                and context.market_volume_24h_usdc < market_filter.min_volume_24h_usdc
            ):
                return TaskAdmissionDecision(False, "market_volume_below_minimum")
            if (
                market_filter.max_days_until_close > 0
                and context.days_until_close > market_filter.max_days_until_close
            ):
                return TaskAdmissionDecision(False, "market_too_far_from_close")
            if context.market_is_expired and not context.market_is_resolved:
                if not market_filter.allow_expired_unresolved:
                    return TaskAdmissionDecision(False, "expired_unresolved_market")

        spend = task.spend
        if spend.enabled and is_buy and include_spend:
            amount = context.intended_notional
            if spend.max_total > 0 and context.task_total_spend + amount > spend.max_total:
                return TaskAdmissionDecision(False, "total_spend_cap")
            if spend.max_daily > 0 and context.task_daily_spend + amount > spend.max_daily:
                return TaskAdmissionDecision(False, "daily_spend_cap")
            if (
                spend.max_per_market > 0
                and context.task_market_spend + amount > spend.max_per_market
            ):
                return TaskAdmissionDecision(False, "market_spend_cap")
            if spend.one_entry_per_market and context.task_has_entered_market:
                return TaskAdmissionDecision(False, "task_one_entry_per_market")
            if spend.global_one_entry_per_market and context.global_market_has_entry:
                return TaskAdmissionDecision(False, "global_one_entry_per_market")
            if (
                spend.one_entry_per_sports_type
                and context.sports_market_type
                and context.task_has_entered_sports_type
            ):
                return TaskAdmissionDecision(False, "task_one_entry_per_sports_type")

        return TaskAdmissionDecision(True, "admitted")


__all__ = ["CopyTaskAdmissionEngine"]
