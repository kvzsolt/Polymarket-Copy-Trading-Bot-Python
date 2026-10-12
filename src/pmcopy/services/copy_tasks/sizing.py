"""Per-task adapter over the shared exact-decimal V5 sizing engine.

The service resolves immutable per-task sizing policies into cached pure
CopySizingEngine instances. It owns no I/O and performs no balance hydration;
the caller supplies a fully populated CopySizingRequest.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace

from pmcopy.services.copy_sizing_v5.domain import CopySizingDecision, CopySizingRequest
from pmcopy.services.copy_sizing_v5.engine import CopySizingEngine
from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy
from pmcopy.services.copy_tasks.domain import CopyTask, TaskSizingMode, TaskSizingPolicy


class CopyTaskSizingService:
    """Resolve immutable per-task sizing into cached pure engines.

    Attributes:
        base_policy: Shared base policy providing global caps and defaults.
        max_cached_policies: Hard bound on the number of distinct task policies
            cached as engines to bound memory across many tasks.
    """

    def __init__(
        self,
        *,
        base_policy: CopySizingPolicy,
        max_cached_policies: int = 1_000,
    ) -> None:
        if max_cached_policies < 1:
            raise ValueError("max_cached_policies must be positive")
        self._base_policy = base_policy
        self._max_cached_policies = int(max_cached_policies)
        self._engines: OrderedDict[TaskSizingPolicy, CopySizingEngine] = OrderedDict()

    def size(
        self,
        task: CopyTask,
        request: CopySizingRequest,
    ) -> CopySizingDecision:
        """Return one task-specific BUY sizing decision.

        Args:
            task: Immutable task whose sizing policy controls the decision.
            request: Fully populated sizing request.

        Returns:
            Immutable sizing decision from the task-specific engine.
        """

        engine = self._engine(task.sizing)
        if not task.sizing.enabled:
            return engine.reject(request, "task_sizing_disabled")
        return engine.size(request)

    def _engine(self, task_policy: TaskSizingPolicy) -> CopySizingEngine:
        cached = self._engines.get(task_policy)
        if cached is not None:
            self._engines.move_to_end(task_policy)
            return cached
        policy = self._policy(task_policy)
        engine = CopySizingEngine(policy)
        self._engines[task_policy] = engine
        while len(self._engines) > self._max_cached_policies:
            self._engines.popitem(last=False)
        return engine

    def _policy(self, task: TaskSizingPolicy) -> CopySizingPolicy:
        """Translate one task policy into the shared engine policy.

        Kreo-style fixed amount is already explicit, so calculated-mode
        minimum/maximum bounds do not alter it. Every account/risk cap still
        applies in the shared engine.
        """

        fixed = task.mode is TaskSizingMode.FIXED
        return replace(
            self._base_policy,
            mode=task.mode.value,
            percentage=task.percentage,
            fixed_amount=task.fixed_amount,
            trade_multiplier=task.trade_multiplier,
            buy_min_amount=(self._base_policy.buy_min_amount * 0 if fixed else task.buy_min_amount),
            buy_max_amount=(self._base_policy.buy_max_amount * 0 if fixed else task.buy_max_amount),
            copyable_balance_percent=task.copyable_balance_percent,
            proportional_balance_cap_enabled=task.proportional_balance_cap_enabled,
            require_leader_balance_for_proportional=task.require_leader_balance,
            portfolio_weight_multiplier=task.weighted_multiplier,
            strict_fixed_amount=task.strict_fixed_amount,
            strict_exact_mirror=task.strict_exact_mirror,
        )


__all__ = ["CopyTaskSizingService"]
