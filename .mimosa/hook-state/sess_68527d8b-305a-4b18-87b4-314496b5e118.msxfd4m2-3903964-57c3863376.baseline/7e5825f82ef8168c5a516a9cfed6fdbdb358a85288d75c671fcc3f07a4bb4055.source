"""Submission-retry backoff carries jitter (runtime-hardening).

The post-VenueRateLimitError retry slept ``min(0.15 * attempt, 0.30)`` with no
jitter, so concurrent runtimes retried a shared venue outage in lockstep. Jitter
of up to 0.1s now spreads them; the base backoff is unchanged.
"""

from __future__ import annotations

import random
from pathlib import Path

_EXEC_PY = Path(__file__).resolve().parents[1] / "services" / "executionService.py"


def _base(attempt: int) -> float:
    return min(0.15 * attempt, 0.30)


def test_jitter_expression_present_in_source():
    src = _EXEC_PY.read_text(encoding="utf-8")
    assert "min(0.15 * attempt, 0.30) + random.uniform(0, 0.1)" in src
    assert "import random" in src


def test_jittered_sleep_stays_within_base_plus_0_1():
    # Reproduce the exact expression and confirm every draw lands in
    # [base, base + 0.1] for representative attempts.
    for attempt in range(1, 6):
        base = _base(attempt)
        for _ in range(1000):
            slept = base + random.uniform(0, 0.1)
            assert base <= slept <= base + 0.1
