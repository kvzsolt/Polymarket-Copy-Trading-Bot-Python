"""Tests for services/observability/metrics.py"""

from __future__ import annotations

from services.observability.metrics import Metrics, get_metrics


class TestMetrics:
    def test_inc_and_get(self):
        m = Metrics()
        m.inc("test_counter")
        assert m.get("test_counter") == 1
        m.inc("test_counter", 5)
        assert m.get("test_counter") == 6

    def test_missing_counter_returns_zero(self):
        m = Metrics()
        assert m.get("nonexistent") == 0

    def test_gauge_set_and_get(self):
        m = Metrics()
        m.set_gauge("price", 0.65)
        assert m.get_gauge("price") == 0.65

    def test_missing_gauge_returns_zero(self):
        m = Metrics()
        assert m.get_gauge("missing") == 0.0

    def test_snapshot_returns_dicts(self):
        m = Metrics()
        m.inc("a", 3)
        m.set_gauge("b", 1.5)
        snap = m.snapshot()
        assert snap["counters"]["a"] == 3
        assert snap["gauges"]["b"] == 1.5

    def test_reset_clears_all(self):
        m = Metrics()
        m.inc("x", 10)
        m.set_gauge("y", 0.5)
        m.reset()
        assert m.get("x") == 0
        assert m.get_gauge("y") == 0.0

    def test_singleton_same_instance(self):
        a = get_metrics()
        b = get_metrics()
        assert a is b

    def test_thread_safe_increments(self):
        import threading
        m = Metrics()
        counter_name = "thread_safe_counter"
        threads = []
        for _ in range(10):
            t = threading.Thread(target=lambda: m.inc(counter_name, 1))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert m.get(counter_name) == 10
