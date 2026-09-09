"""Accounts without a monitored leader still bind their restored task protection."""

from types import SimpleNamespace

import app


def test_task_composition_includes_risk_only_context_and_deduplicates_shared_context():
    regular = SimpleNamespace(user_id="a")
    risk_only = SimpleNamespace(user_id="b")
    contexts = app._copy_task_contexts({"leader": [regular]}, {"a": regular, "b": risk_only})
    assert len(contexts) == 2
    assert regular in contexts
    assert risk_only in contexts
