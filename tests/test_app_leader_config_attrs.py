"""Regression: app.py must only access attributes that exist on LeaderConfig.

The on-chain detection wiring called
``application.config.leader.leader_wallets()`` but ``LeaderConfig`` only
defines the ``wallets`` field (``leader_wallets`` is a method on the env
*parser*, not on the frozen config dataclass). Every startup with a
configured WSS endpoint raised AttributeError inside the guarded block and
the bot silently ran without on-chain detection:

    onchain_detection_start_failed | error=AttributeError

The AST scan below catches any attribute accessed on ``<...>.leader.<attr>``
in app.py that is not a real ``LeaderConfig`` field, so the whole class of
"config attribute typo swallowed by a broad except" bugs fails loudly in CI.
"""

from __future__ import annotations

import ast
import dataclasses
import pathlib

from config.app_config import LeaderConfig

_APP_PY = pathlib.Path(__file__).resolve().parent.parent / "app.py"


def test_app_leader_attribute_accesses_exist_on_leader_config() -> None:
    allowed = {f.name for f in dataclasses.fields(LeaderConfig)}
    tree = ast.parse(_APP_PY.read_text(encoding="utf-8"))
    bad = [
        (node.lineno, node.attr)
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "leader"
        and node.attr not in allowed
    ]
    assert not bad, (
        "app.py accesses attributes that do not exist on LeaderConfig "
        f"(line, attr): {bad}"
    )
