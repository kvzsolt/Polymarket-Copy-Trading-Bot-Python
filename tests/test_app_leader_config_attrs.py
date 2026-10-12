"""Regression: app.py must only access attributes that exist on CopyTradingConfig.

The on-chain detection wiring called
``application.config.leader.leader_wallets()`` but the leader section only
defines the ``wallets`` field (``leader_wallets`` is a method on the env
*parser*, not on the frozen config dataclass). Every startup with a
configured WSS endpoint raised AttributeError inside the guarded block and
the bot silently ran without on-chain detection:

    onchain_detection_start_failed | error=AttributeError

The AST scan below catches any attribute accessed on
``<...>.copy_trading.<attr>`` in app.py that is not a real
``CopyTradingConfig`` field/method, so the whole class of
"config attribute typo swallowed by a broad except" bugs fails loudly in CI.
"""

from __future__ import annotations

import ast
import pathlib

from pmcopy.config.app_config import CopyTradingConfig

_APP_PY = pathlib.Path(__file__).resolve().parent.parent / "src" / "pmcopy" / "app.py"


def test_app_copy_trading_attribute_accesses_exist_on_config() -> None:
    allowed = set(CopyTradingConfig.model_fields) | {
        "leader_override",
        "resolved_for_leader",
    }
    tree = ast.parse(_APP_PY.read_text(encoding="utf-8"))
    bad = [
        (node.lineno, node.attr)
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "copy_trading"
        and node.attr not in allowed
    ]
    assert not bad, (
        f"app.py accesses attributes that do not exist on CopyTradingConfig (line, attr): {bad}"
    )
