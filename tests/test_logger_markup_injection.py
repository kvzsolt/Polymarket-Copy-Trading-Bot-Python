"""P1-l regression: leader-controlled market metadata (title / submarket /
outcome / market) is rendered into Rich console panels. Without escaping, an
attacker-named market injects console styling and clickable ``[link=…]``
phishing into the operator terminal. Escape untrusted fields at the console
boundary (the JSONL file sink is json.dumps-safe and stays raw).

Test mechanism: a recording Console interprets Rich markup. If a field is
escaped, its literal ``[red]``/``[link=…]`` tokens survive in ``export_text``;
if it is interpreted as markup, the tokens are consumed and vanish.
"""

import io

import rich.console

import pmcopy.utils.logger as logger_mod


def _recording_console(monkeypatch):
    rec = rich.console.Console(file=io.StringIO(), record=True, width=300)
    monkeypatch.setattr(logger_mod, "console", rec)
    return rec


def test_new_trade_panel_escapes_leader_markup(monkeypatch):
    rec = _recording_console(monkeypatch)
    logger_mod.print_new_trade_detected(
        {
            "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
            "side": "BUY",
            "title": "[red]EVIL[/red]",
            "outcome": "[link=http://e]C[/link]",
            "usdcSize": 10,
            "price": 0.5,
        }
    )
    out = rec.export_text()
    assert "[red]" in out, "leader title markup must be escaped, not interpreted"
    assert "[link=http://e]" in out, "leader-injected link markup must be escaped"


def test_manual_trade_panel_escapes_leader_markup(monkeypatch):
    rec = _recording_console(monkeypatch)
    logger_mod.print_manual_trade_detected(
        {
            "side": "SELL",
            "size": 1.0,
            "price": 0.4,
            "usdcSize": 5,
            "market": "[blink]X[/blink]",
            "outcome": "[red]NO[/red]",
        }
    )
    out = rec.export_text()
    assert "[blink]" in out, "leader market markup must be escaped"
    assert "[red]" in out, "leader outcome markup must be escaped"
