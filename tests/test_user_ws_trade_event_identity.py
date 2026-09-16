"""P1-r: WSS user-channel ``trade`` events carry a stable ``id`` (trade UUID)
and never a ``tx_hash``; the REST ``getTrades`` Trade object carries
``transaction_hash`` (snake_case).

Verified against Polymarket docs (Context7 ``/llmstxt/polymarket_llms-full_txt``,
User Channel trade message): fields are ``id``, ``asset_id``, ``taker_order_id``,
``maker_orders[].order_id``, ``status`` — no ``tx_hash``/``transactionHash``.

The lifecycle emitter previously (a) only checked the camelCase
``transactionHash`` so a REST trade's ``transaction_hash`` was dropped, and
(b) never surfaced the trade ``id``. This pins the corrected extraction. The
emitter is logging-only; settlement remains reconciler-owned.
"""

import clients.user_ws as user_ws
import utils.logger as logger_mod


def _capture_lifecycle(monkeypatch):
    captured: dict = {}

    def _fake(_logger, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(logger_mod, "log_order_lifecycle", _fake)
    return captured


def test_wss_trade_event_surfaces_trade_id_and_has_no_tx_hash(monkeypatch):
    captured = _capture_lifecycle(monkeypatch)

    # Context7 venue-shaped WSS user 'trade' message (no tx hash anywhere).
    event = {
        "asset_id": "52114319501245915516055106046884209969926127482827954674443846427813813222426",
        "event_type": "trade",
        "id": "28c4d2eb-bbea-40e7-a9f0-b2fdb56b2c2e",
        "market": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
        "outcome": "YES",
        "price": "0.57",
        "side": "BUY",
        "size": "10",
        "status": "MATCHED",
        "taker_order_id": "0x06bc63e346ed4ceddce9efd6b3af37c8f8f440c92fe7da6b2d0f9e4ccbc50c42",
        "type": "TRADE",
    }

    user_ws._emit_lifecycle_for_event(event, token_id=event["asset_id"])

    assert captured, "lifecycle log should have been emitted for a MATCHED trade"
    assert captured.get("tx_hash") is None, "WSS trade events carry no tx hash"
    assert captured.get("order_id") == event["taker_order_id"]
    assert (captured.get("meta") or {}).get("trade_id") == event["id"]


def test_rest_trade_transaction_hash_snake_case_is_captured(monkeypatch):
    captured = _capture_lifecycle(monkeypatch)

    event = {
        "status": "CONFIRMED",
        "side": "SELL",
        "id": "trade-1",
        "asset_id": "a",
        "transaction_hash": "0xdeadbeefcafe",  # REST getTrades field name
    }

    user_ws._emit_lifecycle_for_event(event, token_id="a")

    assert captured.get("tx_hash") == "0xdeadbeefcafe"
