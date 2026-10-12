"""Resolve Polymarket condition IDs for CLOB token IDs."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode

from pmcopy.utils.fetchData import fetch_data
from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("OnchainConditionIdResolver")


class GammaConditionIdResolver:
    """Best-effort token_id -> condition_id resolver backed by Gamma markets."""

    def __init__(
        self,
        *,
        base_url: str = "https://gamma-api.polymarket.com/markets",
    ) -> None:
        self._base_url = base_url.rstrip("?")
        self._condition_by_token: dict[str, str] = {}

    async def resolve(self, token_id: str) -> str:
        token = str(token_id or "").strip()
        if not token:
            return ""
        cached = self._condition_by_token.get(token)
        if cached is not None:
            return cached

        query = urlencode((("clob_token_ids", token), ("limit", "1")))
        url = f"{self._base_url}?{query}"
        try:
            payload = await fetch_data(url, silent=True)
        except Exception as exc:
            logger.debug(
                "gamma_condition_id_lookup_failed | token_id=%s error=%s",
                token,
                redact_error_detail(exc),
            )
            return ""

        market = _first_market(payload)
        if not market:
            return ""
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "").strip()
        if not condition_id:
            return ""

        token_ids = _parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids") or market.get("tokens")
        )
        if not token_ids:
            token_ids = [token]
        for market_token in token_ids:
            self._condition_by_token[str(market_token)] = condition_id
        return condition_id


def _first_market(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, list):
        return payload[0] if payload and isinstance(payload[0], dict) else None
    if not isinstance(payload, dict):
        return None
    if "conditionId" in payload or "condition_id" in payload:
        return payload
    for key in ("data", "markets", "results"):
        value = payload.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value[0]
    return None


def _parse_clob_token_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return [raw]
    if isinstance(value, list):
        tokens: list[str] = []
        for item in value:
            if isinstance(item, dict):
                token = item.get("token_id") or item.get("tokenID") or item.get("id")
            else:
                token = item
            if token:
                tokens.append(str(token))
        return tokens
    return []
