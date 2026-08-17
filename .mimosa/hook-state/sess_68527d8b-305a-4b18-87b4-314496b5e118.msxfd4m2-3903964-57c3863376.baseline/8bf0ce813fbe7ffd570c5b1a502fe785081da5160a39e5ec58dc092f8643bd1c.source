"""JSON serialization and stable HTTP error mapping for the control plane."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from http import HTTPStatus
from typing import TypeAlias

from aiohttp import web

from services.control_plane.contracts import ControlPlaneError, ControlPlaneReason

from .security import ControlPlaneTransportError


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]

_FORBIDDEN_RESPONSE_KEYS = frozenset(
    {
        "api_key",
        "authorization",
        "bearer_token",
        "credential",
        "credentials",
        "password",
        "private_key",
        "secret",
    }
)


class ControlPlaneJsonSerializer:
    """Serialize immutable DTOs without financial float conversion."""

    def __init__(
        self,
        *,
        max_depth: int = 20,
        max_collection_items: int = 10_000,
        max_string_chars: int = 65_536,
        max_total_string_chars: int = 1_000_000,
    ) -> None:
        if not 1 <= max_depth <= 64:
            raise ValueError("max_depth must be between 1 and 64")
        if not 1 <= max_collection_items <= 100_000:
            raise ValueError("max_collection_items must be between 1 and 100000")
        if not 1 <= max_string_chars <= 1_000_000:
            raise ValueError("max_string_chars must be between 1 and 1000000")
        if not 1 <= max_total_string_chars <= 10_000_000:
            raise ValueError("max_total_string_chars must be between 1 and 10000000")
        self._max_depth = max_depth
        self._max_collection_items = max_collection_items
        self._max_string_chars = max_string_chars
        self._max_total_string_chars = max_total_string_chars

    def to_jsonable(self, value: object) -> JsonValue:
        """Return a bounded JSON-safe deep copy of ``value``."""

        budget = [self._max_collection_items, self._max_total_string_chars]
        return self._convert(value, depth=0, budget=budget)

    def _convert(self, value: object, *, depth: int, budget: list[int]) -> JsonValue:
        if depth > self._max_depth:
            raise ValueError("response exceeds maximum serialization depth")
        if value is None or isinstance(value, (bool, int)):
            return value
        if isinstance(value, str):
            if len(value) > self._max_string_chars:
                raise ValueError("response string exceeds maximum length")
            self._consume_string(budget, len(value))
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("non-finite floats are not JSON-safe")
            return value
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ValueError("non-finite decimals are not JSON-safe")
            rendered = format(value, "f")
            if len(rendered) > self._max_string_chars:
                raise ValueError("response string exceeds maximum length")
            self._consume_string(budget, len(rendered))
            return rendered
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("response datetime must be timezone-aware")
            rendered = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            self._consume_string(budget, len(rendered))
            return rendered
        if isinstance(value, Enum):
            return self._convert(value.value, depth=depth + 1, budget=budget)
        if is_dataclass(value) and not isinstance(value, type):
            result: dict[str, JsonValue] = {}
            for item in fields(value):
                self._consume(budget)
                self._validate_key(item.name, budget=budget)
                result[item.name] = self._convert(
                    getattr(value, item.name), depth=depth + 1, budget=budget
                )
            return result
        if isinstance(value, Mapping):
            result = {}
            for raw_key, raw_value in value.items():
                self._consume(budget)
                if not isinstance(raw_key, str):
                    raise ValueError("JSON response mappings require string keys")
                self._validate_key(raw_key, budget=budget)
                result[raw_key] = self._convert(raw_value, depth=depth + 1, budget=budget)
            return result
        if isinstance(value, (tuple, list)):
            result_list: list[JsonValue] = []
            for item in value:
                self._consume(budget)
                result_list.append(self._convert(item, depth=depth + 1, budget=budget))
            return result_list
        raise ValueError(f"unsupported JSON response type: {type(value).__name__}")

    @staticmethod
    def _consume(budget: list[int]) -> None:
        budget[0] -= 1
        if budget[0] < 0:
            raise ValueError("response exceeds maximum collection items")

    @staticmethod
    def _consume_string(budget: list[int], length: int) -> None:
        budget[1] -= length
        if budget[1] < 0:
            raise ValueError("response exceeds maximum total string characters")

    def _validate_key(self, key: str, *, budget: list[int]) -> None:
        if len(key) > self._max_string_chars:
            raise ValueError("response key exceeds maximum length")
        self._consume_string(budget, len(key))
        normalized = key.casefold().replace("-", "_")
        if normalized in _FORBIDDEN_RESPONSE_KEYS:
            raise ValueError("response contains a forbidden sensitive field")


class ControlPlaneErrorMapper:
    """Map application and transport errors to stable JSON responses."""

    _BAD_REQUEST = frozenset(
        {
            ControlPlaneReason.INVALID_REQUEST,
            ControlPlaneReason.INVALID_PAGE,
            ControlPlaneReason.INVALID_DECIMAL_STRING,
            ControlPlaneReason.BULK_LIMIT_EXCEEDED,
            ControlPlaneReason.DUPLICATE_BULK_TARGET,
            ControlPlaneReason.SETTINGS_PATH_FORBIDDEN,
            ControlPlaneReason.SETTINGS_SYMLINK_FORBIDDEN,
            ControlPlaneReason.SETTINGS_INVALID,
            ControlPlaneReason.SETTINGS_SECRET_FORBIDDEN,
        }
    )
    _CONFLICT = frozenset(
        {
            ControlPlaneReason.CONFLICT,
            ControlPlaneReason.REVISION_MISMATCH,
            ControlPlaneReason.TASK_DELETED,
            ControlPlaneReason.BULK_FAILED,
            ControlPlaneReason.DISCOVERY_BUSY,
            ControlPlaneReason.FULL_STOP_ACTIVE,
        }
    )
    _UNAVAILABLE = frozenset(
        {
            ControlPlaneReason.PORTFOLIO_UNAVAILABLE,
            ControlPlaneReason.PORTFOLIO_STALE,
            ControlPlaneReason.DISCOVERY_UNAVAILABLE,
            ControlPlaneReason.RUNTIME_UNAVAILABLE,
            ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE,
            ControlPlaneReason.SETTINGS_UNAVAILABLE,
            ControlPlaneReason.AUDIT_UNAVAILABLE,
        }
    )

    def __init__(self, serializer: ControlPlaneJsonSerializer | None = None) -> None:
        self._serializer = serializer if serializer is not None else ControlPlaneJsonSerializer()

    def response(self, error: Exception, *, correlation_id: str) -> web.Response:
        """Return one sanitized JSON error envelope."""

        headers: dict[str, str] = {}
        if isinstance(error, ControlPlaneTransportError):
            code = error.code
            message = error.public_message
            status = error.status
            headers.update(error.headers)
        elif isinstance(error, ControlPlaneError):
            code = error.reason.value
            message = error.public_message
            status = self._status_for_reason(error.reason)
        elif isinstance(error, web.HTTPRequestEntityTooLarge):
            code = "payload_too_large"
            message = "The request payload is too large."
            status = HTTPStatus.REQUEST_ENTITY_TOO_LARGE
        elif isinstance(error, web.HTTPNotFound):
            code = "not_found"
            message = "The requested endpoint was not found."
            status = HTTPStatus.NOT_FOUND
        elif isinstance(error, web.HTTPMethodNotAllowed):
            code = "method_not_allowed"
            message = "The request method is not allowed."
            status = HTTPStatus.METHOD_NOT_ALLOWED
        elif isinstance(error, web.HTTPException):
            code = "http_error"
            message = "The HTTP request could not be completed."
            status = error.status
        else:
            code = ControlPlaneReason.INTERNAL_ERROR.value
            message = ControlPlaneReason.INTERNAL_ERROR.public_message
            status = HTTPStatus.INTERNAL_SERVER_ERROR
        body = {
            "error": {
                "code": code,
                "message": message,
                "correlation_id": correlation_id,
            }
        }
        return web.json_response(
            self._serializer.to_jsonable(body),
            status=int(status),
            headers=headers,
        )

    def _status_for_reason(self, reason: ControlPlaneReason) -> HTTPStatus:
        if reason in self._BAD_REQUEST:
            return HTTPStatus.BAD_REQUEST
        if reason is ControlPlaneReason.NOT_FOUND:
            return HTTPStatus.NOT_FOUND
        if reason in self._CONFLICT:
            return HTTPStatus.CONFLICT
        if reason in self._UNAVAILABLE:
            return HTTPStatus.SERVICE_UNAVAILABLE
        return HTTPStatus.INTERNAL_SERVER_ERROR


__all__ = [
    "ControlPlaneErrorMapper",
    "ControlPlaneJsonSerializer",
    "JsonScalar",
    "JsonValue",
]
