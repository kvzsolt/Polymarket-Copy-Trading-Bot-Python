#!/usr/bin/env python3
"""Type and validator introspection used to build the API contract snapshot.

Several questions have to be answered by reading the real contract classes
rather than by hand: what wire type each dataclass field carries, which fields
a frozen view actually validates as a decimal string, what members sit behind a
string-typed field's declared vocabulary, and which event type a payload
classifies as. Every answer is derived here so
``scripts/export_api_contract.py`` stays an assembler and
``scripts/api_contract_tables.py`` stays pure declaration.
"""

from __future__ import annotations

import ast
import dataclasses
import importlib
import inspect
import textwrap
import types
import typing
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from enum import Enum
from typing import Any, cast

DECIMAL_VALIDATORS = frozenset({"require_decimal_string", "optional_decimal_string"})
"""``ContractValidation`` helpers that mark a field as a decimal string."""


class TypeRenderer:
    """Render a resolved type annotation as one stable contract type name."""

    _PRIMITIVES: Mapping[object, str] = {
        str: "string",
        bool: "boolean",
        int: "integer",
        float: "number",
        datetime: "timestamp",
    }

    def __init__(self, *, union_aliases: Mapping[frozenset[str], str] | None = None) -> None:
        """Store optional aliases for known unions.

        Args:
            union_aliases: Map from the frozen set of member names onto the
                name the contract should publish instead of a long disjunction.
        """

        self._union_aliases = dict(union_aliases or {})

    def render(self, annotation: object) -> tuple[str, bool]:
        """Return the contract type name and whether ``null`` is allowed.

        Args:
            annotation: Resolved annotation from ``typing.get_type_hints``.

        Returns:
            A ``(type_name, nullable)`` pair.
        """

        origin = typing.get_origin(annotation)
        if origin in (types.UnionType, typing.Union):
            members = list(typing.get_args(annotation))
            nullable = type(None) in members
            present = [member for member in members if member is not type(None)]
            if not present:
                return "null", True
            alias = self._union_aliases.get(frozenset(self._name(item) for item in present))
            if alias is not None:
                return alias, nullable
            return " | ".join(self._name(member) for member in present), nullable
        return self._name(annotation), False

    def _name(self, annotation: object) -> str:
        origin = typing.get_origin(annotation)
        if origin in (tuple, list, Sequence, Iterable):
            args = [arg for arg in typing.get_args(annotation) if arg is not Ellipsis]
            if not args:
                return "array"
            return f"array<{' | '.join(self._name(arg) for arg in args)}>"
        if origin in (dict, Mapping):
            return "object"
        primitive = self._PRIMITIVES.get(annotation)
        if primitive is not None:
            return primitive
        if isinstance(annotation, typing.TypeVar):
            return annotation.__name__
        if isinstance(annotation, type):
            if issubclass(annotation, Mapping):
                return "object"
            return annotation.__name__
        return str(annotation)


class DecimalFieldExtractor:
    """Recover the decimal-string fields a frozen view really validates.

    Every money, price, share, or ratio field is guarded in ``__post_init__``
    by ``ContractValidation.require_decimal_string`` or its optional variant,
    either directly or through a ``for value in (self.a, self.b)`` loop.
    Reading that back from the AST turns :data:`DECIMAL_FIELDS` from a hopeful
    list into a checked one.
    """

    def fields_for(self, view: type) -> tuple[str, ...]:
        """Return the validated decimal field names in declaration order.

        Args:
            view: Frozen dataclass to inspect.

        Returns:
            Field names, ordered as the dataclass declares them.
        """

        post_init = view.__dict__.get("__post_init__")
        if post_init is None:
            return ()
        tree = ast.parse(textwrap.dedent(inspect.getsource(post_init)))
        found: set[str] = set()
        self._walk(tree, {}, found)
        declared = [item.name for item in dataclasses.fields(cast(Any, view))]
        return tuple(name for name in declared if name in found)

    def _walk(self, node: ast.AST, aliases: Mapping[str, tuple[str, ...]], found: set[str]) -> None:
        if isinstance(node, ast.For):
            scoped = dict(aliases)
            if isinstance(node.target, ast.Name):
                scoped[node.target.id] = self._self_attributes(node.iter)
            for child in [*node.body, *node.orelse]:
                self._walk(child, scoped, found)
            return
        if isinstance(node, ast.Call):
            self._collect_call(node, aliases, found)
        for child in ast.iter_child_nodes(node):
            self._walk(child, aliases, found)

    @staticmethod
    def _collect_call(
        node: ast.Call,
        aliases: Mapping[str, tuple[str, ...]],
        found: set[str],
    ) -> None:
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else None
        if name not in DECIMAL_VALIDATORS or not node.args:
            return
        argument = node.args[0]
        if isinstance(argument, ast.Attribute) and _is_self(argument.value):
            found.add(argument.attr)
        elif isinstance(argument, ast.Name):
            found.update(aliases.get(argument.id, ()))

    @staticmethod
    def _self_attributes(node: ast.expr) -> tuple[str, ...]:
        if not isinstance(node, (ast.Tuple, ast.List)):
            return ()
        return tuple(
            element.attr
            for element in node.elts
            if isinstance(element, ast.Attribute) and _is_self(element.value)
        )


def _is_self(node: ast.expr) -> bool:
    return isinstance(node, ast.Name) and node.id == "self"


def resolve_vocabulary(target: str) -> tuple[str, ...]:
    """Resolve a ``module:symbol`` vocabulary into its ordered string members.

    Args:
        target: Dotted module path and symbol name separated by ``:``.

    Returns:
        The enum member values, or the literal tuple's strings, in order.

    Raises:
        ValueError: When the target is malformed or is not a supported kind.
    """

    module_name, separator, symbol_name = target.partition(":")
    if not separator or not module_name or not symbol_name:
        raise ValueError(f"vocabulary target must be 'module:symbol', got {target!r}")
    symbol = getattr(importlib.import_module(module_name), symbol_name)
    if isinstance(symbol, type) and issubclass(symbol, Enum):
        return tuple(str(member.value) for member in symbol)
    if isinstance(symbol, (tuple, list)) and all(isinstance(item, str) for item in symbol):
        return tuple(symbol)
    raise ValueError(f"vocabulary target {target!r} is neither an Enum nor a tuple of strings")


def summary_line(owner: type) -> str:
    """Return the first line of a class docstring, or an empty string."""

    doc = inspect.getdoc(owner) or ""
    return doc.splitlines()[0].strip() if doc else ""


def outcome_values(payload: type) -> tuple[Enum, ...] | None:
    """Return the ``outcome`` enum members a payload can carry, if any."""

    if "outcome" not in getattr(payload, "__dataclass_fields__", {}):
        return None
    annotation = typing.get_type_hints(payload).get("outcome")
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return tuple(annotation)
    return None


def blank_payload(
    payload: type,
    *,
    outcome: Enum | None = None,
    placeholders: Mapping[str, object] | None = None,
) -> object:
    """Build one throwaway payload instance for event-type classification.

    Args:
        payload: Frozen payload dataclass to instantiate.
        outcome: Outcome member to pin, when the payload carries one.
        placeholders: Per-field overrides for fields whose validator
            rejects the generic placeholder.

    Returns:
        One instance suitable only for classification.
    """

    hints = typing.get_type_hints(payload)
    kwargs: dict[str, Any] = {}
    for item in dataclasses.fields(cast(Any, payload)):
        if not item.init or item.default is not dataclasses.MISSING:
            continue
        if item.name == "outcome" and outcome is not None:
            kwargs["outcome"] = outcome
            continue
        kwargs[item.name] = placeholder_value(item.name, hints[item.name], placeholders)
    return payload(**kwargs)


def placeholder_value(
    name: str,
    annotation: object,
    placeholders: Mapping[str, object] | None = None,
) -> Any:
    """Return a contract-valid placeholder value for one payload field.

    Args:
        name: Field name being filled.
        annotation: Resolved field annotation.
        placeholders: Per-field overrides consulted before the defaults.

    Returns:
        A value the payload's own validator accepts.
    """

    override = (placeholders or {}).get(name)
    if override is not None:
        return override
    if isinstance(annotation, type):
        if issubclass(annotation, Enum):
            return next(iter(annotation))
        if issubclass(annotation, bool):
            return False
        if issubclass(annotation, int):
            return 0
    return "placeholder"


__all__ = [
    "DECIMAL_VALIDATORS",
    "DecimalFieldExtractor",
    "TypeRenderer",
    "blank_payload",
    "outcome_values",
    "placeholder_value",
    "resolve_vocabulary",
    "summary_line",
]
