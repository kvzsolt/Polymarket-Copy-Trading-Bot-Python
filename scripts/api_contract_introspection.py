#!/usr/bin/env python3
"""Type and validator introspection used to build the API contract snapshot.

Two questions have to be answered by reading the real contract classes rather
than by hand: what wire type each dataclass field carries, and which fields a
frozen view actually validates as a decimal string. Both answers live here so
``scripts/export_api_contract.py`` stays an assembler and
``scripts/api_contract_tables.py`` stays pure declaration.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import textwrap
import types
import typing
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
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


__all__ = ["DECIMAL_VALIDATORS", "DecimalFieldExtractor", "TypeRenderer"]
