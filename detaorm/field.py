from __future__ import annotations

import typing as t

from detaorm.query import Query

if t.TYPE_CHECKING:
    from detaorm.base import Base

__all__ = ("Field",)

_SELF = t.TypeVar("_SELF", bound="Field[object]")
_TYPE = t.TypeVar("_TYPE")


class _Op:
    def __init__(self, op: str | None) -> None:
        self.op = f"?{op}" if op else ""

    def __get__(
        self, inst: object, cls: type[object]
    ) -> t.Callable[[object], Query]:
        def op_func(other: object) -> Query:
            assert isinstance(inst, Field)
            return Query(inst.qualified_name + self.op, other)

        return op_func


class Field(t.Generic[_TYPE]):
    """Represent a single field on a Base."""

    name: str
    """The name of the field."""
    base_name: str
    """The name of the Base this field belongs to."""

    @t.overload
    def __get__(self: _SELF, inst: None, cls: type[Base]) -> _SELF:
        ...

    @t.overload
    def __get__(self: _SELF, inst: Base, cls: type[Base]) -> _TYPE:
        ...

    def __get__(
        self: _SELF, inst: Base | None, cls: type[Base]
    ) -> _SELF | _TYPE:
        if inst:
            return t.cast(_TYPE, inst.raw[self.name])
        return self

    @property
    def qualified_name(self) -> str:
        """The fully qualified name of this field."""

        return f"{self.base_name}.{self.name}"

    eq = _Op(None)
    neq = _Op("ne")
    gt = _Op("gt")
    lt = _Op("lt")
    lte = _Op("lte")
    gte = _Op("gte")
    prefx = _Op("pfx")
    range = _Op("range")
    contains = _Op("contains")
    not_contains = _Op("not_contains")
