from __future__ import annotations

import typing as t

from detaorm import update
from detaorm.query import Query
from detaorm.undef import UNDEF, UndefOr

if t.TYPE_CHECKING:
    from detaorm.base import Base

__all__ = ("Field",)

_SELF = t.TypeVar("_SELF", bound="Field[t.Any]")
_TYPE = t.TypeVar("_TYPE")


class _Op:
    def __init__(self, op: str | None) -> None:
        self.op = f"?{op}" if op else ""

    def __get__(
        self, inst: object, cls: type[object]
    ) -> t.Callable[[object], Query]:
        def op_func(other: object) -> Query:
            assert isinstance(inst, Field)
            return Query(inst.qual_name + self.op, other)

        return op_func


class Field(t.Generic[_TYPE]):
    """Represent a single field on a Base."""

    name: str
    """The name of the field."""

    def __init__(self, default: UndefOr[_TYPE] = UNDEF) -> None:
        if default is not UNDEF:
            self._default = default

    @property
    def default(self) -> UndefOr[_TYPE]:
        return self._default if hasattr(self, "_default") else UNDEF

    @property
    def qual_name(self) -> str:
        """The qualified name of this field."""
        return self.__qual_name

    @qual_name.setter
    def qual_name(self, val: str) -> None:
        self.__qual_name = val

        for k, v in self.__class__.__dict__.items():
            if isinstance(v, Field):
                v.name = k
                v.qual_name = f"{self.qual_name}.{k}"

    def __init_subclass__(cls) -> None:
        for k, v in cls.__dict__.items():
            if isinstance(v, Field):
                v.name = k

    @t.overload
    def __get__(self: _SELF, inst: None, cls: t.Any) -> _SELF:
        ...

    @t.overload
    def __get__(self, inst: Base | NestedField, cls: t.Any) -> _TYPE:
        ...

    def __get__(
        self: _SELF, inst: Base | NestedField | None, cls: t.Any
    ) -> _SELF | _TYPE:
        if isinstance(inst, NestedField) and not isinstance(
            inst, NestedFieldInst
        ):
            return self

        if inst:
            return t.cast(_TYPE, inst.raw[self.name])
        return self

    # query ops
    eq = _Op(None)
    neq = _Op("ne")
    gt = _Op("gt")
    lt = _Op("lt")
    lte = _Op("lte")
    gte = _Op("gte")
    prefix = _Op("pfx")
    range = _Op("range")
    contains = _Op("contains")
    not_contains = _Op("not_contains")

    # updates
    def set(self, value: object) -> update.SetUpdate:
        return update.SetUpdate({self.qual_name: value})

    def increment(self, value: int) -> update.IncrementUpdate:
        return update.IncrementUpdate({self.qual_name: value})

    def append(self, values: list[object]) -> update.AppendUpdate:
        return update.AppendUpdate({self.qual_name: values})

    def prepend(self, values: list[object]) -> update.PrependUpdate:
        return update.PrependUpdate({self.qual_name: values})

    def delete(self) -> update.DeleteUpdate:
        return update.DeleteUpdate([self.qual_name])

    # magic
    __eq__ = eq  # type: ignore
    __ne__ = neq  # type: ignore
    __gt__ = gt
    __lt__ = lt
    __ge__ = gte
    __le__ = lte


class NestedField(Field[t.Dict[str, object]]):
    def __init__(self) -> None:
        super().__init__()

    @property
    def default(self) -> UndefOr[dict[str, object]]:
        if hasattr(self, "_default"):
            return self._default

        defaults: dict[str, object] = {}
        for k, v in type(self).__dict__.items():
            if not isinstance(v, Field):
                continue

            if v.default is not UNDEF:
                defaults[k] = v.default

        return defaults or UNDEF

    @property
    def raw(self) -> dict[str, object]:
        raise NotImplementedError

    def __get__(  # type: ignore
        self: _SELF, inst: None | Base | NestedField, cls: t.Any
    ) -> _SELF:
        if isinstance(inst, NestedField) and not isinstance(
            inst, NestedFieldInst
        ):
            return self

        if inst:
            return NestedFieldInst(self, inst)  # type: ignore
        return self


class NestedFieldInst:
    def __init__(self, field: NestedField, base: Base) -> None:
        self.field = field
        self.base = base

    @property
    def raw(self) -> dict[str, object]:
        return self.base.raw[self.field.name]  # type: ignore

    def __getattr__(self, key: str) -> object:
        return getattr(self.field, key).__get__(self, type(self))
