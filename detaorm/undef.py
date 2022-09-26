import typing as t
from enum import Enum

__all__ = ("Undef", "UNDEF", "UndefOr")


class Undef(Enum):
    UNDEF = "UNDEFINED"


UNDEF = Undef.UNDEF
_T = t.TypeVar("_T")
UndefOr = t.Union[Undef, _T]
