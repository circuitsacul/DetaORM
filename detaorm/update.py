from __future__ import annotations

from dataclasses import dataclass

__all___ = (
    "Updaate",
    "SetUpdate",
    "IncrementUpdate",
    "AppendUpdate",
    "PrependUpdate",
    "DeleteUpdate",
)


class Update:
    field: str
    payload: object


@dataclass
class SetUpdate:
    field = "set"
    payload: dict[str, object]


@dataclass
class IncrementUpdate:
    field = "increment"
    payload: dict[str, int]


@dataclass
class AppendUpdate:
    field = "append"
    payload: dict[str, list[object]]


@dataclass
class PrependUpdate:
    field = "prepend"
    payload: dict[str, list[object]]


@dataclass
class DeleteUpdate:
    field = "delete"
    payload: list[str]
