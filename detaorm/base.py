from __future__ import annotations

import typing as t

from detaorm.field import Field

if t.TYPE_CHECKING:
    from detaorm.client import Client, RawPutItemsResponse
    from detaorm.paginator import RawPage
    from detaorm.query import Node

__all__ = ("Base", "PutItemsResponse")

_BASE = t.TypeVar("_BASE", bound="Base")


class PutItemsResponse(t.Generic[_BASE]):
    def __init__(
        self, response: RawPutItemsResponse, model: type[_BASE]
    ) -> None:
        self.model = model
        self.raw = response

    @property
    def processed(self) -> list[_BASE]:
        return [self.model(**d) for d in self.raw.processed]

    @property
    def failed(self) -> list[_BASE]:
        return [self.model(**d) for d in self.raw.failed]


class Page(t.Generic[_BASE]):
    def __init__(self, raw: RawPage, model: type[_BASE]) -> None:
        self.model = model
        self.raw = raw

    @property
    def items(self) -> list[_BASE]:
        return [self.model(**d) for d in self.raw.items]

    async def next(self) -> Page[_BASE] | None:
        next = await self.raw.next()
        if next:
            return Page(next, self.model)
        return None


class Base:
    """A Deta "Base", or a single model.

    Args:
        client (Client): The DetaORM Client to use.
        **values (object): Key-value pairs for this model.
    """

    __base_name__: str
    """The name of this Base."""
    _client: Client

    key: Field[str] = Field()

    def __init_subclass__(cls) -> None:
        if not hasattr(cls, "__base_name__"):
            raise AttributeError(f"Base {cls} has no __base_name__ set.")

        for key, value in cls.__dict__.items():
            if isinstance(value, Field):
                value.name = key
                value.base_name = cls.__base_name__

    def __init__(self, **values: object) -> None:
        self.raw = values
        self._updates: dict[str, object] = {}

    # abstracted api methods
    @classmethod
    async def put_items(
        cls: type[_BASE], items: t.Sequence[_BASE]
    ) -> PutItemsResponse[_BASE]:
        return PutItemsResponse(
            await cls._client.put_items(
                cls.__base_name__, [i.raw for i in items]
            ),
            cls,
        )

    @classmethod
    async def get_item(cls: type[_BASE], key: str) -> _BASE:
        return cls(**await cls._client.get_item(cls.__base_name__, key))

    @classmethod
    async def delete_item(cls, key: str) -> None:
        await cls._client.delete_item(cls.__base_name__, key)

    @classmethod
    async def insert_item(cls: type[_BASE], item: _BASE) -> _BASE:
        return cls(
            **await cls._client.insert_item(cls.__base_name__, item.raw)
        )

    # TODO: update_item

    @classmethod
    async def query_items(
        cls: type[_BASE],
        query: Node | t.Sequence[t.Mapping[str, object]] | None = None,
        limit: int = 0,
        last: str | None = None,
    ) -> Page[_BASE]:
        return Page(
            await cls._client.query_items(
                cls.__base_name__, query=query, limit=limit, last=last
            ),
            cls,
        )

    # magic
    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"
