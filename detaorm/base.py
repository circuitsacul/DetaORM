from __future__ import annotations

import typing as t

from detaorm.field import Field

if t.TYPE_CHECKING:
    from detaorm.client import Client, RawPutItemsResponse
    from detaorm.paginator import RawPage, RawPaginator
    from detaorm.query import Node
    from detaorm.update import Update

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


class Paginator(t.Generic[_BASE]):
    def __init__(self, raw: RawPaginator, model: type[_BASE]) -> None:
        self.model = model
        self.raw = raw

    def __await__(self) -> t.Generator[t.Any, None, Page[_BASE]]:
        async def await_page() -> Page[_BASE]:
            return Page(await self.raw, self.model)

        return await_page().__await__()

    def __aiter__(self) -> Paginator[_BASE]:
        return self

    async def __anext__(self) -> Page[_BASE]:
        return Page(await self.raw.__anext__(), self.model)


class Page(t.Generic[_BASE]):
    def __init__(self, raw: RawPage, model: type[_BASE]) -> None:
        self.model = model
        self.raw = raw

    @property
    def items(self) -> list[_BASE]:
        return [self.model(**d) for d in self.raw.items]

    @property
    def size(self) -> int:
        return self.raw.size

    async def next(self, limit: int | None = None) -> Page[_BASE] | None:
        next = await self.raw.next(limit=limit)
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

        cls.key.name = "key"
        cls.key.base_name = cls.__base_name__
        for key, value in cls.__dict__.items():
            if isinstance(value, Field):
                value.name = key
                value.base_name = cls.__base_name__

    def __init__(self, **values: object) -> None:
        self.raw = values
        self._updates: dict[str, object] = {}

    # model methods
    # these methods further abstract the methods on Base.
    async def delete(self) -> None:
        return await self.delete_item(self.key)

    async def insert(self: _BASE) -> _BASE:
        return await self.insert_item(self)

    async def update(
        self: _BASE,
        *updates: Update,
        set: dict[str, object] | None = None,
        increment: dict[str, int] | None = None,
        append: dict[str, list[object]] | None = None,
        prepend: dict[str, list[object]] | None = None,
        delete: list[str] | None = None,
    ) -> _BASE:
        ud = await self.update_item(
            self.key,
            *updates,
            set=set,
            increment=increment,
            append=append,
            prepend=prepend,
            delete=delete,
        )
        new_self = type(self)()
        new_self.raw = self.raw

        def cd(obj: object) -> dict[str, object]:
            assert isinstance(obj, dict)
            assert all(isinstance(k, str) for k in obj.keys())
            return obj

        def cl(obj: object) -> list[str]:
            assert isinstance(obj, list)
            assert all(isinstance(k, str) for k in obj)
            return obj

        for k, v in cd(ud["set"]).items():
            new_self.raw[k] = v
        for k, v in cd(ud["increment"]).items():
            assert isinstance(v, int)
            orig = new_self.raw[k]
            assert isinstance(orig, int)
            new_self.raw[k] = orig + v
        for k, v in cd(ud["append"]).items():
            assert isinstance(v, list)
            orig = new_self.raw[k]
            assert isinstance(orig, list)
            new_self.raw[k] = orig + v
        for k, v in cd(ud["prepend"]).items():
            assert isinstance(v, list)
            orig = new_self.raw[k]
            assert isinstance(orig, list)
            new_self.raw[k] = v + orig
        for k in cl(ud["delete"]):
            new_self.raw.pop(k)

        return new_self

    # abstracted api methods
    # these methods abstract the methods on Client.
    @classmethod
    async def get(cls: type[_BASE], key: str) -> _BASE:
        return cls(**await cls._client.get_item(cls.__base_name__, key))

    @classmethod
    def where(
        cls: type[_BASE],
        query: Node | t.Sequence[t.Mapping[str, object]] | None = None,
        limit: int = 0,
        last: str | None = None,
    ) -> Paginator[_BASE]:
        return Paginator(
            cls._client.query_items(
                cls.__base_name__, query=query, limit=limit, last=last
            ),
            cls,
        )

    # these methods abstract the methods on Client, but are abstracted
    # further on this model.
    @classmethod
    async def insert_many(
        cls: type[_BASE], items: t.Sequence[_BASE]
    ) -> PutItemsResponse[_BASE]:
        return PutItemsResponse(
            await cls._client.put_items(
                cls.__base_name__, [i.raw for i in items]
            ),
            cls,
        )

    @classmethod
    async def delete_item(cls, key: str) -> None:
        await cls._client.delete_item(cls.__base_name__, key)

    @classmethod
    async def insert_item(cls: type[_BASE], item: _BASE) -> _BASE:
        return cls(
            **await cls._client.insert_item(cls.__base_name__, item.raw)
        )

    @classmethod
    async def update_item(
        cls,
        key: str,
        *updates: Update,
        set: dict[str, object] | None = None,
        increment: dict[str, int] | None = None,
        append: dict[str, list[object]] | None = None,
        prepend: dict[str, list[object]] | None = None,
        delete: list[str] | None = None,
    ) -> dict[str, object]:
        return await cls._client.update_item(
            cls.__base_name__,
            key,
            *updates,
            set=set,
            increment=increment,
            append=append,
            prepend=prepend,
            delete=delete,
        )

    # magic
    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"
