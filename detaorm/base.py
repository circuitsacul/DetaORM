from __future__ import annotations

import typing as t
from datetime import datetime, timedelta

from detaorm.field import Field
from detaorm.undef import UNDEF

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
    _defaults: dict[str, object]

    key: Field[str] = Field()

    def __init_subclass__(cls, name: str | None = None) -> None:
        cls.__base_name__ = name or cls.__name__.lower()
        cls._defaults = {}

        cls.key.name = "key"

        for key, value in cls.__dict__.items():
            if isinstance(value, Field):
                value.name = key
                value.qual_name = key

                if value.default is not UNDEF:
                    cls._defaults[key] = value.default

    def __init__(self, **values: object) -> None:
        self.raw = values

    def _with_defaults(self) -> dict[str, object]:
        dct = self._defaults.copy()
        dct.update(self.raw)
        return dct

    # model methods
    # these methods further abstract the methods on Base.
    async def delete(self) -> None:
        return await self.delete_item(self.key)

    async def insert(
        self: _BASE,
        *,
        expire_at: int | datetime | None = None,
        expire_in: int | timedelta | None = None,
    ) -> _BASE:
        return await self.insert_item(
            self, expire_at=expire_at, expire_in=expire_in
        )

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

        def traverse(
            dct: dict[str, object], key: str
        ) -> tuple[dict[str, object], str]:
            keys = key.split(".")

            final_key = keys.pop(-1)
            final_dict = dct
            for k in keys:
                final_dict = final_dict.setdefault(k, {})  # type: ignore

            return final_dict, final_key

        for k, v in cd(ud["set"]).items():
            dct, fk = traverse(new_self.raw, k)
            dct[fk] = v

        for k, v in cd(ud["increment"]).items():
            dct, fk = traverse(new_self.raw, k)
            if fk not in dct:
                continue

            assert isinstance(v, int)
            orig = dct[fk]
            assert isinstance(orig, int)
            dct[fk] = orig + v

        for k, v in cd(ud["append"]).items():
            dct, fk = traverse(new_self.raw, k)
            if fk not in dct:
                continue

            assert isinstance(v, list)
            orig = dct[fk]
            assert isinstance(orig, list)
            dct[fk] = orig + v

        for k, v in cd(ud["prepend"]).items():
            dct, fk = traverse(new_self.raw, k)
            if fk not in dct:
                continue

            assert isinstance(v, list)
            orig = dct[fk]
            assert isinstance(orig, list)
            dct[fk] = v + orig

        for k in cl(ud["delete"]):
            dct, fk = traverse(new_self.raw, k)
            dct.pop(fk)

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
        cls: type[_BASE],
        items: t.Sequence[_BASE],
        *,
        expire_at: int | datetime | None = None,
        expire_in: int | timedelta | None = None,
    ) -> PutItemsResponse[_BASE]:
        return PutItemsResponse(
            await cls._client.put_items(
                cls.__base_name__,
                [i._with_defaults() for i in items],
                expire_at=expire_at,
                expire_in=expire_in,
            ),
            cls,
        )

    @classmethod
    async def delete_item(cls, key: str) -> None:
        await cls._client.delete_item(cls.__base_name__, key)

    @classmethod
    async def insert_item(
        cls: type[_BASE],
        item: _BASE,
        *,
        expire_at: int | datetime | None = None,
        expire_in: int | timedelta | None = None,
    ) -> _BASE:
        return cls(
            **await cls._client.insert_item(
                cls.__base_name__,
                item._with_defaults(),
                expire_at=expire_at,
                expire_in=expire_in,
            )
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
