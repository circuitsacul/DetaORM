from __future__ import annotations

import typing as t

from detaorm.types import RAW_ITEM

if t.TYPE_CHECKING:
    from detaorm.client import Client

__all__ = ("RawPaginator", "RawPage")


class RawPaginator:
    def __init__(self, first_page: t.Awaitable[RawPage]) -> None:
        self.__first_page: t.Awaitable[RawPage] | None = first_page
        self.__current_page: RawPage | None = None

    def __aiter__(self) -> RawPaginator:
        return self

    def __await__(self) -> t.Generator[t.Any, None, RawPage]:
        assert self.__first_page
        return self.__first_page.__await__()

    async def __anext__(self) -> RawPage:
        if self.__first_page:
            self.__current_page = await self.__first_page
            self.__first_page = None
            return self.__current_page

        assert self.__current_page
        self.__current_page = await self.__current_page.next()
        if self.__current_page is None:
            raise StopAsyncIteration
        return self.__current_page


class RawPage:
    def __init__(
        self,
        client: Client,
        base_name: str,
        query: t.Sequence[t.Mapping[str, object]] | None,
        limit: int,
        size: int,
        last: str | None,
        items: t.Sequence[RAW_ITEM],
    ) -> None:
        self._base_name = base_name
        self._query = query
        self._client = client
        self._limit = limit
        self._last = last
        self.size = size
        self.items = items

    async def next(self, limit: int | None = None) -> RawPage | None:
        if not self._last:
            return None

        return await self._client.query_items(
            self._base_name,
            self._query,
            limit=limit if limit is not None else self._limit,
            last=self._last,
        )
