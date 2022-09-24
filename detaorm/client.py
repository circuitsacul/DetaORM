from __future__ import annotations

import logging
import os
import typing as t
from datetime import datetime, timedelta, timezone

import aiohttp

from detaorm.paginator import RawPage, RawPaginator
from detaorm.query import Node
from detaorm.types import RAW_ITEM

if t.TYPE_CHECKING:
    from detaorm.base import Base
    from detaorm.update import Update

__all__ = ("Client", "RawPutItemsResponse")

_LOG = logging.getLogger(__name__)


def _with_ttl(
    dct: dict[str, object],
    expire_at: int | datetime | None,
    expire_in: int | timedelta | None,
) -> dict[str, object]:
    if expire_in and expire_at:
        raise ValueError("You cannot specify 'expire_at' and 'expire_in'.")

    if expire_in:
        if isinstance(expire_in, int):
            expire_in = timedelta(seconds=expire_in)

        expire_at = datetime.now(timezone.utc) + expire_in

    if isinstance(expire_at, datetime):
        expire_at = int(expire_at.timestamp())

    if expire_at:
        dct = dct.copy()
        dct["__expires"] = expire_at

    return dct


class RawPutItemsResponse:
    def __init__(
        self, processed: list[RAW_ITEM], failed: list[RAW_ITEM]
    ) -> None:
        self.processed = processed
        self.failed = failed


class Client:
    """A client for communicating with the DetaBase API.

    Args:
        project_key: The project key.
        bases: All of the models that will be used.
    """

    api_base_url = "https://database.deta.sh"
    api_version = "v1"

    def __init__(
        self, project_key: str | None = None, *, bases: t.Iterable[Base]
    ) -> None:
        for b in bases:
            b._client = self

        project_key = project_key or os.getenv("DETA_PROJECT_KEY")
        if not project_key:
            raise ValueError(
                "No project key provided. Please set a project key as an env "
                "variable, or pass it as an arg to Client."
            )

        self.project_key = project_key
        self.project_id = project_key.split("_")[0]
        self.__session: aiohttp.ClientSession | None = None

    @property
    def ready(self) -> bool:
        """Returns true if the client is ready to be used."""

        return not (self.__session is None or self.__session.closed)

    @property
    def _session(self) -> aiohttp.ClientSession:
        if not self.__session or self.__session.closed:
            raise RuntimeError("Client has not been opened yet.")
        return self.__session

    async def open(self) -> None:
        """Prepares the client to be used."""

        if self.ready:
            _LOG.warning("Client is already opened.")
            return

        self.__session = aiohttp.ClientSession(
            self.api_base_url,
            headers={
                "X-API-Key": self.project_key,
                "Content-Type": "application/json",
            },
            raise_for_status=True,
        )

    async def close(self) -> None:
        """Closes the aiohttp session."""

        if not self.ready:
            _LOG.warning("Client is not open.")
            return

        await self._session.close()
        self.__session = None

    # api methods
    async def put_items(
        self,
        base_name: str,
        items: t.Sequence[RAW_ITEM],
        *,
        expire_at: int | datetime | None = None,
        expire_in: int | timedelta | None = None,
    ) -> RawPutItemsResponse:
        """Stores multiple items in a single request.

        This request overwrites an item if the key already exists.

        Args:
            base_name: The name of the Base.
            items: A list of items to insert.
            expire_at: A Unix timestamp that these items should expire at.
            expire_in: The number of seconds until this item expires."""

        if len(items) > 25:
            _LOG.warning("Only 25 items can be inserted at a time.")

        items = [_with_ttl(i, expire_at, expire_in) for i in items]
        url = self._build_url(base_name, "items")
        resp = await self._session.put(url, json={"items": items})

        data: dict[str, dict[str, list[RAW_ITEM]]] = await resp.json()
        processed = data.get("processed", {}).get("items", [])
        failed = data.get("failed", {}).get("items", [])
        return RawPutItemsResponse(processed, failed)

    async def get_item(self, base_name: str, key: str) -> RAW_ITEM:
        """Get a stored item.

        Args:
            base_name: The name of the Base.
            key: The key of the item to get.

        Returns:
            A mapping of fields to values."""

        url = self._build_url(base_name, f"items/{key}")
        resp = await self._session.get(url)
        return t.cast(RAW_ITEM, await resp.json())

    async def delete_item(self, base_name: str, key: str) -> None:
        """Delete a stored item.

        Args:
            base_name: The name of the Base.
            key: The key of the item to delete."""

        url = self._build_url(base_name, f"items/{key}")
        await self._session.delete(url)

    async def insert_item(
        self,
        base_name: str,
        item: RAW_ITEM,
        *,
        expire_at: int | datetime | None = None,
        expire_in: int | timedelta | None = None,
    ) -> RAW_ITEM:
        """Creates a new item only if no item with the same key exists.

        Args:
            base_name: The name of the Base.
            item: The item to insert.
            expire_at: The time at which the item should expire (UTC timetamp).
            expire_in: The time until this item should expire."""

        json = _with_ttl({"item": item}, expire_at, expire_in)
        url = self._build_url(base_name, "items")
        resp = await self._session.post(url, json=json)
        return t.cast(RAW_ITEM, await resp.json())

    async def update_item(
        self,
        base_name: str,
        key: str,
        *updates: Update,
        set: dict[str, object] | None = None,
        increment: dict[str, int] | None = None,
        append: dict[str, list[object]] | None = None,
        prepend: dict[str, list[object]] | None = None,
        delete: list[str] | None = None,
    ) -> dict[str, object]:
        """Update an item.

        Args:
            base_name: The name of the Base.
            key: The key of the item to update.
            *updates: Update objects.
            set: A mapping of fields to set.
            increment: A mapping of fields to increment.
            append: A mapping of fields to append items to.
            prepend: A mapping of fields to prepend items to.
            delete: A list of fields to delete.

        Returns:
            The generated update query.
        """

        def join(left: object, right: object) -> object:
            if isinstance(left, t.Mapping):
                assert isinstance(right, t.Mapping)
                return {**left, **right}
            elif isinstance(left, t.Sequence):
                assert isinstance(right, t.Sequence)
                return [*left, *right]
            else:
                raise TypeError

        data = {
            "set": set or {},
            "increment": increment or {},
            "append": append or {},
            "prepend": prepend or {},
            "delete": delete or [],
        }

        for update in updates:
            if (val := data.get(update.field)) is not None:
                data[update.field] = join(val, update.payload)
            else:
                raise ValueError(f"Unexpected field {update.field}.")

        url = self._build_url(base_name, f"items/{key}")
        await self._session.patch(url, json=data)
        return data

    def query_items(
        self,
        base_name: str,
        query: Node | t.Sequence[t.Mapping[str, object]] | None = None,
        limit: int = 0,
        last: str | None = None,
    ) -> RawPaginator:
        """Query items.

        Args:
            base_name: The name of the Base.
            query: The query. Defaults to None.
            limit: The maximum page size. Defaults to 0 (no limit).
            last: The last key of the last page. Defaults to None.

        Returns:
            A RawPage. Use RawPage.items to see the items, and RawPage.next()
            to get the next page.
        """

        async def first_page(
            base_name: str,
            query: Node | t.Sequence[t.Mapping[str, object]] | None = None,
            limit: int = 0,
            last: str | None = None,
        ) -> RawPage:
            if isinstance(query, Node):
                query = query.deta_query()

            data: dict[str, object] = {}
            if query:
                data["query"] = query
            if limit:
                data["limit"] = limit
            if last:
                data["last"] = last

            url = self._build_url(base_name, "query")

            resp = await self._session.post(url, json=data)
            resp_data = await resp.json()
            return RawPage(
                client=self,
                base_name=base_name,
                query=query,
                size=resp_data["paging"]["size"],
                limit=limit,
                last=resp_data["paging"].get("last"),
                items=resp_data["items"],
            )

        return RawPaginator(first_page(base_name, query, limit, last))

    def _build_url(self, base_name: str, relative_path: str) -> str:
        return (
            f"/{self.api_version}/{self.project_id}/{base_name}/"
            f"{relative_path}"
        )
