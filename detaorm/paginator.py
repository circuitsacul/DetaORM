from __future__ import annotations

import typing as t

from detaorm.types import RAW_ITEM

if t.TYPE_CHECKING:
    from detaorm.client import Client


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
        self.base_name = base_name
        self.query = query
        self.client = client
        self.limit = limit
        self.size = size
        self.last = last
        self.items = items

    async def next(self, limit: int | None = None) -> RawPage | None:
        if not self.last:
            return None

        return await self.client.query_items(
            self.base_name,
            self.query,
            limit=limit or self.limit,
            last=self.last,
        )
