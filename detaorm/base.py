from __future__ import annotations

import typing as t

from detaorm.field import Field

if t.TYPE_CHECKING:
    from detaorm.client import Client

__all__ = ("Base",)


class Base:
    """A Deta "Base", or a single model.

    Args:
        client (Client): The DetaORM Client to use.
        **values (object): Key-value pairs for this model.
    """

    __base_name__: str
    """The name of this Base."""

    key: Field[str] = Field()

    def __init_subclass__(cls) -> None:
        if not hasattr(cls, "__base_name__"):
            raise AttributeError(f"Base {cls} has no __base_name__ set.")

        for key, value in cls.__dict__.items():
            if isinstance(value, Field):
                value.name = key
                value.base_name = cls.__base_name__

    def __init__(
        self, client: Client | None = None, /, **values: object
    ) -> None:
        if client:
            self.__client = client
        self.raw = values

    @property
    def _client(self) -> Client:
        if not self.__client:
            raise RuntimeError(
                f"Base {type(self)} has no client. Either specify a client on "
                "intialization, or add this Base to the client."
            )
        return self.__client

    @_client.setter
    def _client(self, value: Client) -> None:
        self.__client = value
