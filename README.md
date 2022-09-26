# DetaORM
An async ORM for [DetaBase](https://docs.deta.sh/docs/base/about/).

[Support](https://discord.gg/dGAzZDaTS9) | [PyPI](https://pypi.org/project/detaorm) | [Documentation](https://github.com/CircuitSacul/DetaORM/wiki)

## Example Usage
Here's some examples of DetaORM, with commentary. I'll add real documentation at some point.

```py
from __future__ import annotations

import asyncio

from detaorm import Client, Base, Field


class User(Base, name="users"):
    username: Field[str] = Field()
    nicknames: Field[list[str]] = Field(default=[])


async def main() -> None:
    client = Client("<project key>", bases=[User])
    await client.open()

    new_user = User(username="CircuitSacul")
    print(new_user)  # > User({"username": "CircuitSacul"})
    inserted_user = await new_user.insert()
    print(inserted_user)   # > User({"username": "CircuitSacul", "nicknames": []})

    updated_user = await inserted_user.update(User.nicknames.append(["Circuit", "Sacul"]))
    print(updated_user)  # > User({"username": "CircuitSacul", "nicknames": ["Circuit", "Sacul"]})

    page = await User.where(User.nicknames.contains("Sacul"), limit=1)
    print(page.items[0])  # > User({"username": "CircuitSacul", "nicknames": ["Circuit", "Sacul"]})


if __name__ == "__main__":
    asyncio.run(main())
```
