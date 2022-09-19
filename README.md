# DetaORM
An async ORM for DetaBase.

## Example Usage
Here's some examples of DetaORM, with commentary. I'll add real documentation at some point.

```py
from detaorm import Client, Base, Field


# create your base(s) (or models)
class User(Base):
    # the name of your base
    __base_name__ = "users"

    # all bases have a `key` field already.

    # typehints are optional. You could write this instead:
    # username = Field()
    username: Field[str] = Field()
    nicknames: Field[list[str]] = Field()


# create the client
# you have to list the bases when creating the client to
# allow the client to setup properly.
client = Client("<project key>", [User])

# actual usage:
new_user = User(username="CircuitSacul")

# all fields are optional, but they will raise a KeyError
# if you try to access them.
# The following line will raise a KeyError
new_user.nicknames

# to actually create the user, you have to call .insert():
created_user = await new_user.insert()
# created_user and new_user will be identical

# to update an item:
updated_user = await new_user.update(
    User.nicknames.set(["circuit"])
)
print(updated_user)
# > User({"key": ..., "username": "CircuitSacul", "nicknames": ["Awesome Person"]})
updated_user = await updated_user.update(
    User.nicknames.append(["sacul"])
)
print(updated_user)
# > User({"key": ..., "username": "CircuitSacul", "nicknames": ["circuit", "sacul"]})

# updated_user and new_user will be different now.
# DetaORM sends the query to DetaBase, but also tries
# to determine the updated value and returns a new item.
print(new_user)
# > User({"key": ..., "username": "CircuitSacul"]})

# you can also use Base.insert_many to insert up to 25 items:
await User.insert_many([
    User(username="BatMan", nicknames=["superhero"]),
    User(username="SuperMan", nicknames=["superhero"]),
])

# The easiest way to query items is with .where():
async for page in await User.where(User.nicknames.contains("superhero")):
    for user in page.items:
        print(user)
# > User({"key": ..., "username": "BatMan", "nicknames": ["superhero"]})
# > User({"key": ..., "username": "SuperMan", "nicknames": ["superhero"]})
```
