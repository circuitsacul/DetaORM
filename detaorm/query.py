from __future__ import annotations

import typing as t

__all__ = ("And", "Or", "Query", "Node")


class Node:
    @classmethod
    def _filter_nodes(cls, nodes: t.Sequence[Node]) -> t.Sequence[Node]:
        final: list[Node] = []
        for n in nodes:
            if isinstance(n, cls):
                final.extend(n.nodes)
            else:
                final.append(n)
        return final

    @property
    def nodes(self) -> t.Sequence[Node]:
        raise NotImplementedError

    def deta_query(self) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        tree = self._flatten()
        if not isinstance(tree, Or):
            tree = Or(tree)
        for node in tree.nodes:
            assert isinstance(node, (Query, And))
            assert all(isinstance(q, Query) for q in node.nodes)
            result.append(
                {q.key: q.value for q in t.cast(t.Sequence[Query], node.nodes)}
            )
        return result

    def _flatten(self) -> Node:
        raise NotImplementedError

    def _and(self, other: Node) -> Node:
        raise NotImplementedError

    def __and__(self, other: Node) -> Node:
        return And(self, other)

    def __or__(self, other: Node) -> Node:
        return Or(self, other)


class And(Node):
    def __init__(self, *nodes: Node) -> None:
        self._nodes = self._filter_nodes(nodes)

    @property
    def nodes(self) -> t.Sequence[Node]:
        return self._nodes

    def _flatten(self) -> Node:
        final: Node | None = None
        for _node in self.nodes:
            node = _node._flatten()
            if final:
                final = final._and(node)
            else:
                final = node
        return final or Or()

    def _and(self, other: Node) -> Node:
        if isinstance(other, Query):
            return And(*self.nodes, other)
        elif isinstance(other, And):
            return And(*self.nodes, *other.nodes)
        else:  # Or
            return other._and(self)

    def __repr__(self) -> str:
        return "({})".format(" AND ".join(str(n) for n in self.nodes))


class Or(Node):
    def __init__(self, *nodes: Node) -> None:
        self._nodes = self._filter_nodes(nodes)

    @property
    def nodes(self) -> t.Sequence[Node]:
        return self._nodes

    def _flatten(self) -> Or:
        return Or(*(n._flatten() for n in self.nodes))

    def _and(self, other: Node) -> Or:
        return Or(*(n._and(other) for n in self.nodes))

    def __repr__(self) -> str:
        return "({})".format(" OR ".join(str(n) for n in self.nodes))


class Query(Node):
    def __init__(self, key: str, value: object) -> None:
        self.key = key
        self.value = value

    @property
    def nodes(self) -> t.Sequence[Node]:
        return (self,)

    def _flatten(self) -> Node:
        return self

    def _and(self, other: Node) -> Node:
        if isinstance(other, Query):
            return And(self, other)
        return other._and(self)

    def __repr__(self) -> str:
        return f"{self.key}={self.value}"
