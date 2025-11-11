from typing import Optional
from src.core.models.query import QueryTree


class JoinCommutativityRule:
    def __init__(self, should_swap_fn=None):
        self._should_swap = should_swap_fn or (lambda l, r: True)

    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None

        left_child, right_child = node.children[0], node.children[1]
        if not self._should_swap(left_child, right_child):
            return None

        new_node = QueryTree(type=node.type, value=node.value, children=[right_child, left_child], parent=node.parent)
        right_child.parent = left_child.parent = new_node

        return new_node

    def is_applicable(self, node: QueryTree) -> bool:
        return node.type == 'join' and len(node.children) == 2

    @property
    def name(self) -> str:
        return "JoinCommutativity"
