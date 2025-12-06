from typing import Optional
from src.core.models.query import QueryTree, QueryNodeType
from ..base_rule import OptimizationRule

class JoinAssociativityRule(OptimizationRule):
    def __init__(self, prefer_right_deep=True):
        self._prefer_right_deep = prefer_right_deep

    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None
        return self._to_right_deep(node) if self._prefer_right_deep else self._to_left_deep(node)

    def _to_right_deep(self, node: QueryTree) -> Optional[QueryTree]:
        left_child, right_child = node.children[0], node.children[1]
        if left_child.type != QueryNodeType.JOIN:
            return None

        e1, e2, e3 = left_child.children[0], left_child.children[1], right_child

        new_right_join = QueryTree(type=QueryNodeType.JOIN, value=node.value, children=[e2, e3], parent=None)
        e2.parent = e3.parent = new_right_join

        new_root = QueryTree(type=QueryNodeType.JOIN, value=left_child.value, children=[e1, new_right_join], parent=node.parent)
        e1.parent = new_right_join.parent = new_root

        return new_root

    def _to_left_deep(self, node: QueryTree) -> Optional[QueryTree]:
        left_child, right_child = node.children[0], node.children[1]
        if right_child.type != QueryNodeType.JOIN:
            return None

        e1, e2, e3 = left_child, right_child.children[0], right_child.children[1]

        new_left_join = QueryTree(type=QueryNodeType.JOIN, value=node.value, children=[e1, e2], parent=None)
        e1.parent = e2.parent = new_left_join

        new_root = QueryTree(type=QueryNodeType.JOIN, value=right_child.value, children=[new_left_join, e3], parent=node.parent)
        new_left_join.parent = e3.parent = new_root

        return new_root

    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != QueryNodeType.JOIN or len(node.children) != 2:
            return False

        left_is_join = node.children[0].type == QueryNodeType.JOIN
        right_is_join = node.children[1].type == QueryNodeType.JOIN

        return left_is_join if self._prefer_right_deep else right_is_join

    @property
    def name(self) -> str:
        return "JoinAssociativity"
