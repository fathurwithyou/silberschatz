from typing import Optional, Iterable, Set, Tuple
from src.core.models.query import QueryTree
from ..base_rule import OptimizationRule


class ProjectionEliminationRule(OptimizationRule):
    def __init__(self) -> None:
        pass

    @property
    def name(self) -> str:
        return "ProjectionElimination"

    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != "projection":
            return False
        if len(node.children) != 1:
            return False
        return True

    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None

        child = node.children[0]

        if child.type != "projection" or not child.children:
            return None

        grand = child.children[0]

        outer_cols, outer_has_star = self._parse_cols(node.value)
        inner_cols, inner_has_star = self._parse_cols(child.value)

        if outer_has_star and inner_has_star:
            node.children[0] = grand
            grand.parent = node
            
            node.value = "*"
            return node

        if outer_has_star and not inner_has_star:
            child.parent = node.parent
            return child

        if inner_has_star and not outer_has_star:
            node.children[0] = grand
            grand.parent = node
            return node

        new_cols = outer_cols & inner_cols

        if new_cols == inner_cols:
            child.parent = node.parent
            return child

        if new_cols == outer_cols:
            node.children[0] = grand
            grand.parent = node
            return node

        node.children[0] = grand
        grand.parent = node
        node.value = self._cols_to_value(new_cols)
        return node

    # -------------------- helpers --------------------

    def _parse_cols(self, value: Optional[str]) -> Tuple[Set[str], bool]:
        """
        Parse a comma-separated column list.
        Returns (set_of_columns, has_wildcard)
        Wildcard is True if '*' or any 't.*' present.
        Keeps qualified names as tokens (e.g., 't.c').
        """
        cols: Set[str] = set()
        has_star = False
        if not value:
            return cols, has_star

        parts = [p.strip() for p in value.split(",") if p.strip()]
        for p in parts:
            # detect '*' or 'x.*'
            if p == "*" or p.endswith(".*"):
                has_star = True
            else:
                cols.add(p)
        return cols, has_star

    def _cols_to_value(self, cols: Iterable[str]) -> str:
        lst = sorted(set(cols))
        return ",".join(lst)
