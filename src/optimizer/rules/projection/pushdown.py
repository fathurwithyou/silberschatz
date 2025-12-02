from typing import Optional, Iterable, List, Set, Tuple
from src.core.models.query import QueryTree, QueryNodeType
from ..base_rule import OptimizationRule


class ProjectionPushdownRule(OptimizationRule):
    def __init__(self) -> None:
        pass

    def is_applicable(self, node: QueryTree) -> bool:
        if node.type != QueryNodeType.PROJECTION:
            return False
        return bool(node.children) and len(node.children) == 1

    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        if not self.is_applicable(node):
            return None

        proj_cols = self._parse_projection_cols(node.value)
        if self._is_select_all(proj_cols):
            return None

        child = node.children[0]

        if child.type == QueryNodeType.SELECTION:
            predicate_cols = self._extract_cols(child.value)
            inner_cols = proj_cols | predicate_cols
            self._insert_projection_below(child, inner_cols)
            return node

        if child.type == QueryNodeType.ORDER_BY:
            order_cols = self._extract_order_by_cols(child.value)
            inner_cols = proj_cols | order_cols
            self._insert_projection_below(child, inner_cols)
            return node

        if child.type == QueryNodeType.LIMIT:
            self._insert_projection_below(child, proj_cols)
            return node

        if child.type in {QueryNodeType.JOIN, QueryNodeType.NATURAL_JOIN, QueryNodeType.CARTESIAN_PRODUCT}:
            if len(child.children) != 2:
                return None

            left, right = child.children[0], child.children[1]

            left_tables = self._get_table_names(left)
            right_tables = self._get_table_names(right)

            join_cols_left: Set[str] = set()
            join_cols_right: Set[str] = set()

            if child.type == QueryNodeType.JOIN:
                cond_cols = self._extract_cols(child.value)
                jl, jr = self._split_cols_by_side(cond_cols, left_tables, right_tables)
                join_cols_left |= jl
                join_cols_right |= jr

            elif child.type == QueryNodeType.NATURAL_JOIN:
                pass

            elif child.type == QueryNodeType.CARTESIAN_PRODUCT:
                pass

            proj_left, proj_right = self._split_cols_by_side(proj_cols, left_tables, right_tables)

            cols_left = proj_left | join_cols_left
            cols_right = proj_right | join_cols_right

            if cols_left:
                self._wrap_with_projection(left, cols_left, parent=child, idx=0)
            if cols_right:
                self._wrap_with_projection(right, cols_right, parent=child, idx=1)

            return node

        if child.type in {QueryNodeType.TABLE, QueryNodeType.UNKNOWN}:
            self._insert_projection_below(node, proj_cols)
            return node

        return None
        
    @property
    def name(self) -> str:
        return "ProjectionPushdown"

    # -------------------- Helpers: tree mutations --------------------

    def _insert_projection_below(self, parent_node: QueryTree, cols: Set[str]) -> None:
        """
        Insert a projection as the only child of `parent_node`,
        replacing its current single child, preserving parent/child links.
        """
        if not parent_node.children:
            return
        old_child = parent_node.children[0]
        new_proj = self._make_project(old_child, cols)
        parent_node.children[0] = new_proj
        new_proj.parent = parent_node

    def _wrap_with_projection(self, child: QueryTree, cols: Set[str],
                              parent: QueryTree, idx: int) -> None:
        proj = self._make_project(child, cols)
        parent.children[idx] = proj
        proj.parent = parent

    def _make_project(self, child: QueryTree, cols: Iterable[str]) -> QueryTree:
        """
        Create a projection node. Keep deterministic order.
        """
        cols_list = sorted(set(cols))
        value = ",".join(cols_list) if cols_list else ""
        proj = QueryTree(type=QueryNodeType.PROJECTION, value=value, children=[child], parent=None)
        child.parent = proj
        return proj

    # -------------------- Helpers: parsing & classification --------------------

    def _parse_projection_cols(self, value: Optional[str]) -> Set[str]:
        """
        Parse `a, b, t.c` => {"a","b","t.c"}.
        '*' as select-all. Trim whitespace.
        """
        if not value:
            return set()

        parts = [p.strip() for p in value.split(",") if p.strip()]
        return set(parts)

    def _is_select_all(self, cols: Set[str]) -> bool:
        return any(col == "*" or col.endswith(".*") for col in cols)

    def _extract_cols(self, expr: Optional[str]) -> Set[str]:
        """
        Extract identifiers that look like column refs: table.col or bare col.
        """
        if not expr:
            return set()

        import re
        cols: Set[str] = set()

        # Remove string literals first (both single and double quoted)
        scrubbed = re.sub(r"'[^']*'", ' ', expr)
        scrubbed = re.sub(r'"[^"]*"', ' ', scrubbed)

        # Qualified refs: table.column
        for m in re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b', scrubbed):
            tbl, col = m
            cols.add(f"{tbl}.{col}")

        # Remove qualified refs from a copy, then pick bare identifiers
        scrubbed = re.sub(r'\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b', ' ', scrubbed)

        # Bare identifiers that are not keywords/numbers/strings
        tokens = re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', scrubbed)
        keywords = {
            'and', 'or', 'not', 'in', 'like', 'between', 'is', 'null',
            'true', 'false', 'as', 'on', 'using', 'inner', 'outer', 'left',
            'right', 'full', 'join', 'natural', 'where', 'order', 'by',
            'asc', 'desc', 'limit', 'offset'
        }
        for t in tokens:
            if t.lower() not in keywords:
                cols.add(t)

        return cols

    def _extract_order_by_cols(self, order_value: Optional[str]) -> Set[str]:
        """
        Parse a simple "col1 ASC, t2.col2 DESC" into {"col1","t2.col2"}.
        """
        if not order_value:
            return set()
            
        raw = order_value.replace(" ASC", " ").replace(" DESC", " ")
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        
        out: Set[str] = set()
        for p in parts:
            out |= self._extract_cols(p)
        return out

    def _get_table_names(self, node: QueryTree) -> Set[str]:
        """
        Collect table names and aliases from the subtree.
        """
        tables: Set[str] = set()

        def walk(n: QueryTree):
            if n.type in {QueryNodeType.TABLE}:
                if n.value:
                    parts = n.value.split()

                    tables.add(parts[0])

                    if len(parts) >= 3 and parts[1].lower() == "as":
                        tables.add(parts[2])
                    elif len(parts) == 2:
                        tables.add(parts[1])
            for c in (n.children or []):
                walk(c)

        walk(node)
        return tables

    def _split_cols_by_side(
        self,
        cols: Set[str],
        left_tables: Set[str],
        right_tables: Set[str],
    ) -> Tuple[Set[str], Set[str]]:
        """
        Split columns into (left, right). If qualified with table alias, route accordingly.
        If bare (unqualified), send to both sides.
        """
        left: Set[str] = set()
        right: Set[str] = set()

        for col in cols:
            if "." in col:
                tbl, _c = col.split(".", 1)
                if tbl in left_tables:
                    left.add(col)
                elif tbl in right_tables:
                    right.add(col)
                else:
                    left.add(col)
                    right.add(col)
            else:
                left.add(col)
                right.add(col)

        return left, right
        