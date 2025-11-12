from src.core.models.query import QueryTree, QueryNodeType


class BaseParser:
    """Base class for SQL parsers."""

    def _find_next_clause(self, query: str, start_pos: int, keywords: list) -> int:
        """Find position of next keyword in query."""
        query_upper = query.upper()
        positions = []

        for keyword in keywords:
            pos = query_upper.find(keyword, start_pos)
            if pos != -1:
                positions.append(pos)

        return min(positions) if positions else len(query)

    def _parse_table_with_alias(self, table_expr: str) -> QueryTree:
        """Parse table expression with optional alias."""
        parts = table_expr.strip().split()

        if len(parts) >= 3 and parts[-2].upper() == 'AS':
            table = parts[0]
            alias = parts[-1]
            return QueryTree(type=QueryNodeType.TABLE, value=f"{table} AS {alias}", children=[])
        elif len(parts) == 2:
            table = parts[0]
            alias = parts[1]
            return QueryTree(type=QueryNodeType.TABLE, value=f"{table} AS {alias}", children=[])
        else:
            return QueryTree(type=QueryNodeType.TABLE, value=parts[0], children=[])

    def _extract_aliases(self, from_clause: str) -> dict:
        """Extract table aliases from FROM clause."""
        aliases = {}
        parts = from_clause.split(',')

        for part in parts:
            tokens = part.strip().split()
            if len(tokens) >= 3 and tokens[-2].upper() == 'AS':
                aliases[tokens[-1]] = tokens[0]
            elif len(tokens) == 2:
                aliases[tokens[1]] = tokens[0]

        return aliases
