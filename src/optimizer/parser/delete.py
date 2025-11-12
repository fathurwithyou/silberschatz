from src.core.models.query import QueryTree, QueryNodeType
from .base import BaseParser


class DeleteParser(BaseParser):
    """Parser for DELETE statements."""

    def __call__(self, query: str) -> QueryTree:
        query_upper = query.upper()

        from_pos = query_upper.find('FROM')
        where_pos = query_upper.find('WHERE')

        end_pos = where_pos if where_pos != -1 else len(query)
        table = query[from_pos + 4:end_pos].strip()

        tree = QueryTree(type=QueryNodeType.TABLE, value=table, children=[])

        if where_pos != -1:
            where_clause = query[where_pos + 5:].strip()
            tree = QueryTree(type=QueryNodeType.SELECTION, value=where_clause, children=[tree])

        tree = QueryTree(type=QueryNodeType.DELETE, value='', children=[tree])

        return tree
