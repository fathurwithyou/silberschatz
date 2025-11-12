from src.core.models.query import QueryTree, QueryNodeType
from .base import BaseParser


class UpdateParser(BaseParser):

    def __call__(self, query: str) -> QueryTree:
        """Parse UPDATE statement into AST."""
        query_upper = query.upper()

        update_pos = query_upper.find('UPDATE')
        set_pos = query_upper.find('SET')
        where_pos = query_upper.find('WHERE')

        table = query[update_pos + 6:set_pos].strip()

        end_pos = where_pos if where_pos != -1 else len(query)
        set_clause = query[set_pos + 3:end_pos].strip()

        tree = QueryTree(type=QueryNodeType.TABLE, value=table, children=[])

        if where_pos != -1:
            where_clause = query[where_pos + 5:].strip()
            tree = QueryTree(type=QueryNodeType.SELECTION, value=where_clause, children=[tree])

        tree = QueryTree(type=QueryNodeType.UPDATE, value=set_clause, children=[tree])

        return tree
