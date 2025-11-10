from src.core.models.query import QueryTree, QueryNodeType
from .base import BaseParser


class InsertParser(BaseParser):
    """Parser for INSERT statements."""

    def __call__(self, query: str) -> QueryTree:
        """Parse INSERT statement into AST."""
        query_upper = query.upper()

        into_pos = query_upper.find('INTO')
        values_pos = query_upper.find('VALUES')

        table_part = query[into_pos + 4:values_pos].strip()

        if '(' in table_part:
            table = table_part[:table_part.index('(')].strip()
            columns = table_part[table_part.index('('):].strip()
        else:
            table = table_part
            columns = ''

        values = query[values_pos + 6:].strip()

        tree = QueryTree(type=QueryNodeType.TABLE, value=table, children=[])
        insert_value = f"{columns} {values}".strip()
        tree = QueryTree(type=QueryNodeType.INSERT, value=insert_value, children=[tree])

        return tree
