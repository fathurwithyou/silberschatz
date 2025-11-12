from src.core.models.query import QueryTree, QueryNodeType
from .base import BaseParser


class CreateParser(BaseParser):
    """Parser for CREATE TABLE statements."""

    def __call__(self, query: str) -> QueryTree:
        """Parse CREATE TABLE statement into AST."""
        query_upper = query.upper()

        table_pos = query_upper.find('TABLE')
        table_name_start = table_pos + 5

        paren_pos = query.find('(', table_name_start)
        table = query[table_name_start:paren_pos].strip()

        schema = query[paren_pos:].strip()

        return QueryTree(type=QueryNodeType.CREATE_TABLE, value=f"{table} {schema}", children=[])


class DropParser(BaseParser):
    """Parser for DROP TABLE statements."""

    def __call__(self, query: str) -> QueryTree:
        """Parse DROP TABLE statement into AST."""
        query_upper = query.upper()

        table_pos = query_upper.find('TABLE')
        table = query[table_pos + 5:].strip()

        cascade = 'CASCADE' if 'CASCADE' in query_upper else ''
        restrict = 'RESTRICT' if 'RESTRICT' in query_upper else ''
        modifier = cascade or restrict

        value = f"{table} {modifier}".strip()
        return QueryTree(type=QueryNodeType.DROP_TABLE, value=value, children=[])
