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


class CreateIndexParser(BaseParser):
    """
    Syntax:
        CREATE INDEX ON table_name(column_name)
        CREATE INDEX ON table_name(column_name) USING BTREE
        CREATE INDEX ON table_name(column_name) USING B+TREE
    """

    def __call__(self, query: str) -> QueryTree:
        query_upper = query.upper()

        on_pos = query_upper.find('ON')
        if on_pos == -1:
            raise ValueError("CREATE INDEX syntax error: missing ON keyword")

        on_start = on_pos + 2
        using_pos = query_upper.find('USING', on_start)

        if using_pos != -1:
            table_col_part = query[on_start:using_pos].strip()
            index_type = query[using_pos + 5:].strip()
        else:
            table_col_part = query[on_start:].strip()
            index_type = "BTREE"  # Default

        paren_pos = table_col_part.find('(')
        if paren_pos == -1:
            raise ValueError("CREATE INDEX syntax error: missing column specification")

        table_name = table_col_part[:paren_pos].strip()
        column_part = table_col_part[paren_pos:].strip()

        column_name = column_part.strip('()').strip()

        value = f"{table_name} {column_name} {index_type}"
        return QueryTree(type=QueryNodeType.CREATE_INDEX, value=value, children=[])


class DropIndexParser(BaseParser):
    """
    Syntax:
        DROP INDEX ON table_name(column_name)
    """

    def __call__(self, query: str) -> QueryTree:
        query_upper = query.upper()

        on_pos = query_upper.find('ON')
        if on_pos == -1:
            raise ValueError("DROP INDEX syntax error: missing ON keyword")

        table_col_part = query[on_pos + 2:].strip()

        paren_pos = table_col_part.find('(')
        if paren_pos == -1:
            raise ValueError("DROP INDEX syntax error: missing column specification")

        table_name = table_col_part[:paren_pos].strip()
        column_part = table_col_part[paren_pos:].strip()

        column_name = column_part.strip('()').strip()

        value = f"{table_name} {column_name}"
        return QueryTree(type=QueryNodeType.DROP_INDEX, value=value, children=[])
