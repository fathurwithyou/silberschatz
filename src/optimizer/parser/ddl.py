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
        rest = query[table_pos + 5:].strip()

        # Check for CASCADE or RESTRICT modifier
        cascade_pos = rest.upper().find('CASCADE')
        restrict_pos = rest.upper().find('RESTRICT')

        if cascade_pos != -1:
            table = rest[:cascade_pos].strip()
            value = f"{table} CASCADE"
        elif restrict_pos != -1:
            table = rest[:restrict_pos].strip()
            value = f"{table} RESTRICT"
        else:
            table = rest.strip()
            value = table

        return QueryTree(type=QueryNodeType.DROP_TABLE, value=value, children=[])


class CreateIndexParser(BaseParser):
    """Parser for CREATE INDEX statements."""

    def __call__(self, query: str) -> QueryTree:
        """
        Parse CREATE INDEX statement into AST.

        Supported syntax:
            CREATE INDEX <index_name> ON <table_name>(<column_name>)
            CREATE INDEX <index_name> ON <table_name>(<column_name>) USING <index_type>

        Examples:
            CREATE INDEX idx_employee_name ON employee(name)
            CREATE INDEX idx_employee_dept ON employee(dept_id) USING BTREE

        Returns:
            QueryTree with value format: "<index_name> ON <table_name>(<column_name>) [USING <index_type>]"
        """
        query_upper = query.upper()

        # Find INDEX keyword position
        index_pos = query_upper.find('INDEX')
        if index_pos == -1:
            raise ValueError("Invalid CREATE INDEX syntax: INDEX keyword not found")

        # Extract index name (between INDEX and ON)
        on_pos = query_upper.find('ON', index_pos)
        if on_pos == -1:
            raise ValueError("Invalid CREATE INDEX syntax: ON keyword not found")

        index_name = query[index_pos + 5:on_pos].strip()
        if not index_name:
            raise ValueError("Invalid CREATE INDEX syntax: index name is required")

        # Extract table name and column (between ON and optional USING)
        using_pos = query_upper.find('USING', on_pos)

        if using_pos != -1:
            # Has USING clause
            table_column_part = query[on_pos + 2:using_pos].strip()
            index_type = query[using_pos + 5:].strip()
        else:
            # No USING clause, default to B+TREE
            table_column_part = query[on_pos + 2:].strip()
            index_type = "BTREE"

        # Parse table_name(column_name)
        paren_pos = table_column_part.find('(')
        if paren_pos == -1:
            raise ValueError("Invalid CREATE INDEX syntax: column specification '(column_name)' not found")

        table_name = table_column_part[:paren_pos].strip()
        if not table_name:
            raise ValueError("Invalid CREATE INDEX syntax: table name is required")

        # Extract column name from parentheses
        close_paren = table_column_part.find(')', paren_pos)
        if close_paren == -1:
            raise ValueError("Invalid CREATE INDEX syntax: closing parenthesis ')' not found")

        column_name = table_column_part[paren_pos + 1:close_paren].strip()
        if not column_name:
            raise ValueError("Invalid CREATE INDEX syntax: column name is required")

        # Build value string
        if using_pos != -1:
            value = f"{index_name} ON {table_name}({column_name}) USING {index_type}"
        else:
            value = f"{index_name} ON {table_name}({column_name})"
        return QueryTree(type=QueryNodeType.CREATE_INDEX, value=value, children=[])


class DropIndexParser(BaseParser):
    """Parser for DROP INDEX statements."""

    def __call__(self, query: str) -> QueryTree:
        """
        Parse DROP INDEX statement into AST.

        Supported syntax:
            DROP INDEX <index_name>
            DROP INDEX <index_name> ON <table_name>

        Examples:
            DROP INDEX idx_employee_name
            DROP INDEX idx_employee_name ON employee

        Returns:
            QueryTree with value format: "<index_name> [ON <table_name>]"
        """
        query_upper = query.upper()

        # Find INDEX keyword position
        index_pos = query_upper.find('INDEX')
        if index_pos == -1:
            raise ValueError("Invalid DROP INDEX syntax: INDEX keyword not found")

        # Extract everything after INDEX
        rest = query[index_pos + 5:].strip()

        # Check if there's an ON clause
        on_pos = rest.upper().find('ON')

        if on_pos != -1:
            # Has ON clause: DROP INDEX idx_name ON table_name
            index_name = rest[:on_pos].strip()
            table_name = rest[on_pos + 2:].strip()

            if not index_name:
                raise ValueError("Invalid DROP INDEX syntax: index name is required")
            if not table_name:
                raise ValueError("Invalid DROP INDEX syntax: table name is required after ON")

            value = f"{index_name} ON {table_name}"
        else:
            # No ON clause: DROP INDEX idx_name
            index_name = rest.strip()

            if not index_name:
                raise ValueError("Invalid DROP INDEX syntax: index name is required")

            value = index_name
        return QueryTree(type=QueryNodeType.DROP_INDEX, value=value, children=[])
