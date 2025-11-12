"""Main query parser - orchestrates all SQL statement parsers."""
from src.core.models.query import QueryTree, ParsedQuery, QueryNodeType
from .select import SelectParser
from .update import UpdateParser
from .delete import DeleteParser
from .insert import InsertParser
from .ddl import CreateParser, DropParser


class QueryParser:
    """Main SQL query parser."""

    def __init__(self):
        self.select_parser = SelectParser()
        self.update_parser = UpdateParser()
        self.delete_parser = DeleteParser()
        self.insert_parser = InsertParser()
        self.create_parser = CreateParser()
        self.drop_parser = DropParser()

    def __call__(self, query: str) -> ParsedQuery:
        """Parse SQL query into AST."""
        query = query.strip().rstrip(';')
        statement_type = self._detect_statement_type(query)

        if statement_type == 'SELECT':
            tree = self.select_parser(query)
        elif statement_type == 'UPDATE':
            tree = self.update_parser(query)
        elif statement_type == 'DELETE':
            tree = self.delete_parser(query)
        elif statement_type == 'INSERT':
            tree = self.insert_parser(query)
        elif statement_type == 'CREATE':
            tree = self.create_parser(query)
        elif statement_type == 'DROP':
            tree = self.drop_parser(query)
        elif statement_type == 'BEGIN':
            tree = QueryTree(type=QueryNodeType.BEGIN_TRANSACTION, value='', children=[])
        elif statement_type == 'COMMIT':
            tree = QueryTree(type=QueryNodeType.COMMIT, value='', children=[])
        else:
            tree = QueryTree(type=QueryNodeType.UNKNOWN, value=query, children=[])

        return ParsedQuery(tree=tree, query=query)

    def _detect_statement_type(self, query: str) -> str:
        """Detect SQL statement type."""
        query_upper = query.upper().strip()

        if query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        elif query_upper.startswith('INSERT'):
            return 'INSERT'
        elif query_upper.startswith('CREATE'):
            return 'CREATE'
        elif query_upper.startswith('DROP'):
            return 'DROP'
        elif query_upper.startswith('BEGIN'):
            return 'BEGIN'
        elif query_upper.startswith('COMMIT'):
            return 'COMMIT'

        return 'UNKNOWN'
