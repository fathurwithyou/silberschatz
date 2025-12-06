"""Main query parser - orchestrates all SQL statement parsers."""
from src.core.models.query import QueryTree, ParsedQuery, QueryNodeType
from .select import SelectParser
from .update import UpdateParser
from .delete import DeleteParser
from .insert import InsertParser
from .ddl import CreateParser, DropParser, CreateIndexParser, DropIndexParser


class QueryParser:
    """Main SQL query parser."""

    def __init__(self):
        self.select_parser = SelectParser()
        self.update_parser = UpdateParser()
        self.delete_parser = DeleteParser()
        self.insert_parser = InsertParser()
        self.create_parser = CreateParser()
        self.drop_parser = DropParser()
        self.create_index_parser = CreateIndexParser()
        self.drop_index_parser = DropIndexParser()

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
        elif statement_type == 'CREATE_TABLE':
            tree = self.create_parser(query)
        elif statement_type == 'CREATE_INDEX':
            tree = self.create_index_parser(query)
        elif statement_type == 'DROP_TABLE':
            tree = self.drop_parser(query)
        elif statement_type == 'DROP_INDEX':
            tree = self.drop_index_parser(query)
        elif statement_type == 'BEGIN':
            tree = QueryTree(type=QueryNodeType.BEGIN_TRANSACTION, value='', children=[])
        elif statement_type == 'COMMIT':
            tree = QueryTree(type=QueryNodeType.COMMIT, value='', children=[])
        elif statement_type == 'ABORT':
            tree = QueryTree(type=QueryNodeType.ABORT, value='', children=[])
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
            # Distinguish between CREATE TABLE and CREATE INDEX
            if 'INDEX' in query_upper.split('TABLE')[0]:
                return 'CREATE_INDEX'
            else:
                return 'CREATE_TABLE'
        elif query_upper.startswith('DROP'):
            # Distinguish between DROP TABLE and DROP INDEX
            if 'INDEX' in query_upper.split('TABLE')[0]:
                return 'DROP_INDEX'
            else:
                return 'DROP_TABLE'
        elif query_upper.startswith('BEGIN'):
            return 'BEGIN'
        elif query_upper.startswith('COMMIT'):
            return 'COMMIT'
        elif query_upper.startswith('ABORT'):
            return 'ABORT'

        return 'UNKNOWN'
