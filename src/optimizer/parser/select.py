"""SELECT statement parser."""
from src.core.models.query import QueryTree, QueryNodeType
from .base import BaseParser


class SelectParser(BaseParser):

    def __call__(self, query: str) -> QueryTree:
        """Parse SELECT statement into AST."""
        tokens = self._tokenize(query)
        tree = self._build_base(tokens)

        if 'where' in tokens:
            tree = QueryTree(type=QueryNodeType.SELECTION, value=tokens['where'], children=[tree])

        if 'select' in tokens:
            tree = QueryTree(type=QueryNodeType.PROJECTION, value=tokens['select'], children=[tree])

        if 'order_by' in tokens:
            tree = QueryTree(type=QueryNodeType.ORDER_BY, value=tokens['order_by'], children=[tree])

        if 'limit' in tokens:
            tree = QueryTree(type=QueryNodeType.LIMIT, value=tokens['limit'], children=[tree])

        return tree

    def _tokenize(self, query: str) -> dict:
        """Extract SELECT, FROM, WHERE, JOIN, ORDER BY, LIMIT clauses."""
        query_upper = query.upper()
        tokens = {}

        select_pos = query_upper.find('SELECT')
        from_pos = query_upper.find('FROM')
        where_pos = query_upper.find('WHERE')
        join_pos = query_upper.find('JOIN')
        order_pos = query_upper.find('ORDER BY')
        limit_pos = query_upper.find('LIMIT')

        # SELECT clause
        if select_pos != -1 and from_pos != -1:
            tokens['select'] = query[select_pos + 6:from_pos].strip()

        # FROM clause
        if from_pos != -1:
            end_pos = self._find_next_clause(query, from_pos + 4, ['WHERE', 'JOIN', 'ORDER BY', 'LIMIT'])
            tokens['from'] = query[from_pos + 4:end_pos].strip()

        # JOIN clause
        if join_pos != -1:
            end_pos = self._find_next_clause(query, join_pos, ['WHERE', 'ORDER BY', 'LIMIT'])
            tokens['join'] = query[join_pos:end_pos].strip()

        # WHERE clause
        if where_pos != -1:
            end_pos = self._find_next_clause(query, where_pos + 5, ['ORDER BY', 'LIMIT'])
            tokens['where'] = query[where_pos + 5:end_pos].strip()

        # ORDER BY clause
        if order_pos != -1:
            end_pos = self._find_next_clause(query, order_pos + 8, ['LIMIT'])
            tokens['order_by'] = query[order_pos + 8:end_pos].strip()

        # LIMIT clause
        if limit_pos != -1:
            tokens['limit'] = query[limit_pos + 5:].strip()

        return tokens

    def _build_base(self, tokens: dict) -> QueryTree:
        """Build base tree (table scan, join, or cartesian product)."""
        if 'join' in tokens:
            return self._parse_join(tokens)

        if 'from' in tokens:
            tables = self._parse_from_clause(tokens['from'])

            if len(tables) == 1:
                return tables[0]
            else:
                tree = tables[0]
                for table in tables[1:]:
                    tree = QueryTree(type=QueryNodeType.CARTESIAN_PRODUCT, value='', children=[tree, table])
                return tree

        return QueryTree(type=QueryNodeType.TABLE, value='unknown', children=[])

    def _parse_from_clause(self, from_clause: str) -> list:
        """Parse FROM clause with multiple tables."""
        tables = []
        parts = from_clause.split(',')

        for part in parts:
            table_tree = self._parse_table_with_alias(part.strip())
            tables.append(table_tree)

        return tables

    def _parse_join(self, tokens: dict) -> QueryTree:
        """Parse JOIN clause."""
        from_tables = self._parse_from_clause(tokens['from'])
        join_clause = tokens['join']

        if 'NATURAL' in join_clause.upper():
            join_table_expr = join_clause.replace('NATURAL', '').replace('JOIN', '').strip()
            join_table = self._parse_table_with_alias(join_table_expr)

            return QueryTree(
                type=QueryNodeType.NATURAL_JOIN,
                value='',
                children=[from_tables[0], join_table]
            )

        parts = join_clause.split('ON')
        join_table_expr = parts[0].replace('JOIN', '').strip()
        join_table = self._parse_table_with_alias(join_table_expr)
        condition = parts[1].strip() if len(parts) > 1 else ''

        return QueryTree(
            type=QueryNodeType.JOIN,
            value=condition,
            children=[from_tables[0], join_table]
        )
