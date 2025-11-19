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

        join_keywords = ['NATURAL JOIN', 'JOIN']
        join_pos = -1
        for keyword in join_keywords:
            pos = query_upper.find(keyword, from_pos if from_pos != -1 else 0)
            if pos != -1 and (join_pos == -1 or pos < join_pos):
                join_pos = pos

        order_pos = query_upper.find('ORDER BY')
        limit_pos = query_upper.find('LIMIT')

        # SELECT clause
        if select_pos != -1 and from_pos != -1:
            tokens['select'] = query[select_pos + 6:from_pos].strip()

        # FROM clause
        if from_pos != -1:
            end_pos = self._find_next_clause(query, from_pos + 4, ['WHERE', 'NATURAL JOIN', 'JOIN', 'ORDER BY', 'LIMIT'])
            tokens['from'] = query[from_pos + 4:end_pos].strip()

        # JOIN clause (including all chained JOINs)
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
        """Parse JOIN clause, supporting multiple and chained JOINs."""
        from_tables = self._parse_from_clause(tokens['from'])
        join_clause = tokens['join']

        result_tree = from_tables[0]

        if len(from_tables) > 1:
            for table in from_tables[1:]:
                result_tree = QueryTree(
                    type=QueryNodeType.CARTESIAN_PRODUCT,
                    value='',
                    children=[result_tree, table]
                )

        result_tree = self._parse_join_chain(result_tree, join_clause)

        return result_tree

    def _parse_join_chain(self, base_tree: QueryTree, join_clause: str) -> QueryTree:
        """Parse chained JOIN operations (including multiple NATURAL JOINs)."""
        import re

        current_tree = base_tree
        join_clause_upper = join_clause.upper()

        join_pattern = r'(?:NATURAL\s+)?JOIN'

        join_matches = list(re.finditer(join_pattern, join_clause_upper))

        if not join_matches:
            return current_tree

        # Process each JOIN
        for i, match in enumerate(join_matches):
            join_start = match.start()

            if i + 1 < len(join_matches):
                clause_end = join_matches[i + 1].start()
            else:
                clause_end = len(join_clause)

            full_join_clause = join_clause[join_start:clause_end].strip()

            current_tree = self._parse_single_join(current_tree, full_join_clause)

        return current_tree

    def _parse_single_join(self, left_tree: QueryTree, join_clause: str) -> QueryTree:
        """Parse a single JOIN operation."""
        join_clause_upper = join_clause.upper()

        if 'NATURAL' in join_clause_upper:
            table_start = join_clause_upper.find('NATURAL JOIN') + len('NATURAL JOIN')
            table_expr = join_clause[table_start:].strip()

            if ' ON ' in table_expr.upper():
                table_expr = table_expr.split(' ON ')[0].strip()

            join_table = self._parse_table_with_alias(table_expr)

            return QueryTree(
                type=QueryNodeType.NATURAL_JOIN,
                value='',
                children=[left_tree, join_table]
            )

        if ' ON ' in join_clause_upper:
            parts = join_clause.split(' ON ', 1)
            if len(parts) == 2:
                # Extract table from first part (remove JOIN keyword)
                table_part = parts[0].replace('JOIN', '').replace('join', '').strip()

                join_table = self._parse_table_with_alias(table_part)
                condition = parts[1].strip()

                return QueryTree(
                    type=QueryNodeType.JOIN,
                    value=condition,
                    children=[left_tree, join_table]
                )

        raise SyntaxError(f"JOIN requires ON clause: {join_clause}")
