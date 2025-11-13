"""Unit tests for BaseParser utilities."""
import pytest
from src.optimizer.parser.base import BaseParser
from src.core.models.query import QueryNodeType, QueryTree


class TestBaseParser:
    """Test suite for BaseParser utility methods."""

    @pytest.fixture
    def parser(self):
        """Create a BaseParser instance."""
        return BaseParser()

    def test_find_next_clause_single_keyword(self, parser):
        """Test finding next clause with single keyword."""
        query = "SELECT * FROM Employee WHERE salary > 50000"
        pos = parser._find_next_clause(query, 0, ['WHERE'])

        assert pos == query.index('WHERE')

    def test_find_next_clause_multiple_keywords(self, parser):
        """Test finding next clause with multiple keywords."""
        query = "SELECT * FROM Employee WHERE salary > 50000 ORDER BY name"
        pos = parser._find_next_clause(query, 0, ['WHERE', 'ORDER BY'])

        # Should find the earliest keyword (WHERE)
        assert pos == query.index('WHERE')

    def test_find_next_clause_no_match(self, parser):
        """Test finding next clause when no keyword matches."""
        query = "SELECT * FROM Employee"
        pos = parser._find_next_clause(query, 0, ['WHERE', 'ORDER BY'])

        # Should return length of query
        assert pos == len(query)

    def test_find_next_clause_case_insensitive(self, parser):
        """Test that keyword search is case-insensitive."""
        query = "select * from employee where salary > 50000"
        pos = parser._find_next_clause(query, 0, ['WHERE'])

        assert pos == query.upper().index('WHERE')

    def test_find_next_clause_after_start_position(self, parser):
        """Test finding clause after specific start position."""
        query = "SELECT * FROM Employee WHERE salary > 50000 ORDER BY name"
        where_pos = query.index('WHERE')
        pos = parser._find_next_clause(query, where_pos + 1, ['ORDER BY'])

        assert pos == query.index('ORDER BY')

    def test_parse_table_with_alias_using_as(self, parser):
        """Test parsing table with alias using AS keyword."""
        table_expr = "Employee AS e"
        tree = parser._parse_table_with_alias(table_expr)

        assert tree.type == QueryNodeType.TABLE
        assert "Employee" in tree.value
        assert "AS" in tree.value
        assert "e" in tree.value

    def test_parse_table_with_alias_without_as(self, parser):
        """Test parsing table with alias without AS keyword."""
        table_expr = "Employee e"
        tree = parser._parse_table_with_alias(table_expr)

        assert tree.type == QueryNodeType.TABLE
        assert "Employee" in tree.value
        assert "AS" in tree.value
        assert "e" in tree.value

    def test_parse_table_without_alias(self, parser):
        """Test parsing table without alias."""
        table_expr = "Employee"
        tree = parser._parse_table_with_alias(table_expr)

        assert tree.type == QueryNodeType.TABLE
        assert tree.value == "Employee"

    def test_parse_table_preserves_case(self, parser):
        """Test that table name case is preserved."""
        table_expr = "MyTable AS mt"
        tree = parser._parse_table_with_alias(table_expr)

        assert "MyTable" in tree.value
        assert "mt" in tree.value

    def test_extract_aliases_with_as(self, parser):
        """Test extracting aliases with AS keyword."""
        from_clause = "Employee AS e, Department AS d"
        aliases = parser._extract_aliases(from_clause)

        assert 'e' in aliases
        assert aliases['e'] == 'Employee'
        assert 'd' in aliases
        assert aliases['d'] == 'Department'

    def test_extract_aliases_without_as(self, parser):
        """Test extracting aliases without AS keyword."""
        from_clause = "Employee e, Department d"
        aliases = parser._extract_aliases(from_clause)

        assert 'e' in aliases
        assert aliases['e'] == 'Employee'
        assert 'd' in aliases
        assert aliases['d'] == 'Department'

    def test_extract_aliases_mixed(self, parser):
        """Test extracting aliases with mixed AS usage."""
        from_clause = "Employee AS e, Department d"
        aliases = parser._extract_aliases(from_clause)

        assert 'e' in aliases
        assert aliases['e'] == 'Employee'
        assert 'd' in aliases
        assert aliases['d'] == 'Department'

    def test_extract_aliases_no_aliases(self, parser):
        """Test extracting aliases when no aliases present."""
        from_clause = "Employee, Department"
        aliases = parser._extract_aliases(from_clause)

        assert len(aliases) == 0

    def test_extract_aliases_single_table(self, parser):
        """Test extracting alias for single table."""
        from_clause = "Employee AS e"
        aliases = parser._extract_aliases(from_clause)

        assert len(aliases) == 1
        assert aliases['e'] == 'Employee'

    def test_extract_aliases_preserves_case(self, parser):
        """Test that alias extraction preserves case."""
        from_clause = "MyTable AS MT"
        aliases = parser._extract_aliases(from_clause)

        assert 'MT' in aliases
        assert aliases['MT'] == 'MyTable'

    def test_parse_table_with_complex_alias(self, parser):
        """Test parsing table with multi-word expressions."""
        table_expr = "schema.Employee AS e"
        tree = parser._parse_table_with_alias(table_expr)

        assert tree.type == QueryNodeType.TABLE
        assert "schema.Employee" in tree.value

    def test_base_parser_returns_query_tree(self, parser):
        """Test that BaseParser methods return QueryTree objects."""
        table_expr = "Employee"
        tree = parser._parse_table_with_alias(table_expr)

        assert isinstance(tree, QueryTree)
        assert hasattr(tree, 'type')
        assert hasattr(tree, 'value')
        assert hasattr(tree, 'children')

    def test_parse_table_empty_children(self, parser):
        """Test that parsed table has empty children list."""
        table_expr = "Employee AS e"
        tree = parser._parse_table_with_alias(table_expr)

        assert tree.children == []
