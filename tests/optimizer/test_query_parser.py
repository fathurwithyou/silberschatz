"""Unit tests for QueryParser main class."""
import pytest
from src.optimizer.parser import QueryParser
from src.core.models.query import QueryNodeType, ParsedQuery, QueryTree


class TestQueryParser:
    """Test suite for QueryParser main class."""

    @pytest.fixture
    def parser(self):
        """Create a QueryParser instance."""
        return QueryParser()

    def test_parser_is_callable(self, parser):
        """Test that parser can be called directly."""
        query = "SELECT * FROM Employee"
        result = parser(query)
        assert isinstance(result, ParsedQuery)
        assert result.query == query

    def test_detect_select_statement(self, parser):
        """Test detection of SELECT statement."""
        result = parser("SELECT name FROM Employee")
        assert result.tree.type == QueryNodeType.PROJECTION

    def test_detect_update_statement(self, parser):
        """Test detection of UPDATE statement."""
        result = parser("UPDATE employee SET salary = 50000")
        assert result.tree.type == QueryNodeType.UPDATE

    def test_detect_delete_statement(self, parser):
        """Test detection of DELETE statement."""
        result = parser("DELETE FROM employee WHERE id = 1")
        assert result.tree.type == QueryNodeType.DELETE

    def test_detect_insert_statement(self, parser):
        """Test detection of INSERT statement."""
        result = parser("INSERT INTO employee (id, name) VALUES (1, 'John')")
        assert result.tree.type == QueryNodeType.INSERT

    def test_detect_create_statement(self, parser):
        """Test detection of CREATE TABLE statement."""
        result = parser("CREATE TABLE employee (id integer PRIMARY KEY)")
        assert result.tree.type == QueryNodeType.CREATE_TABLE

    def test_detect_drop_statement(self, parser):
        """Test detection of DROP TABLE statement."""
        result = parser("DROP TABLE employee")
        assert result.tree.type == QueryNodeType.DROP_TABLE

    def test_detect_begin_transaction(self, parser):
        """Test detection of BEGIN TRANSACTION statement."""
        result = parser("BEGIN TRANSACTION")
        assert result.tree.type == QueryNodeType.BEGIN_TRANSACTION
        assert result.tree.value == ''
        assert result.tree.children == []

    def test_detect_commit(self, parser):
        """Test detection of COMMIT statement."""
        result = parser("COMMIT")
        assert result.tree.type == QueryNodeType.COMMIT
        assert result.tree.value == ''
        assert result.tree.children == []

    def test_unknown_statement(self, parser):
        """Test handling of unknown statement."""
        query = "INVALID SQL STATEMENT"
        result = parser(query)
        assert result.tree.type == QueryNodeType.UNKNOWN
        assert result.tree.value == query

    def test_strip_semicolon(self, parser):
        """Test that semicolons are stripped from query."""
        result = parser("SELECT * FROM Employee;")
        assert result.query == "SELECT * FROM Employee"

    def test_strip_whitespace(self, parser):
        """Test that whitespace is stripped from query."""
        result = parser("  SELECT * FROM Employee  ")
        assert result.query == "SELECT * FROM Employee"

    def test_case_insensitive_statement_detection(self, parser):
        """Test that statement type detection is case-insensitive."""
        queries = [
            "select * from employee",
            "SELECT * FROM Employee",
            "SeLeCt * FrOm EmPlOyEe"
        ]
        for query in queries:
            result = parser(query)
            assert result.tree.type == QueryNodeType.PROJECTION

    def test_multiple_parsers_same_instance(self, parser):
        """Test parsing multiple queries with same parser instance."""
        queries = [
            "SELECT * FROM Employee",
            "UPDATE employee SET salary = 50000",
            "DELETE FROM employee WHERE id = 1"
        ]
        results = [parser(q) for q in queries]

        assert results[0].tree.type == QueryNodeType.PROJECTION
        assert results[1].tree.type == QueryNodeType.UPDATE
        assert results[2].tree.type == QueryNodeType.DELETE

    def test_parser_uses_correct_subparser(self, parser):
        """Test that QueryParser delegates to correct subparser."""
        # Verify that subparsers are initialized
        assert parser.select_parser is not None
        assert parser.update_parser is not None
        assert parser.delete_parser is not None
        assert parser.insert_parser is not None
        assert parser.create_parser is not None
        assert parser.drop_parser is not None
