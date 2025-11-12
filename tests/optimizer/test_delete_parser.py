"""Unit tests for DeleteParser."""
import pytest
from src.optimizer.parser.delete import DeleteParser
from src.core.models.query import QueryNodeType


class TestDeleteParser:
    """Test suite for DeleteParser."""

    @pytest.fixture
    def parser(self):
        """Create a DeleteParser instance."""
        return DeleteParser()

    def test_simple_delete(self, parser):
        """Test simple DELETE without WHERE."""
        query = "DELETE FROM employee"
        tree = parser(query)

        assert tree.type == QueryNodeType.DELETE
        assert tree.value == ""

        # Child is table
        assert len(tree.children) == 1
        assert tree.children[0].type == QueryNodeType.TABLE
        assert tree.children[0].value == "employee"

    def test_delete_with_where(self, parser):
        """Test DELETE with WHERE clause."""
        query = "DELETE FROM employee WHERE id = 1"
        tree = parser(query)

        assert tree.type == QueryNodeType.DELETE

        # Child is selection
        assert tree.children[0].type == QueryNodeType.SELECTION
        assert tree.children[0].value == "id = 1"

        # Selection child is table
        assert tree.children[0].children[0].type == QueryNodeType.TABLE
        assert tree.children[0].children[0].value == "employee"

    def test_delete_with_string_condition(self, parser):
        """Test DELETE with string condition."""
        query = "DELETE FROM employee WHERE department = 'RnD'"
        tree = parser(query)

        assert tree.type == QueryNodeType.DELETE
        assert tree.children[0].type == QueryNodeType.SELECTION
        assert "department = 'RnD'" in tree.children[0].value

    def test_delete_with_comparison_operators(self, parser):
        """Test DELETE with various comparison operators."""
        queries = [
            "DELETE FROM employee WHERE salary > 50000",
            "DELETE FROM employee WHERE salary >= 50000",
            "DELETE FROM employee WHERE salary < 50000",
            "DELETE FROM employee WHERE salary <= 50000",
            "DELETE FROM employee WHERE salary <> 50000",
            "DELETE FROM employee WHERE salary = 50000"
        ]
        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.DELETE
            assert tree.children[0].type == QueryNodeType.SELECTION

    def test_delete_with_complex_condition(self, parser):
        """Test DELETE with complex WHERE condition."""
        query = "DELETE FROM employee WHERE department = 'Sales' AND salary < 40000"
        tree = parser(query)

        assert tree.type == QueryNodeType.DELETE
        assert tree.children[0].type == QueryNodeType.SELECTION
        assert "AND" in tree.children[0].value

    def test_delete_case_insensitive(self, parser):
        """Test that DELETE keyword is case-insensitive."""
        queries = [
            "delete from employee",
            "DELETE FROM employee",
            "DeLeTe FrOm employee"
        ]
        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.DELETE

    def test_delete_preserves_table_name_case(self, parser):
        """Test that table name case is preserved."""
        query = "DELETE FROM MyTable WHERE id = 1"
        tree = parser(query)

        assert tree.children[0].children[0].value == "MyTable"

    def test_delete_no_children_without_where(self, parser):
        """Test DELETE without WHERE has only table as child."""
        query = "DELETE FROM employee"
        tree = parser(query)

        # Should have only one child (table)
        assert len(tree.children) == 1
        assert tree.children[0].type == QueryNodeType.TABLE
