"""Unit tests for UpdateParser."""
import pytest
from src.optimizer.parser.update import UpdateParser
from src.core.models.query import QueryNodeType


class TestUpdateParser:
    """Test suite for UpdateParser."""

    @pytest.fixture
    def parser(self):
        """Create an UpdateParser instance."""
        return UpdateParser()

    def test_simple_update(self, parser):
        """Test simple UPDATE without WHERE."""
        query = "UPDATE employee SET salary = 50000"
        tree = parser(query)

        assert tree.type == QueryNodeType.UPDATE
        assert tree.value == "salary = 50000"

        # Child is table
        assert len(tree.children) == 1
        assert tree.children[0].type == QueryNodeType.TABLE
        assert tree.children[0].value == "employee"

    def test_update_with_where(self, parser):
        """Test UPDATE with WHERE clause."""
        query = "UPDATE employee SET salary = 60000 WHERE id = 1"
        tree = parser(query)

        assert tree.type == QueryNodeType.UPDATE
        assert tree.value == "salary = 60000"

        # Child is selection
        assert tree.children[0].type == QueryNodeType.SELECTION
        assert tree.children[0].value == "id = 1"

        # Selection child is table
        assert tree.children[0].children[0].type == QueryNodeType.TABLE
        assert tree.children[0].children[0].value == "employee"

    def test_update_with_expression(self, parser):
        """Test UPDATE with expression in SET clause."""
        query = "UPDATE employee SET salary = 1.05 * salary WHERE salary > 1000"
        tree = parser(query)

        assert tree.type == QueryNodeType.UPDATE
        assert "1.05 * salary" in tree.value

        assert tree.children[0].type == QueryNodeType.SELECTION
        assert tree.children[0].value == "salary > 1000"

    def test_update_multiple_columns(self, parser):
        """Test UPDATE with multiple columns."""
        query = "UPDATE employee SET salary = 50000, bonus = 5000"
        tree = parser(query)

        assert tree.type == QueryNodeType.UPDATE
        assert "salary = 50000" in tree.value
        assert "bonus = 5000" in tree.value

    def test_update_with_complex_where(self, parser):
        """Test UPDATE with complex WHERE condition."""
        query = "UPDATE employee SET salary = 70000 WHERE department = 'Engineering' AND salary < 60000"
        tree = parser(query)

        assert tree.type == QueryNodeType.UPDATE
        assert tree.children[0].type == QueryNodeType.SELECTION
        assert "department = 'Engineering'" in tree.children[0].value
        assert "AND" in tree.children[0].value

    def test_update_case_insensitive(self, parser):
        """Test that UPDATE keyword is case-insensitive."""
        queries = [
            "update employee set salary = 50000",
            "UPDATE employee SET salary = 50000",
            "UpDaTe employee SeT salary = 50000"
        ]
        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.UPDATE

    def test_update_preserves_table_name_case(self, parser):
        """Test that table name case is preserved."""
        query = "UPDATE MyTable SET MyColumn = 123"
        tree = parser(query)

        assert tree.children[0].value == "MyTable"
