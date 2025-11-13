"""Unit tests for InsertParser."""
import pytest
from src.optimizer.parser.insert import InsertParser
from src.core.models.query import QueryNodeType


class TestInsertParser:
    """Test suite for InsertParser."""

    @pytest.fixture
    def parser(self):
        """Create an InsertParser instance."""
        return InsertParser()

    def test_insert_with_columns(self, parser):
        """Test INSERT with column specification."""
        query = "INSERT INTO employee (id, name, salary) VALUES (1, 'John', 50000)"
        tree = parser(query)

        assert tree.type == QueryNodeType.INSERT
        assert "(id, name, salary)" in tree.value
        assert "(1, 'John', 50000)" in tree.value

        # Child is table
        assert len(tree.children) == 1
        assert tree.children[0].type == QueryNodeType.TABLE
        assert tree.children[0].value == "employee"

    def test_insert_without_columns(self, parser):
        """Test INSERT without column specification."""
        query = "INSERT INTO employee VALUES (1, 'John', 50000)"
        tree = parser(query)

        assert tree.type == QueryNodeType.INSERT
        assert "(1, 'John', 50000)" in tree.value

        assert tree.children[0].type == QueryNodeType.TABLE
        assert tree.children[0].value == "employee"

    def test_insert_single_column(self, parser):
        """Test INSERT with single column."""
        query = "INSERT INTO employee (id) VALUES (1)"
        tree = parser(query)

        assert tree.type == QueryNodeType.INSERT
        assert "(id)" in tree.value
        assert "(1)" in tree.value

    def test_insert_string_values(self, parser):
        """Test INSERT with string values."""
        query = "INSERT INTO employee (name, department) VALUES ('Alice', 'Engineering')"
        tree = parser(query)

        assert tree.type == QueryNodeType.INSERT
        assert "'Alice'" in tree.value
        assert "'Engineering'" in tree.value

    def test_insert_numeric_values(self, parser):
        """Test INSERT with numeric values."""
        query = "INSERT INTO employee (id, salary, bonus) VALUES (1, 50000.50, 5000)"
        tree = parser(query)

        assert tree.type == QueryNodeType.INSERT
        assert "50000.50" in tree.value
        assert "5000" in tree.value

    def test_insert_mixed_values(self, parser):
        """Test INSERT with mixed value types."""
        query = "INSERT INTO employee (id, name, salary, active) VALUES (1, 'Bob', 60000, true)"
        tree = parser(query)

        assert tree.type == QueryNodeType.INSERT
        assert "1" in tree.value
        assert "'Bob'" in tree.value
        assert "60000" in tree.value
        assert "true" in tree.value

    def test_insert_case_insensitive(self, parser):
        """Test that INSERT keyword is case-insensitive."""
        queries = [
            "insert into employee (id) values (1)",
            "INSERT INTO employee (id) VALUES (1)",
            "InSeRt InTo employee (id) VaLuEs (1)"
        ]
        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.INSERT

    def test_insert_preserves_table_name_case(self, parser):
        """Test that table name case is preserved."""
        query = "INSERT INTO MyTable (MyColumn) VALUES (123)"
        tree = parser(query)

        assert tree.children[0].value == "MyTable"

    def test_insert_preserves_column_name_case(self, parser):
        """Test that column names case is preserved."""
        query = "INSERT INTO employee (FirstName, LastName) VALUES ('John', 'Doe')"
        tree = parser(query)

        assert "FirstName" in tree.value
        assert "LastName" in tree.value

    def test_insert_with_null_values(self, parser):
        """Test INSERT with NULL values."""
        query = "INSERT INTO employee (id, name, manager_id) VALUES (1, 'John', NULL)"
        tree = parser(query)

        assert tree.type == QueryNodeType.INSERT
        assert "NULL" in tree.value

    def test_insert_extracts_table_name_correctly(self, parser):
        """Test that table name is extracted correctly."""
        queries = [
            ("INSERT INTO employee (id) VALUES (1)", "employee"),
            ("INSERT INTO department (id) VALUES (1)", "department"),
            ("INSERT INTO users (id) VALUES (1)", "users")
        ]
        for query, expected_table in queries:
            tree = parser(query)
            assert tree.children[0].value == expected_table
