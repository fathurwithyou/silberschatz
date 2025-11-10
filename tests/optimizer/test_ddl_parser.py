"""Unit tests for DDL parsers (CREATE, DROP)."""
import pytest
from src.optimizer.parser.ddl import CreateParser, DropParser
from src.core.models.query import QueryNodeType


class TestCreateParser:
    """Test suite for CreateParser."""

    @pytest.fixture
    def parser(self):
        """Create a CreateParser instance."""
        return CreateParser()

    def test_create_table_simple(self, parser):
        """Test simple CREATE TABLE."""
        query = "CREATE TABLE employee (id integer, name varchar(100))"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_TABLE
        assert "employee" in tree.value
        assert "(id integer, name varchar(100))" in tree.value
        assert tree.children == []

    def test_create_table_with_primary_key(self, parser):
        """Test CREATE TABLE with PRIMARY KEY."""
        query = "CREATE TABLE employee (id integer PRIMARY KEY, name varchar(100))"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_TABLE
        assert "PRIMARY KEY" in tree.value

    def test_create_table_with_foreign_key(self, parser):
        """Test CREATE TABLE with FOREIGN KEY."""
        query = "CREATE TABLE employee (id integer, dept_id integer, FOREIGN KEY (dept_id) REFERENCES department(id))"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_TABLE
        assert "FOREIGN KEY" in tree.value
        assert "REFERENCES" in tree.value

    def test_create_table_multiple_columns(self, parser):
        """Test CREATE TABLE with multiple columns."""
        query = "CREATE TABLE employee (id integer, name varchar(100), salary float, hire_date char(10))"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_TABLE
        assert "integer" in tree.value
        assert "varchar(100)" in tree.value
        assert "float" in tree.value
        assert "char(10)" in tree.value

    def test_create_table_case_insensitive(self, parser):
        """Test that CREATE TABLE keyword is case-insensitive."""
        queries = [
            "create table employee (id integer)",
            "CREATE TABLE employee (id integer)",
            "CrEaTe TaBlE employee (id integer)"
        ]
        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.CREATE_TABLE

    def test_create_table_preserves_table_name_case(self, parser):
        """Test that table name case is preserved."""
        query = "CREATE TABLE MyTable (id integer)"
        tree = parser(query)

        assert "MyTable" in tree.value

    def test_create_table_preserves_column_name_case(self, parser):
        """Test that column names case is preserved."""
        query = "CREATE TABLE employee (EmployeeID integer, FirstName varchar(100))"
        tree = parser(query)

        assert "EmployeeID" in tree.value
        assert "FirstName" in tree.value

    def test_create_table_with_constraints(self, parser):
        """Test CREATE TABLE with various constraints."""
        query = "CREATE TABLE employee (id integer PRIMARY KEY NOT NULL, email varchar(100) UNIQUE)"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_TABLE
        assert "NOT NULL" in tree.value
        assert "UNIQUE" in tree.value


class TestDropParser:
    """Test suite for DropParser."""

    @pytest.fixture
    def parser(self):
        """Create a DropParser instance."""
        return DropParser()

    def test_drop_table_simple(self, parser):
        """Test simple DROP TABLE."""
        query = "DROP TABLE employee"
        tree = parser(query)

        assert tree.type == QueryNodeType.DROP_TABLE
        assert "employee" in tree.value
        assert tree.children == []

    def test_drop_table_with_cascade(self, parser):
        """Test DROP TABLE with CASCADE."""
        query = "DROP TABLE employee CASCADE"
        tree = parser(query)

        assert tree.type == QueryNodeType.DROP_TABLE
        assert "employee" in tree.value
        assert "CASCADE" in tree.value

    def test_drop_table_with_restrict(self, parser):
        """Test DROP TABLE with RESTRICT."""
        query = "DROP TABLE employee RESTRICT"
        tree = parser(query)

        assert tree.type == QueryNodeType.DROP_TABLE
        assert "employee" in tree.value
        assert "RESTRICT" in tree.value

    def test_drop_table_case_insensitive(self, parser):
        """Test that DROP TABLE keyword is case-insensitive."""
        queries = [
            "drop table employee",
            "DROP TABLE employee",
            "DrOp TaBlE employee"
        ]
        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.DROP_TABLE

    def test_drop_table_preserves_table_name_case(self, parser):
        """Test that table name case is preserved."""
        query = "DROP TABLE MyTable"
        tree = parser(query)

        assert "MyTable" in tree.value

    def test_drop_table_cascade_case_insensitive(self, parser):
        """Test that CASCADE keyword is case-insensitive."""
        queries = [
            "DROP TABLE employee cascade",
            "DROP TABLE employee CASCADE",
            "DROP TABLE employee CaScAdE"
        ]
        for query in queries:
            tree = parser(query)
            assert "CASCADE" in tree.value

    def test_drop_table_no_modifier(self, parser):
        """Test DROP TABLE without CASCADE or RESTRICT."""
        query = "DROP TABLE employee"
        tree = parser(query)

        assert "CASCADE" not in tree.value
        assert "RESTRICT" not in tree.value

    def test_drop_table_value_format(self, parser):
        """Test that DROP TABLE value is formatted correctly."""
        query = "DROP TABLE employee CASCADE"
        tree = parser(query)

        # Value should be "employee CASCADE"
        assert tree.value == "employee CASCADE"

    def test_drop_table_empty_children(self, parser):
        """Test that DROP TABLE has no children."""
        query = "DROP TABLE employee"
        tree = parser(query)

        assert len(tree.children) == 0
