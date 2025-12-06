"""Unit tests for QueryParser with CREATE INDEX and DROP INDEX."""
import pytest
from src.optimizer.parser.query import QueryParser
from src.core.models.query import QueryNodeType


class TestQueryParserIndex:
    """Test suite for QueryParser with index DDL operations."""

    @pytest.fixture
    def parser(self):
        """Create a QueryParser instance."""
        return QueryParser()

    def test_query_parser_create_index(self, parser):
        """Test QueryParser correctly parses CREATE INDEX."""
        query = "CREATE INDEX idx_employee_name ON employee(name)"
        parsed = parser(query)

        assert parsed.tree.type == QueryNodeType.CREATE_INDEX
        assert "employee" in parsed.tree.value
        assert "name" in parsed.tree.value

    def test_query_parser_create_index_with_using(self, parser):
        """Test QueryParser correctly parses CREATE INDEX with USING."""
        query = "CREATE INDEX idx_test ON users(id) USING BTREE"
        parsed = parser(query)

        assert parsed.tree.type == QueryNodeType.CREATE_INDEX
        assert "USING BTREE" in parsed.tree.value

    def test_query_parser_drop_index(self, parser):
        """Test QueryParser correctly parses DROP INDEX."""
        query = "DROP INDEX idx_employee_name ON employee(name)"
        parsed = parser(query)

        assert parsed.tree.type == QueryNodeType.DROP_INDEX
        assert "employee" in parsed.tree.value
        assert "name" in parsed.tree.value

    def test_query_parser_drop_index_with_on(self, parser):
        """Test QueryParser correctly parses DROP INDEX with ON clause."""
        query = "DROP INDEX idx_test ON employee(dept_id)"
        parsed = parser(query)

        assert parsed.tree.type == QueryNodeType.DROP_INDEX
        assert "employee" in parsed.tree.value
        assert "dept_id" in parsed.tree.value

    def test_query_parser_create_table_still_works(self, parser):
        """Test QueryParser still correctly identifies CREATE TABLE."""
        query = "CREATE TABLE employee (id integer, name varchar(100))"
        parsed = parser(query)

        assert parsed.tree.type == QueryNodeType.CREATE_TABLE
        assert "employee" in parsed.tree.value

    def test_query_parser_drop_table_still_works(self, parser):
        """Test QueryParser still correctly identifies DROP TABLE."""
        query = "DROP TABLE employee"
        parsed = parser(query)

        assert parsed.tree.type == QueryNodeType.DROP_TABLE
        assert "employee" in parsed.tree.value

    def test_query_parser_detects_create_index_vs_create_table(self, parser):
        """Test QueryParser correctly distinguishes CREATE INDEX from CREATE TABLE."""
        create_index = "CREATE INDEX idx_test ON employee(name)"
        create_table = "CREATE TABLE employee (id integer)"

        parsed_index = parser(create_index)
        parsed_table = parser(create_table)

        assert parsed_index.tree.type == QueryNodeType.CREATE_INDEX
        assert parsed_table.tree.type == QueryNodeType.CREATE_TABLE

    def test_query_parser_detects_drop_index_vs_drop_table(self, parser):
        """Test QueryParser correctly distinguishes DROP INDEX from DROP TABLE."""
        drop_index = "DROP INDEX idx_test ON employee(name)"
        drop_table = "DROP TABLE employee"

        parsed_index = parser(drop_index)
        parsed_table = parser(drop_table)

        assert parsed_index.tree.type == QueryNodeType.DROP_INDEX
        assert parsed_table.tree.type == QueryNodeType.DROP_TABLE

    def test_query_parser_create_index_with_semicolon(self, parser):
        """Test QueryParser strips semicolon from CREATE INDEX."""
        query = "CREATE INDEX idx_test ON employee(name);"
        parsed = parser(query)

        assert parsed.tree.type == QueryNodeType.CREATE_INDEX
        assert parsed.query == "CREATE INDEX idx_test ON employee(name)"

    def test_query_parser_drop_index_with_semicolon(self, parser):
        """Test QueryParser strips semicolon from DROP INDEX."""
        query = "DROP INDEX idx_test ON employee(name);"
        parsed = parser(query)

        assert parsed.tree.type == QueryNodeType.DROP_INDEX
        assert parsed.query == "DROP INDEX idx_test ON employee(name)"

    def test_query_parser_create_index_case_insensitive(self, parser):
        """Test QueryParser handles case-insensitive CREATE INDEX."""
        queries = [
            "create index idx_test on employee(name)",
            "CREATE INDEX idx_test ON employee(name)",
            "CrEaTe InDeX idx_test oN employee(name)"
        ]
        for query in queries:
            parsed = parser(query)
            assert parsed.tree.type == QueryNodeType.CREATE_INDEX

    def test_query_parser_drop_index_case_insensitive(self, parser):
        """Test QueryParser handles case-insensitive DROP INDEX."""
        queries = [
            "drop index idx_test on employee(name)",
            "DROP INDEX idx_test ON employee(name)",
            "DrOp InDeX idx_test oN employee(name)"
        ]
        for query in queries:
            parsed = parser(query)
            assert parsed.tree.type == QueryNodeType.DROP_INDEX
