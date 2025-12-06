"""Unit tests for Index DDL parsers (CREATE INDEX, DROP INDEX)."""
import pytest
from src.optimizer.parser.ddl import CreateIndexParser, DropIndexParser
from src.core.models.query import QueryNodeType


class TestCreateIndexParser:
    """Test suite for CreateIndexParser."""

    @pytest.fixture
    def parser(self):
        """Create a CreateIndexParser instance."""
        return CreateIndexParser()

    def test_create_index_simple(self, parser):
        """Test simple CREATE INDEX."""
        query = "CREATE INDEX idx_employee_name ON employee(name)"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_INDEX
        assert "employee" in tree.value
        assert "name" in tree.value
        assert tree.children == []

    def test_create_index_with_using_btree(self, parser):
        """Test CREATE INDEX with USING BTREE."""
        query = "CREATE INDEX idx_employee_dept ON employee(dept_id) USING BTREE"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_INDEX
        assert "employee" in tree.value
        assert "dept_id" in tree.value
        assert "USING BTREE" in tree.value

    def test_create_index_with_using_hash(self, parser):
        """Test CREATE INDEX with USING HASH."""
        query = "CREATE INDEX idx_user_email ON users(email) USING HASH"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_INDEX
        assert "USING HASH" in tree.value

    def test_create_index_case_insensitive(self, parser):
        """Test that CREATE INDEX keywords are case-insensitive."""
        queries = [
            "create index idx_test on employee(name)",
            "CREATE INDEX idx_test ON employee(name)",
            "CrEaTe InDeX idx_test oN employee(name)"
        ]
        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.CREATE_INDEX

    def test_create_index_preserves_names_case(self, parser):
        """Test that table and column names case is preserved."""
        query = "CREATE INDEX MyIndex ON MyTable(MyColumn)"
        tree = parser(query)

        assert "MyTable" in tree.value
        assert "MyColumn" in tree.value

    def test_create_index_with_spaces(self, parser):
        """Test CREATE INDEX with various spacing."""
        query = "CREATE   INDEX   idx_test   ON   employee  (  name  )"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_INDEX
        assert "employee" in tree.value
        assert "name" in tree.value

    def test_create_index_value_format_no_using(self, parser):
        """Test that CREATE INDEX value is formatted correctly without USING."""
        query = "CREATE INDEX idx_test ON employee(name)"
        tree = parser(query)

        assert tree.value == "employee(name)"

    def test_create_index_value_format_with_using(self, parser):
        """Test that CREATE INDEX value is formatted correctly with USING."""
        query = "CREATE INDEX idx_test ON employee(name) USING BTREE"
        tree = parser(query)

        assert tree.value == "employee(name) USING BTREE"

    def test_create_index_missing_index_keyword(self, parser):
        """Test CREATE INDEX without INDEX keyword raises error."""
        query = "CREATE idx_test ON employee(name)"
        with pytest.raises(ValueError, match="INDEX keyword not found"):
            parser(query)

    def test_create_index_missing_on_keyword(self, parser):
        """Test CREATE INDEX without ON keyword raises error."""
        query = "CREATE INDEX idx_test employee(name)"
        with pytest.raises(ValueError, match="ON keyword not found"):
            parser(query)

    def test_create_index_missing_index_name(self, parser):
        """Test CREATE INDEX without index name still works (index name is optional/ignored)."""
        query = "CREATE INDEX ON employee(name)"
        tree = parser(query)
        assert tree.type == QueryNodeType.CREATE_INDEX
        assert "employee" in tree.value
        assert "name" in tree.value

    def test_create_index_missing_table_name(self, parser):
        """Test CREATE INDEX without table name raises error."""
        query = "CREATE INDEX idx_test ON (name)"
        with pytest.raises(ValueError, match="table name is required"):
            parser(query)

    def test_create_index_missing_column_parentheses(self, parser):
        """Test CREATE INDEX without column parentheses raises error."""
        query = "CREATE INDEX idx_test ON employee"
        with pytest.raises(ValueError, match="column specification.*not found"):
            parser(query)

    def test_create_index_missing_closing_parenthesis(self, parser):
        """Test CREATE INDEX without closing parenthesis raises error."""
        query = "CREATE INDEX idx_test ON employee(name"
        with pytest.raises(ValueError, match="closing parenthesis.*not found"):
            parser(query)

    def test_create_index_missing_column_name(self, parser):
        """Test CREATE INDEX without column name raises error."""
        query = "CREATE INDEX idx_test ON employee()"
        with pytest.raises(ValueError, match="column name is required"):
            parser(query)

    def test_create_index_empty_children(self, parser):
        """Test that CREATE INDEX has no children."""
        query = "CREATE INDEX idx_test ON employee(name)"
        tree = parser(query)

        assert len(tree.children) == 0

    def test_create_index_complex_names(self, parser):
        """Test CREATE INDEX with complex naming."""
        query = "CREATE INDEX idx_employee_dept_salary ON employee_records(department_id) USING BTREE"
        tree = parser(query)

        assert tree.type == QueryNodeType.CREATE_INDEX
        assert "employee_records" in tree.value
        assert "department_id" in tree.value


class TestDropIndexParser:
    """Test suite for DropIndexParser."""

    @pytest.fixture
    def parser(self):
        """Create a DropIndexParser instance."""
        return DropIndexParser()

    def test_drop_index_simple(self, parser):
        """Test simple DROP INDEX."""
        query = "DROP INDEX idx_employee_name ON employee(name)"
        tree = parser(query)

        assert tree.type == QueryNodeType.DROP_INDEX
        assert "employee" in tree.value
        assert "name" in tree.value
        assert tree.children == []

    def test_drop_index_with_on_clause(self, parser):
        """Test DROP INDEX with ON clause."""
        query = "DROP INDEX idx_employee_name ON employee(name)"
        tree = parser(query)

        assert tree.type == QueryNodeType.DROP_INDEX
        assert "employee" in tree.value
        assert "name" in tree.value

    def test_drop_index_case_insensitive(self, parser):
        """Test that DROP INDEX keywords are case-insensitive."""
        queries = [
            "drop index idx_test on employee(name)",
            "DROP INDEX idx_test ON employee(name)",
            "DrOp InDeX idx_test oN employee(name)"
        ]
        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.DROP_INDEX

    def test_drop_index_preserves_names_case(self, parser):
        """Test that table and column names case is preserved."""
        query = "DROP INDEX MyIndex ON MyTable(MyColumn)"
        tree = parser(query)

        assert "MyTable" in tree.value
        assert "MyColumn" in tree.value

    def test_drop_index_with_spaces(self, parser):
        """Test DROP INDEX with various spacing."""
        query = "DROP   INDEX   idx_test   ON   employee(name)"
        tree = parser(query)

        assert tree.type == QueryNodeType.DROP_INDEX
        assert "employee" in tree.value
        assert "name" in tree.value

    def test_drop_index_value_format_no_on(self, parser):
        """Test that DROP INDEX without ON raises error."""
        query = "DROP INDEX idx_test"
        with pytest.raises(ValueError, match="ON clause not found"):
            parser(query)

    def test_drop_index_value_format_with_on(self, parser):
        """Test that DROP INDEX value is formatted correctly with ON."""
        query = "DROP INDEX idx_test ON employee(name)"
        tree = parser(query)

        assert tree.value == "employee(name)"

    def test_drop_index_missing_index_keyword(self, parser):
        """Test DROP INDEX without INDEX keyword raises error."""
        query = "DROP idx_test"
        with pytest.raises(ValueError, match="INDEX keyword not found"):
            parser(query)

    def test_drop_index_missing_index_name(self, parser):
        """Test DROP INDEX without index name still works (index name optional/ignored)."""
        query = "DROP INDEX ON employee(name)"
        tree = parser(query)
        assert tree.type == QueryNodeType.DROP_INDEX
        assert "employee" in tree.value

    def test_drop_index_missing_index_name_with_on(self, parser):
        """Test DROP INDEX with ON but missing column specification."""
        query = "DROP INDEX ON employee"
        with pytest.raises(ValueError, match="column specification.*not found"):
            parser(query)

    def test_drop_index_missing_table_name_after_on(self, parser):
        """Test DROP INDEX with ON but no table name raises error."""
        query = "DROP INDEX idx_test ON"
        with pytest.raises(ValueError, match="table name is required after ON"):
            parser(query)

    def test_drop_index_empty_children(self, parser):
        """Test that DROP INDEX has no children."""
        query = "DROP INDEX idx_test ON employee(name)"
        tree = parser(query)

        assert len(tree.children) == 0

    def test_drop_index_complex_names(self, parser):
        """Test DROP INDEX with complex naming."""
        query = "DROP INDEX idx_employee_dept_salary ON employee_records(department_id)"
        tree = parser(query)

        assert tree.type == QueryNodeType.DROP_INDEX
        assert "employee_records" in tree.value
        assert "department_id" in tree.value
