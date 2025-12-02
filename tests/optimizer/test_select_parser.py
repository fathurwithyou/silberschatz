"""Unit tests for SelectParser."""
import pytest
from src.optimizer.parser.select import SelectParser
from src.core.models.query import QueryNodeType, QueryTree


class TestSelectParser:
    """Test suite for SelectParser."""

    @pytest.fixture
    def parser(self):
        """Create a SelectParser instance."""
        return SelectParser()

    def test_simple_select(self, parser):
        """Test simple SELECT query."""
        query = "SELECT name FROM Employee"
        tree = parser(query)

        # Root should be projection
        assert tree.type == QueryNodeType.PROJECTION
        assert tree.value == "name"

        # Child should be table
        assert len(tree.children) == 1
        assert tree.children[0].type == QueryNodeType.TABLE
        assert tree.children[0].value == "Employee"

    def test_select_all(self, parser):
        """Test SELECT * query."""
        query = "SELECT * FROM Employee"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION
        assert tree.value == "*"

    def test_select_multiple_columns(self, parser):
        """Test SELECT with multiple columns."""
        query = "SELECT name, age, salary FROM Employee"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION
        assert "name," in tree.value
        assert "age," in tree.value
        assert "salary" in tree.value

    def test_select_with_where(self, parser):
        """Test SELECT with WHERE clause."""
        query = "SELECT name FROM Employee WHERE salary > 50000"
        tree = parser(query)

        # Root is projection
        assert tree.type == QueryNodeType.PROJECTION

        # First child is selection
        assert tree.children[0].type == QueryNodeType.SELECTION
        assert tree.children[0].value == "salary > 50000"

        # Selection child is table
        assert tree.children[0].children[0].type == QueryNodeType.TABLE

    def test_select_with_join(self, parser):
        """Test SELECT with JOIN ON."""
        query = "SELECT * FROM Employee JOIN Department ON Employee.dept_id = Department.id"
        tree = parser(query)

        # Root is projection
        assert tree.type == QueryNodeType.PROJECTION

        # Child is join
        assert tree.children[0].type == QueryNodeType.JOIN
        assert "Employee.dept_id = Department.id" in tree.children[0].value

        # Join has two children (left and right tables)
        assert len(tree.children[0].children) == 2
        assert tree.children[0].children[0].type == QueryNodeType.TABLE
        assert tree.children[0].children[1].type == QueryNodeType.TABLE

    def test_select_with_natural_join(self, parser):
        """Test SELECT with NATURAL JOIN."""
        query = "SELECT * FROM Employee NATURAL JOIN Department"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION
        assert tree.children[0].type == QueryNodeType.NATURAL_JOIN
        assert tree.children[0].value == ""

        # Verify structure: NATURAL JOIN has two children
        join_node = tree.children[0]
        assert len(join_node.children) == 2
        assert join_node.children[0].type == QueryNodeType.TABLE
        assert join_node.children[1].type == QueryNodeType.TABLE

    def test_natural_join_with_alias(self, parser):
        """Test NATURAL JOIN with table aliases."""
        query = "SELECT * FROM employees AS e NATURAL JOIN departments AS d"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION
        join_node = tree.children[0]
        assert join_node.type == QueryNodeType.NATURAL_JOIN

        # Both tables should have aliases
        assert "AS" in join_node.children[0].value
        assert "AS" in join_node.children[1].value

    def test_multiple_natural_joins(self, parser):
        """Test chained NATURAL JOINs."""
        query = "SELECT * FROM employees NATURAL JOIN departments NATURAL JOIN locations"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION

        # Root join should be NATURAL_JOIN
        first_join = tree.children[0]
        assert first_join.type == QueryNodeType.NATURAL_JOIN

        # Left child should also be a NATURAL_JOIN (chained)
        left_child = first_join.children[0]
        assert left_child.type == QueryNodeType.NATURAL_JOIN

        # Verify the chain: (employees NATURAL JOIN departments) NATURAL JOIN locations
        assert left_child.children[0].type == QueryNodeType.TABLE  # employees
        assert left_child.children[1].type == QueryNodeType.TABLE  # departments
        assert first_join.children[1].type == QueryNodeType.TABLE  # locations

    def test_natural_join_case_insensitive(self, parser):
        """Test that NATURAL JOIN parsing is case-insensitive."""
        queries = [
            "SELECT * FROM employees NATURAL JOIN departments",
            "SELECT * FROM employees natural join departments",
            "SELECT * FROM employees Natural Join departments",
        ]

        for query in queries:
            tree = parser(query)
            assert tree.type == QueryNodeType.PROJECTION
            assert tree.children[0].type == QueryNodeType.NATURAL_JOIN

    def test_natural_join_with_where(self, parser):
        """Test NATURAL JOIN combined with WHERE clause."""
        query = "SELECT * FROM employees NATURAL JOIN departments WHERE salary > 50000"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION

        # Should have selection node
        selection_node = tree.children[0]
        assert selection_node.type == QueryNodeType.SELECTION
        assert selection_node.value == "salary > 50000"

        # Selection's child should be NATURAL_JOIN
        join_node = selection_node.children[0]
        assert join_node.type == QueryNodeType.NATURAL_JOIN

    def test_select_with_order_by(self, parser):
        """Test SELECT with ORDER BY."""
        query = "SELECT name FROM Employee ORDER BY salary DESC"
        tree = parser(query)

        # Root should be projection (ORDER BY is evaluated before projection to access all columns)
        assert tree.type == QueryNodeType.PROJECTION
        assert tree.value == "name"

        # Child is ORDER BY
        assert tree.children[0].type == QueryNodeType.ORDER_BY
        assert tree.children[0].value == "salary DESC"

    def test_select_with_limit(self, parser):
        """Test SELECT with LIMIT."""
        query = "SELECT name FROM Employee LIMIT 10"
        tree = parser(query)

        assert tree.type == QueryNodeType.LIMIT
        assert tree.value == "10"

        # Child is projection
        assert tree.children[0].type == QueryNodeType.PROJECTION

    def test_select_with_all_clauses(self, parser):
        """Test SELECT with WHERE, ORDER BY, and LIMIT."""
        query = "SELECT name FROM Employee WHERE salary > 50000 ORDER BY salary DESC LIMIT 10"
        tree = parser(query)

        # Top level is LIMIT
        assert tree.type == QueryNodeType.LIMIT
        assert tree.value == "10"

        # Next is PROJECTION
        assert tree.children[0].type == QueryNodeType.PROJECTION
        assert tree.children[0].value == "name"

        # Next is ORDER BY (evaluated before projection to access all columns)
        assert tree.children[0].children[0].type == QueryNodeType.ORDER_BY
        assert tree.children[0].children[0].value == "salary DESC"

        # Next is SELECTION
        assert tree.children[0].children[0].children[0].type == QueryNodeType.SELECTION
        assert tree.children[0].children[0].children[0].value == "salary > 50000"

    def test_select_cartesian_product(self, parser):
        """Test SELECT from multiple tables (cartesian product)."""
        query = "SELECT * FROM Employee, Department"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION

        # Child is cartesian product
        assert tree.children[0].type == QueryNodeType.CARTESIAN_PRODUCT

        # Has two children (tables)
        assert len(tree.children[0].children) == 2
        assert tree.children[0].children[0].type == QueryNodeType.TABLE
        assert tree.children[0].children[1].type == QueryNodeType.TABLE

    def test_select_with_table_alias(self, parser):
        """Test SELECT with table alias using AS."""
        query = "SELECT * FROM Employee AS e"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION

        # Table has alias
        assert tree.children[0].type == QueryNodeType.TABLE
        assert "AS" in tree.children[0].value
        assert "Employee" in tree.children[0].value
        assert "e" in tree.children[0].value

    def test_select_with_table_alias_no_as(self, parser):
        """Test SELECT with table alias without AS keyword."""
        query = "SELECT * FROM Employee e"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION
        assert tree.children[0].type == QueryNodeType.TABLE
        assert "AS" in tree.children[0].value

    def test_select_multiple_tables_with_aliases(self, parser):
        """Test SELECT from multiple tables with aliases."""
        query = "SELECT * FROM student AS s, lecturer AS l WHERE s.lecturer_id = l.id"
        tree = parser(query)

        assert tree.type == QueryNodeType.PROJECTION

        # Has selection
        assert tree.children[0].type == QueryNodeType.SELECTION
        assert tree.children[0].value == "s.lecturer_id = l.id"

        # Selection child is cartesian product
        assert tree.children[0].children[0].type == QueryNodeType.CARTESIAN_PRODUCT

    def test_tokenize_select_clause(self, parser):
        """Test tokenization of SELECT clause."""
        query = "SELECT name, age FROM Employee"
        tokens = parser._tokenize(query)

        assert 'select' in tokens
        assert tokens['select'] == "name, age"
        assert 'from' in tokens
        assert tokens['from'] == "Employee"

    def test_tokenize_all_clauses(self, parser):
        """Test tokenization of all clauses."""
        query = "SELECT * FROM Employee WHERE salary > 50000 ORDER BY name LIMIT 5"
        tokens = parser._tokenize(query)

        assert tokens['select'] == "*"
        assert tokens['from'] == "Employee"
        assert tokens['where'] == "salary > 50000"
        assert tokens['order_by'] == "name"
        assert tokens['limit'] == "5"

    def test_case_sensitivity_preserved(self, parser):
        """Test that column/table names preserve case."""
        query = "SELECT MyColumn FROM MyTable WHERE MyCondition = 'Value'"
        tree = parser(query)

        # Projection value preserves case
        assert tree.value == "MyColumn"

        # Table name preserves case
        table_node = tree.children[0].children[0]
        assert table_node.value == "MyTable"

        # Condition preserves case
        selection_node = tree.children[0]
        assert "MyCondition" in selection_node.value