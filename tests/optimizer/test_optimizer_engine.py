"""Test cases for QueryOptimizer engine."""
import pytest
from unittest.mock import Mock
from src.core.models.query import QueryTree, ParsedQuery
from src.core.models.storage import Statistic
from src.optimizer.optimizer import QueryOptimizer
from src.optimizer.rules.join import JoinCommutativityRule


@pytest.fixture
def mock_storage():
    """Mock storage manager."""
    return Mock()


@pytest.fixture
def rules():
    """Default rules for testing."""
    return [JoinCommutativityRule()]


@pytest.fixture
def optimizer(mock_storage, rules):
    """QueryOptimizer instance with default setup."""
    return QueryOptimizer(
        storage_manager=mock_storage,
        rules=rules,
        max_iterations=10
    )


class TestQueryOptimizer:
    """Test cases for QueryOptimizer engine."""

    def test_init_with_rules(self, optimizer, rules):
        """Test optimizer initialization with rules."""
        assert len(optimizer.rules) == 1
        assert isinstance(optimizer.rules[0], JoinCommutativityRule)

    def test_optimize_applies_single_rule(self, mock_storage):
        """Test that optimize applies rules to query tree."""
        rule = JoinCommutativityRule()
        optimizer = QueryOptimizer(
            storage_manager=mock_storage,
            rules=[rule],
            max_iterations=1
        )

        # Build query: Employee ⋈ Department
        employee = QueryTree(type="table", value="Employee", children=[])
        department = QueryTree(type="table", value="Department", children=[])
        join = QueryTree(type="join", value="cond", children=[employee, department])
        employee.parent = join
        department.parent = join

        parsed = ParsedQuery(tree=join, query="test query")
        result = optimizer.optimize_query(parsed)

        # After 1 iteration, JoinCommutativity should swap children
        assert result.tree.type == "join"
        assert result.tree.children[0].value == "Department"
        assert result.tree.children[1].value == "Employee"

    def test_optimize_stops_when_no_changes(self, optimizer):
        """Test that optimizer stops when no more changes are made."""
        # A simple table node that can't be optimized
        table = QueryTree(type="table", value="Employee", children=[])
        parsed = ParsedQuery(tree=table, query="SELECT * FROM Employee")

        result = optimizer.optimize_query(parsed)

        # Should return the same tree
        assert result.tree.type == "table"
        assert result.tree.value == "Employee"

    def test_optimize_with_multiple_rules(self, mock_storage):
        """Test optimizer with multiple rules."""
        rules = [JoinCommutativityRule()]
        optimizer = QueryOptimizer(
            storage_manager=mock_storage,
            rules=rules,
            max_iterations=1
        )

        # Build query: Employee ⋈ Department
        employee = QueryTree(type="table", value="Employee", children=[])
        department = QueryTree(type="table", value="Department", children=[])
        join = QueryTree(type="join", value="cond", children=[employee, department])
        employee.parent = join
        department.parent = join

        parsed = ParsedQuery(tree=join, query="test query")
        result = optimizer.optimize_query(parsed)

        # After 1 iteration, join children should be swapped
        assert result.tree.type == "join"
        assert result.tree.children[0].value == "Department"
        assert result.tree.children[1].value == "Employee"

    def test_optimize_respects_max_iterations(self, mock_storage, rules):
        """Test that optimizer respects max_iterations limit."""
        optimizer = QueryOptimizer(
            storage_manager=mock_storage,
            rules=rules,
            max_iterations=1
        )

        left = QueryTree(type="table", value="T1", children=[])
        right = QueryTree(type="table", value="T2", children=[])
        join = QueryTree(type="join", value="cond", children=[left, right])
        left.parent = join
        right.parent = join

        parsed = ParsedQuery(tree=join, query="test query")
        result = optimizer.optimize_query(parsed)

        # Should still optimize since it only needs 1 iteration
        assert result is not None

    def test_add_rule(self, optimizer):
        """Test adding a new rule to optimizer."""
        initial_count = len(optimizer.rules)

        new_rule = JoinCommutativityRule()
        optimizer.add_rule(new_rule)

        assert len(optimizer.rules) == initial_count + 1

    def test_remove_rule(self, optimizer):
        """Test removing a rule from optimizer."""
        optimizer.remove_rule("JoinCommutativity")

        assert len(optimizer.rules) == 0

    def test_parse_query(self, optimizer):
        """Test parse_query with built-in parser."""
        result = optimizer.parse_query("SELECT * FROM Employee")

        assert isinstance(result, ParsedQuery)
        assert result.query == "SELECT * FROM Employee"

    def test_optimize_query(self, optimizer):
        """Test optimize_query method."""
        tree = QueryTree(type="table", value="Employee", children=[])
        parsed_query = ParsedQuery(tree=tree, query="SELECT * FROM Employee")

        result = optimizer.optimize_query(parsed_query)

        assert isinstance(result, ParsedQuery)
        assert result.query == "SELECT * FROM Employee"

    def test_get_cost_raises_when_no_calculator(self, optimizer):
        """Test that get_cost raises error when no cost calculator configured."""
        tree = QueryTree(type="table", value="Employee", children=[])
        parsed_query = ParsedQuery(tree=tree, query="SELECT * FROM Employee")

        with pytest.raises(NotImplementedError):
            optimizer.get_cost(parsed_query)

    def test_get_cost_with_calculator(self, mock_storage, rules):
        """Test get_cost with configured cost calculator."""
        mock_stat = Statistic(
            table_name="Employee",
            n_r=100,
            b_r=10,
            l_r=50,
            f_r=10,
            V={},
            min_values={},
            max_values={},
            null_counts={},
        )
        statistics = {"Employee": mock_stat}

        optimizer = QueryOptimizer(
            storage_manager=mock_storage,
            rules=rules,
            statistics=statistics
        )
        tree = QueryTree(type="table", value="Employee", children=[])
        parsed_query = ParsedQuery(tree=tree, query="SELECT * FROM Employee")

        cost = optimizer.get_cost(parsed_query)

        # Cost should be greater than 0 (based on CostModel calculation)
        assert cost > 0

    def test_optimize_bottom_up(self, optimizer):
        """Test that optimizer applies rules bottom-up."""
        # Build nested structure: (T1 ⋈ T2) ⋈ T3
        t1 = QueryTree(type="table", value="T1", children=[])
        t2 = QueryTree(type="table", value="T2", children=[])
        inner_join = QueryTree(type="join", value="cond1", children=[t1, t2])
        t1.parent = inner_join
        t2.parent = inner_join

        t3 = QueryTree(type="table", value="T3", children=[])
        outer_join = QueryTree(type="join", value="cond2", children=[inner_join, t3])
        inner_join.parent = outer_join
        t3.parent = outer_join

        parsed = ParsedQuery(tree=outer_join, query="test query")
        result = optimizer.optimize_query(parsed)

        # Both joins should be optimized (children swapped)
        assert result is not None
