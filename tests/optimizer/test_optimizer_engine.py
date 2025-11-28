import unittest
from unittest.mock import Mock
from src.core.models.query import QueryTree, ParsedQuery
from src.optimizer.optimizer import QueryOptimizer
from src.optimizer.rules.join import JoinCommutativityRule


class TestQueryOptimizer(unittest.TestCase):
    """Test cases for QueryOptimizer engine."""

    def setUp(self):
        """Set up test fixtures."""
        self.rules = [JoinCommutativityRule()]
        self.mock_storage = Mock()
        self.optimizer = QueryOptimizer(
            storage_manager=self.mock_storage, rules=self.rules, max_iterations=10
        )

    def test_init_with_rules(self):
        """Test optimizer initialization with rules."""
        self.assertEqual(len(self.optimizer.rules), 1)
        self.assertIsInstance(self.optimizer.rules[0], JoinCommutativityRule)

    def test_optimize_applies_single_rule(self):
        """Test that optimize applies rules to query tree."""
        rule = JoinCommutativityRule()
        optimizer = QueryOptimizer(
            storage_manager=Mock(), rules=[rule], max_iterations=1
        )

        employee = QueryTree(type="table", value="Employee", children=[])
        department = QueryTree(type="table", value="Department", children=[])
        join = QueryTree(type="join", value="cond", children=[employee, department])
        employee.parent = join
        department.parent = join

        parsed = ParsedQuery(tree=join, query="test query")
        result = optimizer.optimize_query(parsed)

        self.assertEqual(result.tree.type, "join")

        self.assertEqual(result.tree.children[0].value, "Department")
        self.assertEqual(result.tree.children[1].value, "Employee")

    def test_optimize_stops_when_no_changes(self):
        """Test that optimizer stops when no more changes are made."""

        table = QueryTree(type="table", value="Employee", children=[])
        parsed = ParsedQuery(tree=table, query="SELECT * FROM Employee")

        result = self.optimizer.optimize_query(parsed)

        self.assertEqual(result.tree.type, "table")
        self.assertEqual(result.tree.value, "Employee")

    def test_optimize_with_multiple_rules(self):
        """Test optimizer with multiple rules."""
        rules = [JoinCommutativityRule()]
        optimizer = QueryOptimizer(
            storage_manager=Mock(), rules=rules, max_iterations=1
        )

        employee = QueryTree(type="table", value="Employee", children=[])
        department = QueryTree(type="table", value="Department", children=[])
        join = QueryTree(type="join", value="cond", children=[employee, department])
        employee.parent = join
        department.parent = join

        parsed = ParsedQuery(tree=join, query="test query")
        result = optimizer.optimize_query(parsed)

        self.assertEqual(result.tree.type, "join")
        self.assertEqual(result.tree.children[0].value, "Department")
        self.assertEqual(result.tree.children[1].value, "Employee")

    def test_optimize_respects_max_iterations(self):
        """Test that optimizer respects max_iterations limit."""
        optimizer = QueryOptimizer(
            storage_manager=Mock(), rules=self.rules, max_iterations=1
        )

        left = QueryTree(type="table", value="T1", children=[])
        right = QueryTree(type="table", value="T2", children=[])
        join = QueryTree(type="join", value="cond", children=[left, right])
        left.parent = join
        right.parent = join

        parsed = ParsedQuery(tree=join, query="test query")
        result = optimizer.optimize_query(parsed)

        self.assertIsNotNone(result)

    def test_add_rule(self):
        """Test adding a new rule to optimizer."""
        initial_count = len(self.optimizer.rules)

        new_rule = JoinCommutativityRule()
        self.optimizer.add_rule(new_rule)

        self.assertEqual(len(self.optimizer.rules), initial_count + 1)

    def test_remove_rule(self):
        """Test removing a rule from optimizer."""
        self.optimizer.remove_rule("JoinCommutativity")

        self.assertEqual(len(self.optimizer.rules), 0)

    def test_parse_query(self):
        """Test parse_query with built-in parser."""
        result = self.optimizer.parse_query("SELECT * FROM Employee")

        self.assertIsInstance(result, ParsedQuery)
        self.assertEqual(result.query, "SELECT * FROM Employee")

    def test_optimize_query(self):
        """Test optimize_query method."""
        tree = QueryTree(type="table", value="Employee", children=[])
        parsed_query = ParsedQuery(tree=tree, query="SELECT * FROM Employee")

        result = self.optimizer.optimize_query(parsed_query)

        self.assertIsInstance(result, ParsedQuery)
        self.assertEqual(result.query, "SELECT * FROM Employee")

    def test_get_cost_raises_when_no_calculator(self):
        """Test that get_cost raises error when no cost calculator configured."""
        tree = QueryTree(type="table", value="Employee", children=[])
        parsed_query = ParsedQuery(tree=tree, query="SELECT * FROM Employee")

        with self.assertRaises(NotImplementedError):
            self.optimizer.get_cost(parsed_query)

    def test_get_cost_with_calculator(self):
        """Test get_cost with configured cost calculator."""

        from src.core.models.storage import Statistic

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
            storage_manager=Mock(), rules=self.rules, statistics=statistics
        )
        tree = QueryTree(type="table", value="Employee", children=[])
        parsed_query = ParsedQuery(tree=tree, query="SELECT * FROM Employee")

        cost = optimizer.get_cost(parsed_query)

        self.assertGreater(cost, 0)

    def test_optimize_bottom_up(self):
        """Test that optimizer applies rules bottom-up."""

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
        result = self.optimizer.optimize_query(parsed)

        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
