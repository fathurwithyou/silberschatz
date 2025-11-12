import unittest
from unittest.mock import MagicMock
from src.optimizer.rules.selection.join_distribution import SelectionJoinDistributionRule
from src.core.models.query import QueryTree, QueryNodeType

class TestSelectionJoinDistributionRule(unittest.TestCase):
    def setUp(self):
        # Mock storage_manager agar tidak perlu database nyata
        self.mock_storage_manager = MagicMock()
        self.mock_storage_manager.get_table_schema.return_value = None
        self.rule = SelectionJoinDistributionRule(storage_manager=self.mock_storage_manager)

    def test_rule_7a_left_only(self):
        # LEFT & RIGHT tables
        left = QueryTree(type=QueryNodeType.TABLE, value="employees", children=[], parent=None)
        right = QueryTree(type=QueryNodeType.TABLE, value="departments", children=[], parent=None)

        # JOIN Node
        join_node = QueryTree(
            type=QueryNodeType.JOIN,
            value="employees.dept_id = departments.id",
            children=[left, right],
            parent=None
        )

        node = QueryTree(
            type=QueryNodeType.SELECTION,
            value="employees.age > 30",
            children=[join_node],
            parent=None
        )

        # Rule must apply
        self.assertTrue(self.rule.is_applicable(node))

        new_tree = self.rule.apply(node)
        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, QueryNodeType.JOIN)

        new_left = new_tree.children[0]
        new_right = new_tree.children[1]

        # LEFT becomes selection
        self.assertEqual(new_left.type, QueryNodeType.SELECTION)
        self.assertEqual(new_left.value, "employees.age > 30")
        self.assertEqual(new_left.children[0].value, "employees")

        # RIGHT remains table
        self.assertEqual(new_right.value, "departments")

    def test_rule_7b_left_and_right(self):
        # LEFT & RIGHT tables
        left = QueryTree(type=QueryNodeType.TABLE, value="employees", children=[], parent=None)
        right = QueryTree(type=QueryNodeType.TABLE, value="departments", children=[], parent=None)

        # JOIN node
        join_node = QueryTree(
            type=QueryNodeType.JOIN,
            value="employees.dept_id = departments.id",
            children=[left, right],
            parent=None
        )

        # Condition consists of 2 parts:
        # employees.age > 30 → LEFT
        # departments.name = 'IT' → RIGHT
        node = QueryTree(
            type=QueryNodeType.SELECTION,
            value="employees.age > 30 AND departments.name = 'IT'",
            children=[join_node],
            parent=None
        )

        self.assertTrue(self.rule.is_applicable(node))
        new_tree = self.rule.apply(node)
        self.assertIsNotNone(new_tree)

        # Root should still be JOIN
        self.assertEqual(new_tree.type, QueryNodeType.JOIN)

        new_left = new_tree.children[0]
        new_right = new_tree.children[1]

        # LEFT side should get employees.age > 30
        self.assertEqual(new_left.type, QueryNodeType.SELECTION)
        self.assertEqual(new_left.value, "employees.age > 30")
        self.assertEqual(new_left.children[0].value, "employees")

        # RIGHT side should get departments.name = 'IT'
        self.assertEqual(new_right.type, QueryNodeType.SELECTION)
        self.assertEqual(new_right.value, "departments.name = 'IT'")
        self.assertEqual(new_right.children[0].value, "departments")