import unittest
from src.optimizer.rules.selection.join_distribution import SelectionJoinDistributionRule
from src.core.models.query import QueryTree

class TestSelectionJoinDistributionRule(unittest.TestCase):
    def test_rule_7a_left_only(self):
        rule = SelectionJoinDistributionRule()

        # LEFT & RIGHT tables
        left = QueryTree(type="TABLE", value="employees", children=[], parent=None)
        right = QueryTree(type="TABLE", value="departments", children=[], parent=None)

        # JOIN Node
        join_node = QueryTree(
            type="JOIN",
            value="employees.dept_id = departments.id",
            children=[left, right],
            parent=None
        )

        node = QueryTree(
            type="SELECTION",
            value="employees.age > 30",
            children=[join_node],
            parent=None
        )

        self.assertTrue(rule.is_applicable(node))
        new_tree = rule.apply(node)

        self.assertEqual(new_tree.type, "JOIN")

        new_left = new_tree.children[0]
        new_right = new_tree.children[1]

        # LEFT becomes selection
        self.assertEqual(new_left.type, "SELECTION")
        self.assertEqual(new_left.value, "employees.age > 30")

        self.assertEqual(new_left.children[0].value, "employees")

        self.assertEqual(new_right.value, "departments")

    def test_rule_7b_left_and_right(self):
        rule = SelectionJoinDistributionRule()

        # LEFT & RIGHT tables
        left = QueryTree(type="TABLE", value="employees", children=[], parent=None)
        right = QueryTree(type="TABLE", value="departments", children=[], parent=None)

        # JOIN node
        join_node = QueryTree(
            type="JOIN",
            value="employees.dept_id = departments.id",
            children=[left, right],
            parent=None
        )

        # Condition consists of 2 parts:
        # employees.age > 30: LEFT
        # departments.name = 'IT':RIGHT
        node = QueryTree(
            type="SELECTION",
            value="employees.age > 30 AND departments.name = 'IT'",
            children=[join_node],
            parent=None
        )

        self.assertTrue(rule.is_applicable(node))
        new_tree = rule.apply(node)

        # Root should still be JOIN
        self.assertEqual(new_tree.type, "JOIN")

        new_left = new_tree.children[0]
        new_right = new_tree.children[1]

        # LEFT side should get employees.age > 30
        self.assertEqual(new_left.type, "SELECTION")
        self.assertEqual(new_left.value, "employees.age > 30")
        self.assertEqual(new_left.children[0].value, "employees")

        # RIGHT side should get departments.name = 'IT'
        self.assertEqual(new_right.type, "SELECTION")
        self.assertEqual(new_right.value, "departments.name = 'IT'")
        self.assertEqual(new_right.children[0].value, "departments")
