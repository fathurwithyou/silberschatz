import unittest
from src.optimizer.rules.selection.theta_join import SelectionThetaJoinRule
from src.core.models.query import QueryTree, QueryNodeType

class TestSelectionThetaJoinRule(unittest.TestCase):

    def setUp(self):
        self.rule = SelectionThetaJoinRule()

    def test_is_applicable_valid(self):
        join = QueryTree(type=QueryNodeType.THETA_JOIN, value="A.id = B.id", children=[])
        sel = QueryTree(type=QueryNodeType.SELECTION, value="A.age > 30", children=[join])

        self.assertTrue(self.rule.is_applicable(sel))

    def test_is_applicable_invalid_type(self):
        node = QueryTree(type=QueryNodeType.JOIN, value=None, children=[])
        self.assertFalse(self.rule.is_applicable(node))

    def test_is_applicable_invalid_child(self):
        child = QueryTree(type=QueryNodeType.TABLE, value="A", children=[])
        node = QueryTree(type=QueryNodeType.SELECTION, value="x > 0", children=[child])
        self.assertFalse(self.rule.is_applicable(node))

    def test_apply_merge_conditions(self):
        left = QueryTree(type=QueryNodeType.TABLE, value="A", children=[])
        right = QueryTree(type=QueryNodeType.TABLE, value="B", children=[])
        join = QueryTree(type=QueryNodeType.THETA_JOIN, value="A.id = B.id", children=[left, right])
        sel = QueryTree(type=QueryNodeType.SELECTION, value="A.age > 30", children=[join])

        new_node = self.rule.apply(sel)

        self.assertIsNotNone(new_node)
        self.assertEqual(new_node.type, QueryNodeType.THETA_JOIN)
        self.assertEqual(new_node.value, "A.id = B.id AND A.age > 30")
        self.assertEqual(len(new_node.children), 2)

    def test_apply_when_only_selection_condition(self):
        # Jika join tidak punya kondisi, pakai selection condition
        left = QueryTree(type=QueryNodeType.TABLE, value="A", children=[])
        right = QueryTree(type=QueryNodeType.TABLE, value="B", children=[])
        join = QueryTree(type=QueryNodeType.THETA_JOIN, value="", children=[left, right])
        sel = QueryTree(type=QueryNodeType.SELECTION, value="A.age > 30", children=[join])

        new_node = self.rule.apply(sel)

        self.assertIsNotNone(new_node)
        self.assertEqual(new_node.type, QueryNodeType.THETA_JOIN)
        self.assertEqual(new_node.value, "A.age > 30")

    def test_apply_not_applicable(self):
        table = QueryTree(type=QueryNodeType.TABLE, value="A", children=[])
        sel = QueryTree(type=QueryNodeType.SELECTION, value="A.age > 30", children=[table])

        self.assertIsNone(self.rule.apply(sel))

if __name__ == "__main__":
    unittest.main()
