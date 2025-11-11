import unittest
from src.optimizer.rules.selection.cartesian_product import SelectionCartesianProductRule
from src.core.models.query import QueryTree, QueryNodeType

class TestSelectionCartesianProductRule(unittest.TestCase):
    def setUp(self):
        self.rule = SelectionCartesianProductRule()

    def test_convert_cartesian_to_theta_join(self):
        # σ(A.x = B.y)(A × B)  to  A ⋈(A.x = B.y) B
        left = QueryTree(type=QueryNodeType.TABLE, value="A", children=[], parent=None)
        right = QueryTree(type=QueryNodeType.TABLE, value="B", children=[], parent=None)

        cart = QueryTree(type=QueryNodeType.CARTESIAN_PRODUCT, value=None, children=[left, right], parent=None)
        sel = QueryTree(type=QueryNodeType.SELECTION, value="A.x = B.y", children=[cart], parent=None)

        # set parent links as in a real tree
        left.parent = cart
        right.parent = cart
        cart.parent = sel

        assert self.rule.is_applicable(sel)
        new_tree = self.rule.apply(sel)

        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, QueryNodeType.THETA_JOIN)
        self.assertEqual(new_tree.value, "A.x = B.y")
        self.assertEqual(len(new_tree.children), 2)
        self.assertEqual(new_tree.children[0].type, QueryNodeType.TABLE)
        self.assertEqual(new_tree.children[0].value, "A")
        self.assertEqual(new_tree.children[1].type, QueryNodeType.TABLE)
        self.assertEqual(new_tree.children[1].value, "B")

        # parent pointers updated
        self.assertIs(new_tree.children[0].parent, new_tree)
        self.assertIs(new_tree.children[1].parent, new_tree)

    def test_not_applicable_when_not_cartesian(self):
        left = QueryTree(type=QueryNodeType.TABLE, value="A", children=[], parent=None)
        join = QueryTree(type=QueryNodeType.JOIN, value=None, children=[left], parent=None)  # JOIN, bukan cartesian
        sel = QueryTree(type=QueryNodeType.SELECTION, value="A.x = 1", children=[join], parent=None)

        self.assertFalse(self.rule.is_applicable(sel))
        self.assertIsNone(self.rule.apply(sel))

    def test_not_applicable_when_cartesian_has_less_than_two_children(self):
        only_one = QueryTree(type=QueryNodeType.TABLE, value="A", children=[], parent=None)
        cart = QueryTree(type=QueryNodeType.CARTESIAN_PRODUCT, value=None, children=[only_one], parent=None)
        sel = QueryTree(type=QueryNodeType.SELECTION, value="A.x = 1", children=[cart], parent=None)

        self.assertTrue(self.rule.is_applicable(sel))
        self.assertIsNone(self.rule.apply(sel))


if __name__ == "__main__":
    unittest.main()
