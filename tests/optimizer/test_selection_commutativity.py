import unittest
from src.optimizer.rules.selection.commutativity import SelectionCommutativityRule
from src.core.models.query import QueryTree, QueryNodeType

class TestSelectionCommutativityRule(unittest.TestCase):
    def setUp(self):
        self.rule = SelectionCommutativityRule()

    def test_swap_when_child_more_selective(self):
        # Kasus: outer = "A != 3" (less selective), inner = "id = 5" (more selective).
        table = QueryTree(type=QueryNodeType.TABLE, value="T", children=[], parent=None)
        inner = QueryTree(type=QueryNodeType.SELECTION, value="id = 5", children=[table], parent=None)
        outer = QueryTree(type=QueryNodeType.SELECTION, value="A != 3", children=[inner], parent=None)
        table.parent = inner
        inner.parent = outer

        self.assertTrue(self.rule.is_applicable(outer))

        new_tree = self.rule.apply(outer)

        self.assertIsNotNone(new_tree)
        self.assertEqual(new_tree.type, QueryNodeType.SELECTION)
        self.assertEqual(new_tree.value, "id = 5")

        inner_after = new_tree.children[0]
        self.assertEqual(inner_after.type, QueryNodeType.SELECTION)
        self.assertEqual(inner_after.value, "A != 3")

        self.assertEqual(inner_after.children[0].type, QueryNodeType.TABLE)
        self.assertEqual(inner_after.children[0].value, "T")

        # parent pointers should be set correctly
        self.assertIs(new_tree.children[0].parent, new_tree)
        self.assertIs(inner_after.children[0].parent, inner_after)

    def test_no_swap_when_parent_more_or_equal_selective(self):
        # Kasus: outer = "id = 5" (very selective), inner = "A != 3" (less selective).
        table = QueryTree(type=QueryNodeType.TABLE, value="T", children=[], parent=None)
        inner = QueryTree(type=QueryNodeType.SELECTION, value="A != 3", children=[table], parent=None)
        outer = QueryTree(type=QueryNodeType.SELECTION, value="id = 5", children=[inner], parent=None)
        table.parent = inner
        inner.parent = outer

        self.assertTrue(self.rule.is_applicable(outer))

        new_tree = self.rule.apply(outer)

        self.assertIsNone(new_tree)

    def test_not_applicable_when_no_nested_selection(self):
        # Kalau node bukan selection atau child bukan selection, then is_applicable False
        table = QueryTree(type=QueryNodeType.TABLE, value="T", children=[], parent=None)
        selection_not_nested = QueryTree(type=QueryNodeType.SELECTION, value="id = 5", children=[table], parent=None)
        self.assertFalse(self.rule.is_applicable(table))               # node is TABLE
        self.assertFalse(self.rule.is_applicable(selection_not_nested))# child is TABLE, not SELECTION

if __name__ == "__main__":
    unittest.main()
