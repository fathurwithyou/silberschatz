import unittest
from src.optimizer.rules.selection.decomposition import SelectionDecompositionRule
from src.core.models.query import QueryTree

class TestSelectionDecompositionRule(unittest.TestCase):
    def test_decompose_simple_and(self):
        rule = SelectionDecompositionRule()

        # QueryTree: selection with AND
        child = QueryTree(type="TABLE", value="employees", children=[], parent=None)
        node = QueryTree(type="SELECTION", value="age > 30 AND salary < 5000", children=[child], parent=None)

        self.assertTrue(rule.is_applicable(node))

        new_tree = rule.apply(node)
        self.assertIsNotNone(new_tree)

        # Should produce nested selections
        self.assertEqual(new_tree.type, "SELECTION")
        self.assertEqual(new_tree.value, "age > 30")
        self.assertEqual(new_tree.children[0].type, "SELECTION")
        self.assertEqual(new_tree.children[0].value, "salary < 5000")


if __name__ == '__main__':
    unittest.main()
